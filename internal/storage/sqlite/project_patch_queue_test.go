package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/repoauthority"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestProjectPatchQueueDurabilityProofReportsMigratedQueue(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	proof := store.ProjectPatchQueueDurabilityProof(context.Background())

	if proof.Contract != sqlite.ProjectPatchQueueDurabilityProofContract {
		t.Fatalf("contract = %q", proof.Contract)
	}
	if proof.State != "ok" || !proof.Durable {
		t.Fatalf("expected durable ok proof, got %+v", proof)
	}
	if !proof.TablePresent || !proof.PrimaryKeyPresent || !proof.LiveBranchIndexPresent || !proof.ClaimIndexPresent || !proof.MaterializationIndexPresent || !proof.SupersessionIndexPresent || !proof.ReviewTaskReceiptPresent || !proof.DecisionContinuationPresent || !proof.LifecycleColumnsPresent || !proof.BindingColumnsPresent {
		t.Fatalf("expected all durability primitives to be present, got %+v", proof)
	}
	if proof.MaterializationStoragePolicy.MaxFiles != sqlite.ProjectPatchQueueMaterializationMaxFiles ||
		proof.MaterializationStoragePolicy.MaxFileBytes != sqlite.ProjectPatchQueueMaterializationMaxFileBytes ||
		proof.MaterializationStoragePolicy.MaxTotalBytes != sqlite.ProjectPatchQueueMaterializationMaxTotalBytes ||
		proof.MaterializationStoragePolicy.MaxMaterializationJSONBytes != sqlite.ProjectPatchQueueMaterializationMaxJSONBytes ||
		proof.MaterializationStoragePolicy.MaxAuthorityProofJSONBytes != sqlite.ProjectPatchQueueMaterializationMaxAuthorityProofJSONBytes {
		t.Fatalf("expected materialization storage policy to be surfaced in durability proof, got %+v", proof.MaterializationStoragePolicy)
	}
	if proof.MaterializedItemCount != 0 || proof.MaterializationJSONBytes != 0 || proof.LiveMaterializationJSONBytes != 0 {
		t.Fatalf("expected empty store materialization byte accounting to start at zero, got %+v", proof)
	}
	for _, column := range []string{
		"materialization_schema",
		"materialization_accepted",
		"materialization_json",
		"materialization_digest",
		"materialization_recorded_by",
		"materialization_recorded_at",
		"materialization_authority_proof_json",
		"materialization_authority_proof_digest",
	} {
		if stringSliceContainsForPatchQueueTest(proof.MissingBindingColumns, column) {
			t.Fatalf("materialization durability column %s reported missing: %+v", column, proof)
		}
	}
	if proof.Digest == "" || len(proof.Digest) != 64 {
		t.Fatalf("expected canonical durability digest, got %q", proof.Digest)
	}
	if err := sqlite.VerifyProjectPatchQueueDurabilityProof(proof); err != nil {
		t.Fatalf("verify durability proof: %v", err)
	}
}

func TestProjectPatchQueueDecisionRejectsBlockedActorAuthorityGap(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-blocked-authority-gap"
		projectID   = "project-patchq-blocked-authority-gap"
		repoID      = "repo-patchq-blocked-authority-gap"
		leadID      = "alpha"
		ownerID     = "beta"
		reviewerID  = "iota"
		branchID    = "branch-patchq-blocked-authority-gap"
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
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               reviewerID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim patch queue item: %v", err)
	}

	_, _, err = store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     claimed.QueueID,
		ItemID:      claimed.ItemID,
		Decision:    sqlite.ProjectPatchQueueStateBlocked,
		DecisionSummary: strings.Join([]string{
			"Fresh review passed for this lane candidate.",
			"However, controlled-queue completion is blocked because iota lacks INTEGRATOR authority.",
			"This is an actor authority gap, not a candidate defect.",
		}, " "),
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "reviewer's missing integration authority") {
		t.Fatalf("expected actor authority BLOCKED decision to be rejected, got %v", err)
	}
	items, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{WorkspaceID: workspaceID, ProjectID: projectID})
	if err != nil {
		t.Fatalf("list patch queue items: %v", err)
	}
	if len(items) != 1 || items[0].State != sqlite.ProjectPatchQueueStateClaimed || items[0].DecisionSummary != "" {
		t.Fatalf("authority-gap decision must not terminalize candidate, got %+v", items)
	}
}

func TestProjectPatchQueueSubmitRejectsDirectTaskRelabel(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patchq-task-binding-relabel"
		projectID   = "project-patchq-task-binding-relabel"
		repoID      = "repo-main"
		leadID      = "alpha"
		workerID    = "beta"
		reviewerID  = "iota"
		parserTask  = "task-parser-lane"
		stdlibTask  = "task-stdlib-lane"
		branchID    = "branch-parser-ready"
		reviewKey   = "project.project-patchq-task-binding-relabel.branch.branch-parser-ready.review"
		evidenceKey = "project.project-patchq-task-binding-relabel.branch.branch-parser-ready.supersede"
		claimScope  = `{"paths":["internal/parser/**"]}`
		readyScope  = `{"paths":["internal/parser/**","go.mod"]}`
	)
	baseSHA := strings.Repeat("1", 40)
	headSHA := strings.Repeat("2", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["internal/parser/**","internal/ast/**","go.mod","go.sum"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, parserTask)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectID, stdlibTask)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\beta\patchq-task-binding-relabel`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              branchID,
		BranchName:            "agent/beta/parser-ready",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        claimScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register parser branch: %v", err)
	}
	if _, err := store.ClaimTaskWithEvent(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                parserTask,
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		BranchID:              branch.BranchID,
		WriteScopeJSON:        claimScope,
		Summary:               "claim parser lane",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, parserTask, workerID),
	}); err != nil {
		t.Fatalf("claim parser task: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Parser Branch Review Packet",
		Content:     "# Parser Branch Review Packet\n\nREADY_FOR_REVIEW evidence for parser lane.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write parser review doc: %v", err)
	}
	ready, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              branch.BranchID,
		ActiveTaskID:          parserTask,
		ActiveClaimID:         parserTask,
		BranchName:            branch.BranchName,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        readyScope,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register parser branch ready: %v", err)
	}
	relabelInput := sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 ready.BranchID,
		QueueID:                  "queue-parser",
		ItemID:                   "item-parser",
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   stdlibTask,
		SessionID:                "session-parser",
		RunID:                    "run-parser",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-parser",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes:           map[string]string{"internal/parser/parser.go": "sha256:parser", "go.mod": "sha256:gomod"},
		RepoLeaseID:              "lease-parser",
		LeaseTerm:                1,
		ActorID:                  workerID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:     "project.patch_queue.submit",
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, relabelInput); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "must match branch active_task_id") {
		t.Fatalf("expected direct RPC task relabel to be rejected, got %v", err)
	}
	relabelInput.TaskID = parserTask
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, relabelInput)
	if err != nil {
		t.Fatalf("genuine parser task submit should pass: %v", err)
	}
	if item.TaskID != parserTask || item.BranchID != ready.BranchID {
		t.Fatalf("unexpected submitted item binding: %+v", item)
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
		t.Fatalf("claim parser item: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionSummary:       "Needs same-head validation evidence before integration.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("block parser item: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceKey,
		Title:       "Parser Requeue Validation Evidence",
		Content: fmt.Sprintf(`Browser-smoke provenance records a PASS for the exact same branch/head.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
browser-smoke user scenario result: PASS`, item.QueueID, item.ItemID, ready.BranchID, ready.HeadSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write supersede evidence: %v", err)
	}
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		QueueID:                  item.QueueID,
		ItemID:                   item.ItemID,
		NewItemID:                item.ItemID + "-stdlib-relabel",
		EvidenceDocKey:           evidenceKey,
		TaskID:                   stdlibTask,
		SessionID:                "session-parser-requeue",
		RunID:                    "run-parser-requeue",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-parser-requeue",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes:           map[string]string{"internal/parser/parser.go": "sha256:parser", "go.mod": "sha256:gomod"},
		RepoLeaseID:              "lease-parser-requeue",
		LeaseTerm:                1,
		ActorID:                  reviewerID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:     "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "must match branch active_task_id") {
		t.Fatalf("expected supersede task relabel to be rejected, got %v", err)
	}
}

func TestK2E_ProjectPatchQueueSubmitRejectsCoordinationLaneRelabelWithFullBindingContext(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID      = "ws-k2e-patchq-coordination-relabel"
		projectID        = "project-k2e-patchq-coordination-relabel"
		repoID           = "repo-main"
		leadID           = "alpha"
		workerID         = "beta"
		coordinationTask = "task-coordination-relabel"
		branchID         = "branch-parser-ready"
		reviewKey        = "project.project-k2e-patchq-coordination-relabel.branch.branch-parser-ready.review"
		parserScope      = `{"paths":["internal/parser/**"]}`
	)
	baseSHA := strings.Repeat("1", 40)
	headSHA := strings.Repeat("2", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	createProjectTaskWithLane(t, ctx, store, workspaceID, projectID, coordinationTask, "coordination", false)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\beta\k2e-patchq-coordination-relabel`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              branchID,
		BranchName:            "agent/beta/k2e-parser-ready",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        parserScope,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register parser branch: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Parser Branch Review Packet",
		Content:     "# Parser Branch Review Packet\n\nREADY_FOR_REVIEW evidence for parser lane.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write parser review doc: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO task_claims(
  task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at,
  project_role_id, repo_id, checkout_id, branch_id, write_scope_json
) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, '', ?, ?, ?, ?)`,
		coordinationTask, workspaceID, workerID, model.TaskClaimStatusClaimed, "pre-existing coordination relabel claim", now, now, repoID, checkout.CheckoutID, branch.BranchID, parserScope); err != nil {
		t.Fatalf("seed pre-existing coordination claim: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
UPDATE project_branch_registry
   SET active_task_id = ?,
       active_claim_id = ?,
       status = ?,
       base_sha = ?,
       head_sha = ?,
       review_doc_key = ?,
       write_scope_json = ?,
       updated_at = ?
 WHERE workspace_id = ?
   AND branch_id = ?`,
		coordinationTask, coordinationTask, sqlite.ProjectBranchStatusReadyForReview, baseSHA, headSHA, reviewKey, parserScope, now, workspaceID, branch.BranchID); err != nil {
		t.Fatalf("force pre-existing coordination READY branch: %v", err)
	}

	_, _, err = store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		QueueID:                  "queue-k2e",
		ItemID:                   "item-k2e",
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   coordinationTask,
		SessionID:                "session-k2e",
		RunID:                    "run-k2e",
		AgentID:                  workerID,
		PrincipalType:            "agent",
		PrincipalID:              workerID,
		CapabilitySnapshotID:     "cap-k2e",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes:           map[string]string{"internal/parser/parser.go": "sha256:parser"},
		RepoLeaseID:              "lease-k2e",
		LeaseTerm:                1,
		ActorID:                  workerID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "no authoritative implementation lane scope") {
		t.Fatalf("expected full-context coordination relabel submit to fail on authoritative lane scope, got %v", err)
	}
	items, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{WorkspaceID: workspaceID, ProjectID: projectID})
	if err != nil {
		t.Fatalf("list patch queue items after rejected relabel: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("rejected coordination relabel submit must not create patch queue item, got %+v", items)
	}
}

func TestProjectPatchQueueLateLaneDefectDefeatsAcceptedItem(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	item := createAcceptedControlledPatchQueueItemForTest(t, ctx, store, "late-defect")
	// The branch owner (beta) holds a scoped IMPLEMENTER role in production (they implemented the branch), so the
	// defeat's revision continuation routes satisfiable_now and materializes - the claimable carrier this consumes.
	assignProjectImplementerRoleForGitTest(t, ctx, store, item.WorkspaceID, item.ProjectID, "beta", "alpha", `{"paths":["src/**"]}`)
	const reviewerID = "iota"
	reviewDocKey := "task.review.late-defect.result"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: item.WorkspaceID,
		DocKey:      reviewDocKey,
		Title:       "Late Lane Review Result",
		Content:     "# Review Result\n\nVerdict: REJECT\n\nBlocking lexer defect on the accepted head.",
		UpdatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("write late review doc: %v", err)
	}

	defeated, _, err := store.RecordProjectPatchQueueReviewerAdvisoryWithEvent(ctx, sqlite.ProjectPatchQueueReviewerAdvisoryRecordInput{
		WorkspaceID: item.WorkspaceID,
		ProjectID:   item.ProjectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ActorID:     reviewerID,
		ActorType:   "agent",
		ReviewerAdvisory: repoauthority.PatchQueueReviewerAdvisory{
			Verdict:           repoauthority.PatchQueueReviewerAdvisoryVerdictRepairRequired,
			Scope:             repoauthority.PatchQueueReviewerAdvisoryScopeLaneCorrectness,
			HeadSHA:           item.HeadSHA,
			ReviewDocKey:      reviewDocKey,
			Summary:           "Blocking lexer defect: invalid unicode escapes cascade into extra tokens; repair required before integration.",
			DefeatsAcceptance: true,
		},
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.reviewer_advisory_record", "server_rpc", item.WorkspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.reviewer_advisory_record",
	})
	if err != nil {
		t.Fatalf("record late defect advisory: %v", err)
	}
	if defeated.State != sqlite.ProjectPatchQueueStateBlocked {
		t.Fatalf("late defect should move accepted item to BLOCKED, got %+v", defeated)
	}
	if !defeated.ReviewerAdvisoryAccepted || !defeated.ReviewerAdvisory.DefeatsAcceptance || defeated.ReviewerAdvisory.HeadSHA != item.HeadSHA {
		t.Fatalf("defeating advisory was not durably bound to item/head: %+v", defeated)
	}
	if !strings.Contains(defeated.DecisionSummary, "Acceptance defeated") || defeated.DecisionDocKey != reviewDocKey {
		t.Fatalf("defeat should write decision evidence, got summary=%q doc=%q", defeated.DecisionSummary, defeated.DecisionDocKey)
	}

	status, continuation, consumed, err := store.ConsumeProjectPatchQueueDecisionContinuation(ctx, sqlite.ProjectPatchQueueDecisionContinuationConsumeInput{
		WorkspaceID: item.WorkspaceID,
		ProjectID:   item.ProjectID,
		ActorID:     reviewerID,
		ActorType:   "agent",
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
	})
	if err != nil {
		t.Fatalf("consume defeat continuation: %v", err)
	}
	// The defeat's advisory eagerly materialized the revision continuation (beta holds IMPLEMENTER), so this
	// explicit consume idempotently REUSES the already-minted task (created=false); the route still resolves to a
	// claimable, CONSUMED revision carrier. The `consumed` name here is the third return = `created`; pinning it
	// FALSE locally re-asserts the idempotent-reuse expectation (no second mint).
	if consumed || continuation.FollowupKind != "revision" || status.TaskID == "" || continuation.State != "CONSUMED" {
		t.Fatalf("defeated acceptance should REUSE (not re-create) the eager-materialized revision continuation, created=%v continuation=%+v status=%+v", consumed, continuation, status)
	}
}

// findPatchQueueContinuationByDecision returns the continuation outbox row for an item keyed on a specific
// decision (an item can carry more than one: e.g. a defeated ACCEPTED item keeps its stale ACCEPTED/integration
// row AND a fresh BLOCKED/revision row).
func findPatchQueueContinuationByDecision(t *testing.T, ctx context.Context, store *sqlite.Store, item sqlite.ProjectPatchQueueItemRecord, decision string) sqlite.ProjectPatchQueueDecisionContinuationRecord {
	t.Helper()
	conts, err := store.ListProjectPatchQueueDecisionContinuations(ctx, sqlite.ProjectPatchQueueDecisionContinuationFilter{
		WorkspaceID: item.WorkspaceID, ProjectID: item.ProjectID, QueueID: item.QueueID, ItemID: item.ItemID,
	})
	if err != nil {
		t.Fatalf("list continuations: %v", err)
	}
	for _, c := range conts {
		if strings.EqualFold(strings.TrimSpace(c.Decision), strings.TrimSpace(decision)) {
			return c
		}
	}
	t.Fatalf("no continuation with decision %s among %+v", decision, conts)
	return sqlite.ProjectPatchQueueDecisionContinuationRecord{}
}

// Regression (stage-4 adversarial review, EAGER-MATERIALIZATION dimension): a decide-ACCEPTED item whose
// integration continuation DEFERS for want of an INTEGRATOR, then is defeated to BLOCKED, leaves a stale
// ACCEPTED/integration outbox row keyed on the prior decision. When an INTEGRATOR later appears, the sweep MUST
// NOT mint an integration carrier for the now-BLOCKED item (it could never pass the integration-receipt gate) -
// the sweep's staleness guard terminalizes the orphaned row to SUPPRESSED instead. No dead-end carrier, no
// CANCELLED leg. This is the asymmetry the explicit-consume path already guarded but the sweep did not.
func TestProjectPatchQueueDefeatedAcceptanceStaleIntegrationContinuationDoesNotMintViaSweep(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	item := createAcceptedControlledPatchQueueItemForTest(t, ctx, store, "defeat-stale-integration")
	const reviewerID = "iota"

	// ACCEPTED routed an integration continuation; no INTEGRATOR is active, so it DEFERS (the #4(b) survive path).
	integrationTaskID := sqlite.ProjectPatchQueueDecisionContinuationTaskID(item.ProjectID, item, "integration")
	accCont := findPatchQueueContinuationByDecision(t, ctx, store, item, sqlite.ProjectPatchQueueStateAccepted)
	if accCont.FollowupKind != "integration" || accCont.State != "DEFERRED" {
		t.Fatalf("accepted item with no integrator must leave a DEFERRED integration continuation, got %+v", accCont)
	}

	// A late defect defeats the acceptance -> item flips to BLOCKED (+ a BLOCKED/revision continuation). The prior
	// ACCEPTED/integration DEFERRED row is now stale (keyed on a decision the item no longer holds).
	reviewDocKey := "task.review.defeat-stale-integration.result"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: item.WorkspaceID, DocKey: reviewDocKey, Title: "Late Review", Content: "Verdict: REJECT\n\nBlocking lane defect.", UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	defeated, _, err := store.RecordProjectPatchQueueReviewerAdvisoryWithEvent(ctx, sqlite.ProjectPatchQueueReviewerAdvisoryRecordInput{
		WorkspaceID: item.WorkspaceID, ProjectID: item.ProjectID, QueueID: item.QueueID, ItemID: item.ItemID,
		ActorID: reviewerID, ActorType: "agent",
		ReviewerAdvisory: repoauthority.PatchQueueReviewerAdvisory{
			Verdict:           repoauthority.PatchQueueReviewerAdvisoryVerdictRepairRequired,
			Scope:             repoauthority.PatchQueueReviewerAdvisoryScopeLaneCorrectness,
			HeadSHA:           item.HeadSHA,
			ReviewDocKey:      reviewDocKey,
			Summary:           "Blocking lane defect; repair required before integration.",
			DefeatsAcceptance: true,
		},
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.reviewer_advisory_record", "server_rpc", item.WorkspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.reviewer_advisory_record",
	})
	if err != nil {
		t.Fatalf("record defeating advisory: %v", err)
	}
	if defeated.State != sqlite.ProjectPatchQueueStateBlocked {
		t.Fatalf("defeat must move item to BLOCKED, got %s", defeated.State)
	}

	// An INTEGRATOR now appears. Pre-fix, the sweep would re-materialize the stale ACCEPTED/integration row into a
	// dead-end integration carrier for the now-BLOCKED item. The staleness guard must terminalize it instead.
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{WorkspaceID: item.WorkspaceID, AgentID: "delta", OwnerUserID: "developer", DisplayName: "delta"}); err != nil {
		t.Fatalf("register integrator agent: %v", err)
	}
	assignAgentWorkProjectRole(t, ctx, store, item.WorkspaceID, item.ProjectID, "delta", sqlite.ProjectRoleIntegrator, "alpha")
	if err := store.ReconcileDeferredProjectPatchQueueContinuations(ctx, item.WorkspaceID); err != nil {
		t.Fatalf("sweep deferred continuations: %v", err)
	}

	// The orphaned ACCEPTED/integration row is terminalized (SUPPRESSED), and NO integration task was minted.
	accCont = findPatchQueueContinuationByDecision(t, ctx, store, item, sqlite.ProjectPatchQueueStateAccepted)
	if accCont.State != "SUPPRESSED" {
		t.Fatalf("stale ACCEPTED/integration continuation must terminalize to SUPPRESSED after the item left ACCEPTED, got %+v", accCont)
	}
	if goldenWorkspaceTaskExists(t, ctx, store, item.WorkspaceID, integrationTaskID) {
		t.Fatalf("sweep must NOT mint a dead-end integration carrier for a defeated (BLOCKED) item, task %s exists", integrationTaskID)
	}
}

func TestProjectPatchQueueRejectsMisScopedAcceptedLaneAdvisory(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	item := createAcceptedControlledPatchQueueItemForTest(t, ctx, store, "mis-scoped")
	const reviewerID = "iota"

	_, _, err := store.RecordProjectPatchQueueReviewerAdvisoryWithEvent(ctx, sqlite.ProjectPatchQueueReviewerAdvisoryRecordInput{
		WorkspaceID: item.WorkspaceID,
		ProjectID:   item.ProjectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ActorID:     reviewerID,
		ActorType:   "agent",
		ReviewerAdvisory: repoauthority.PatchQueueReviewerAdvisory{
			Verdict:      repoauthority.PatchQueueReviewerAdvisoryVerdictRepairRequired,
			Scope:        repoauthority.PatchQueueReviewerAdvisoryScopeIntegrationCompleteness,
			HeadSHA:      item.HeadSHA,
			ReviewDocKey: "task.review.full-product.result",
			Summary:      "Full product is not assembled yet; missing canonical integration receipt.",
		},
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.reviewer_advisory_record", "server_rpc", item.WorkspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.reviewer_advisory_record",
	})
	if !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "integration-completeness") {
		t.Fatalf("expected mis-scoped integration-completeness advisory to be rejected, got %v", err)
	}
	items, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{WorkspaceID: item.WorkspaceID, ProjectID: item.ProjectID})
	if err != nil {
		t.Fatalf("list items: %v", err)
	}
	if len(items) != 1 || items[0].State != sqlite.ProjectPatchQueueStateAccepted || items[0].ReviewerAdvisoryAccepted {
		t.Fatalf("mis-scoped advisory must not mutate accepted item, got %+v", items)
	}
}

func TestProjectPatchQueueIntegrationReceiptRequiresMaterializationOrDirectMerge(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID        = "ws-project-patch-queue-integration-receipt"
		projectID          = "project-patch-queue-integration-receipt"
		leadID             = "lead-agent"
		workerID           = "worker-agent"
		replayIntegratorID = "replay-integrator"
		repoID             = "repo-main"
		reviewKey          = "project.project-patch-queue-integration-receipt.branch.branch-ready.review"
	)
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, replayIntegratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, replayIntegratorID, sqlite.ProjectRoleIntegrator, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\patch-queue-integration-receipt`)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Controlled Branch Review Packet",
		Content:     "# Controlled Branch Review Packet\n\ncontrolled queue evidence",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	taskID := "task-controlled"
	reserved := registerReservedProjectBranchForReadyBindingTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, workerID, "branch-ready-controlled", "agent/worker-agent/patch-queue-integration-receipt", `{"paths":["cmd/app.go"]}`)
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, reserved.BranchID, taskID, `{"paths":["cmd/app.go"]}`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              "branch-ready-controlled",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/worker-agent/patch-queue-integration-receipt",
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["cmd/app.go"]}`,
		ReviewDocKey:          reviewKey,
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
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-controlled",
		RunID:                    "run-controlled",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-controlled",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes:           map[string]string{"cmd/app.go": "sha256:cmd"},
		RepoLeaseID:              "lease-controlled",
		LeaseTerm:                7,
		ActorID:                  workerID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit controlled item: %v", err)
	}
	if _, _, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeRepair,
		TargetBranch:          "main",
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		RepairReason:          "repair receipts cannot be recorded before an accepted integration boundary exists",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.integration_repair", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.integration_repair",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "must be ACCEPTED") {
		t.Fatalf("expected repair receipt before ACCEPTED to fail, got %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET state = 'ACCEPTED', decision_summary = 'accepted for integration', decided_by = ?, decided_at = updated_at
 WHERE queue_id = ? AND item_id = ?`,
		leadID, item.QueueID, item.ItemID); err != nil {
		t.Fatalf("force accepted controlled item: %v", err)
	}

	if _, _, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeAdmitted,
		TargetBranch:          "main",
		IntegrationMode:       "",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "requires durable materialization") {
		t.Fatalf("expected controlled integration without materialization/direct_merge to fail, got %v", err)
	}
	if _, _, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeAdmitted,
		TargetBranch:          "main",
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		SourceBranchID:        "branch-wrong",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "source_branch_id guard") {
		t.Fatalf("expected mismatched source_branch_id guard to fail, got %v", err)
	}
	if _, _, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeAdmitted,
		TargetBranch:          "main",
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		SourceHeadSHA:         strings.Repeat("c", 40),
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "source_head_sha guard") {
		t.Fatalf("expected mismatched source_head_sha guard to fail, got %v", err)
	}

	admitted, event, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeAdmitted,
		TargetBranch:          "main",
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("record direct merge admission receipt: %v", err)
	}
	if admitted.State != sqlite.ProjectPatchQueueStateAccepted || event.EventType != sqlite.ProjectPatchQueueIntegrationAdmittedEventType {
		t.Fatalf("unexpected admission receipt: item=%+v event=%+v", admitted, event)
	}
	firstAdmissionEventID := event.EventID
	replayedAdmission, replayedAdmissionEvent, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               replayIntegratorID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeAdmitted,
		TargetBranch:          "refs/heads/main",
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", replayIntegratorID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("idempotent admission replay by second integrator: %v", err)
	}
	if replayedAdmission.State != sqlite.ProjectPatchQueueStateAccepted || replayedAdmissionEvent.EventID != firstAdmissionEventID {
		t.Fatalf("admission replay should reuse existing receipt without actor-scoped dedup conflict, item=%+v event=%+v first=%s", replayedAdmission, replayedAdmissionEvent, firstAdmissionEventID)
	}
	if _, _, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeIntegrated,
		TargetBranch:          "main",
		TargetHeadBefore:      baseSHA,
		TargetHeadAfter:       headSHA,
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		MergePerformed:        true,
		PushAttempted:         false,
		PushSucceeded:         false,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "push_succeeded") {
		t.Fatalf("expected integrated receipt without canonical remote proof to fail, got %v", err)
	}
	if _, _, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeIntegrated,
		TargetHeadBefore:      baseSHA,
		TargetHeadAfter:       headSHA,
		RemoteTargetHeadAfter: headSHA,
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		PushAttempted:         true,
		PushSucceeded:         true,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "target_branch") {
		t.Fatalf("expected integrated receipt without target_branch identity to fail, got %v", err)
	}
	integrated, event, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeIntegrated,
		TargetBranch:          "main",
		TargetHeadBefore:      baseSHA,
		TargetHeadAfter:       headSHA,
		RemoteTargetHeadAfter: headSHA,
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		PushAttempted:         true,
		PushSucceeded:         true,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("record integrated receipt: %v", err)
	}
	if integrated.State != sqlite.ProjectPatchQueueStateIntegrated || event.EventType != sqlite.ProjectPatchQueueIntegratedEventType {
		t.Fatalf("unexpected integrated receipt: item=%+v event=%+v", integrated, event)
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode integrated receipt payload: %v", err)
	}
	if payload["source_branch_id"] != branch.BranchID || payload["source_head_sha"] != headSHA {
		t.Fatalf("expected integrated receipt to bind source refs to accepted item, got %+v", payload)
	}
	firstIntegratedEventID := event.EventID
	retry, retryEvent, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeIntegrated,
		TargetBranch:          "refs/heads/main",
		TargetHeadBefore:      headSHA,
		TargetHeadAfter:       headSHA,
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		AlreadyIntegrated:     true,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("idempotent integrated receipt retry: %v", err)
	}
	if retry.State != sqlite.ProjectPatchQueueStateIntegrated || retryEvent.EventID != firstIntegratedEventID {
		t.Fatalf("integrated retry should reuse existing receipt without state regression, item=%+v event=%+v first=%s", retry, retryEvent, firstIntegratedEventID)
	}
	differentTarget, differentTargetEvent, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeIntegrated,
		TargetBranch:          "release",
		TargetHeadBefore:      headSHA,
		TargetHeadAfter:       headSHA,
		RemoteTargetHeadAfter: headSHA,
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		PushAttempted:         true,
		PushSucceeded:         true,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("record same-head integration receipt on a different target branch: %v", err)
	}
	if differentTarget.State != sqlite.ProjectPatchQueueStateIntegrated || differentTargetEvent.EventID == firstIntegratedEventID {
		t.Fatalf("different target branch must not collapse into the first integrated receipt, item=%+v event=%+v first=%s", differentTarget, differentTargetEvent, firstIntegratedEventID)
	}
	postIntegratedRepair, repairEvent, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeRepair,
		TargetBranch:          "refs/heads/main",
		TargetHeadBefore:      headSHA,
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		RepairReason:          "source branch close failed after integrated receipt",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.integration_repair", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.integration_repair",
	})
	if err != nil {
		t.Fatalf("post-integrated repair receipt: %v", err)
	}
	if postIntegratedRepair.State != sqlite.ProjectPatchQueueStateIntegrated || repairEvent.EventType != sqlite.ProjectPatchQueueIntegrationRepairEventType {
		t.Fatalf("post-integrated repair should record evidence without blocking integrated item, item=%+v event=%+v", postIntegratedRepair, repairEvent)
	}
}

func TestProjectPatchQueueIntegratedReceiptTerminalizesQueueBoundWork(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID       = "ws-patchq-integrated-terminalizes-work"
		projectID         = "project-patchq-integrated-terminalizes-work"
		repoID            = "repo-patchq-integrated-terminalizes-work"
		leadID            = "lead"
		ownerID           = "owner"
		reviewerID        = "reviewer"
		integratorID      = "integrator"
		branchID          = "branch-patchq-integrated-terminalizes-work"
		reviewTaskID      = "task-patchq-review-terminalized"
		integrationTaskID = "task-patchq-integration-terminalized"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID, integratorID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, integratorID, sqlite.ProjectRoleIntegrator, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	branch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, branchID)
	const headSHA = "1234567890abcdef1234567890abcdef12345678"
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE project_branch_registry SET head_sha = ?, base_sha = ?, updated_at = ? WHERE workspace_id = ? AND branch_id = ?`, headSHA, strings.Repeat("0", 40), "2026-06-08T00:00:00Z", workspaceID, branch.BranchID); err != nil {
		t.Fatalf("seed branch head: %v", err)
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
		t.Fatalf("submit patch queue item: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET state = 'ACCEPTED', head_sha = ?, decision_summary = 'accepted for integration', decided_by = ?, decided_at = updated_at
 WHERE queue_id = ? AND item_id = ?`,
		headSHA, reviewerID, item.QueueID, item.ItemID); err != nil {
		t.Fatalf("force accepted item: %v", err)
	}
	item.HeadSHA = headSHA

	graph := taskSubmitGateGraph(t)
	for _, task := range []sqlite.TaskCreateInput{
		{
			WorkspaceID:          workspaceID,
			TaskID:               reviewTaskID,
			OwnerUserID:          reviewerID,
			Priority:             "high",
			Title:                "Review accepted patch queue candidate",
			Description:          "Review exact patch queue item before integration.",
			TaskKind:             model.TaskKindExecution,
			TaskTemplate:         model.TaskTemplateGeneric,
			ProjectID:            projectID,
			ProjectLane:          "review",
			Tags:                 []string{"patch-queue", "review"},
			RequiresProjectGate:  true,
			TaskRequirementsJSON: taskSubmitGateRequirements(item.QueueID, item.ItemID, item.BranchID, item.HeadSHA, "review"),
		},
		{
			WorkspaceID:          workspaceID,
			TaskID:               integrationTaskID,
			OwnerUserID:          integratorID,
			Priority:             "high",
			Title:                "Integrate accepted patch queue candidate",
			Description:          "Call project_patch_queue_integrate for the exact accepted item.",
			TaskKind:             model.TaskKindExecution,
			TaskTemplate:         model.TaskTemplateGeneric,
			ProjectID:            projectID,
			ProjectLane:          "integration",
			Tags:                 []string{"patch-queue", "integration"},
			RequiresProjectGate:  true,
			TaskRequirementsJSON: taskSubmitGateRequirements(item.QueueID, item.ItemID, item.BranchID, item.HeadSHA, "integration"),
		},
	} {
		if err := createTaskSubmitGateTask(ctx, store, task, graph); err != nil {
			t.Fatalf("create queue-bound task %s: %v", task.TaskID, err)
		}
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO task_claims(task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at)
VALUES (?, ?, ?, ?, 'released review claim', '2026-06-08T00:01:00Z', '2026-06-08T00:02:00Z', '2026-06-08T00:02:00Z'),
       (?, ?, ?, ?, 'active integration claim', '2026-06-08T00:03:00Z', NULL, '2026-06-08T00:03:00Z')`,
		reviewTaskID, workspaceID, reviewerID, model.TaskClaimStatusReleased,
		integrationTaskID, workspaceID, integratorID, model.TaskClaimStatusClaimed); err != nil {
		t.Fatalf("seed queue-bound claims: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET status = ? WHERE task_id = ?`, model.TaskStatusRunning, integrationTaskID); err != nil {
		t.Fatalf("mark integration task running: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE project_branch_registry SET active_task_id = ?, active_claim_id = ? WHERE workspace_id = ? AND branch_id = ?`, integrationTaskID, integrationTaskID, workspaceID, branch.BranchID); err != nil {
		t.Fatalf("seed branch active refs: %v", err)
	}

	_, event, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               integratorID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeIntegrated,
		TargetBranch:          "main",
		TargetHeadBefore:      strings.Repeat("0", 40),
		TargetHeadAfter:       strings.Repeat("1", 40),
		RemoteTargetHeadAfter: strings.Repeat("1", 40),
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		PushAttempted:         true,
		PushSucceeded:         true,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.integration_record", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.integration_record",
	})
	if err != nil {
		t.Fatalf("record integrated receipt: %v", err)
	}
	for _, taskID := range []string{reviewTaskID, integrationTaskID} {
		status, err := store.GetTaskStatus(ctx, workspaceID, taskID)
		if err != nil {
			t.Fatalf("get task status %s: %v", taskID, err)
		}
		if status.Status != model.TaskStatusResolved {
			t.Fatalf("task %s status = %s, want RESOLVED", taskID, status.Status)
		}
		assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, taskID, model.TaskClaimStatusCompleted)
	}
	var activeTaskID, activeClaimID string
	if err := store.DB().QueryRowContext(ctx, `SELECT active_task_id, active_claim_id FROM project_branch_registry WHERE workspace_id = ? AND branch_id = ?`, workspaceID, branch.BranchID).Scan(&activeTaskID, &activeClaimID); err != nil {
		t.Fatalf("query branch active refs: %v", err)
	}
	if activeTaskID != "" || activeClaimID != "" {
		t.Fatalf("branch active refs = task:%q claim:%q, want cleared", activeTaskID, activeClaimID)
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode integrated event payload: %v", err)
	}
	if payload["integration_terminalization"] == nil {
		t.Fatalf("integrated payload missing terminalization evidence: %s", event.PayloadJSON)
	}

	const replayIntegrationTaskID = "task-patchq-integration-terminalized-replay"
	if err := createTaskSubmitGateTask(ctx, store, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               replayIntegrationTaskID,
		OwnerUserID:          integratorID,
		Priority:             "high",
		Title:                "Replay integration for accepted legacy item",
		Description:          "Retry integration after a legacy accepted item already has an integrated receipt.",
		TaskKind:             model.TaskKindExecution,
		TaskTemplate:         model.TaskTemplateGeneric,
		ProjectID:            projectID,
		ProjectLane:          "integration",
		Tags:                 []string{"patch-queue", "integration"},
		RequiresProjectGate:  true,
		TaskRequirementsJSON: taskSubmitGateRequirements(item.QueueID, item.ItemID, item.BranchID, item.HeadSHA, "integration"),
	}, graph); err != nil {
		t.Fatalf("create replay integration task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET state = 'ACCEPTED', claimed_by = '', claim_token = '', claimed_at = '', claim_expires_at = '', updated_at = ?
 WHERE queue_id = ? AND item_id = ?`,
		"2026-06-08T00:04:00Z", item.QueueID, item.ItemID); err != nil {
		t.Fatalf("force legacy accepted item after integrated receipt: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO task_claims(task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at)
VALUES (?, ?, ?, ?, 'active replay integration claim', '2026-06-08T00:05:00Z', NULL, '2026-06-08T00:05:00Z')`,
		replayIntegrationTaskID, workspaceID, integratorID, model.TaskClaimStatusClaimed); err != nil {
		t.Fatalf("seed replay integration claim: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET status = ? WHERE task_id = ?`, model.TaskStatusRunning, replayIntegrationTaskID); err != nil {
		t.Fatalf("mark replay integration task running: %v", err)
	}

	replayed, replayEvent, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               integratorID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeIntegrated,
		TargetBranch:          "refs/heads/main",
		TargetHeadBefore:      strings.Repeat("1", 40),
		TargetHeadAfter:       strings.Repeat("1", 40),
		RemoteTargetHeadAfter: strings.Repeat("1", 40),
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		AlreadyIntegrated:     true,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.integration_record", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.integration_record",
	})
	if err != nil {
		t.Fatalf("replay integrated receipt against legacy accepted item: %v", err)
	}
	if replayed.State != sqlite.ProjectPatchQueueStateIntegrated || replayEvent.EventID != event.EventID {
		t.Fatalf("integrated replay should reuse original receipt and terminalize accepted item, item=%+v event=%+v first=%s", replayed, replayEvent, event.EventID)
	}
	status, err := store.GetTaskStatus(ctx, workspaceID, replayIntegrationTaskID)
	if err != nil {
		t.Fatalf("get replay integration task status: %v", err)
	}
	if status.Status != model.TaskStatusResolved {
		t.Fatalf("replay integration task status = %s, want RESOLVED", status.Status)
	}
	assertWorkspaceTaskClaimStatus(t, ctx, store, workspaceID, replayIntegrationTaskID, model.TaskClaimStatusCompleted)

	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET state = 'ACCEPTED', claimed_by = '', claim_token = '', claimed_at = '', claim_expires_at = '', updated_at = ?
 WHERE queue_id = ? AND item_id = ?`,
		"2026-06-08T00:06:00Z", item.QueueID, item.ItemID); err != nil {
		t.Fatalf("force legacy accepted item before repair replay: %v", err)
	}
	repairReplayed, repairReplayEvent, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               integratorID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeRepair,
		TargetBranch:          "main",
		TargetHeadAfter:       strings.Repeat("1", 40),
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		RepairReason:          "retry saw terminalization dedup after existing integrated receipt",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.integration_repair", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.integration_repair",
	})
	if err != nil {
		t.Fatalf("repair replay after existing integrated receipt: %v", err)
	}
	if repairReplayed.State != sqlite.ProjectPatchQueueStateIntegrated || repairReplayEvent.EventID != event.EventID {
		t.Fatalf("repair replay should reuse integrated receipt without blocking item, item=%+v event=%+v first=%s", repairReplayed, repairReplayEvent, event.EventID)
	}
}

func TestReleaseTaskClaimReleasesBoundPatchQueueClaim(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-patchq-task-release-claim"
		projectID    = "project-patchq-task-release-claim"
		repoID       = "repo-patchq-task-release-claim"
		leadID       = "lead"
		ownerID      = "owner"
		reviewerID   = "reviewer"
		branchID     = "branch-patchq-task-release-claim"
		reviewTaskID = "task-patchq-review-release-claim"
		headSHA      = "abcdefabcdefabcdefabcdefabcdefabcdefabcd"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	branch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, branchID)
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE project_branch_registry SET head_sha = ?, base_sha = ?, updated_at = ? WHERE workspace_id = ? AND branch_id = ?`, headSHA, strings.Repeat("0", 40), "2026-06-11T00:00:00Z", workspaceID, branch.BranchID); err != nil {
		t.Fatalf("seed branch head: %v", err)
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
		t.Fatalf("submit patch queue item: %v", err)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               reviewerID,
		ActorType:             "agent",
		LeaseSeconds:          1080,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim patch queue item: %v", err)
	}
	if claimed.State != sqlite.ProjectPatchQueueStateClaimed || claimed.ClaimedBy != reviewerID || claimed.ClaimToken == "" {
		t.Fatalf("expected reviewer to hold live patch queue claim, got %+v", claimed)
	}

	graph := taskSubmitGateGraph(t)
	if err := createTaskSubmitGateTask(ctx, store, sqlite.TaskCreateInput{
		WorkspaceID:          workspaceID,
		TaskID:               reviewTaskID,
		OwnerUserID:          reviewerID,
		Priority:             "high",
		Title:                "Review patch queue candidate",
		Description:          "Review exact patch queue item before integration.",
		TaskKind:             model.TaskKindExecution,
		TaskTemplate:         model.TaskTemplateGeneric,
		ProjectID:            projectID,
		ProjectLane:          "review",
		Tags:                 []string{"patch-queue", "review"},
		RequiresProjectGate:  true,
		TaskRequirementsJSON: taskSubmitGateRequirements(item.QueueID, item.ItemID, item.BranchID, item.HeadSHA, "review_receipt"),
	}, graph); err != nil {
		t.Fatalf("create review task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
INSERT INTO task_claims(task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at)
VALUES (?, ?, ?, ?, 'claimed review task', '2026-06-11T00:01:00Z', NULL, '2026-06-11T00:01:00Z')`,
		reviewTaskID, workspaceID, reviewerID, model.TaskClaimStatusClaimed); err != nil {
		t.Fatalf("seed review task claim: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE tasks SET status = ?, updated_at = ? WHERE task_id = ?`, model.TaskStatusRunning, "2026-06-11T00:01:00Z", reviewTaskID); err != nil {
		t.Fatalf("mark review task running: %v", err)
	}

	releaseEvent, err := store.ReleaseTaskClaimWithEvent(ctx, sqlite.TaskReleaseInput{
		WorkspaceID: workspaceID,
		TaskID:      reviewTaskID,
		AgentID:     reviewerID,
		Reason:      "provider timeout before patch queue decision",
	})
	if err != nil {
		t.Fatalf("release review task claim: %v", err)
	}

	items, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{WorkspaceID: workspaceID, ProjectID: projectID})
	if err != nil {
		t.Fatalf("list patch queue items: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one patch queue item, got %+v", items)
	}
	got := items[0]
	if got.State != sqlite.ProjectPatchQueueStateProposed || got.ClaimedBy != "" || got.ClaimToken != "" || got.ClaimedAt != "" || got.ClaimExpiresAt != "" {
		t.Fatalf("released task should return patch queue item to unclaimed PROPOSED, got %+v", got)
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(releaseEvent.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode release event payload: %v", err)
	}
	report, ok := payload["patch_queue_claim_release"].(map[string]any)
	if !ok {
		t.Fatalf("release event missing patch_queue_claim_release report: %s", releaseEvent.PayloadJSON)
	}
	released, ok := report["released"].([]any)
	if !ok || len(released) != 1 {
		t.Fatalf("expected one released patch queue claim in payload, got %+v", report)
	}
	var releaseEventCount int
	if err := store.DB().QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM runtime_events
 WHERE workspace_id = ?
   AND event_type = 'project.patch_queue.released'
   AND entity_id = ?
   AND task_id = ?`,
		workspaceID, item.QueueID+"/"+item.ItemID, reviewTaskID).Scan(&releaseEventCount); err != nil {
		t.Fatalf("count patch queue release runtime event: %v", err)
	}
	if releaseEventCount != 1 {
		t.Fatalf("expected one durable patch queue release event for task release, got %d", releaseEventCount)
	}
}

func createAcceptedControlledPatchQueueItemForTest(t *testing.T, ctx context.Context, store *sqlite.Store, suffix string) sqlite.ProjectPatchQueueItemRecord {
	t.Helper()

	workspaceID := "ws-patchq-" + suffix
	projectID := "project-patchq-" + suffix
	repoID := "repo-patchq-" + suffix
	leadID := "alpha"
	ownerID := "beta"
	reviewerID := "iota"
	branchID := "branch-patchq-" + suffix
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	branch := registerReadyOwnerBranchForAgentWorkTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, branchID)
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   branch.ActiveTaskID,
		SessionID:                "session-controlled-" + suffix,
		RunID:                    "run-controlled-" + suffix,
		AgentID:                  ownerID,
		CapabilitySnapshotID:     "cap-controlled-" + suffix,
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 `C:\fixtures\agents\` + ownerID + `\` + branchID,
		BaseTreeHash:             branch.BaseSHA,
		BaseFileHashes:           map[string]string{"src/app.go": "sha256:src-app"},
		RepoLeaseID:              "lease-controlled-" + suffix,
		LeaseTerm:                7,
		ActorID:                  ownerID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit controlled item: %v", err)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               reviewerID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim patch queue item: %v", err)
	}
	accepted, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateAccepted,
		DecisionSummary:       "Accepted for lane-scoped integration after bounded source review.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("accept controlled item: %v", err)
	}
	return accepted
}

func TestProjectPatchQueueIntegrationRepairReceiptBlocksAcceptedItemAndBindsSourceRefs(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-patch-queue-integration-repair"
		projectID   = "project-patch-queue-integration-repair"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
	)
	baseSHA := strings.Repeat("1", 40)
	headSHA := strings.Repeat("2", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	branch := createAcceptedReadyBranchForMergeTest(t, ctx, store, workspaceID, projectID, repoID, workerID, leadID, "branch-integration-repair", "agent/worker-agent/integration-repair", headSHA, baseSHA, `C:\fixtures\agents\worker-agent\integration-repair`)
	items, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		BranchID:    branch.BranchID,
		State:       sqlite.ProjectPatchQueueStateAccepted,
	})
	if err != nil {
		t.Fatalf("list accepted patch queue item: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one accepted item, got %+v", items)
	}
	repaired, event, err := store.RecordProjectPatchQueueIntegrationWithEvent(ctx, sqlite.ProjectPatchQueueIntegrationRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               items[0].QueueID,
		ItemID:                items[0].ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeRepair,
		TargetBranch:          "main",
		TargetHeadBefore:      baseSHA,
		TargetHeadAfter:       headSHA,
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		SourceBranchID:        branch.BranchID,
		SourceHeadSHA:         headSHA,
		MergePerformed:        true,
		PushAttempted:         true,
		PushSucceeded:         false,
		RepairReason:          "canonical push failed after local merge",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.integration_repair", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.integration_repair",
	})
	if err != nil {
		t.Fatalf("record integration repair receipt: %v", err)
	}
	if repaired.State != sqlite.ProjectPatchQueueStateBlocked || !strings.Contains(repaired.DecisionSummary, "canonical push failed") {
		t.Fatalf("expected repair receipt to block accepted item, got %+v", repaired)
	}
	if event.EventType != sqlite.ProjectPatchQueueIntegrationRepairEventType {
		t.Fatalf("expected repair event, got %+v", event)
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode repair payload: %v", err)
	}
	if payload["source_branch_id"] != branch.BranchID || payload["source_head_sha"] != headSHA {
		t.Fatalf("expected repair receipt to bind source refs to accepted item, got %+v", payload)
	}
	if payload["repair_reason"] != "canonical push failed after local merge" {
		t.Fatalf("expected repair reason in payload, got %+v", payload)
	}
	// Drain model (stage 4): the repair blocks the item and records a continuation receipt. The branch owner
	// (worker-agent) holds no claimable role in this setup, so the receipt route-classifies awaiting_role and
	// rests DEFERRED (observably awaiting its consumer via the sweep) rather than PENDING-with-no-consumer.
	var outboxCount int
	if err := store.WriteDB().QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM project_patch_queue_decision_continuation_outbox
 WHERE workspace_id = ? AND project_id = ? AND queue_id = ? AND item_id = ? AND decision = 'BLOCKED' AND state = 'DEFERRED'`,
		workspaceID, projectID, items[0].QueueID, items[0].ItemID,
	).Scan(&outboxCount); err != nil {
		t.Fatalf("count repair continuation outbox: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("expected integration repair to create one deferred blocked continuation receipt, got %d", outboxCount)
	}
}

func TestProjectPatchQueueDurabilityProofDegradesWithoutMigrations(t *testing.T) {
	t.Parallel()

	store, err := sqlite.NewStore(t.TempDir() + "/empty.db")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer func() { _ = store.Close() }()

	proof := store.ProjectPatchQueueDurabilityProof(context.Background())
	if proof.Contract != sqlite.ProjectPatchQueueDurabilityProofContract {
		t.Fatalf("contract = %q", proof.Contract)
	}
	if proof.State != "degraded" || proof.Durable {
		t.Fatalf("expected non-migrated store to degrade durability proof, got %+v", proof)
	}
	if proof.TablePresent {
		t.Fatalf("expected patch queue table to be absent before migrations, got %+v", proof)
	}
	if err := sqlite.VerifyProjectPatchQueueDurabilityProof(proof); err != nil {
		t.Fatalf("verify degraded durability proof: %v", err)
	}
}

func TestProjectPatchQueueDurabilityProofVerifierRejectsTampering(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	proof := store.ProjectPatchQueueDurabilityProof(context.Background())
	proof.Durable = false

	if err := sqlite.VerifyProjectPatchQueueDurabilityProof(proof); err == nil {
		t.Fatalf("expected tampered durability proof to fail verification")
	}
}

func TestProjectPatchQueueDurabilityProofRejectsWrongLiveBranchIndexShape(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	if _, err := store.WriteDB().ExecContext(ctx, `DROP INDEX IF EXISTS idx_project_patch_queue_items_live_branch`); err != nil {
		t.Fatalf("drop live branch index: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
CREATE UNIQUE INDEX idx_project_patch_queue_items_live_branch
  ON project_patch_queue_items(workspace_id, repo_id)
  WHERE state IN ('PROPOSED', 'CLAIMED')`); err != nil {
		t.Fatalf("create malformed live branch index: %v", err)
	}

	proof := store.ProjectPatchQueueDurabilityProof(ctx)
	if proof.LiveBranchIndexPresent {
		t.Fatalf("malformed live branch index should not satisfy durability proof: %+v", proof)
	}
	if proof.Durable || proof.State == "ok" {
		t.Fatalf("malformed live branch index should degrade durability proof: %+v", proof)
	}
}

func TestProjectPatchQueueDurabilityProofRejectsNonEquivalentLiveBranchPredicate(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	if _, err := store.WriteDB().ExecContext(ctx, `DROP INDEX IF EXISTS idx_project_patch_queue_items_live_branch`); err != nil {
		t.Fatalf("drop live branch index: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
CREATE UNIQUE INDEX idx_project_patch_queue_items_live_branch
  ON project_patch_queue_items(workspace_id, branch_id)
  WHERE state IN ('PROPOSED') AND state IN ('CLAIMED')`); err != nil {
		t.Fatalf("create non-equivalent live branch index: %v", err)
	}

	proof := store.ProjectPatchQueueDurabilityProof(ctx)
	if proof.LiveBranchIndexPresent {
		t.Fatalf("non-equivalent live branch predicate should not satisfy durability proof: %+v", proof)
	}
	if proof.Durable || proof.State == "ok" {
		t.Fatalf("non-equivalent live branch predicate should degrade durability proof: %+v", proof)
	}
}

func TestProjectPatchQueueDurabilityProofRejectsWrongMaterializationIndexShape(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	if _, err := store.WriteDB().ExecContext(ctx, `DROP INDEX IF EXISTS idx_project_patch_queue_items_materialization`); err != nil {
		t.Fatalf("drop materialization index: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
CREATE INDEX idx_project_patch_queue_items_materialization
  ON project_patch_queue_items(workspace_id, project_id, materialization_recorded_at)
  WHERE materialization_accepted = 1`); err != nil {
		t.Fatalf("create malformed materialization index: %v", err)
	}

	proof := store.ProjectPatchQueueDurabilityProof(ctx)
	if proof.MaterializationIndexPresent {
		t.Fatalf("malformed materialization index should not satisfy durability proof: %+v", proof)
	}
	if proof.Durable || proof.State == "ok" {
		t.Fatalf("malformed materialization index should degrade durability proof: %+v", proof)
	}
}

func TestProjectPatchQueueDurabilityProofRejectsMalformedDecisionContinuationOutbox(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	if _, err := store.WriteDB().ExecContext(ctx, `DROP TABLE IF EXISTS project_patch_queue_decision_continuation_outbox`); err != nil {
		t.Fatalf("drop decision continuation outbox: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
CREATE TABLE project_patch_queue_decision_continuation_outbox (
    outbox_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    queue_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    branch_id TEXT NOT NULL DEFAULT '',
    head_sha TEXT NOT NULL DEFAULT '',
    decision TEXT NOT NULL,
    followup_kind TEXT NOT NULL,
    continuation_task_id TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL,
    decision_event_id TEXT NOT NULL,
    decision_doc_key TEXT NOT NULL DEFAULT '',
    decision_summary TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX idx_project_patch_queue_decision_continuation_pending
    ON project_patch_queue_decision_continuation_outbox(workspace_id, project_id, state, followup_kind, updated_at);
CREATE INDEX idx_project_patch_queue_decision_continuation_item
    ON project_patch_queue_decision_continuation_outbox(workspace_id, project_id, queue_id, item_id);`); err != nil {
		t.Fatalf("create malformed decision continuation outbox: %v", err)
	}

	proof := store.ProjectPatchQueueDurabilityProof(ctx)
	if proof.DecisionContinuationPresent {
		t.Fatalf("malformed decision continuation outbox should not satisfy durability proof: %+v", proof)
	}
	if proof.Durable || proof.State == "ok" {
		t.Fatalf("malformed decision continuation outbox should degrade durability proof: %+v", proof)
	}
}

func TestProjectPatchQueueDecisionClearsLiveClaimOwnership(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patch-queue-decision-clears-claim"
		projectID   = "project-patch-queue-decision-clears-claim"
		leadID      = "lead-agent"
		ownerID     = "owner-agent"
		reviewerID  = "reviewer-agent"
		repoID      = "repo-main"
		branchID    = "branch-decision-clears-claim"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)

	decided := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, ownerID, reviewerID, `{"paths":["src/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	if decided.State != sqlite.ProjectPatchQueueStateBlocked {
		t.Fatalf("decision state = %q", decided.State)
	}
	if decided.ClaimedBy != "" || decided.ClaimToken != "" || decided.ClaimedAt != "" || decided.ClaimExpiresAt != "" {
		t.Fatalf("terminal decision should clear live claim fields, got claimed_by=%q token=%q claimed_at=%q expires=%q", decided.ClaimedBy, decided.ClaimToken, decided.ClaimedAt, decided.ClaimExpiresAt)
	}
	if decided.DecidedBy != reviewerID || decided.DecidedAt == "" {
		t.Fatalf("terminal decision should preserve decision authority, got decided_by=%q decided_at=%q", decided.DecidedBy, decided.DecidedAt)
	}

	stored, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		BranchID:    branchID,
		State:       sqlite.ProjectPatchQueueStateBlocked,
	})
	if err != nil {
		t.Fatalf("list patch queue items: %v", err)
	}
	if len(stored) != 1 {
		t.Fatalf("expected one stored blocked item, got %d", len(stored))
	}
	if stored[0].ClaimedBy != "" || stored[0].ClaimToken != "" {
		t.Fatalf("stored terminal item should not advertise a live owner, got %+v", stored[0])
	}
	if stored[0].DecidedBy != reviewerID {
		t.Fatalf("stored terminal item should retain decided_by, got %+v", stored[0])
	}
}

func TestProjectPatchQueueAcceptedUIRequiresVisualAcceptanceAtStorageBoundary(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patch-queue-accepted-visual-boundary"
		projectID   = "project-patch-queue-accepted-visual-boundary"
		leadID      = "lead-agent"
		ownerID     = "owner-agent"
		reviewerID  = "reviewer-agent"
		repoID      = "repo-main"
		reviewKey   = "project.project-patch-queue-accepted-visual-boundary.branch.review"
	)
	headSHA := strings.Repeat("b", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\owner-agent\ui-acceptance`)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nReady for UI patch queue review.",
		UpdatedBy:   ownerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	_, taskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, ownerID, "branch-ui-acceptance", "agent/owner-agent/ui-acceptance", `{"paths":["web/app.tsx","src/components/App.tsx"]}`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              "branch-ui-acceptance",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/owner-agent/ui-acceptance",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["web/app.tsx","src/components/App.tsx"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register UI branch: %v", err)
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
		t.Fatalf("submit UI patch queue item: %v", err)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               reviewerID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim UI patch queue item: %v", err)
	}

	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              "ACCEPTED",
		DecisionSummary:       "Accepted frontend UI candidate without visual packet.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "rhizome_visual_acceptance_v1") {
		t.Fatalf("expected ACCEPTED UI decision without visual packet to fail, got %v", err)
	}

	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              "ACCEPTED",
		DecisionSummary:       "Accepted non-ui core-only candidate without browser surface.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "rhizome_visual_acceptance_v1") {
		t.Fatalf("explicit UI pathset must not be bypassed by core-only prose, got %v", err)
	}

	blockingKey := "project." + projectID + ".visual_acceptance.blocking"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      blockingKey,
		Title:       "Blocking Visual Acceptance",
		Content:     acceptedVisualPacketForPatchQueueTest(item, branch, "fail") + "\nblocking_findings: primary button overlaps output panel\n",
		UpdatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("write blocking visual packet: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              "ACCEPTED",
		DecisionDocKey:        blockingKey,
		DecisionSummary:       "Accepted frontend UI candidate with a blocking packet.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "non-pass verdict") {
		t.Fatalf("expected ACCEPTED UI decision with blocking visual packet to fail, got %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2025-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, blockingKey); err != nil {
		t.Fatalf("force blocking review timestamp older than follow-up packets: %v", err)
	}

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Older Passing Visual Acceptance",
		Content:     acceptedVisualPacketForPatchQueueTest(item, branch, "pass"),
		UpdatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("write older passing review packet: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2026-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, reviewKey); err != nil {
		t.Fatalf("force older passing review timestamp: %v", err)
	}
	incompleteKey := "project." + projectID + ".visual_acceptance.newer_incomplete"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      incompleteKey,
		Title:       "Newer Incomplete Visual Acceptance",
		Content: strings.Join([]string{
			"schema: rhizome_visual_acceptance_v1",
			"visual_verdict: pass",
			"queue_id: " + item.QueueID,
			"item_id: " + item.ItemID,
			"branch_id: " + branch.BranchID,
			"head_sha: " + item.HeadSHA,
		}, "\n"),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write newer incomplete visual packet: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2026-02-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, incompleteKey); err != nil {
		t.Fatalf("force newer incomplete timestamp: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              "ACCEPTED",
		DecisionDocKey:        incompleteKey,
		DecisionSummary:       "Accepted frontend UI candidate with newer incomplete visual packet.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "screenshot") {
		t.Fatalf("newer incomplete visual packet must not fall back to older passing packet, got %v", err)
	}

	sameSecondPassKey := "project." + projectID + ".visual_acceptance.same_second_pass"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      sameSecondPassKey,
		Title:       "Same Second Passing Visual Acceptance",
		Content:     acceptedVisualPacketForPatchQueueTest(item, branch, "pass"),
		UpdatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("write same-second passing visual packet: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-04-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, sameSecondPassKey); err != nil {
		t.Fatalf("force same-second passing timestamp: %v", err)
	}
	sameSecondFailKey := "project." + projectID + ".visual_acceptance.same_second_fail"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      sameSecondFailKey,
		Title:       "Same Second Failing Visual Acceptance",
		Content:     acceptedVisualPacketForPatchQueueTest(item, branch, "fail") + "\nblocking_findings: output panel overlaps the action bar\n",
		UpdatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("write same-second failing visual packet: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-04-01T00:00:00.5Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, sameSecondFailKey); err != nil {
		t.Fatalf("force same-second failing timestamp: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              "ACCEPTED",
		DecisionDocKey:        sameSecondFailKey,
		DecisionSummary:       "Accepted frontend UI candidate while a newer same-second visual packet fails.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "non-pass verdict") {
		t.Fatalf("newer RFC3339Nano visual fail must outrank same-second older pass, got %v", err)
	}

	passKey := "project." + projectID + ".visual_acceptance.pass"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      passKey,
		Title:       "Passing Visual Acceptance",
		Content:     acceptedVisualPacketForPatchQueueTest(item, branch, "pass"),
		UpdatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("write passing visual packet: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-05-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, passKey); err != nil {
		t.Fatalf("force final passing visual timestamp: %v", err)
	}
	decided, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              "ACCEPTED",
		DecisionDocKey:        passKey,
		DecisionSummary:       "Accepted frontend UI candidate with complete rhizome_visual_acceptance_v1 evidence.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("accept UI patch queue item with visual evidence: %v", err)
	}
	if decided.State != sqlite.ProjectPatchQueueStateAccepted || decided.DecisionDocKey != passKey {
		t.Fatalf("unexpected accepted UI item: %+v", decided)
	}
}

func TestProjectPatchQueueAcceptedVisualProseDoesNotShadowOlderPacket(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patch-queue-visual-prose-shadow"
		projectID   = "project-patch-queue-visual-prose-shadow"
		leadID      = "lead-agent"
		ownerID     = "owner-agent"
		reviewerID  = "reviewer-agent"
		repoID      = "repo-main"
	)
	claimed, branch := createClaimedPatchQueueVisualAcceptanceItemForTest(t, ctx, store, workspaceID, projectID, leadID, ownerID, reviewerID, repoID, "branch-visual-prose-shadow", `{"paths":["web/app.tsx","index.html"]}`)

	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      branch.ReviewDocKey,
		Title:       "Older Passing Visual Acceptance",
		Content:     acceptedVisualPacketForPatchQueueTest(claimed, branch, "pass"),
		UpdatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("write older passing visual packet: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-02-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, branch.ReviewDocKey); err != nil {
		t.Fatalf("force older passing visual packet timestamp: %v", err)
	}
	proseKey := "project." + projectID + ".visual_acceptance.prose_only"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      proseKey,
		Title:       "Newer Visual Acceptance Notes",
		Content: strings.Join([]string{
			"The reviewer discussed visual acceptance and browser smoke.",
			"This is only prose: no packet schema, no screenshot refs, no viewport matrix, and no structured verdict.",
		}, "\n"),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write prose-only visual notes: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-03-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, proseKey); err != nil {
		t.Fatalf("force newer prose visual notes timestamp: %v", err)
	}

	decided, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              "ACCEPTED",
		DecisionDocKey:        proseKey,
		DecisionSummary:       "Accepted with a prose note while a complete older packet remains bound to the branch review doc.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("newer prose-only note should not be treated as packet or shadow older complete visual packet: %v", err)
	}
	if decided.State != sqlite.ProjectPatchQueueStateAccepted || decided.DecisionDocKey != proseKey {
		t.Fatalf("unexpected accepted item after prose doc decision: %+v", decided)
	}
}

func TestProjectPatchQueueAcceptedCoreOnlyStillBlocksExplicitFailVisualPacket(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patch-queue-core-only-fail-visual"
		projectID   = "project-patch-queue-core-only-fail-visual"
		leadID      = "lead-agent"
		ownerID     = "owner-agent"
		reviewerID  = "reviewer-agent"
		repoID      = "repo-main"
	)
	claimed, branch := createClaimedPatchQueueVisualAcceptanceItemForTest(t, ctx, store, workspaceID, projectID, leadID, ownerID, reviewerID, repoID, "branch-core-only-fail-visual", `{"paths":["internal/core/normalize.go","tests/core/normalize_test.go"]}`)
	failKey := "project." + projectID + ".visual_acceptance.fail"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      failKey,
		Title:       "Explicit Failing Visual Acceptance",
		Content:     acceptedVisualPacketForPatchQueueTest(claimed, branch, "fail") + "\nblocking_findings: screenshot shows the public output panel overlaps the export controls\n",
		UpdatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("write explicit failing visual packet: %v", err)
	}

	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              "ACCEPTED",
		DecisionDocKey:        failKey,
		DecisionSummary:       "Accepted core-only non-ui slice; no browser surface should be needed.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "non-pass verdict") {
		t.Fatalf("explicit failing visual packet must block even when decision prose says core-only, got %v", err)
	}

	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              "ACCEPTED",
		DecisionSummary:       "Accepted core-only non-ui slice without linking the known exact visual failure.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "non-pass verdict") {
		t.Fatalf("unlinked exact visual fail packet must block core-only ACCEPT, got %v", err)
	}
}

func TestProjectPatchQueueSubmitRequiresReadyBranchEvidence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID          = "ws-project-patch-queue"
		projectID            = "project-patch-queue"
		leadID               = "lead-agent"
		workerID             = "worker-agent"
		otherID              = "other-agent"
		integratorID         = "integrator-agent"
		reviewerRoleID       = "project-reviewer-agent"
		registeredReviewerID = "registered-reviewer-agent"
		observerID           = "observer-agent"
		repoID               = "repo-main"
		blockedRepoID        = "repo-not-ready"
		reviewKey            = "project.project-patch-queue.branch.branch-ready.review"
	)
	readyBaseSHA := strings.Repeat("a", 40)
	readyHeadSHA := strings.Repeat("b", 40)
	differentHeadSHA := strings.Repeat("c", 40)
	blockedBaseSHA := strings.Repeat("d", 40)
	blockedHeadSHA := strings.Repeat("e", 40)
	replacementHeadSHA := strings.Repeat("f", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, otherID, integratorID, reviewerRoleID})
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     registeredReviewerID,
		OwnerUserID: "developer",
		DisplayName: registeredReviewerID,
		Role:        "reviewer",
	}); err != nil {
		t.Fatalf("register reviewer-role fallback agent: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     observerID,
		OwnerUserID: "developer",
		DisplayName: observerID,
		Role:        "observer",
	}); err != nil {
		t.Fatalf("register observer agent: %v", err)
	}
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
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               otherID,
		RoleType:              sqlite.ProjectRoleIntegrator,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign second integrator role: %v", err)
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               reviewerRoleID,
		RoleType:              sqlite.ProjectRoleReviewer,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign reviewer role: %v", err)
	}
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                blockedRepoID,
		RepoStatus:            sqlite.ProjectRepositoryStatusCreated,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.repository.upsert",
	}); err != nil {
		t.Fatalf("upsert blocked repo: %v", err)
	}
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\patch-queue`)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\npatch queue evidence",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	activeBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              "branch-active",
		BranchName:            "agent/worker-agent/patch-queue-active",
		BaseBranch:            "main",
		HeadSHA:               "abc123-active",
		WriteScopeJSON:        `{"paths":["web/app.js"]}`,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register active branch: %v", err)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              activeBranch.BranchID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "READY_FOR_REVIEW") {
		t.Fatalf("expected non-ready branch to be rejected, got %v", err)
	}

	_, taskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, workerID, "branch-ready", "agent/worker-agent/patch-queue-ready", `{"paths":["web/app.js","api/server.go"]}`)
	readyBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              "branch-ready",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/worker-agent/patch-queue-ready",
		BaseBranch:            "main",
		BaseSHA:               readyBaseSHA,
		HeadSHA:               readyHeadSHA,
		WriteScopeJSON:        `{"paths":["web/app.js","api/server.go"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register ready branch: %v", err)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              readyBranch.BranchID,
		ReviewDocKey:          "different-doc",
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "review_doc_key") {
		t.Fatalf("expected mismatched review_doc_key to be rejected, got %v", err)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              readyBranch.BranchID,
		AutoMerge:             true,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "auto_merge") {
		t.Fatalf("expected auto_merge to be rejected, got %v", err)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              readyBranch.BranchID,
		ActorID:               otherID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", otherID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "belongs to agent") {
		t.Fatalf("expected non-owner agent to be rejected, got %v", err)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              readyBranch.BranchID,
		PathsetJSON:           `{"paths":["api/**"]}`,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "cannot widen") {
		t.Fatalf("expected widened pathset to be rejected, got %v", err)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              readyBranch.BranchID,
		HeadSHA:               differentHeadSHA,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "head_sha") {
		t.Fatalf("expected mismatched head_sha to be rejected, got %v", err)
	}

	blockedCheckout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, blockedRepoID, workerID, `C:\fixtures\agents\worker-agent\patch-queue-blocked-repo`)
	_, blockedTaskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, blockedRepoID, blockedCheckout.CheckoutID, workerID, "branch-blocked-repo", "agent/worker-agent/patch-queue-blocked-repo", `{"paths":["blocked/app.js"]}`)
	blockedBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                blockedRepoID,
		CheckoutID:            blockedCheckout.CheckoutID,
		AgentID:               workerID,
		BranchID:              "branch-blocked-repo",
		ActiveTaskID:          blockedTaskID,
		ActiveClaimID:         blockedTaskID,
		BranchName:            "agent/worker-agent/patch-queue-blocked-repo",
		BaseBranch:            "main",
		BaseSHA:               blockedBaseSHA,
		HeadSHA:               blockedHeadSHA,
		WriteScopeJSON:        `{"paths":["blocked/app.js"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register ready branch for blocked repo: %v", err)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                blockedRepoID,
		BranchID:              blockedBranch.BranchID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "READY with remote_url") {
		t.Fatalf("expected not-ready repo to be rejected, got %v", err)
	}

	item, event, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              readyBranch.BranchID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit ready branch to patch queue: %v", err)
	}
	if event.EventType != "project.patch_queue.submitted" || item.State != sqlite.ProjectPatchQueueStateProposed || item.AutoMerge {
		t.Fatalf("unexpected patch queue item=%+v event=%+v", item, event)
	}
	if item.QueueID == "" || item.ItemID == "" || item.ReviewDocKey != reviewKey || item.HeadSHA != readyHeadSHA || item.BaseSHA != readyBaseSHA {
		t.Fatalf("unexpected patch queue identity/evidence: %+v", item)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              readyBranch.BranchID,
		BranchName:            readyBranch.BranchName,
		BaseBranch:            "main",
		BaseSHA:               readyBaseSHA,
		HeadSHA:               readyHeadSHA,
		WriteScopeJSON:        `{"paths":["web/app.js","api/server.go"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) || !strings.Contains(err.Error(), "leaving READY_FOR_REVIEW") {
		t.Fatalf("expected live patch queue to block leaving READY_FOR_REVIEW, got %v", err)
	}
	if _, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              readyBranch.BranchID,
		BranchName:            readyBranch.BranchName,
		BaseBranch:            "main",
		BaseSHA:               readyBaseSHA,
		HeadSHA:               replacementHeadSHA,
		WriteScopeJSON:        `{"paths":["web/app.js","api/server.go"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	}); !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) || !strings.Contains(err.Error(), "live patch queue item") {
		t.Fatalf("expected live patch queue to block branch evidence drift, got %v", err)
	}
	if len(item.Pathset) != 2 || item.Pathset[0] != "api/server.go" || item.Pathset[1] != "web/app.js" {
		t.Fatalf("expected normalized pathset from branch scope, got %+v", item.Pathset)
	}
	list, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		BranchID:    readyBranch.BranchID,
	})
	if err != nil {
		t.Fatalf("list patch queue: %v", err)
	}
	if len(list) != 1 || list[0].ItemID != item.ItemID {
		t.Fatalf("unexpected patch queue list: %+v", list)
	}
	coordination, err := store.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get coordination: %v", err)
	}
	if len(coordination.PatchQueueItems) != 1 || !strings.Contains(coordination.CoordinationVersion, "|patch_queue:") {
		t.Fatalf("expected patch queue in project coordination, got %+v", coordination)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               "patchq-other",
		ItemID:                "patchitem-other",
		RepoID:                repoID,
		BranchID:              readyBranch.BranchID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "already has live patch queue item") {
		t.Fatalf("expected duplicate live branch queue item to be rejected, got %v", err)
	}
	if _, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.claim",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "INTEGRATOR/REVIEWER") {
		t.Fatalf("expected non-reviewer claim to fail, got %v", err)
	}
	if _, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               observerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", observerID),
		PromptContextSurface:  "project.patch_queue.claim",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "INTEGRATOR/REVIEWER") {
		t.Fatalf("expected unrelated registered role claim to fail, got %v", err)
	}
	registeredReviewerClaim, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               registeredReviewerID,
		ActorType:             "agent",
		LeaseSeconds:          600,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", registeredReviewerID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("registered reviewer role should claim patch queue item: %v", err)
	}
	if registeredReviewerClaim.ClaimedBy != registeredReviewerID || registeredReviewerClaim.ClaimToken == "" {
		t.Fatalf("unexpected registered reviewer claim: %+v", registeredReviewerClaim)
	}
	if _, _, err := store.ReleaseProjectPatchQueueClaimWithEvent(ctx, sqlite.ProjectPatchQueueReleaseInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ClaimToken:            registeredReviewerClaim.ClaimToken,
		ActorID:               registeredReviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.release", "server_rpc", workspaceID, "agent", registeredReviewerID),
		PromptContextSurface:  "project.patch_queue.release",
	}); err != nil {
		t.Fatalf("release registered reviewer claim: %v", err)
	}
	projectReviewerClaim, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               reviewerRoleID,
		ActorType:             "agent",
		LeaseSeconds:          600,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", reviewerRoleID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("project REVIEWER role should claim patch queue item: %v", err)
	}
	if projectReviewerClaim.ClaimedBy != reviewerRoleID || projectReviewerClaim.ClaimToken == "" {
		t.Fatalf("unexpected project reviewer claim: %+v", projectReviewerClaim)
	}
	if _, _, err := store.ReleaseProjectPatchQueueClaimWithEvent(ctx, sqlite.ProjectPatchQueueReleaseInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ClaimToken:            projectReviewerClaim.ClaimToken,
		ActorID:               reviewerRoleID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.release", "server_rpc", workspaceID, "agent", reviewerRoleID),
		PromptContextSurface:  "project.patch_queue.release",
	}); err != nil {
		t.Fatalf("release project reviewer claim: %v", err)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               integratorID,
		ActorType:             "agent",
		LeaseSeconds:          600,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim patch queue item: %v", err)
	}
	if claimed.State != sqlite.ProjectPatchQueueStateClaimed || claimed.ClaimedBy != integratorID || claimed.ClaimToken == "" {
		t.Fatalf("unexpected claimed item: %+v", claimed)
	}
	coordination, err = store.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get coordination after claim: %v", err)
	}
	if len(coordination.PatchQueueItems) != 1 || coordination.PatchQueueItems[0].State != sqlite.ProjectPatchQueueStateClaimed || coordination.PatchQueueItems[0].ClaimedBy != integratorID {
		t.Fatalf("expected claimed patch queue in project coordination, got %+v", coordination.PatchQueueItems)
	}
	if _, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.claim",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "already claimed") {
		t.Fatalf("expected active claim collision, got %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_agent_roles
   SET status = 'RELEASED'
 WHERE workspace_id = ? AND project_id = ? AND agent_id = ? AND role_type = ?`,
		workspaceID, projectID, integratorID, sqlite.ProjectRoleIntegrator); err != nil {
		t.Fatalf("release integrator role for decision gate test: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		Decision:              "ACCEPTED",
		DecisionSummary:       "This stale authority decision should fail.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "INTEGRATOR") {
		t.Fatalf("expected decision after role loss to fail, got %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_agent_roles
   SET status = 'ACTIVE'
 WHERE workspace_id = ? AND project_id = ? AND agent_id = ? AND role_type = ?`,
		workspaceID, projectID, integratorID, sqlite.ProjectRoleIntegrator); err != nil {
		t.Fatalf("restore integrator role: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET claim_expires_at = '2000-01-01T00:00:00Z'
 WHERE queue_id = ? AND item_id = ?`,
		item.QueueID, item.ItemID); err != nil {
		t.Fatalf("expire patch queue claim: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		Decision:              "ACCEPTED",
		DecisionSummary:       "This expired claim decision should fail.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected expired claim decision to fail, got %v", err)
	}
	stolen, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               otherID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", otherID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("reclaim expired patch queue item by second integrator: %v", err)
	}
	if stolen.ClaimedBy != otherID || stolen.ClaimToken == claimed.ClaimToken {
		t.Fatalf("unexpected expired claim takeover: %+v", stolen)
	}
	if _, _, err := store.ReleaseProjectPatchQueueClaimWithEvent(ctx, sqlite.ProjectPatchQueueReleaseInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ClaimToken:            stolen.ClaimToken,
		ActorID:               otherID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.release", "server_rpc", workspaceID, "agent", otherID),
		PromptContextSurface:  "project.patch_queue.release",
	}); err != nil {
		t.Fatalf("release stolen claim: %v", err)
	}
	claimed, _, err = store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("restore original integrator claim: %v", err)
	}
	if _, _, err := store.ReleaseProjectPatchQueueClaimWithEvent(ctx, sqlite.ProjectPatchQueueReleaseInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ClaimToken:            "wrong-token",
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.release", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.release",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "claim_token") {
		t.Fatalf("expected wrong release token to fail, got %v", err)
	}
	released, _, err := store.ReleaseProjectPatchQueueClaimWithEvent(ctx, sqlite.ProjectPatchQueueReleaseInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ClaimToken:            claimed.ClaimToken,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.release", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.release",
	})
	if err != nil {
		t.Fatalf("release patch queue item: %v", err)
	}
	if released.State != sqlite.ProjectPatchQueueStateProposed || released.ClaimedBy != "" || released.ClaimToken != "" {
		t.Fatalf("unexpected released item: %+v", released)
	}
	claimed, _, err = store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("reclaim patch queue item: %v", err)
	}
	decisionKey := "project.project-patch-queue.patchq.branch-ready.decision"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      decisionKey,
		Title:       "Patch Queue Decision",
		Content: "# Patch Queue Decision\n\nAccepted after integration review.\n\n" +
			acceptedVisualPacketForPatchQueueTest(item, readyBranch, "pass"),
		UpdatedBy: integratorID,
	}); err != nil {
		t.Fatalf("write decision doc: %v", err)
	}
	decided, event, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		Decision:              "ACCEPTED",
		DecisionDocKey:        decisionKey,
		DecisionSummary:       "Accepted reviewed branch as the current integration candidate.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("decide patch queue item: %v", err)
	}
	if decided.State != sqlite.ProjectPatchQueueStateAccepted || decided.DecisionDocKey != decisionKey || decided.DecidedBy != integratorID || event.EventType != "project.patch_queue.accepted" {
		t.Fatalf("unexpected decided item/event: %+v %+v", decided, event)
	}
	coordination, err = store.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get coordination after decision: %v", err)
	}
	if len(coordination.PatchQueueItems) != 1 || coordination.PatchQueueItems[0].State != sqlite.ProjectPatchQueueStateAccepted || coordination.PatchQueueItems[0].DecisionSummary == "" {
		t.Fatalf("expected decided patch queue in project coordination, got %+v", coordination.PatchQueueItems)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              readyBranch.BranchID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "already ACCEPTED") {
		t.Fatalf("expected terminal item resubmit to fail, got %v", err)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              readyBranch.BranchID,
		ItemID:                "patchitem-branch-ready-fresh",
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "fresh item_id") {
		t.Fatalf("expected same-head ACCEPTED fresh item submit to fail, got %v", err)
	}
}

func TestProjectPatchQueueSubmitStoresSupersessionEvidenceForBlockedRequeue(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-project-patch-queue-requeue"
		projectID     = "project-patch-queue-requeue"
		leadID        = "lead-agent"
		workerID      = "worker-agent"
		integratorID  = "integrator-agent"
		repoID        = "repo-main"
		branchID      = "branch-ready"
		reviewKey     = "project.project-patch-queue-requeue.branch.branch-ready.review"
		decisionKey   = "project.project-patch-queue-requeue.patch_queue.patchitem-branch-ready.decision"
		rejectKey     = "project.project-patch-queue-requeue.patch_queue.patchitem-branch-ready-requeue.decision"
		staleKey      = "task.task-patchq-validation-project-patch-queue-requeue.stale_evidence"
		validationKey = "task.task-patchq-validation-project-patch-queue-requeue.validation_evidence"
	)
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, integratorID})
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
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\patch-queue-requeue`)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nReady for review.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	taskID := "task-patch-queue-supersede"
	reserved := registerReservedProjectBranchForReadyBindingTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, workerID, branchID, "agent/worker-agent/patch-queue-requeue", `{"paths":["web/app.js"]}`)
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, reserved.BranchID, taskID, `{"paths":["web/app.js"]}`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              branchID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/worker-agent/patch-queue-requeue",
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["web/app.js"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register ready branch: %v", err)
	}
	oldItem, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-patch-queue-supersede",
		RunID:                    "run-patch-queue-supersede",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-patch-queue-supersede",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes: map[string]string{
			"web/app.js": "sha256:web",
		},
		RepoLeaseID:           "lease-patch-queue-supersede",
		LeaseTerm:             7,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit initial item: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      staleKey,
		Title:       "Stale Validation Evidence",
		Content: fmt.Sprintf(`Old validation note.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
browser smoke: passed before blocker`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: workerID,
	}); err != nil {
		t.Fatalf("write stale validation doc: %v", err)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim initial item: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      decisionKey,
		Title:       "Patch Queue Decision",
		Content:     "Blocked pending browser smoke evidence.",
		UpdatedBy:   integratorID,
	}); err != nil {
		t.Fatalf("write decision doc: %v", err)
	}
	blocked, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionDocKey:        decisionKey,
		DecisionSummary:       "Missing browser smoke evidence.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("block initial item: %v", err)
	}
	if blocked.State != sqlite.ProjectPatchQueueStateBlocked {
		t.Fatalf("expected blocked item, got %+v", blocked)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID + "-missing-evidence",
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "same-head BLOCKED") {
		t.Fatalf("expected same-head blocked requeue without evidence to fail, got %v", err)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID + "-stale-evidence",
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		SupersedesQueueID:     oldItem.QueueID,
		SupersedesItemID:      oldItem.ItemID,
		EvidenceDocKey:        staleKey,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "must be newer") {
		t.Fatalf("expected stale same-head evidence to fail, got %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      validationKey,
		Title:       "Validation Evidence",
		Content: fmt.Sprintf(`Validation passed.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
browser smoke: passed`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: workerID,
	}); err != nil {
		t.Fatalf("write validation doc: %v", err)
	}
	requeue, event, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID + "-requeue",
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		SupersedesQueueID:     oldItem.QueueID,
		SupersedesItemID:      oldItem.ItemID,
		EvidenceDocKey:        validationKey,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit evidence-bound requeue: %v", err)
	}
	if requeue.State != sqlite.ProjectPatchQueueStateProposed ||
		requeue.SupersedesQueueID != oldItem.QueueID ||
		requeue.SupersedesItemID != oldItem.ItemID ||
		requeue.EvidenceDocKey != validationKey {
		t.Fatalf("unexpected requeue item: %+v", requeue)
	}
	payload := mustEventPayloadMapForPatchQueueTest(t, event)
	if payload["supersedes_queue_id"] != oldItem.QueueID ||
		payload["supersedes_item_id"] != oldItem.ItemID ||
		payload["evidence_doc_key"] != validationKey {
		t.Fatalf("event lost supersession provenance: %+v", payload)
	}
	items, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		BranchID:    branch.BranchID,
	})
	if err != nil {
		t.Fatalf("list branch queue items: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected old blocked plus fresh proposed item, got %+v", items)
	}
	requeueClaimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               requeue.QueueID,
		ItemID:                requeue.ItemID,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim evidence-bound requeue: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      rejectKey,
		Title:       "Patch Queue Rejection",
		Content:     "Rejected after review; requires a new commit.",
		UpdatedBy:   integratorID,
	}); err != nil {
		t.Fatalf("write reject doc: %v", err)
	}
	rejected, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               requeue.QueueID,
		ItemID:                requeue.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateRejected,
		DecisionDocKey:        rejectKey,
		DecisionSummary:       "Requires a new commit.",
		ClaimToken:            requeueClaimed.ClaimToken,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("reject evidence-bound requeue: %v", err)
	}
	if rejected.State != sqlite.ProjectPatchQueueStateRejected {
		t.Fatalf("expected rejected requeue, got %+v", rejected)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID + "-after-reject",
		RepoID:                repoID,
		BranchID:              branch.BranchID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "already REJECTED") {
		t.Fatalf("expected same-head rejected requeue to fail, got %v", err)
	}
}

func TestProjectPatchQueueSupersedeAllowsReviewerWithoutBranchOwnership(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID   = "ws-project-patch-queue-supersede"
		projectID     = "project-patch-queue-supersede"
		leadID        = "lead-agent"
		workerID      = "worker-agent"
		reviewerID    = "reviewer-agent"
		outsiderID    = "outsider-agent"
		repoID        = "repo-main"
		branchID      = "branch-ready"
		reviewKey     = "project.project-patch-queue-supersede.branch.branch-ready.review"
		decisionKey   = "project.project-patch-queue-supersede.patch_queue.patchitem-branch-ready.decision"
		validationKey = "task.task-patchq-validation-project-patch-queue-supersede.validation_evidence"
	)
	baseSHA := strings.Repeat("c", 40)
	headSHA := strings.Repeat("d", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, reviewerID, outsiderID})
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
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\patch-queue-supersede`)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nReady for review.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	taskID := "task-patch-queue-supersede-drift"
	reserved := registerReservedProjectBranchForReadyBindingTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, workerID, branchID, "agent/worker-agent/patch-queue-supersede", `{"paths":["web/app.js"]}`)
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, reserved.BranchID, taskID, `{"paths":["web/app.js"]}`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              branchID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/worker-agent/patch-queue-supersede",
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["web/app.js"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register ready branch: %v", err)
	}
	oldItem, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-patch-queue-supersede-drift",
		RunID:                    "run-patch-queue-supersede-drift",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-patch-queue-supersede-drift",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes: map[string]string{
			"web/app.js": "sha256:web",
		},
		RepoLeaseID:           "lease-patch-queue-supersede-drift",
		LeaseTerm:             7,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit initial item: %v", err)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim initial item: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      decisionKey,
		Title:       "Patch Queue Decision",
		Content:     "Blocked pending browser smoke evidence.",
		UpdatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("write decision doc: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionDocKey:        decisionKey,
		DecisionSummary:       "Missing browser smoke evidence.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("block initial item: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      validationKey,
		Title:       "Validation Evidence",
		Content: fmt.Sprintf(`The previous blocker claiming missing fresh browser-smoke evidence is stale.
Browser-smoke provenance records a PASS for the exact same branch/head and closes the validation gap.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
browser-smoke user scenario result: PASS`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: outsiderID,
	}); err != nil {
		t.Fatalf("write validation doc: %v", err)
	}
	blockingReviewKey := "task.task-review-project-patch-queue-supersede.blocked_result"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      blockingReviewKey,
		Title:       "Task Result - Review patch queue candidate",
		Content: fmt.Sprintf(`BLOCKED: fresh same-head browser-smoke evidence is still missing.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
not ready for acceptance`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write blocking review doc: %v", err)
	}
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             oldItem.ItemID + "-review-loop",
		EvidenceDocKey:        blockingReviewKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "requires positive same-head validation evidence") {
		t.Fatalf("expected blocking review result to fail supersede evidence validation, got %v", err)
	}
	failedProvenanceKey := "task.task-review-project-patch-queue-supersede.failed_browser_smoke"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      failedProvenanceKey,
		Title:       "Browser Smoke Provenance",
		Content: fmt.Sprintf(`Browser-smoke provenance shows failure for this same-head candidate.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
browser smoke failed and did not pass`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write failed provenance doc: %v", err)
	}
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             oldItem.ItemID + "-failed-provenance",
		EvidenceDocKey:        failedProvenanceKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "requires positive same-head validation evidence") {
		t.Fatalf("expected failed browser-smoke provenance to fail supersede evidence validation, got %v", err)
	}
	agentResponseKey := "task.task-review-project-patch-queue-supersede.agent_response.areq-test"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      agentResponseKey,
		Title:       "Agent response - reviewer to integrator",
		Content: fmt.Sprintf(`# Agent Request Response Evidence

evidence_scope: public workspace coordination

This message coordinates handoff and repeats candidate selectors, but it is not browser validation output.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
browser-smoke evidence: PASS`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write coordination response evidence doc: %v", err)
	}
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             oldItem.ItemID + "-coordination-response",
		EvidenceDocKey:        agentResponseKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "coordination response evidence") {
		t.Fatalf("expected coordination response evidence to fail supersede validation, got %v", err)
	}
	suffixAgentRuntimeResponseKey := "task.task-review-project-patch-queue-supersede.agent_response"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      suffixAgentRuntimeResponseKey,
		Title:       "Agent response",
		Content: fmt.Sprintf(`queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
browser-smoke evidence: PASS`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write suffix-only coordination response evidence doc: %v", err)
	}
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             oldItem.ItemID + "-coordination-response-suffix",
		EvidenceDocKey:        suffixAgentRuntimeResponseKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "coordination response evidence") {
		t.Fatalf("expected suffix-only coordination response evidence to fail supersede validation, got %v", err)
	}
	agentStateKey := "agent.zeta.claimed_work"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      agentStateKey,
		Title:       "Claimed Work Ledger",
		Content: fmt.Sprintf(`# Claimed Work Ledger

active_claimed_work: none
last_summary: repeated the queue selectors and browser smoke result, but this is private agent state, not validation evidence.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
browser-smoke evidence: PASS`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write agent state doc: %v", err)
	}
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             oldItem.ItemID + "-agent-state",
		EvidenceDocKey:        agentStateKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "agent state evidence") {
		t.Fatalf("expected agent state evidence to fail supersede validation, got %v", err)
	}
	reflectionKey := "project." + projectID + ".reflection_board"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reflectionKey,
		Title:       "Project Reflection Board",
		Content: fmt.Sprintf(`Reflection board update.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
browser-smoke evidence: PASS
validation gap closed`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write reflection board evidence doc: %v", err)
	}
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             oldItem.ItemID + "-reflection-board",
		EvidenceDocKey:        reflectionKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "reflection/heartbeat evidence") {
		t.Fatalf("expected reflection board evidence to fail supersede validation, got %v", err)
	}
	heartbeatKey := "project." + projectID + ".heartbeat.theta.2099-01-01T00-00-00Z"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      heartbeatKey,
		Title:       "Theta Heartbeat",
		Content: fmt.Sprintf(`Agent heartbeat observation.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
browser-smoke evidence: PASS
validation gap closed`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write heartbeat evidence doc: %v", err)
	}
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             oldItem.ItemID + "-heartbeat",
		EvidenceDocKey:        heartbeatKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "reflection/heartbeat evidence") {
		t.Fatalf("expected heartbeat evidence to fail supersede validation, got %v", err)
	}
	blockedVisualKey := "task.task-review-project-patch-queue-supersede.blocked_visual_acceptance"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      blockedVisualKey,
		Title:       "Visual Acceptance Attempt",
		Content: fmt.Sprintf(`{"schema":"rhizome_visual_acceptance_v1","status":"complete","visual_verdict":"block","queue_id":"%s","item_id":"%s","branch_id":"%s","head_sha":"%s"}

The previous blocker is stale and screenshot capture result: pass, but the visual verdict remains blocked.`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write blocked visual doc: %v", err)
	}
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             oldItem.ItemID + "-blocked-visual",
		EvidenceDocKey:        blockedVisualKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "describes missing/blocking validation evidence") {
		t.Fatalf("expected blocked visual evidence to fail supersede validation, got %v", err)
	}
	nonPassVisualKey := "task.task-review-project-patch-queue-supersede.non_pass_visual_acceptance"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      nonPassVisualKey,
		Title:       "Provisional Visual Acceptance",
		Content: fmt.Sprintf(`schema: rhizome_visual_acceptance_v1
status: provisional_non_pass
visual_verdict: under_evidenced
acceptance_status: not_accepted
pass_for_acceptance: false

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
state_evidence:
  initial_state: screenshot_path C:/tmp/initial.png
  primary_flow: screenshot_path C:/tmp/flow.png
  result_state: screenshot_path C:/tmp/result.png
viewport_matrix: desktop 1440x900 and mobile narrow 390x844
product_intent: primary user path exercised
checks: overlap checked; clipping checked; contrast/readability checked; responsive typography hierarchy spacing usability checked
screenshot_provenance: browser screenshots captured
browser smoke: passed
tests passed
required_next_packet: visual_verdict: pass`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write non-pass visual doc: %v", err)
	}
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             oldItem.ItemID + "-non-pass-visual",
		EvidenceDocKey:        nonPassVisualKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "describes missing/blocking validation evidence") {
		t.Fatalf("expected non-pass visual evidence to fail supersede validation, got %v", err)
	}
	taskBriefKey := "task.task-review-project-patch-queue-supersede.visual_review_task"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      taskBriefKey,
		Title:       "Task Brief - Review visual acceptance on exact owned HEAD",
		Content: fmt.Sprintf(`# Task Brief - Review visual acceptance on exact owned HEAD

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s

Required output: publish evidence with visual_verdict: pass or blocking findings.
This canonical task document was created by task_submit at task.task-review-project-patch-queue-supersede.visual_review_task.`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write task brief doc: %v", err)
	}
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             oldItem.ItemID + "-task-brief",
		EvidenceDocKey:        taskBriefKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "task brief") {
		t.Fatalf("expected task brief evidence to fail supersede validation, got %v", err)
	}
	skippedBrowserKey := "task.task-review-project-patch-queue-supersede.skipped_browser_smoke"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      skippedBrowserKey,
		Title:       "Validation Attempt",
		Content: fmt.Sprintf(`tests passed, but browser smoke not run and visual check not exercised.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write skipped browser smoke doc: %v", err)
	}
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             oldItem.ItemID + "-skipped-browser",
		EvidenceDocKey:        skippedBrowserKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "describes missing/blocking validation evidence") {
		t.Fatalf("expected skipped browser smoke evidence to fail supersede validation, got %v", err)
	}
	pendingVisualKey := "task.task-review-project-patch-queue-supersede.pending_visual_acceptance"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      pendingVisualKey,
		Title:       "Implementation Evidence",
		Content: fmt.Sprintf(`build_check: passed
test_result: passed
tests passed, but visual acceptance evidence is still pending for this same-head candidate.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
visual acceptance pending: primary_flow and result_state are not evidenced`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write pending visual evidence doc: %v", err)
	}
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             oldItem.ItemID + "-pending-visual",
		EvidenceDocKey:        pendingVisualKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "describes missing/blocking validation evidence") {
		t.Fatalf("expected pending visual acceptance evidence to fail supersede validation, got %v", err)
	}
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             oldItem.ItemID + "-supersede-outsider",
		EvidenceDocKey:        validationKey,
		ActorID:               outsiderID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", outsiderID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "requires active INTEGRATOR/REVIEWER") {
		t.Fatalf("expected outsider supersede to fail reviewer authority, got %v", err)
	}
	staleEnvelope := sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID)
	staleEnvelope["item_id"] = oldItem.ItemID
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             oldItem.ItemID + "-stale-envelope",
		EvidenceDocKey:        validationKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: staleEnvelope,
		PromptContextSurface:  "project.patch_queue.supersede",
	}); err == nil || !strings.Contains(err.Error(), `prompt_context_envelope has item_id="`+oldItem.ItemID+`"`) {
		t.Fatalf("expected stale old-item prompt envelope to fail closed, got %v", err)
	}
	freshItemID := oldItem.ItemID + "-supersede"
	requeue, event, alreadyQueued, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             freshItemID,
		EvidenceDocKey:        validationKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	})
	if err != nil {
		t.Fatalf("reviewer supersede: %v", err)
	}
	if alreadyQueued || requeue.ItemID != freshItemID || requeue.SubmittedBy != reviewerID || requeue.SupersedesItemID != oldItem.ItemID || requeue.EvidenceDocKey != validationKey {
		t.Fatalf("unexpected reviewer supersede item already=%v item=%+v", alreadyQueued, requeue)
	}
	payload := mustEventPayloadMapForPatchQueueTest(t, event)
	if payload["mutation_operation"] != "patch_queue.supersede" {
		t.Fatalf("event lost supersede operation: %+v", payload)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("event lost prompt context envelope: %+v", payload)
	}
	for key, want := range map[string]string{
		"item_id":             freshItemID,
		"new_item_id":         freshItemID,
		"supersedes_queue_id": oldItem.QueueID,
		"supersedes_item_id":  oldItem.ItemID,
		"evidence_doc_key":    validationKey,
	} {
		if got, ok := envelope[key].(string); !ok || got != want {
			t.Fatalf("prompt_context_envelope[%s] = %v, want %q in %+v", key, envelope[key], want, envelope)
		}
	}
	requeueReviewTaskID := sqlite.ProjectPatchQueueReviewTaskID(requeue)
	listed, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
	})
	if err != nil {
		t.Fatalf("list patch queue items before receipt deletion: %v", err)
	}
	requeueReviewEventID := ""
	for _, item := range listed {
		if item.QueueID == requeue.QueueID && item.ItemID == requeue.ItemID {
			requeueReviewEventID = item.ReviewTaskEventID
			break
		}
	}
	if requeueReviewEventID == "" {
		t.Fatalf("expected requeue review task event before deletion, items=%+v", listed)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `DELETE FROM runtime_event_firehose_outbox WHERE workspace_id = ? AND event_id = ?`, workspaceID, requeueReviewEventID); err != nil {
		t.Fatalf("delete requeue review task outbox row: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `DELETE FROM runtime_events WHERE event_id = ?`, requeueReviewEventID); err != nil {
		t.Fatalf("delete requeue review task runtime event: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `DELETE FROM workspace_tasks WHERE workspace_id = ? AND task_id = ?`, workspaceID, requeueReviewTaskID); err != nil {
		t.Fatalf("detach requeue review task: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `DELETE FROM tasks WHERE task_id = ?`, requeueReviewTaskID); err != nil {
		t.Fatalf("delete requeue review task: %v", err)
	}
	existing, _, alreadyQueued, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             freshItemID,
		EvidenceDocKey:        validationKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	})
	if err != nil {
		t.Fatalf("idempotent reviewer supersede: %v", err)
	}
	if !alreadyQueued || existing.ItemID != requeue.ItemID {
		t.Fatalf("expected idempotent existing supersede, already=%v existing=%+v", alreadyQueued, existing)
	}
	status, err := store.GetTaskStatus(ctx, workspaceID, requeueReviewTaskID)
	if err != nil {
		t.Fatalf("get repaired requeue review task: %v", err)
	}
	if status.TaskID != requeueReviewTaskID || status.ProjectLane != "review" {
		t.Fatalf("expected idempotent requeue to repair review task %s, got %+v", requeueReviewTaskID, status)
	}
	listed, err = store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
	})
	if err != nil {
		t.Fatalf("list patch queue items after receipt repair: %v", err)
	}
	for _, item := range listed {
		if item.QueueID == requeue.QueueID && item.ItemID == requeue.ItemID {
			if item.MissingReviewTask || item.ReviewTaskID != requeueReviewTaskID || item.ReviewTaskEventID == "" || item.ReviewTaskEventID == requeueReviewEventID {
				t.Fatalf("expected idempotent supersede to repair review receipt, got %+v old_event_id=%s", item, requeueReviewEventID)
			}
			return
		}
	}
	t.Fatalf("requeue item %s/%s not found after receipt repair, items=%+v", requeue.QueueID, requeue.ItemID, listed)
}

func TestProjectPatchQueueSupersedeRejectsReusedEvidenceAfterBlockedSuccessor(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-patch-queue-supersede-reused-evidence"
		projectID   = "project-patch-queue-supersede-reused-evidence"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		reviewerID  = "reviewer-agent"
		repoID      = "repo-main"
		branchID    = "branch-reused-evidence"
	)
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

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, workerID, reviewerID, `{"paths":["web/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	evidenceDocKey := "task.task-reused-evidence.validation_evidence"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceDocKey,
		Title:       "Validation Evidence",
		Content: fmt.Sprintf(`Validation passed.
browser smoke: passed

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s`, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write validation evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, evidenceDocKey); err != nil {
		t.Fatalf("force evidence timestamp: %v", err)
	}

	successor, _, alreadyQueued, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		NewItemID:             item.ItemID + "-successor",
		EvidenceDocKey:        evidenceDocKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	})
	if err != nil {
		t.Fatalf("create first supersede successor: %v", err)
	}
	if alreadyQueued || successor.SupersedesItemID != item.ItemID || successor.EvidenceDocKey != evidenceDocKey {
		t.Fatalf("unexpected successor already=%v item=%+v", alreadyQueued, successor)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               successor.QueueID,
		ItemID:                successor.ItemID,
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
		DecisionSummary:       "The same evidence basis did not clear the queue decision.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("block successor: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE project_patch_queue_items SET decided_at = '2100-01-01T00:00:00Z', updated_at = '2100-01-01T00:00:00Z' WHERE workspace_id = ? AND queue_id = ? AND item_id = ?`, workspaceID, claimed.QueueID, claimed.ItemID); err != nil {
		t.Fatalf("force successor decision timestamp: %v", err)
	}

	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               successor.QueueID,
		ItemID:                successor.ItemID,
		NewItemID:             successor.ItemID + "-same-evidence",
		EvidenceDocKey:        evidenceDocKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "already consumed") {
		t.Fatalf("expected reused evidence to fail after blocked successor, got %v", err)
	}
	copiedEvidenceDocKey := "task.task-reused-evidence.copied_validation"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      copiedEvidenceDocKey,
		Title:       "Copied Validation Evidence",
		Content: fmt.Sprintf(`Validation passed.
browser smoke: passed

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s`, successor.QueueID, successor.ItemID, successor.BranchID, successor.HeadSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write copied validation evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2100-01-01T00:00:01Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, copiedEvidenceDocKey); err != nil {
		t.Fatalf("force copied evidence timestamp: %v", err)
	}
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               successor.QueueID,
		ItemID:                successor.ItemID,
		NewItemID:             successor.ItemID + "-copied-evidence",
		EvidenceDocKey:        copiedEvidenceDocKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "repeats evidence") {
		t.Fatalf("expected copied evidence basis to fail after blocked successor, got %v", err)
	}

	freshEvidenceDocKey := "task.task-reused-evidence.fresh_validation"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      freshEvidenceDocKey,
		Title:       "Fresh Validation Evidence",
		Content: fmt.Sprintf(`Validation passed.
browser smoke: passed
validation_run_id: run-fresh-after-blocked-successor
observed_at: 2100-01-02T00:00:00Z

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s`, successor.QueueID, successor.ItemID, successor.BranchID, successor.HeadSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write fresh validation evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2100-01-02T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, freshEvidenceDocKey); err != nil {
		t.Fatalf("force fresh evidence timestamp: %v", err)
	}
	requeue, _, alreadyQueued, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               successor.QueueID,
		ItemID:                successor.ItemID,
		NewItemID:             successor.ItemID + "-fresh-evidence",
		EvidenceDocKey:        freshEvidenceDocKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	})
	if err != nil {
		t.Fatalf("fresh evidence should allow superseding latest blocked successor: %v", err)
	}
	if alreadyQueued || requeue.SupersedesItemID != successor.ItemID || requeue.EvidenceDocKey != freshEvidenceDocKey {
		t.Fatalf("unexpected fresh-evidence requeue already=%v item=%+v", alreadyQueued, requeue)
	}
}

func TestProjectPatchQueueSupersedeAcceptsBackendBuildTestEvidence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-patch-queue-supersede-backend-evidence"
		projectID   = "project-patch-queue-supersede-backend-evidence"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		reviewerID  = "reviewer-agent"
		repoID      = "repo-main"
		branchID    = "branch-backend-evidence"
	)
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

	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, workerID, reviewerID, `{"paths":["internal/eval/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	evidenceDocKey := "task.task-backend-evidence.validation"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceDocKey,
		Title:       "Backend Validation Evidence",
		Content: fmt.Sprintf(`schema: rhizome_validation_evidence_v1
validation_verdict: pass
command: go test ./...
exit_code: 0
tests passed
source_fidelity_status: passed

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s`, item.QueueID, item.ItemID, item.BranchID, item.HeadSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write backend validation evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, evidenceDocKey); err != nil {
		t.Fatalf("force evidence timestamp: %v", err)
	}

	successor, _, alreadyQueued, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		NewItemID:             item.ItemID + "-backend-evidence",
		EvidenceDocKey:        evidenceDocKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	})
	if err != nil {
		t.Fatalf("backend evidence should supersede blocked patch queue item: %v", err)
	}
	if alreadyQueued || successor.State != sqlite.ProjectPatchQueueStateProposed || successor.SupersedesItemID != item.ItemID || successor.EvidenceDocKey != evidenceDocKey {
		t.Fatalf("unexpected backend supersede result already=%v item=%+v", alreadyQueued, successor)
	}
}

func TestProjectPatchQueueSupersedeAcceptsVisualEvidenceByBranchNameAndHead(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-patch-queue-supersede-visual"
		projectID   = "project-patch-queue-supersede-visual"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		reviewerID  = "reviewer-agent"
		repoID      = "repo-main"
		branchID    = "branch-visual-pass"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, reviewerID})
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
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	item := createAgentWorkDecidedPatchQueueItem(t, ctx, store, workspaceID, projectID, repoID, branchID, workerID, reviewerID, `{"paths":["web/**"]}`, sqlite.ProjectPatchQueueStateBlocked)
	evidenceDocKey := "task.task-visual-pass.visual_acceptance"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      evidenceDocKey,
		Title:       "Visual Acceptance Evidence",
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
screenshot_provenance: browser screenshot passed real user scenario checks`, item.QueueID, item.ItemID, workerID, branchID, item.HeadSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write visual evidence doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE workspace_docs SET updated_at = '2099-02-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, evidenceDocKey); err != nil {
		t.Fatalf("force visual evidence timestamp: %v", err)
	}
	requeue, _, alreadyQueued, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		NewItemID:             item.ItemID + "-visual-pass",
		EvidenceDocKey:        evidenceDocKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	})
	if err != nil {
		t.Fatalf("visual evidence supersede: %v", err)
	}
	if alreadyQueued || requeue.EvidenceDocKey != evidenceDocKey || requeue.SupersedesItemID != item.ItemID {
		t.Fatalf("unexpected visual supersede result already=%v item=%+v", alreadyQueued, requeue)
	}
}

func TestProjectPatchQueueSupersedeRejectsDriftedReviewEvidence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID    = "ws-project-patch-queue-supersede-drift"
		projectID      = "project-patch-queue-supersede-drift"
		leadID         = "lead-agent"
		workerID       = "worker-agent"
		reviewerID     = "reviewer-agent"
		repoID         = "repo-main"
		branchID       = "branch-ready"
		reviewKey      = "project.project-patch-queue-supersede-drift.branch.branch-ready.review"
		driftReviewKey = "project.project-patch-queue-supersede-drift.branch.branch-ready.review.drifted"
		decisionKey    = "project.project-patch-queue-supersede-drift.patch_queue.patchitem-branch-ready.decision"
		validationKey  = "task.task-patchq-validation-project-patch-queue-supersede-drift.validation_evidence"
	)
	baseSHA := strings.Repeat("e", 40)
	headSHA := strings.Repeat("f", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, reviewerID})
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
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\patch-queue-supersede-drift`)
	for _, doc := range []struct {
		key   string
		title string
		body  string
	}{
		{reviewKey, "Branch Review Packet", "# Branch Review Packet\n\nReady for review."},
		{driftReviewKey, "Drifted Branch Review Packet", "# Drifted Branch Review Packet\n\nNewer branch packet."},
	} {
		if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
			WorkspaceID: workspaceID,
			DocKey:      doc.key,
			Title:       doc.title,
			Content:     doc.body,
			UpdatedBy:   workerID,
		}); err != nil {
			t.Fatalf("write %s: %v", doc.key, err)
		}
	}
	taskID := "task-patch-queue-supersede-visual"
	reserved := registerReservedProjectBranchForReadyBindingTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, workerID, branchID, "agent/worker-agent/patch-queue-supersede-drift", `{"paths":["web/app.js"]}`)
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, reserved.BranchID, taskID, `{"paths":["web/app.js"]}`)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              branchID,
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/worker-agent/patch-queue-supersede-drift",
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["web/app.js"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register ready branch: %v", err)
	}
	oldItem, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-patch-queue-supersede-visual",
		RunID:                    "run-patch-queue-supersede-visual",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-patch-queue-supersede-visual",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes: map[string]string{
			"web/app.js": "sha256:web",
		},
		RepoLeaseID:           "lease-patch-queue-supersede-visual",
		LeaseTerm:             7,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit initial item: %v", err)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim initial item: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      decisionKey,
		Title:       "Patch Queue Decision",
		Content:     "Blocked pending browser smoke evidence.",
		UpdatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("write decision doc: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateBlocked,
		DecisionDocKey:        decisionKey,
		DecisionSummary:       "Missing browser smoke evidence.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("block initial item: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      validationKey,
		Title:       "Validation Evidence",
		Content: fmt.Sprintf(`Validation passed.

queue_id: %s
item_id: %s
branch_id: %s
head_sha: %s
browser smoke: passed`, oldItem.QueueID, oldItem.ItemID, branch.BranchID, headSHA),
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write validation doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_branch_registry
   SET review_doc_key = ?, updated_at = ?
 WHERE workspace_id = ? AND project_id = ? AND branch_id = ?`,
		driftReviewKey, time.Now().UTC().Format(time.RFC3339Nano), workspaceID, projectID, branchID); err != nil {
		t.Fatalf("drift branch review evidence: %v", err)
	}
	newItemID := oldItem.ItemID + "-supersede"
	if _, _, _, err := store.SupersedeProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSupersedeInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               oldItem.QueueID,
		ItemID:                oldItem.ItemID,
		NewItemID:             newItemID,
		EvidenceDocKey:        validationKey,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.supersede", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.supersede",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "review_doc_key drifted") {
		t.Fatalf("expected supersede to reject drifted review evidence, got %v", err)
	}
	items, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
	})
	if err != nil {
		t.Fatalf("list patch queue items: %v", err)
	}
	for _, item := range items {
		if item.ItemID == newItemID {
			t.Fatalf("supersede created live item despite evidence drift: %+v", item)
		}
	}
}

func TestProjectPatchQueueSubmitControlledQueueBindsRootReadmePath(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-patch-queue-root-readme"
		projectID   = "project-patch-queue-root-readme"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		reviewKey   = "project.project-patch-queue-root-readme.branch.branch-ready.review"
	)
	baseSHA := strings.Repeat("c", 40)
	headSHA := strings.Repeat("d", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\patch-queue-root-readme`)
	registerIntegrationCheckoutForPatchQueueTest(t, ctx, store, workspaceID, projectID, repoID, leadID, `C:\fixtures\agents\integration\patch-queue-root-readme`, "main", baseSHA, baseSHA, "")
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Root README Branch Review Packet",
		Content:     "# Root README Branch Review Packet\n\nsource fidelity evidence",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	taskID := "task-root-readme"
	reserved := registerReservedProjectBranchForReadyBindingTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, workerID, "branch-ready-root-readme", "agent/worker-agent/patch-queue-root-readme", `{"paths":["cmd/rq/**","README.md"]}`)
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, reserved.BranchID, taskID, `{"paths":["cmd/rq/**","README.md"]}`)
	readyBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              "branch-ready-root-readme",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/worker-agent/patch-queue-root-readme",
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["cmd/rq/**","README.md"]}`,
		ReviewDocKey:          reviewKey,
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
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 readyBranch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-root-readme",
		RunID:                    "run-root-readme",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-root-readme",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             "sha256:base-tree-root-readme",
		BaseFileHashes: map[string]string{
			"README.md": "sha256:base-readme",
		},
		RepoLeaseID: "lease-root-readme",
		LeaseTerm:   11,
		ActorID:     workerID,
		ActorType:   "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope(
			"project.patch_queue.submit",
			"server_rpc",
			workspaceID,
			"agent",
			workerID,
		),
		PromptContextSurface: "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit controlled queue item with root README binding: %v", err)
	}
	expectedDigest, err := repoauthority.Context{
		Mode:        repoauthority.ModeControlledQueue,
		WorkspaceID: workspaceID,
		TaskID:      "task-root-readme",
		SessionID:   "session-root-readme",
		RunID:       "run-root-readme",
		AgentID:     workerID,
		Principal: repoauthority.PrincipalRef{
			Type: "agent",
			ID:   workerID,
		},
		CapabilitySnapshot: repoauthority.CapabilitySnapshotRef{
			ID:     "cap-root-readme",
			Schema: "daemon_capability_snapshot.v1",
		},
		RepoRoot: checkout.LocalPath,
		Base: repoauthority.BaseIdentity{
			Ref:      "main",
			TreeHash: "sha256:base-tree-root-readme",
			FileHashes: map[string]string{
				"README.md": "sha256:base-readme",
			},
		},
		Pathset: item.Pathset,
		Lease: repoauthority.LeaseRef{
			ID:   "lease-root-readme",
			Term: 11,
		},
		PatchQueue: repoauthority.PatchQueueRef{
			QueueID: item.QueueID,
			ItemID:  item.ItemID,
		},
	}.Digest()
	if err != nil {
		t.Fatalf("compute expected binding digest: %v", err)
	}
	if item.ContextDigest != expectedDigest ||
		item.BaseFileHashes["README.md"] != "sha256:base-readme" ||
		len(item.Pathset) != 2 ||
		item.Pathset[0] != "README.md" ||
		item.Pathset[1] != "cmd/rq/**" {
		t.Fatalf("root README binding did not normalize and round-trip: %+v", item)
	}
}

func TestProjectPatchQueueSubmitAcceptsControlledAuthorityModeWithoutAutoMerge(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-patch-queue-controlled"
		projectID   = "project-patch-queue-controlled"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		reviewKey   = "project.project-patch-queue-controlled.branch.branch-ready.review"
	)
	controlledBaseSHA := strings.Repeat("a", 40)
	controlledHeadSHA := strings.Repeat("b", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\patch-queue-controlled`)
	registerIntegrationCheckoutForPatchQueueTest(t, ctx, store, workspaceID, projectID, repoID, leadID, `C:\fixtures\agents\integration\patch-queue-controlled-stale`, "main", controlledBaseSHA, controlledBaseSHA, "2000-01-01T00:00:00Z")
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Controlled Branch Review Packet",
		Content:     "# Controlled Branch Review Packet\n\ncontrolled queue evidence",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	taskID := "task-controlled"
	reserved := registerReservedProjectBranchForReadyBindingTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, workerID, "branch-ready-controlled", "agent/worker-agent/patch-queue-controlled", `{"paths":["cmd/app.go","web/app.js"]}`)
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, reserved.BranchID, taskID, `{"paths":["cmd/app.go","web/app.js"]}`)
	readyBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              "branch-ready-controlled",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/worker-agent/patch-queue-controlled",
		BaseBranch:            "main",
		BaseSHA:               controlledBaseSHA,
		HeadSHA:               controlledHeadSHA,
		WriteScopeJSON:        `{"paths":["cmd/app.go","web/app.js"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register ready branch: %v", err)
	}

	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              readyBranch.BranchID,
		RepoAuthorityMode:     "direct_merge",
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "unsupported repo_authority_mode") {
		t.Fatalf("expected unsupported authority mode to be rejected, got %v", err)
	}

	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              readyBranch.BranchID,
		RepoAuthorityMode:     sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "requires complete binding refs") {
		t.Fatalf("expected controlled authority mode without binding refs to be rejected, got %v", err)
	}
	list, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		BranchID:    readyBranch.BranchID,
	})
	if err != nil {
		t.Fatalf("list controlled patch queue item: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("controlled authority mode without binding refs created an item: %+v", list)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              readyBranch.BranchID,
		RepoAuthorityMode:     sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                taskID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "binding context is incomplete") {
		t.Fatalf("expected partial binding refs to be rejected, got %v", err)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              readyBranch.BranchID,
		RepoAuthorityMode:     sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		OperationID:           "op-self-asserted",
		OperationKind:         "repo_patch_apply",
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "operation refs cannot be submitted") {
		t.Fatalf("expected self-asserted operation refs to be rejected, got %v", err)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 readyBranch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   "task-controlled",
		SessionID:                "session-controlled",
		RunID:                    "run-controlled",
		AgentID:                  workerID,
		PrincipalType:            "agent",
		PrincipalID:              "different-agent",
		CapabilitySnapshotID:     "cap-controlled",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             controlledBaseSHA,
		BaseFileHashes: map[string]string{
			"cmd/app.go": "sha256:cmd",
			"web/app.js": "sha256:web",
		},
		RepoLeaseID:           "lease-controlled",
		LeaseTerm:             7,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "principal_id must match") {
		t.Fatalf("expected controlled queue principal mismatch to be rejected, got %v", err)
	}
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 readyBranch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   "task-controlled",
		SessionID:                "session-controlled",
		RunID:                    "run-controlled",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-controlled",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             controlledBaseSHA,
		BaseFileHashes: map[string]string{
			"cmd/app.go": "sha256:cmd",
			"web/app.js": "sha256:web",
		},
		RepoLeaseID: "lease-controlled",
		LeaseTerm:   7,
		ActorID:     workerID,
		ActorType:   "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope(
			"project.patch_queue.submit",
			"server_rpc",
			workspaceID,
			"agent",
			workerID,
		),
		PromptContextSurface: "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit controlled authority mode item with binding refs: %v", err)
	}
	expectedDigest, err := repoauthority.Context{
		Mode:        repoauthority.ModeControlledQueue,
		WorkspaceID: workspaceID,
		TaskID:      "task-controlled",
		SessionID:   "session-controlled",
		RunID:       "run-controlled",
		AgentID:     workerID,
		Principal: repoauthority.PrincipalRef{
			Type: "agent",
			ID:   workerID,
		},
		CapabilitySnapshot: repoauthority.CapabilitySnapshotRef{
			ID:     "cap-controlled",
			Schema: "daemon_capability_snapshot.v1",
		},
		RepoRoot: checkout.LocalPath,
		Base: repoauthority.BaseIdentity{
			Ref:      "main",
			TreeHash: controlledBaseSHA,
			FileHashes: map[string]string{
				"cmd/app.go": "sha256:cmd",
				"web/app.js": "sha256:web",
			},
		},
		Pathset: []string{"cmd/app.go", "web/app.js"},
		Lease: repoauthority.LeaseRef{
			ID:   "lease-controlled",
			Term: 7,
		},
		PatchQueue: repoauthority.PatchQueueRef{
			QueueID: item.QueueID,
			ItemID:  item.ItemID,
		},
	}.Digest()
	if err != nil {
		t.Fatalf("compute expected binding digest: %v", err)
	}
	if item.ContextDigest != expectedDigest ||
		item.TaskID != "task-controlled" ||
		item.SessionID != "session-controlled" ||
		item.RunID != "run-controlled" ||
		item.AgentID != workerID ||
		item.PrincipalType != "agent" ||
		item.PrincipalID != workerID ||
		item.RepoLeaseID != "lease-controlled" ||
		item.LeaseTerm != 7 ||
		item.Attempt != 1 ||
		item.MaxAttempts != 1 ||
		item.BaseFileHashes["cmd/app.go"] != "sha256:cmd" ||
		item.BaseFileHashes["web/app.js"] != "sha256:web" {
		t.Fatalf("controlled binding refs did not normalize and round-trip: %+v", item)
	}
	candidate, ok, err := store.FirstProjectRepoMutationActivationCandidate(ctx)
	if err != nil {
		t.Fatalf("select repo mutation activation candidate: %v", err)
	}
	if !ok {
		t.Fatalf("expected controlled patch queue item to be selected as mutation activation candidate")
	}
	if candidate.QueueItem.QueueID != item.QueueID || candidate.QueueItem.ItemID != item.ItemID {
		t.Fatalf("unexpected selected queue item: %+v", candidate.QueueItem)
	}
	if candidate.QueueItem.ContextDigest != expectedDigest || candidate.QueueItem.RepoLeaseID != "lease-controlled" || candidate.QueueItem.TaskID != "task-controlled" {
		t.Fatalf("selected mutation candidate lost binding refs: %+v", candidate.QueueItem)
	}
	if candidate.QueueItem.Attempt != 1 || candidate.QueueItem.MaxAttempts != 1 {
		t.Fatalf("selected mutation candidate lost retry bounds: %+v", candidate.QueueItem)
	}
	if candidate.TargetCheckout.CheckoutID != "" {
		t.Fatalf("stale integration checkout should not be selected as target: %+v", candidate.TargetCheckout)
	}
	freshIntegration := registerIntegrationCheckoutForPatchQueueTest(t, ctx, store, workspaceID, projectID, repoID, leadID, `C:\fixtures\agents\integration\patch-queue-controlled`, "main", controlledBaseSHA, controlledBaseSHA, "")
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate with fresh integration target: %v", err)
	} else if !ok {
		t.Fatalf("expected controlled patch queue item to remain selectable with fresh integration target")
	}
	if candidate.TargetCheckout.CheckoutID != freshIntegration.CheckoutID {
		t.Fatalf("expected fresh integration checkout target %s, got %+v", freshIntegration.CheckoutID, candidate.TargetCheckout)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET attempt = 2, max_attempts = 1
 WHERE queue_id = ? AND item_id = ?`,
		item.QueueID, item.ItemID); err != nil {
		t.Fatalf("corrupt controlled candidate retry bounds: %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after retry bound drift: %v", err)
	} else if ok {
		t.Fatalf("invalid retry bounds should not be selected as activation candidate: %+v", candidate)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET attempt = 1, max_attempts = 1
 WHERE queue_id = ? AND item_id = ?`,
		item.QueueID, item.ItemID); err != nil {
		t.Fatalf("repair controlled candidate retry bounds: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET operation_id = 'op-drifted', operation_kind = 'repo_patch_apply'
 WHERE queue_id = ? AND item_id = ?`,
		item.QueueID, item.ItemID); err != nil {
		t.Fatalf("corrupt controlled candidate operation refs: %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after operation ref drift: %v", err)
	} else if ok {
		t.Fatalf("live self-asserted operation refs should not be selected as activation candidate: %+v", candidate)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET operation_id = '', operation_kind = ''
 WHERE queue_id = ? AND item_id = ?`,
		item.QueueID, item.ItemID); err != nil {
		t.Fatalf("repair controlled candidate operation refs: %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after repairing operation ref drift: %v", err)
	} else if !ok {
		t.Fatalf("expected controlled patch queue item to be selected after operation refs were cleared")
	}
	if candidate.Branch.BranchID != readyBranch.BranchID || candidate.Checkout.CheckoutID != checkout.CheckoutID {
		t.Fatalf("expected candidate to include branch and checkout evidence, got %+v", candidate)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          600,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim controlled patch queue item for operation binding: %v", err)
	}
	if _, _, err := store.BindProjectPatchQueueMutationOperationWithEvent(ctx, sqlite.ProjectPatchQueueOperationBindInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		OperationID:           "op-controlled-apply",
		OperationKind:         "repo_patch_apply",
		MutationPathsJSON:     `{"paths":["cmd/app.go"]}`,
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.operation_bind", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.operation_bind",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "must exactly match") {
		t.Fatalf("expected narrowed operation mutation paths to fail, got %v", err)
	}
	if _, _, err := store.BindProjectPatchQueueMutationOperationWithEvent(ctx, sqlite.ProjectPatchQueueOperationBindInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		OperationID:           "op-controlled-apply",
		OperationKind:         "repo_patch_apply",
		ClaimToken:            "wrong-token",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.operation_bind", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.operation_bind",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "claim_token") {
		t.Fatalf("expected wrong claim token operation bind to fail, got %v", err)
	}
	if _, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		RunID:       "op-wrong-ledger-kind",
		WorkspaceID: workspaceID,
		AgentID:     leadID,
		Title:       "Wrong repo patch operation ledger",
		Status:      "ACTIVE",
		Outcome:     "RUNNING",
		Verification: map[string]any{
			"operation_ledger": map[string]any{
				"schema":         sqlite.ProjectPatchQueueOperationLedgerSchema,
				"operation_id":   "op-wrong-ledger-kind",
				"operation_kind": "tool_call",
				"status":         "running",
				"terminal":       false,
			},
		},
		PromptContextEnvelope: sqlite.BuildExecutionPromptContextEnvelope("project.patch_queue.operation_bind", "server_operation_ledger", workspaceID, "agent", leadID),
	}); err != nil {
		t.Fatalf("seed wrong operation ledger kind: %v", err)
	}
	if _, _, err := store.BindProjectPatchQueueMutationOperationWithEvent(ctx, sqlite.ProjectPatchQueueOperationBindInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		OperationID:           "op-wrong-ledger-kind",
		OperationKind:         sqlite.ProjectPatchQueueOperationKindRepoPatchApply,
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.operation_bind", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.operation_bind",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "operation_kind") {
		t.Fatalf("expected wrong operation ledger kind to fail, got %v", err)
	}
	bound, _, err := store.BindProjectPatchQueueMutationOperationWithEvent(ctx, sqlite.ProjectPatchQueueOperationBindInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.operation_bind", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.operation_bind",
	})
	if err != nil {
		t.Fatalf("bind controlled operation evidence: %v", err)
	}
	operationContext := repoauthority.Context{
		Mode:        repoauthority.ModeControlledQueue,
		WorkspaceID: workspaceID,
		TaskID:      "task-controlled",
		SessionID:   "session-controlled",
		RunID:       "run-controlled",
		AgentID:     workerID,
		Principal: repoauthority.PrincipalRef{
			Type: "agent",
			ID:   workerID,
		},
		CapabilitySnapshot: repoauthority.CapabilitySnapshotRef{
			ID:     "cap-controlled",
			Schema: "daemon_capability_snapshot.v1",
		},
		RepoRoot: checkout.LocalPath,
		Base: repoauthority.BaseIdentity{
			Ref:      "main",
			TreeHash: controlledBaseSHA,
			FileHashes: map[string]string{
				"cmd/app.go": "sha256:cmd",
				"web/app.js": "sha256:web",
			},
		},
		Pathset: []string{"cmd/app.go", "web/app.js"},
		Lease: repoauthority.LeaseRef{
			ID:   "lease-controlled",
			Term: 7,
		},
		PatchQueue: repoauthority.PatchQueueRef{
			QueueID: item.QueueID,
			ItemID:  item.ItemID,
		},
		Operation: repoauthority.OperationRef{ID: bound.OperationID, Kind: bound.OperationKind},
	}
	expectedOperationDigest, err := operationContext.Digest()
	if err != nil {
		t.Fatalf("compute expected operation digest: %v", err)
	}
	leaseContext := operationContext
	leaseContext.Lease = repoauthority.LeaseRef{}
	leaseContext.PatchQueue = repoauthority.PatchQueueRef{}
	leaseContext.Operation = repoauthority.OperationRef{}
	expectedLeaseDigest, err := leaseContext.Digest()
	if err != nil {
		t.Fatalf("compute expected operation lease digest: %v", err)
	}
	if bound.OperationID == "" ||
		bound.OperationKind != sqlite.ProjectPatchQueueOperationKindRepoPatchApply ||
		bound.OperationBindingSchema != sqlite.ProjectPatchQueueOperationBindingSchema ||
		!bound.OperationBindingAccepted ||
		bound.OperationContextDigest != expectedOperationDigest ||
		bound.OperationLeaseContextDigest != expectedLeaseDigest ||
		bound.OperationBoundBy != leadID ||
		bound.OperationBoundAt == "" ||
		!sqlite.ProjectPatchQueueOperationBindingReady(bound) {
		t.Fatalf("operation binding evidence did not normalize and round-trip: %+v", bound)
	}
	ledgerRun, err := store.GetExecutionRun(ctx, workspaceID, bound.OperationID)
	if err != nil {
		t.Fatalf("load operation ledger run: %v", err)
	}
	ledger, ok := ledgerRun.Run.VerificationJSON["operation_ledger"].(map[string]any)
	if !ok {
		t.Fatalf("operation binding must create durable operation_ledger evidence, got %+v", ledgerRun.Run.VerificationJSON)
	}
	if ledger["schema"] != sqlite.ProjectPatchQueueOperationLedgerSchema ||
		ledger["operation_id"] != bound.OperationID ||
		ledger["operation_kind"] != bound.OperationKind ||
		ledger["status"] != "running" ||
		ledger["terminal"] != false {
		t.Fatalf("unexpected repo patch operation ledger evidence: %+v", ledger)
	}
	ledgerBinding, ok := ledger["binding"].(map[string]any)
	if !ok ||
		ledgerBinding["queue_id"] != item.QueueID ||
		ledgerBinding["item_id"] != item.ItemID ||
		ledgerBinding["task_id"] != "task-controlled" ||
		ledgerBinding["session_id"] != "session-controlled" ||
		ledgerBinding["parent_run_id"] != "run-controlled" ||
		ledgerBinding["agent_id"] != workerID ||
		ledgerBinding["principal_id"] != workerID ||
		ledgerBinding["recorded_by_id"] != leadID {
		t.Fatalf("operation ledger binding does not reflect patch queue context: %+v", ledgerBinding)
	}
	originalLedgerVerificationJSON, err := json.Marshal(ledgerRun.Run.VerificationJSON)
	if err != nil {
		t.Fatalf("marshal original operation ledger verification: %v", err)
	}
	restoreOperationLedger := func(label string) {
		t.Helper()
		var verification map[string]any
		if err := json.Unmarshal(originalLedgerVerificationJSON, &verification); err != nil {
			t.Fatalf("%s: decode original operation ledger verification: %v", label, err)
		}
		if _, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
			RunID:        bound.OperationID,
			WorkspaceID:  workspaceID,
			AgentID:      leadID,
			Title:        ledgerRun.Run.Title,
			Summary:      ledgerRun.Run.Summary,
			Status:       "ACTIVE",
			Outcome:      "RUNNING",
			Verification: verification,
		}); err != nil {
			t.Fatalf("%s: restore operation ledger run: %v", label, err)
		}
	}
	expectNoActivationCandidate := func(label string) {
		t.Helper()
		candidate, ok, err := store.FirstProjectRepoMutationActivationCandidate(ctx)
		if err != nil {
			t.Fatalf("%s: select activation candidate: %v", label, err)
		}
		if ok {
			t.Fatalf("%s: expected operation ledger corruption to block activation candidate, got %+v", label, candidate.QueueItem)
		}
	}
	tamperedOperationLedgerVerificationJSON := func(label string, mutate func(map[string]any)) string {
		t.Helper()
		var verification map[string]any
		if err := json.Unmarshal(originalLedgerVerificationJSON, &verification); err != nil {
			t.Fatalf("%s: decode operation ledger verification: %v", label, err)
		}
		ledger, ok := verification["operation_ledger"].(map[string]any)
		if !ok {
			t.Fatalf("%s: operation ledger verification is missing operation_ledger: %+v", label, verification)
		}
		mutate(ledger)
		encoded, err := json.Marshal(verification)
		if err != nil {
			t.Fatalf("%s: encode tampered operation ledger verification: %v", label, err)
		}
		return string(encoded)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `DELETE FROM execution_runs WHERE workspace_id = ? AND run_id = ?`, workspaceID, bound.OperationID); err != nil {
		t.Fatalf("delete operation ledger run: %v", err)
	}
	expectNoActivationCandidate("deleted operation ledger")
	restoreOperationLedger("restore deleted operation ledger")
	badRecordedByJSON := tamperedOperationLedgerVerificationJSON("recorded_by_id mismatch", func(ledger map[string]any) {
		binding, ok := ledger["binding"].(map[string]any)
		if !ok {
			t.Fatalf("recorded_by_id mismatch: operation ledger missing binding: %+v", ledger)
		}
		binding["recorded_by_id"] = "other-agent"
	})
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE execution_runs SET verification_json = ? WHERE workspace_id = ? AND run_id = ?`, badRecordedByJSON, workspaceID, bound.OperationID); err != nil {
		t.Fatalf("corrupt operation ledger recorded_by_id: %v", err)
	}
	expectNoActivationCandidate("mismatched operation ledger actor")
	restoreOperationLedger("restore actor operation ledger")
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{
			name: "missing terminal bool",
			mutate: func(ledger map[string]any) {
				delete(ledger, "terminal")
			},
		},
		{
			name: "string terminal bool",
			mutate: func(ledger map[string]any) {
				ledger["terminal"] = "false"
			},
		},
		{
			name: "missing canonical mutation bool",
			mutate: func(ledger map[string]any) {
				fence, ok := ledger["fence"].(map[string]any)
				if !ok {
					t.Fatalf("missing canonical mutation bool: operation ledger missing fence: %+v", ledger)
				}
				delete(fence, "canonical_mutation_allowed")
			},
		},
		{
			name: "string canonical mutation bool",
			mutate: func(ledger map[string]any) {
				fence, ok := ledger["fence"].(map[string]any)
				if !ok {
					t.Fatalf("string canonical mutation bool: operation ledger missing fence: %+v", ledger)
				}
				fence["canonical_mutation_allowed"] = "false"
			},
		},
	} {
		tamperedJSON := tamperedOperationLedgerVerificationJSON(tc.name, tc.mutate)
		if _, err := store.WriteDB().ExecContext(ctx, `UPDATE execution_runs SET verification_json = ? WHERE workspace_id = ? AND run_id = ?`, tamperedJSON, workspaceID, bound.OperationID); err != nil {
			t.Fatalf("%s: corrupt operation ledger bool field: %v", tc.name, err)
		}
		expectNoActivationCandidate(tc.name)
		restoreOperationLedger("restore " + tc.name)
	}
	closedAt := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE execution_runs SET status = 'ACTIVE', outcome = 'COMPLETED', closed_at = ? WHERE workspace_id = ? AND run_id = ?`, closedAt, workspaceID, bound.OperationID); err != nil {
		t.Fatalf("terminalize operation ledger outcome: %v", err)
	}
	expectNoActivationCandidate("terminal operation ledger outcome")
	restoreOperationLedger("restore terminal operation ledger")
	if _, _, err := store.ReleaseProjectPatchQueueClaimWithEvent(ctx, sqlite.ProjectPatchQueueReleaseInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.release", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.release",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "operation binding evidence") {
		t.Fatalf("expected operation-bound item release to fail, got %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after operation bind: %v", err)
	} else if !ok || candidate.QueueItem.OperationID != bound.OperationID || !sqlite.ProjectPatchQueueOperationBindingReady(candidate.QueueItem) {
		t.Fatalf("expected verified operation-bound candidate, got ok=%v candidate=%+v", ok, candidate.QueueItem)
	}
	conflictCAS := repoauthority.EvaluateCASPatchApply(repoauthority.CASPatchApplyInput{
		Context: operationContext,
		CurrentFileHashes: map[string]string{
			"cmd/app.go": "sha256:drifted-cmd",
			"web/app.js": "sha256:web",
		},
		CandidateFileHashes: map[string]string{
			"cmd/app.go": "sha256:new-cmd",
			"web/app.js": "sha256:new-web",
		},
	})
	if _, err := store.WriteDB().ExecContext(ctx, `UPDATE execution_runs SET status = 'ACTIVE', outcome = 'COMPLETED', closed_at = ? WHERE workspace_id = ? AND run_id = ?`, closedAt, workspaceID, bound.OperationID); err != nil {
		t.Fatalf("terminalize operation ledger before CAS record: %v", err)
	}
	if _, _, err := store.RecordProjectPatchQueueCASEvidenceWithEvent(ctx, sqlite.ProjectPatchQueueCASRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		CASResult:             conflictCAS,
		TestEvidence:          repoauthority.PatchQueueTestEvidence{Schema: repoauthority.PatchQueueTestEvidenceSchemaVersion, Name: "unit", Command: "go test ./...", Status: repoauthority.PatchQueueTestStatusPassed, ExitCode: 0, OutputDigest: "sha256:" + strings.Repeat("1", 64)},
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.cas_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.cas_record",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "operation ledger") {
		t.Fatalf("expected CAS record to fail closed on terminal operation ledger, got %v", err)
	}
	restoreOperationLedger("restore terminal operation ledger before CAS record")
	if _, _, err := store.RecordProjectPatchQueueCASEvidenceWithEvent(ctx, sqlite.ProjectPatchQueueCASRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		CASResult:             conflictCAS,
		TestEvidence:          repoauthority.PatchQueueTestEvidence{Schema: repoauthority.PatchQueueTestEvidenceSchemaVersion, Name: "unit", Command: "go test ./...", Status: repoauthority.PatchQueueTestStatusPassed, ExitCode: 0, OutputDigest: "sha256:" + strings.Repeat("1", 64)},
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.cas_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.cas_record",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "merge admission") {
		t.Fatalf("expected conflict CAS evidence to fail merge admission, got %v", err)
	}
	cmdCandidateContent := "package main\n\nfunc main() {}\n"
	webCandidateContent := "console.log('new-web');\n"
	cmdCandidateHash := repoauthority.PatchMaterializationContentDigest(cmdCandidateContent)
	webCandidateHash := repoauthority.PatchMaterializationContentDigest(webCandidateContent)
	appliedCAS := repoauthority.EvaluateCASPatchApply(repoauthority.CASPatchApplyInput{
		Context: operationContext,
		CurrentFileHashes: map[string]string{
			"cmd/app.go": "sha256:cmd",
			"web/app.js": "sha256:web",
		},
		CandidateFileHashes: map[string]string{
			"cmd/app.go": cmdCandidateHash,
			"web/app.js": webCandidateHash,
		},
	})
	if _, _, err := store.RecordProjectPatchQueueCASEvidenceWithEvent(ctx, sqlite.ProjectPatchQueueCASRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		CASResult:             appliedCAS,
		TestEvidence:          repoauthority.PatchQueueTestEvidence{Schema: repoauthority.PatchQueueTestEvidenceSchemaVersion, Name: "unit", Command: "go test ./...", Status: repoauthority.PatchQueueTestStatusPassed, ExitCode: 0, OutputDigest: "sha256:" + strings.Repeat("2", 64)},
		ClaimToken:            "wrong-token",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.cas_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.cas_record",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "claim_token") {
		t.Fatalf("expected wrong claim token CAS record to fail, got %v", err)
	}
	casRecorded, _, err := store.RecordProjectPatchQueueCASEvidenceWithEvent(ctx, sqlite.ProjectPatchQueueCASRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		CASResult:             appliedCAS,
		TestEvidence:          repoauthority.PatchQueueTestEvidence{Schema: repoauthority.PatchQueueTestEvidenceSchemaVersion, Name: "unit", Command: "go test ./...", Status: repoauthority.PatchQueueTestStatusPassed, ExitCode: 0, OutputDigest: "sha256:" + strings.Repeat("2", 64)},
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.cas_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.cas_record",
	})
	if err != nil {
		t.Fatalf("record CAS evidence: %v", err)
	}
	if casRecorded.CASEvidenceSchema != sqlite.ProjectPatchQueueCASEvidenceSchema ||
		!casRecorded.CASEvidenceAccepted ||
		casRecorded.CASStatus != repoauthority.CASPatchStatusApplied ||
		casRecorded.CASPatchDigest != appliedCAS.PatchDigest ||
		casRecorded.CASEvaluationDigest != repoauthority.PatchQueueCASEvaluationDigest(appliedCAS) ||
		casRecorded.CASTestEvidenceDigest == "" ||
		casRecorded.CASRecordedBy != leadID ||
		casRecorded.CASRecordedAt == "" ||
		!sqlite.ProjectPatchQueueCASEvidenceReady(casRecorded) {
		t.Fatalf("CAS evidence did not normalize and round-trip: %+v", casRecorded)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after CAS evidence: %v", err)
	} else if !ok || !sqlite.ProjectPatchQueueCASEvidenceReady(candidate.QueueItem) {
		t.Fatalf("expected verified CAS evidence candidate, got ok=%v candidate=%+v", ok, candidate.QueueItem)
	}
	if _, _, err := store.RecordProjectPatchQueueMaterializationWithEvent(ctx, sqlite.ProjectPatchQueueMaterializationRecordInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ClaimToken:  "wrong-token",
		ActorID:     leadID,
		ActorType:   "agent",
		Materialization: repoauthority.PatchMaterialization{
			Files: []repoauthority.PatchMaterializedFile{
				{Path: "cmd/app.go", Content: cmdCandidateContent},
				{Path: "web/app.js", Content: webCandidateContent},
			},
		},
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.materialization_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.materialization_record",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "claim_token") {
		t.Fatalf("expected wrong claim token materialization record to fail, got %v", err)
	}
	if _, _, err := store.RecordProjectPatchQueueMaterializationWithEvent(ctx, sqlite.ProjectPatchQueueMaterializationRecordInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ClaimToken:  claimed.ClaimToken,
		ActorID:     leadID,
		ActorType:   "agent",
		Materialization: repoauthority.PatchMaterialization{
			Files: []repoauthority.PatchMaterializedFile{
				{Path: "cmd/app.go", Content: cmdCandidateContent},
			},
		},
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.materialization_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.materialization_record",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "file count") {
		t.Fatalf("expected incomplete materialization to fail exact pathset validation, got %v", err)
	}
	oversizedFileContent := strings.Repeat("x", int(sqlite.ProjectPatchQueueMaterializationMaxFileBytes)+1)
	if _, _, err := store.RecordProjectPatchQueueMaterializationWithEvent(ctx, sqlite.ProjectPatchQueueMaterializationRecordInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ClaimToken:  claimed.ClaimToken,
		ActorID:     leadID,
		ActorType:   "agent",
		Materialization: repoauthority.PatchMaterialization{
			Files: []repoauthority.PatchMaterializedFile{
				{Path: "cmd/app.go", Content: oversizedFileContent},
			},
		},
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.materialization_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.materialization_record",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "materialization storage policy") || !strings.Contains(err.Error(), "file") {
		t.Fatalf("expected oversized file materialization to fail storage policy, got %v", err)
	}
	tooManyFiles := make([]repoauthority.PatchMaterializedFile, 0, sqlite.ProjectPatchQueueMaterializationMaxFiles+1)
	for i := 0; i <= sqlite.ProjectPatchQueueMaterializationMaxFiles; i++ {
		tooManyFiles = append(tooManyFiles, repoauthority.PatchMaterializedFile{Path: fmt.Sprintf("bulk/%03d.txt", i), Content: "x"})
	}
	if _, _, err := store.RecordProjectPatchQueueMaterializationWithEvent(ctx, sqlite.ProjectPatchQueueMaterializationRecordInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ClaimToken:  claimed.ClaimToken,
		ActorID:     leadID,
		ActorType:   "agent",
		Materialization: repoauthority.PatchMaterialization{
			Files: tooManyFiles,
		},
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.materialization_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.materialization_record",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "materialization storage policy") || !strings.Contains(err.Error(), "file count") {
		t.Fatalf("expected excessive file count materialization to fail storage policy, got %v", err)
	}
	totalLimitFiles := make([]repoauthority.PatchMaterializedFile, 0, 5)
	totalLimitChunk := strings.Repeat("y", int(sqlite.ProjectPatchQueueMaterializationMaxFileBytes)-1024)
	for i := 0; i < 5; i++ {
		totalLimitFiles = append(totalLimitFiles, repoauthority.PatchMaterializedFile{Path: fmt.Sprintf("total/%03d.txt", i), Content: totalLimitChunk})
	}
	if _, _, err := store.RecordProjectPatchQueueMaterializationWithEvent(ctx, sqlite.ProjectPatchQueueMaterializationRecordInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ClaimToken:  claimed.ClaimToken,
		ActorID:     leadID,
		ActorType:   "agent",
		Materialization: repoauthority.PatchMaterialization{
			Files: totalLimitFiles,
		},
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.materialization_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.materialization_record",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "materialization storage policy") || !strings.Contains(err.Error(), "total size") {
		t.Fatalf("expected excessive total materialization to fail storage policy, got %v", err)
	}
	afterRejectedMaterializations, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		BranchID:    readyBranch.BranchID,
	})
	if err != nil {
		t.Fatalf("list patch queue after rejected materializations: %v", err)
	}
	if len(afterRejectedMaterializations) != 1 {
		t.Fatalf("expected one patch queue item after rejected materializations, got %d", len(afterRejectedMaterializations))
	}
	if afterRejectedMaterializations[0].MaterializationAccepted ||
		strings.TrimSpace(afterRejectedMaterializations[0].MaterializationDigest) != "" ||
		strings.TrimSpace(afterRejectedMaterializations[0].MaterializationJSON) != "{}" {
		t.Fatalf("rejected materialization left partial durable state: %+v", afterRejectedMaterializations[0])
	}
	materializationEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.patch_queue.materialization_recorded",
		EntityType:  "project_patch_queue_item",
		EntityID:    item.QueueID + "/" + item.ItemID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list materialization runtime events after rejected materializations: %v", err)
	}
	if len(materializationEvents) != 0 {
		t.Fatalf("rejected materialization emitted materialization runtime events: %+v", materializationEvents)
	}
	materialized, materializedEvent, err := store.RecordProjectPatchQueueMaterializationWithEvent(ctx, sqlite.ProjectPatchQueueMaterializationRecordInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ClaimToken:  claimed.ClaimToken,
		ActorID:     leadID,
		ActorType:   "agent",
		Materialization: repoauthority.PatchMaterialization{
			Files: []repoauthority.PatchMaterializedFile{
				{Path: "cmd/app.go", Content: cmdCandidateContent},
				{Path: "web/app.js", Content: webCandidateContent},
			},
		},
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.materialization_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.materialization_record",
	})
	if err != nil {
		t.Fatalf("record materialization: %v", err)
	}
	if materializedEvent.EventType != "project.patch_queue.materialization_recorded" ||
		materializedEvent.EntityType != "project_patch_queue_item" ||
		materializedEvent.EntityID != item.QueueID+"/"+item.ItemID ||
		materializedEvent.ActorID != leadID {
		t.Fatalf("unexpected materialization runtime event: %+v", materializedEvent)
	}
	if strings.Contains(materializedEvent.PayloadJSON, cmdCandidateContent) ||
		strings.Contains(materializedEvent.PayloadJSON, "package main\\n\\nfunc main() {}\\n") ||
		strings.Contains(materializedEvent.PayloadJSON, webCandidateContent) ||
		strings.Contains(materializedEvent.PayloadJSON, "console.log('new-web');\\n") {
		t.Fatalf("materialization runtime event must not duplicate raw candidate content: %s", materializedEvent.PayloadJSON)
	}
	if materialized.MaterializationSchema != sqlite.ProjectPatchQueueMaterializationSchema ||
		!materialized.MaterializationAccepted ||
		materialized.MaterializationDigest != repoauthority.PatchMaterializationDigest(materialized.Materialization) ||
		materialized.MaterializationRecordedBy != leadID ||
		materialized.MaterializationRecordedAt == "" ||
		materialized.MaterializationAuthorityProofDigest != repoauthority.PatchMaterializationAuthorityProofDigest(materialized.MaterializationAuthorityProof) ||
		materialized.MaterializationAuthorityProof.MaterializationDigest != materialized.MaterializationDigest ||
		len(materialized.Materialization.Files) != 2 ||
		materialized.Materialization.Files[0].Content != cmdCandidateContent ||
		materialized.Materialization.Files[1].Content != webCandidateContent ||
		!sqlite.ProjectPatchQueueMaterializationReady(materialized) {
		t.Fatalf("materialization did not normalize and round-trip: %+v", materialized)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after materialization: %v", err)
	} else if !ok || !sqlite.ProjectPatchQueueMaterializationReady(candidate.QueueItem) || candidate.QueueItem.MaterializationDigest != materialized.MaterializationDigest {
		t.Fatalf("expected verified materialization candidate, got ok=%v candidate=%+v", ok, candidate.QueueItem)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET materialization_json = ?
 WHERE queue_id = ? AND item_id = ?`,
		"{}", item.QueueID, item.ItemID); err != nil {
		t.Fatalf("corrupt controlled candidate materialization json: %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after materialization json drift: %v", err)
	} else if ok {
		t.Fatalf("materialization json drift should not be selected as activation candidate: %+v", candidate.QueueItem)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET materialization_json = ?
 WHERE queue_id = ? AND item_id = ?`,
		materialized.MaterializationJSON, item.QueueID, item.ItemID); err != nil {
		t.Fatalf("repair controlled candidate materialization json: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET materialization_digest = ?
 WHERE queue_id = ? AND item_id = ?`,
		"sha256:"+strings.Repeat("7", 64), item.QueueID, item.ItemID); err != nil {
		t.Fatalf("corrupt controlled candidate materialization digest: %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after materialization digest drift: %v", err)
	} else if ok {
		t.Fatalf("materialization digest drift should not be selected as activation candidate: %+v", candidate.QueueItem)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET materialization_digest = ?
 WHERE queue_id = ? AND item_id = ?`,
		materialized.MaterializationDigest, item.QueueID, item.ItemID); err != nil {
		t.Fatalf("repair controlled candidate materialization digest: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET materialization_authority_proof_digest = ?
 WHERE queue_id = ? AND item_id = ?`,
		"sha256:"+strings.Repeat("8", 64), item.QueueID, item.ItemID); err != nil {
		t.Fatalf("corrupt controlled candidate materialization authority proof digest: %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after authority proof digest drift: %v", err)
	} else if ok {
		t.Fatalf("materialization authority proof digest drift should not be selected as activation candidate: %+v", candidate.QueueItem)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET materialization_authority_proof_digest = ?
 WHERE queue_id = ? AND item_id = ?`,
		materialized.MaterializationAuthorityProofDigest, item.QueueID, item.ItemID); err != nil {
		t.Fatalf("repair controlled candidate materialization authority proof digest: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET materialization_authority_proof_json = '{}',
       materialization_authority_proof_digest = ''
 WHERE queue_id = ? AND item_id = ?`,
		item.QueueID, item.ItemID); err != nil {
		t.Fatalf("clear controlled candidate materialization authority proof: %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after authority proof clear: %v", err)
	} else if ok {
		t.Fatalf("missing materialization authority proof should not be selected as activation candidate: %+v", candidate.QueueItem)
	}
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("backfill materialization authority proof through ApplyMigrations: %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after authority proof backfill: %v", err)
	} else if !ok || !sqlite.ProjectPatchQueueMaterializationReady(candidate.QueueItem) || candidate.QueueItem.MaterializationAuthorityProofDigest != materialized.MaterializationAuthorityProofDigest {
		t.Fatalf("expected materialization authority proof backfill to restore candidate, got ok=%v candidate=%+v", ok, candidate.QueueItem)
	}
	rollbackItem := repoauthority.PatchQueueItem{
		Schema:              repoauthority.PatchQueueItemSchemaVersion,
		ID:                  item.QueueID + "/" + item.ItemID,
		QueueID:             item.QueueID,
		ItemID:              item.ItemID,
		State:               repoauthority.PatchQueueStateApplied,
		Attempt:             1,
		MaxAttempts:         1,
		ContextDigest:       bound.ContextDigest,
		RepoLeaseID:         bound.RepoLeaseID,
		LeaseTerm:           bound.LeaseTerm,
		Pathset:             []string{"cmd/app.go", "web/app.js"},
		CASResult:           appliedCAS,
		CASPatchDigest:      appliedCAS.PatchDigest,
		CASEvaluationDigest: repoauthority.PatchQueueCASEvaluationDigest(appliedCAS),
		TestEvidence:        repoauthority.PatchQueueTestEvidence{Schema: repoauthority.PatchQueueTestEvidenceSchemaVersion, Name: "unit", Command: "go test ./...", Status: repoauthority.PatchQueueTestStatusPassed, ExitCode: 0, OutputDigest: "sha256:" + strings.Repeat("2", 64)},
		TestEvidenceDigest:  repoauthority.PatchQueueTestEvidenceDigest(repoauthority.PatchQueueTestEvidence{Schema: repoauthority.PatchQueueTestEvidenceSchemaVersion, Name: "unit", Command: "go test ./...", Status: repoauthority.PatchQueueTestStatusPassed, ExitCode: 0, OutputDigest: "sha256:" + strings.Repeat("2", 64)}),
		OperationID:         bound.OperationID,
		OperationKind:       bound.OperationKind,
	}
	rollbackEvidence, err := repoauthority.NormalizePatchQueueRollbackEvidence(repoauthority.PatchQueueRollback{
		Reason:                     "prove rollback before mutation activation",
		SourcePatchDigest:          appliedCAS.PatchDigest,
		VerificationCommand:        "go test ./...",
		VerificationStatus:         repoauthority.PatchQueueTestStatusPassed,
		VerificationExitCode:       0,
		VerificationOutputDigest:   "sha256:" + strings.Repeat("4", 64),
		VerificationOutputSummary:  "ok",
		VerificationDurationMillis: 25,
		RollbackPaths: []repoauthority.PatchQueueRollbackPath{
			{Path: "cmd/app.go", SourceBaseHash: "sha256:cmd", SourceAppliedHash: cmdCandidateHash, RollbackCandidateHash: "sha256:cmd"},
			{Path: "web/app.js", SourceBaseHash: "sha256:web", SourceAppliedHash: webCandidateHash, RollbackCandidateHash: "sha256:web"},
		},
	}, rollbackItem, repoauthority.OperationRef{ID: "op-controlled-rollback", Kind: "repo_patch_apply"}, time.Date(2026, 4, 26, 0, 0, 2, 0, time.UTC))
	if err != nil {
		t.Fatalf("normalize rollback evidence: %v", err)
	}
	if _, _, err := store.RecordProjectPatchQueueRollbackEvidenceWithEvent(ctx, sqlite.ProjectPatchQueueRollbackRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		RollbackEvidence:      rollbackEvidence,
		ClaimToken:            "wrong-token",
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.rollback_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.rollback_record",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "claim_token") {
		t.Fatalf("expected wrong claim token rollback record to fail, got %v", err)
	}
	rollbackRecorded, _, err := store.RecordProjectPatchQueueRollbackEvidenceWithEvent(ctx, sqlite.ProjectPatchQueueRollbackRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		RollbackEvidence:      rollbackEvidence,
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.rollback_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.rollback_record",
	})
	if err != nil {
		t.Fatalf("record rollback evidence: %v", err)
	}
	if rollbackRecorded.RollbackEvidenceSchema != sqlite.ProjectPatchQueueRollbackEvidenceSchema ||
		!rollbackRecorded.RollbackEvidenceAccepted ||
		rollbackRecorded.RollbackEvidenceDigest != repoauthority.PatchQueueRollbackEvidenceDigest(rollbackRecorded.RollbackEvidence) ||
		rollbackRecorded.RollbackRecordedBy != leadID ||
		rollbackRecorded.RollbackRecordedAt == "" ||
		!sqlite.ProjectPatchQueueRollbackEvidenceReady(rollbackRecorded) {
		t.Fatalf("rollback evidence did not normalize and round-trip: %+v", rollbackRecorded)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after rollback evidence: %v", err)
	} else if !ok || !sqlite.ProjectPatchQueueRollbackEvidenceReady(candidate.QueueItem) {
		t.Fatalf("expected verified rollback evidence candidate, got ok=%v candidate=%+v", ok, candidate.QueueItem)
	}
	operatorID := "operator-human"
	if _, _, err := store.RecordProjectPatchQueueOperatorEnablementWithEvent(ctx, sqlite.ProjectPatchQueueOperatorEnablementRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		OperatorEnablement:    repoauthority.PatchQueueOperatorEnablement{Enabled: true, Reason: "operator checked gates"},
		ClaimToken:            claimed.ClaimToken,
		ActorID:               operatorID,
		ActorType:             "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("cli.project.patch_queue.operator_enablement_record", "cli_local", workspaceID, "human", operatorID),
		PromptContextSurface:  "cli.project.patch_queue.operator_enablement_record",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "reviewer advisory") {
		t.Fatalf("expected operator enablement before reviewer advisory to fail, got %v", err)
	}
	if _, _, err := store.RecordProjectPatchQueueReviewerAdvisoryWithEvent(ctx, sqlite.ProjectPatchQueueReviewerAdvisoryRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ReviewerAdvisory:      repoauthority.PatchQueueReviewerAdvisory{ReviewerID: "other-reviewer", Summary: "reviewer attempted false attribution"},
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.reviewer_advisory_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.reviewer_advisory_record",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "reviewer_id must match recording principal") {
		t.Fatalf("expected reviewer advisory false attribution to fail, got %v", err)
	}
	reviewerRecorded, _, err := store.RecordProjectPatchQueueReviewerAdvisoryWithEvent(ctx, sqlite.ProjectPatchQueueReviewerAdvisoryRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ReviewerAdvisory:      repoauthority.PatchQueueReviewerAdvisory{Summary: "reviewed CAS and rollback evidence"},
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.reviewer_advisory_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.reviewer_advisory_record",
	})
	if err != nil {
		t.Fatalf("record reviewer advisory: %v", err)
	}
	if reviewerRecorded.ReviewerAdvisorySchema != sqlite.ProjectPatchQueueReviewerAdvisorySchema ||
		!reviewerRecorded.ReviewerAdvisoryAccepted ||
		reviewerRecorded.ReviewerAdvisoryDigest != repoauthority.PatchQueueReviewerAdvisoryDigest(reviewerRecorded.ReviewerAdvisory) ||
		reviewerRecorded.ReviewerRecordedBy != leadID ||
		reviewerRecorded.ReviewerRecordedAt == "" ||
		!sqlite.ProjectPatchQueueReviewerAdvisoryReady(reviewerRecorded) {
		t.Fatalf("reviewer advisory did not normalize and round-trip: %+v", reviewerRecorded)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after reviewer advisory: %v", err)
	} else if !ok || !sqlite.ProjectPatchQueueReviewerAdvisoryReady(candidate.QueueItem) {
		t.Fatalf("expected verified reviewer advisory candidate, got ok=%v candidate=%+v", ok, candidate.QueueItem)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET reviewer_advisory_digest = ?
 WHERE queue_id = ? AND item_id = ?`,
		"sha256:"+strings.Repeat("5", 64), item.QueueID, item.ItemID); err != nil {
		t.Fatalf("corrupt controlled candidate reviewer advisory digest: %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after reviewer advisory digest drift: %v", err)
	} else if ok {
		t.Fatalf("reviewer advisory digest drift should not be selected as activation candidate: %+v", candidate.QueueItem)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET reviewer_advisory_digest = ?
 WHERE queue_id = ? AND item_id = ?`,
		reviewerRecorded.ReviewerAdvisoryDigest, item.QueueID, item.ItemID); err != nil {
		t.Fatalf("repair controlled candidate reviewer advisory digest: %v", err)
	}
	if _, _, err := store.RecordProjectPatchQueueOperatorEnablementWithEvent(ctx, sqlite.ProjectPatchQueueOperatorEnablementRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		OperatorEnablement:    repoauthority.PatchQueueOperatorEnablement{Enabled: true, Reason: "agent tried to self-enable mutation activation"},
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.operator_enablement_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.operator_enablement_record",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "non-agent operator principal") {
		t.Fatalf("expected agent operator enablement to fail, got %v", err)
	}
	if _, _, err := store.RecordProjectPatchQueueOperatorEnablementWithEvent(ctx, sqlite.ProjectPatchQueueOperatorEnablementRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		OperatorEnablement:    repoauthority.PatchQueueOperatorEnablement{Reason: "operator omitted explicit enablement"},
		ClaimToken:            claimed.ClaimToken,
		ActorID:               operatorID,
		ActorType:             "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("cli.project.patch_queue.operator_enablement_record", "cli_local", workspaceID, "human", operatorID),
		PromptContextSurface:  "cli.project.patch_queue.operator_enablement_record",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "explicitly enabled") {
		t.Fatalf("expected operator enablement without explicit true to fail, got %v", err)
	}
	if _, _, err := store.RecordProjectPatchQueueOperatorEnablementWithEvent(ctx, sqlite.ProjectPatchQueueOperatorEnablementRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		OperatorEnablement:    repoauthority.PatchQueueOperatorEnablement{Enabled: true, EnabledBy: "other-human", Reason: "operator attempted false attribution"},
		ClaimToken:            claimed.ClaimToken,
		ActorID:               operatorID,
		ActorType:             "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("cli.project.patch_queue.operator_enablement_record", "cli_local", workspaceID, "human", operatorID),
		PromptContextSurface:  "cli.project.patch_queue.operator_enablement_record",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "enabled_by must match operator principal") {
		t.Fatalf("expected operator enablement false attribution to fail, got %v", err)
	}
	operatorRecorded, _, err := store.RecordProjectPatchQueueOperatorEnablementWithEvent(ctx, sqlite.ProjectPatchQueueOperatorEnablementRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		OperatorEnablement:    repoauthority.PatchQueueOperatorEnablement{Enabled: true, Reason: "operator explicitly enabled mutation activation gate"},
		ClaimToken:            claimed.ClaimToken,
		ActorID:               operatorID,
		ActorType:             "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("cli.project.patch_queue.operator_enablement_record", "cli_local", workspaceID, "human", operatorID),
		PromptContextSurface:  "cli.project.patch_queue.operator_enablement_record",
	})
	if err != nil {
		t.Fatalf("record operator enablement: %v", err)
	}
	if operatorRecorded.OperatorEnablementSchema != sqlite.ProjectPatchQueueOperatorEnablementSchema ||
		!operatorRecorded.OperatorEnablementAccepted ||
		operatorRecorded.OperatorEnablementDigest != repoauthority.PatchQueueOperatorEnablementDigest(operatorRecorded.OperatorEnablement) ||
		operatorRecorded.OperatorEnabledBy != operatorID ||
		operatorRecorded.OperatorEnabledAt == "" ||
		!sqlite.ProjectPatchQueueOperatorEnablementReady(operatorRecorded) {
		t.Fatalf("operator enablement did not normalize and round-trip: %+v", operatorRecorded)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after operator enablement: %v", err)
	} else if !ok || !sqlite.ProjectPatchQueueOperatorEnablementReady(candidate.QueueItem) {
		t.Fatalf("expected verified operator enablement candidate, got ok=%v candidate=%+v", ok, candidate.QueueItem)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET operator_enablement_digest = ?
 WHERE queue_id = ? AND item_id = ?`,
		"sha256:"+strings.Repeat("6", 64), item.QueueID, item.ItemID); err != nil {
		t.Fatalf("corrupt controlled candidate operator enablement digest: %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after operator enablement digest drift: %v", err)
	} else if ok {
		t.Fatalf("operator enablement digest drift should not be selected as activation candidate: %+v", candidate.QueueItem)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET operator_enablement_digest = ?
 WHERE queue_id = ? AND item_id = ?`,
		operatorRecorded.OperatorEnablementDigest, item.QueueID, item.ItemID); err != nil {
		t.Fatalf("repair controlled candidate operator enablement digest: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET cas_evaluation_digest = ?
 WHERE queue_id = ? AND item_id = ?`,
		"sha256:"+strings.Repeat("3", 64), item.QueueID, item.ItemID); err != nil {
		t.Fatalf("corrupt controlled candidate CAS digest: %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after CAS digest drift: %v", err)
	} else if ok {
		t.Fatalf("CAS digest drift should not be selected as activation candidate: %+v", candidate.QueueItem)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET cas_evaluation_digest = ?
 WHERE queue_id = ? AND item_id = ?`,
		casRecorded.CASEvaluationDigest, item.QueueID, item.ItemID); err != nil {
		t.Fatalf("repair controlled candidate CAS digest: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET operation_context_digest = ?
 WHERE queue_id = ? AND item_id = ?`,
		"sha256:"+strings.Repeat("0", 64), item.QueueID, item.ItemID); err != nil {
		t.Fatalf("corrupt controlled candidate operation binding digest: %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after operation binding digest drift: %v", err)
	} else if ok {
		t.Fatalf("operation binding digest drift should not be selected as activation candidate: %+v", candidate.QueueItem)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET operation_context_digest = ?
 WHERE queue_id = ? AND item_id = ?`,
		bound.OperationContextDigest, item.QueueID, item.ItemID); err != nil {
		t.Fatalf("repair controlled candidate operation binding digest: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_branch_registry
   SET status = ?, updated_at = '2026-04-26T00:00:00Z'
 WHERE branch_id = ?`,
		sqlite.ProjectBranchStatusActive, readyBranch.BranchID); err != nil {
		t.Fatalf("corrupt controlled candidate branch status: %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after stale branch evidence: %v", err)
	} else if ok {
		t.Fatalf("stale controlled queue evidence should not be selected as activation candidate: %+v", candidate)
	}
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_branch_registry
   SET status = ?, updated_at = '2026-04-26T00:00:01Z'
 WHERE branch_id = ?`,
		sqlite.ProjectBranchStatusReadyForReview, readyBranch.BranchID); err != nil {
		t.Fatalf("repair controlled candidate branch status: %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate before actuator-applied event: %v", err)
	} else if !ok {
		t.Fatalf("expected repaired controlled queue item to be selected before actuator-applied event")
	}
	if _, err := store.RecordRuntimeEventWithLocalWorkspaceAuthority(ctx, workspaceID, sqlite.RuntimeEventInput{
		EventType:   sqlite.ProjectPatchQueueActuatorAppliedEventType,
		EntityType:  "project_patch_queue_item",
		EntityID:    item.QueueID + "/" + item.ItemID,
		ActorType:   "system",
		ActorID:     sqlite.ProjectPatchQueueActuatorActorID,
		PayloadJSON: `{"result_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`,
		CreatedAt:   "2026-04-26T00:00:02Z",
	}); err != nil {
		t.Fatalf("record actuator-applied runtime event: %v", err)
	}
	if candidate, ok, err = store.FirstProjectRepoMutationActivationCandidate(ctx); err != nil {
		t.Fatalf("select repo mutation activation candidate after actuator-applied event: %v", err)
	} else if ok {
		t.Fatalf("actuator-applied queue item should not be selected again: %+v", candidate.QueueItem)
	}
}

func TestProjectPatchQueueActuatorHealthDetectsStaleStartedWithoutApplied(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-actuator-health"
		projectID   = "project-actuator-health"
		repoID      = "repo-main"
		queueID     = "queue-actuator-health"
		itemID      = "item-actuator-health"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{"lead-agent"})
	materializationDigest := "sha256:" + strings.Repeat("a", 64)
	startedAt := time.Now().UTC().Add(-sqlite.ProjectPatchQueueActuatorStartedStaleAfter - time.Second).Format(time.RFC3339Nano)
	startedPayload := fmt.Sprintf(`{
		"schema":"repo_mutation_actuator_started.v1",
		"workspace_id":%q,
		"project_id":%q,
		"repo_id":%q,
		"queue_id":%q,
		"item_id":%q,
		"target_checkout_id":"checkout-integration",
		"target_branch_name":"main",
		"activation_digest":"sha256:%s",
		"materialization_digest":%q,
		"materialization_authority_proof_digest":"sha256:%s"
	}`, workspaceID, projectID, repoID, queueID, itemID, strings.Repeat("b", 64), materializationDigest, strings.Repeat("c", 64))
	if _, err := store.RecordRuntimeEventWithLocalWorkspaceAuthority(ctx, workspaceID, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   sqlite.ProjectPatchQueueActuatorStartedEventType,
		EntityType:  "project_patch_queue_item",
		EntityID:    queueID + "/" + itemID,
		ActorType:   "system",
		ActorID:     sqlite.ProjectPatchQueueActuatorActorID,
		PayloadJSON: startedPayload,
		CreatedAt:   startedAt,
	}); err != nil {
		t.Fatalf("record actuator started event: %v", err)
	}

	snapshot := store.CurrentProjectPatchQueueActuatorHealthSnapshot(ctx)
	if snapshot.State != "degraded" ||
		snapshot.OpenStartedCount != 1 ||
		snapshot.StaleOpenStartedCount != 1 ||
		snapshot.OldestStaleOpenStartedAt == "" ||
		len(snapshot.StaleOpenStartedExamples) != 1 ||
		snapshot.StaleOpenStartedExamples[0].MaterializationDigest != materializationDigest {
		t.Fatalf("expected stale open actuator start to degrade health, got %+v", snapshot)
	}

	appliedPayload := fmt.Sprintf(`{"result_digest":"sha256:%s","materialization_digest":%q}`, strings.Repeat("d", 64), materializationDigest)
	if _, err := store.RecordRuntimeEventWithLocalWorkspaceAuthority(ctx, workspaceID, sqlite.RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   sqlite.ProjectPatchQueueActuatorAppliedEventType,
		EntityType:  "project_patch_queue_item",
		EntityID:    queueID + "/" + itemID,
		ActorType:   "system",
		ActorID:     sqlite.ProjectPatchQueueActuatorActorID,
		PayloadJSON: appliedPayload,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record actuator applied event: %v", err)
	}

	snapshot = store.CurrentProjectPatchQueueActuatorHealthSnapshot(ctx)
	if snapshot.State != "ok" || snapshot.OpenStartedCount != 0 || snapshot.StaleOpenStartedCount != 0 {
		t.Fatalf("expected applied event to close actuator health start, got %+v", snapshot)
	}
}

func TestProjectPatchQueueScopedPathsetAcceptsConcreteCASAndMaterialization(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-patch-queue-scoped"
		projectID   = "project-patch-queue-scoped"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		reviewKey   = "project.project-patch-queue-scoped.branch.branch-ready.review"
	)
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\patch-queue-scoped`)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Scoped Branch Review Packet",
		Content:     "# Scoped Branch Review Packet\n\nscoped queue evidence",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write scoped review doc: %v", err)
	}
	taskID := "task-scoped"
	reserved := registerReservedProjectBranchForReadyBindingTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, workerID, "branch-ready-scoped", "agent/worker-agent/patch-queue-scoped", `{"paths":["web/**"]}`)
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, reserved.BranchID, taskID, `{"paths":["web/**"]}`)
	readyBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              "branch-ready-scoped",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/worker-agent/patch-queue-scoped",
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["web/**"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register scoped ready branch: %v", err)
	}
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 readyBranch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-scoped",
		RunID:                    "run-scoped",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-scoped",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes: map[string]string{
			"web/app.js": "sha256:web",
		},
		RepoLeaseID:           "lease-scoped",
		LeaseTerm:             7,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit scoped controlled patch queue item: %v", err)
	}
	if len(item.Pathset) != 1 || item.Pathset[0] != "web/**" {
		t.Fatalf("expected scoped queue pathset to round-trip, got %+v", item.Pathset)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          600,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim scoped patch queue item: %v", err)
	}
	bound, _, err := store.BindProjectPatchQueueMutationOperationWithEvent(ctx, sqlite.ProjectPatchQueueOperationBindInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.operation_bind", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.operation_bind",
	})
	if err != nil {
		t.Fatalf("bind scoped operation evidence: %v", err)
	}
	candidateContent := "console.log('scoped');\n"
	candidateHash := repoauthority.PatchMaterializationContentDigest(candidateContent)
	operationContext := repoauthority.Context{
		Mode:        repoauthority.ModeControlledQueue,
		WorkspaceID: workspaceID,
		TaskID:      "task-scoped",
		SessionID:   "session-scoped",
		RunID:       "run-scoped",
		AgentID:     workerID,
		Principal: repoauthority.PrincipalRef{
			Type: "agent",
			ID:   workerID,
		},
		CapabilitySnapshot: repoauthority.CapabilitySnapshotRef{
			ID:     "cap-scoped",
			Schema: "daemon_capability_snapshot.v1",
		},
		RepoRoot: checkout.LocalPath,
		Base: repoauthority.BaseIdentity{
			Ref:      "main",
			TreeHash: baseSHA,
			FileHashes: map[string]string{
				"web/app.js": "sha256:web",
			},
		},
		Pathset: []string{"web/**"},
		Lease: repoauthority.LeaseRef{
			ID:   "lease-scoped",
			Term: 7,
		},
		PatchQueue: repoauthority.PatchQueueRef{
			QueueID: item.QueueID,
			ItemID:  item.ItemID,
		},
		Operation: repoauthority.OperationRef{ID: bound.OperationID, Kind: bound.OperationKind},
	}
	appliedCAS := repoauthority.EvaluateCASPatchApply(repoauthority.CASPatchApplyInput{
		Context: operationContext,
		CurrentFileHashes: map[string]string{
			"web/app.js": "sha256:web",
		},
		CandidateFileHashes: map[string]string{
			"web/app.js": candidateHash,
		},
	})
	if appliedCAS.Status != repoauthority.CASPatchStatusApplied {
		t.Fatalf("scoped CAS did not apply: %+v", appliedCAS)
	}
	casRecorded, _, err := store.RecordProjectPatchQueueCASEvidenceWithEvent(ctx, sqlite.ProjectPatchQueueCASRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		CASResult:             appliedCAS,
		TestEvidence:          repoauthority.PatchQueueTestEvidence{Schema: repoauthority.PatchQueueTestEvidenceSchemaVersion, Name: "unit", Command: "go test ./...", Status: repoauthority.PatchQueueTestStatusPassed, ExitCode: 0, OutputDigest: "sha256:" + strings.Repeat("3", 64)},
		ClaimToken:            claimed.ClaimToken,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.cas_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.cas_record",
	})
	if err != nil {
		t.Fatalf("record scoped CAS evidence: %v", err)
	}
	if !sqlite.ProjectPatchQueueCASEvidenceReady(casRecorded) {
		t.Fatalf("scoped CAS evidence was not ready after record: %+v", casRecorded)
	}
	materialized, _, err := store.RecordProjectPatchQueueMaterializationWithEvent(ctx, sqlite.ProjectPatchQueueMaterializationRecordInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ClaimToken:  claimed.ClaimToken,
		ActorID:     leadID,
		ActorType:   "agent",
		Materialization: repoauthority.PatchMaterialization{
			Files: []repoauthority.PatchMaterializedFile{
				{Path: "web/app.js", Content: candidateContent},
			},
		},
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.materialization_record", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.patch_queue.materialization_record",
	})
	if err != nil {
		t.Fatalf("record scoped materialization: %v", err)
	}
	if !sqlite.ProjectPatchQueueMaterializationReady(materialized) ||
		len(materialized.Materialization.Files) != 1 ||
		materialized.Materialization.Files[0].Path != "web/app.js" {
		t.Fatalf("scoped materialization was not durable/ready: %+v", materialized)
	}
}

func TestProjectPatchQueueSubmitControlledQueueCancelsLegacyPatchOnlyProposal(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-patch-queue-legacy-replace"
		projectID   = "project-patch-queue-legacy-replace"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
		reviewKey   = "project.project-patch-queue-legacy-replace.branch.branch-ready.review"
	)
	baseSHA := strings.Repeat("c", 40)
	headSHA := strings.Repeat("d", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-agent\patch-queue-legacy-replace`)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Legacy Replace Review Packet",
		Content:     "# Legacy Replace Review Packet\n\ncontrolled queue evidence",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	taskID := "task-controlled-replacement"
	reserved := registerReservedProjectBranchForReadyBindingTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, workerID, "branch-ready-legacy-replace", "agent/worker-agent/patch-queue-legacy-replace", `{"paths":["web/app.js"]}`)
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, reserved.BranchID, taskID, `{"paths":["web/app.js"]}`)
	readyBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              "branch-ready-legacy-replace",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/worker-agent/patch-queue-legacy-replace",
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["web/app.js"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register ready branch: %v", err)
	}
	legacy, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		BranchID:              readyBranch.BranchID,
		RepoAuthorityMode:     sqlite.ProjectPatchQueueAuthorityModePatchOnly,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit legacy patch-only item: %v", err)
	}
	staleLegacyHead := strings.Repeat("e", 40)
	if _, err := store.WriteDB().ExecContext(ctx, `
UPDATE project_patch_queue_items
   SET head_sha = ?
 WHERE queue_id = ? AND item_id = ?`,
		staleLegacyHead, legacy.QueueID, legacy.ItemID); err != nil {
		t.Fatalf("make legacy item stale-head: %v", err)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		QueueID:                  "patchq-drift",
		ItemID:                   "patchitem-drift-controlled",
		RepoID:                   repoID,
		BranchID:                 readyBranch.BranchID,
		SupersedesQueueID:        legacy.QueueID,
		SupersedesItemID:         legacy.ItemID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-controlled-replacement",
		RunID:                    "run-controlled-replacement",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-controlled-replacement",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes: map[string]string{
			"web/app.js": "sha256:web",
		},
		RepoLeaseID:           "lease-controlled-replacement",
		LeaseTerm:             11,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "must keep queue_id") {
		t.Fatalf("expected legacy replacement to reject queue drift, got %v", err)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		QueueID:                  legacy.QueueID,
		ItemID:                   legacy.ItemID + "-controlled-evidence",
		RepoID:                   repoID,
		BranchID:                 readyBranch.BranchID,
		SupersedesQueueID:        legacy.QueueID,
		SupersedesItemID:         legacy.ItemID,
		EvidenceDocKey:           "project.project-patch-queue-legacy-replace.legacy.evidence",
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   "task-controlled-replacement",
		SessionID:                "session-controlled-replacement",
		RunID:                    "run-controlled-replacement",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-controlled-replacement",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes: map[string]string{
			"web/app.js": "sha256:web",
		},
		RepoLeaseID:           "lease-controlled-replacement",
		LeaseTerm:             11,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "does not accept evidence_doc_key") {
		t.Fatalf("expected legacy replacement to reject unvalidated evidence_doc_key, got %v", err)
	}
	replacementItemID := legacy.ItemID + "-controlled"
	replacement, event, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		QueueID:                  legacy.QueueID,
		ItemID:                   replacementItemID,
		RepoID:                   repoID,
		BranchID:                 readyBranch.BranchID,
		SupersedesQueueID:        legacy.QueueID,
		SupersedesItemID:         legacy.ItemID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   "task-controlled-replacement",
		SessionID:                "session-controlled-replacement",
		RunID:                    "run-controlled-replacement",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-controlled-replacement",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes: map[string]string{
			"web/app.js": "sha256:web",
		},
		RepoLeaseID:           "lease-controlled-replacement",
		LeaseTerm:             11,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("cancel+replace legacy patch-only item: %v", err)
	}
	if event.EventType != "project.patch_queue.submitted" ||
		replacement.ItemID != replacementItemID ||
		replacement.RepoAuthorityMode != sqlite.ProjectPatchQueueAuthorityModeControlledQueue ||
		replacement.SupersedesQueueID != legacy.QueueID ||
		replacement.SupersedesItemID != legacy.ItemID {
		t.Fatalf("unexpected replacement item/event: item=%+v event=%+v", replacement, event)
	}
	replayed, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		QueueID:                  legacy.QueueID,
		ItemID:                   replacementItemID,
		RepoID:                   repoID,
		BranchID:                 readyBranch.BranchID,
		SupersedesQueueID:        legacy.QueueID,
		SupersedesItemID:         legacy.ItemID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   "task-controlled-replacement",
		SessionID:                "session-controlled-replacement",
		RunID:                    "run-controlled-replacement",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-controlled-replacement",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes: map[string]string{
			"web/app.js": "sha256:web",
		},
		RepoLeaseID:           "lease-controlled-replacement",
		LeaseTerm:             11,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("idempotent replay of legacy cancel+replace failed: %v", err)
	}
	if replayed.ItemID != replacement.ItemID || replayed.SupersedesItemID != legacy.ItemID {
		t.Fatalf("idempotent replay returned different replacement: %+v", replayed)
	}
	originalDigest := replacement.ContextDigest
	drifted, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		QueueID:                  legacy.QueueID,
		ItemID:                   replacementItemID,
		RepoID:                   repoID,
		BranchID:                 readyBranch.BranchID,
		SupersedesQueueID:        legacy.QueueID,
		SupersedesItemID:         legacy.ItemID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-controlled-replacement-drift",
		RunID:                    "run-controlled-replacement-drift",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-controlled-replacement-drift",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             strings.Repeat("f", 40),
		BaseFileHashes: map[string]string{
			"web/app.js": "sha256:drift",
		},
		RepoLeaseID:           "lease-controlled-replacement-drift",
		LeaseTerm:             99,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("drifted replay of legacy cancel+replace failed: %v", err)
	}
	if drifted.TaskID != replacement.TaskID ||
		drifted.SessionID != replacement.SessionID ||
		drifted.RunID != replacement.RunID ||
		drifted.RepoLeaseID != replacement.RepoLeaseID ||
		drifted.LeaseTerm != replacement.LeaseTerm ||
		drifted.ContextDigest != originalDigest {
		t.Fatalf("drifted replay mutated replacement receipt: before=%+v after=%+v", replacement, drifted)
	}
	items, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		BranchID:    readyBranch.BranchID,
	})
	if err != nil {
		t.Fatalf("list replacement lineage: %v", err)
	}
	byItem := map[string]sqlite.ProjectPatchQueueItemRecord{}
	for _, item := range items {
		byItem[item.ItemID] = item
	}
	if got := byItem[legacy.ItemID]; got.State != sqlite.ProjectPatchQueueStateCanceled ||
		!strings.Contains(got.DecisionSummary, "legacy patch-only proposal canceled") ||
		!strings.Contains(got.DecisionSummary, replacementItemID) ||
		got.DecidedBy != workerID ||
		got.HeadSHA != staleLegacyHead {
		t.Fatalf("legacy item was not canceled as historical evidence: %+v", got)
	}
	if got := byItem[replacementItemID]; got.State != sqlite.ProjectPatchQueueStateProposed ||
		got.RepoAuthorityMode != sqlite.ProjectPatchQueueAuthorityModeControlledQueue ||
		got.SupersedesItemID != legacy.ItemID ||
		got.ContextDigest == "" ||
		got.TaskID != replacement.TaskID ||
		got.RepoLeaseID != replacement.RepoLeaseID ||
		got.ContextDigest != originalDigest {
		t.Fatalf("replacement item lost controlled/provenance evidence: %+v", got)
	}
	liveItems, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		BranchID:    readyBranch.BranchID,
		State:       sqlite.ProjectPatchQueueStateProposed,
	})
	if err != nil {
		t.Fatalf("list live replacement item: %v", err)
	}
	if len(liveItems) != 1 || liveItems[0].ItemID != replacementItemID {
		t.Fatalf("expected only replacement to remain live, got %+v", liveItems)
	}
	cancelEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.patch_queue.canceled",
		EntityType:  "project_patch_queue_item",
		EntityID:    legacy.QueueID + "/" + legacy.ItemID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list legacy cancellation events: %v", err)
	}
	if len(cancelEvents) != 1 || !strings.Contains(cancelEvents[0].PayloadJSON, replacementItemID) || !strings.Contains(cancelEvents[0].PayloadJSON, "legacy_cancel_replace") {
		t.Fatalf("legacy cancellation event lost replacement provenance: %+v", cancelEvents)
	}
	if _, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               legacy.QueueID,
		ItemID:                legacy.ItemID,
		RepoID:                repoID,
		BranchID:              readyBranch.BranchID,
		RepoAuthorityMode:     sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.patch_queue.submit",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "cannot upgrade legacy patch-only proposal") {
		t.Fatalf("expected in-place controlled upgrade to remain invalid, got %v", err)
	}
}

func TestProjectPatchQueueControlledAddFileHandoffRecordsDurableEvidence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-project-patch-queue-add-handoff"
		projectID    = "project-patch-queue-add-handoff"
		leadID       = "lead-agent"
		workerID     = "worker-alpha"
		integratorID = "integrator-beta"
		repoID       = "repo-main"
		reviewKey    = "project.project-patch-queue-add-handoff.branch.branch-ready.review"
		addedPath    = "web/subpixel_lab.js"
	)
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	baseTreeHash := strings.Repeat("c", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, integratorID})
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
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, workerID, `C:\fixtures\agents\worker-alpha\patch-queue-add-handoff`)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Add File Branch Review Packet",
		Content:     "# Add File Branch Review Packet\n\nWorker alpha proposes a new frontend slice; integrator beta owns CAS/materialization.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write add-file review doc: %v", err)
	}
	taskID := "task-add-handoff"
	reserved := registerReservedProjectBranchForReadyBindingTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, workerID, "branch-ready-add-handoff", "agent/worker-alpha/patch-queue-add-handoff", `{"paths":["`+addedPath+`"]}`)
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, reserved.BranchID, taskID, `{"paths":["`+addedPath+`"]}`)
	readyBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               workerID,
		BranchID:              "branch-ready-add-handoff",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/worker-alpha/patch-queue-add-handoff",
		BaseBranch:            "main",
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		WriteScopeJSON:        `{"paths":["` + addedPath + `"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register add-file ready branch: %v", err)
	}
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 readyBranch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-add-handoff",
		RunID:                    "run-add-handoff",
		AgentID:                  workerID,
		CapabilitySnapshotID:     "cap-add-handoff",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseTreeHash,
		BaseFileHashes:           map[string]string{},
		RepoLeaseID:              "lease-add-handoff",
		LeaseTerm:                9,
		ActorID:                  workerID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit add-file controlled patch queue item: %v", err)
	}
	if item.SubmittedBy != workerID || item.AgentID != workerID || item.PrincipalID != workerID || len(item.BaseFileHashes) != 0 {
		t.Fatalf("submit evidence did not preserve worker-owned empty-base add context: %+v", item)
	}

	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               integratorID,
		ActorType:             "agent",
		LeaseSeconds:          600,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim add-file patch queue item: %v", err)
	}
	if claimed.ClaimedBy != integratorID || claimed.ClaimToken == "" {
		t.Fatalf("integrator claim did not round-trip: %+v", claimed)
	}
	coordination, err := store.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get coordination after add-file claim: %v", err)
	}
	if len(coordination.PatchQueueItems) != 1 ||
		coordination.PatchQueueItems[0].SubmittedBy != workerID ||
		coordination.PatchQueueItems[0].ClaimedBy != integratorID {
		t.Fatalf("coordination snapshot lost worker-to-integrator handoff: %+v", coordination.PatchQueueItems)
	}

	bound, _, err := store.BindProjectPatchQueueMutationOperationWithEvent(ctx, sqlite.ProjectPatchQueueOperationBindInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ClaimToken:            claimed.ClaimToken,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.operation_bind", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.operation_bind",
	})
	if err != nil {
		t.Fatalf("bind add-file operation evidence: %v", err)
	}
	addedContent := "export function renderSubpixelLab() {\n  return 'ok';\n}\n"
	addedHash := repoauthority.PatchMaterializationContentDigest(addedContent)
	operationContext := repoauthority.Context{
		Mode:        repoauthority.ModeControlledQueue,
		WorkspaceID: workspaceID,
		TaskID:      "task-add-handoff",
		SessionID:   "session-add-handoff",
		RunID:       "run-add-handoff",
		AgentID:     workerID,
		Principal: repoauthority.PrincipalRef{
			Type: "agent",
			ID:   workerID,
		},
		CapabilitySnapshot: repoauthority.CapabilitySnapshotRef{
			ID:     "cap-add-handoff",
			Schema: "daemon_capability_snapshot.v1",
		},
		RepoRoot: checkout.LocalPath,
		Base: repoauthority.BaseIdentity{
			Ref:        "main",
			TreeHash:   baseTreeHash,
			FileHashes: map[string]string{},
		},
		Pathset: []string{addedPath},
		Lease: repoauthority.LeaseRef{
			ID:   "lease-add-handoff",
			Term: 9,
		},
		PatchQueue: repoauthority.PatchQueueRef{
			QueueID: item.QueueID,
			ItemID:  item.ItemID,
		},
		Operation: repoauthority.OperationRef{ID: bound.OperationID, Kind: bound.OperationKind},
	}
	appliedCAS := repoauthority.EvaluateCASPatchApply(repoauthority.CASPatchApplyInput{
		Context:             operationContext,
		CurrentFileHashes:   map[string]string{},
		CandidateFileHashes: map[string]string{addedPath: addedHash},
	})
	if appliedCAS.Status != repoauthority.CASPatchStatusApplied ||
		len(appliedCAS.Paths) != 1 ||
		appliedCAS.Paths[0].ChangeKind != repoauthority.CASPatchChangeAdd ||
		appliedCAS.Paths[0].BaseHash != "" ||
		appliedCAS.Paths[0].CurrentHash != "" ||
		appliedCAS.Paths[0].CandidateHash != addedHash {
		t.Fatalf("add-file CAS did not produce an applied add path: %+v", appliedCAS)
	}
	testEvidence := repoauthority.PatchQueueTestEvidence{
		Schema:       repoauthority.PatchQueueTestEvidenceSchemaVersion,
		Name:         "unit",
		Command:      "go test ./internal/storage/sqlite -run TestProjectPatchQueueControlledAddFileHandoffRecordsDurableEvidence",
		Status:       repoauthority.PatchQueueTestStatusPassed,
		ExitCode:     0,
		OutputDigest: "sha256:" + strings.Repeat("7", 64),
	}
	casRecorded, _, err := store.RecordProjectPatchQueueCASEvidenceWithEvent(ctx, sqlite.ProjectPatchQueueCASRecordInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		CASResult:             appliedCAS,
		TestEvidence:          testEvidence,
		ClaimToken:            claimed.ClaimToken,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.cas_record", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.cas_record",
	})
	if err != nil {
		t.Fatalf("record add-file CAS evidence: %v", err)
	}
	if !sqlite.ProjectPatchQueueCASEvidenceReady(casRecorded) ||
		casRecorded.CASRecordedBy != integratorID ||
		len(casRecorded.CASResult.Paths) != 1 ||
		casRecorded.CASResult.Paths[0].ChangeKind != repoauthority.CASPatchChangeAdd {
		t.Fatalf("add-file CAS evidence did not normalize and round-trip: %+v", casRecorded)
	}

	materialized, materializedEvent, err := store.RecordProjectPatchQueueMaterializationWithEvent(ctx, sqlite.ProjectPatchQueueMaterializationRecordInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ClaimToken:  claimed.ClaimToken,
		ActorID:     integratorID,
		ActorType:   "agent",
		Materialization: repoauthority.PatchMaterialization{
			Files: []repoauthority.PatchMaterializedFile{
				{Path: addedPath, Content: addedContent},
			},
		},
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.materialization_record", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.materialization_record",
	})
	if err != nil {
		t.Fatalf("record add-file materialization: %v", err)
	}
	if materializedEvent.EventType != "project.patch_queue.materialization_recorded" ||
		materializedEvent.ActorID != integratorID ||
		strings.Contains(materializedEvent.PayloadJSON, addedContent) {
		t.Fatalf("unexpected add-file materialization runtime event: %+v", materializedEvent)
	}
	if !sqlite.ProjectPatchQueueMaterializationReady(materialized) ||
		materialized.MaterializationRecordedBy != integratorID ||
		len(materialized.Materialization.Files) != 1 ||
		materialized.Materialization.Files[0].Path != addedPath ||
		materialized.Materialization.Files[0].ChangeKind != repoauthority.CASPatchChangeAdd ||
		materialized.Materialization.Files[0].BaseHash != "" ||
		materialized.Materialization.Files[0].CandidateHash != addedHash ||
		materialized.Materialization.Files[0].ContentDigest != addedHash ||
		materialized.MaterializationAuthorityProof.FileCount != 1 ||
		len(materialized.MaterializationAuthorityProof.Files) != 1 ||
		materialized.MaterializationAuthorityProof.Files[0].ChangeKind != repoauthority.CASPatchChangeAdd ||
		materialized.MaterializationAuthorityProof.Files[0].BaseHash != "" {
		t.Fatalf("add-file materialization did not preserve add semantics: %+v", materialized)
	}
	rollbackItem := repoauthority.PatchQueueItem{
		Schema:              repoauthority.PatchQueueItemSchemaVersion,
		ID:                  item.QueueID + "/" + item.ItemID,
		QueueID:             item.QueueID,
		ItemID:              item.ItemID,
		State:               repoauthority.PatchQueueStateApplied,
		Attempt:             casRecorded.Attempt,
		MaxAttempts:         casRecorded.MaxAttempts,
		ContextDigest:       bound.ContextDigest,
		RepoLeaseID:         bound.RepoLeaseID,
		LeaseTerm:           bound.LeaseTerm,
		Pathset:             []string{"web/**"},
		CASResult:           appliedCAS,
		CASPatchDigest:      appliedCAS.PatchDigest,
		CASEvaluationDigest: repoauthority.PatchQueueCASEvaluationDigest(appliedCAS),
		TestEvidence:        testEvidence,
		TestEvidenceDigest:  repoauthority.PatchQueueTestEvidenceDigest(testEvidence),
		OperationID:         bound.OperationID,
		OperationKind:       bound.OperationKind,
	}
	if _, err := repoauthority.NormalizePatchQueueRollbackEvidence(repoauthority.PatchQueueRollback{
		Reason:                     "added files need deletion rollback semantics before live activation",
		SourcePatchDigest:          appliedCAS.PatchDigest,
		VerificationCommand:        "go test ./internal/storage/sqlite",
		VerificationStatus:         repoauthority.PatchQueueTestStatusPassed,
		VerificationExitCode:       0,
		VerificationOutputDigest:   "sha256:" + strings.Repeat("8", 64),
		VerificationOutputSummary:  "ok",
		VerificationDurationMillis: 25,
		RollbackPaths: []repoauthority.PatchQueueRollbackPath{
			{Path: addedPath, SourceAppliedHash: addedHash},
		},
	}, rollbackItem, repoauthority.OperationRef{ID: "op-add-handoff-rollback", Kind: sqlite.ProjectPatchQueueOperationKindRepoPatchApply}, time.Date(2026, 4, 26, 0, 0, 2, 0, time.UTC)); err == nil || !strings.Contains(err.Error(), "deletion rollback support") {
		t.Fatalf("expected added-file rollback to remain blocked until deletion support exists, got %v", err)
	}
}

func registerIntegrationCheckoutForPatchQueueTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID, actorID, path, branchName, baseSHA, headSHA, lastSeenAt string) sqlite.ProjectCheckoutRecord {
	t.Helper()
	checkout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		LocalPath:             path,
		CheckoutKind:          sqlite.ProjectCheckoutKindIntegration,
		BranchName:            branchName,
		BaseBranch:            branchName,
		BaseSHA:               baseSHA,
		HeadSHA:               headSHA,
		DirtyState:            "clean",
		Status:                sqlite.ProjectCheckoutStatusActive,
		LastSeenAt:            lastSeenAt,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register integration checkout: %v", err)
	}
	return checkout
}

func stringSliceContainsForPatchQueueTest(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func acceptedVisualPacketForPatchQueueTest(item sqlite.ProjectPatchQueueItemRecord, branch sqlite.ProjectBranchRecord, verdict string) string {
	return strings.Join([]string{
		"schema: rhizome_visual_acceptance_v1",
		"visual_verdict: " + verdict,
		"product_intent:",
		"  acceptance_criteria: AC-visual-boundary",
		"  core_user_promise: user can open the primary screen, follow the main interaction, and inspect the resulting state.",
		"provenance:",
		"  queue_id: " + item.QueueID,
		"  item_id: " + item.ItemID,
		"  branch_id: " + branch.BranchID,
		"  branch_name: " + branch.BranchName,
		"  head_sha: " + item.HeadSHA,
		"  observed_url: http://127.0.0.1:51955/",
		"  validation_checkout: C:/fixtures/agents/owner-agent/ui-acceptance",
		"viewport_matrix:",
		"  desktop: 1440x900",
		"  mobile: 390x844",
		"state_evidence:",
		"  initial_state: screenshot_path C:/tmp/rhizome-initial.png",
		"  mobile_state: screenshot_path C:/tmp/rhizome-mobile.png",
		"  primary_flow: screenshot_path C:/tmp/rhizome-primary.png",
		"  result_state: screenshot_path C:/tmp/rhizome-result.png",
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

func createClaimedPatchQueueVisualAcceptanceItemForTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, leadID, ownerID, reviewerID, repoID, branchID, writeScopeJSON string) (sqlite.ProjectPatchQueueItemRecord, sqlite.ProjectBranchRecord) {
	t.Helper()
	reviewKey := "project." + projectID + ".branch." + branchID + ".review"
	headSHA := strings.Repeat("b", 40)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, "C:/fixtures/agents/owner-agent/"+branchID)
	taskID := "task-" + branchID
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              branchID,
		BranchName:            "agent/owner-agent/" + branchID,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        writeScopeJSON,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register reserved branch: %v", err)
	}
	seedProjectBranchClaimForReadyTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, checkout.CheckoutID, branch.BranchID, taskID, writeScopeJSON)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nReady for patch queue review.",
		UpdatedBy:   ownerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
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
		BranchName:            "agent/owner-agent/" + branchID,
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               headSHA,
		WriteScopeJSON:        writeScopeJSON,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register branch: %v", err)
	}
	item, _, err := store.SubmitProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueSubmitInput{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-" + branchID,
		RunID:                    "run-" + branchID,
		AgentID:                  ownerID,
		CapabilitySnapshotID:     "cap-" + branchID,
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             branch.BaseSHA,
		BaseFileHashes:           agentWorkTestBaseFileHashesForScope(writeScopeJSON),
		RepoLeaseID:              "lease-" + branchID,
		LeaseTerm:                7,
		ActorID:                  ownerID,
		ActorType:                "agent",
		PromptContextEnvelope:    sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.submit", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:     "project.patch_queue.submit",
	})
	if err != nil {
		t.Fatalf("submit patch queue item: %v", err)
	}
	claimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               reviewerID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim patch queue item: %v", err)
	}
	return claimed, branch
}

func mustEventPayloadMapForPatchQueueTest(t *testing.T, event sqlite.RuntimeEventRecord) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode runtime event payload: %v; payload=%q", err, event.PayloadJSON)
	}
	return payload
}

// CT-10: a claim_token from a released item is terminal ownership and must not be
// usable as live. Once a claim is released the item leaves CLAIMED, so any later
// op carrying the now-stale token must fail closed on the must-be-CLAIMED state
// guard — released ownership cannot drive a decision/release, and the stale token
// cannot revive ownership. This proves the released-token leg of the claim_token
// staleness invariant at the storage boundary.
func TestProjectPatchQueueReleasedClaimTokenCannotDriveLiveDecision(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patch-queue-released-token-stale"
		projectID   = "project-patch-queue-released-token-stale"
		leadID      = "lead-agent"
		ownerID     = "owner-agent"
		reviewerID  = "reviewer-agent"
		repoID      = "repo-main"
		branchID    = "branch-released-token-stale"
		scopeJSON   = `{"paths":["src/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\`+ownerID+`\`+branchID)
	reviewKey := "project." + projectID + ".branch." + branchID + ".review"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nReady for patch queue decision.",
		UpdatedBy:   ownerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	taskID := "task-" + branchID
	seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, ownerID, branchID, "agent/"+ownerID+"/"+branchID, scopeJSON)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
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
		t.Fatalf("register ready branch: %v", err)
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
	staleToken := strings.TrimSpace(claimed.ClaimToken)
	if staleToken == "" {
		t.Fatalf("claim must mint a non-empty token, got %+v", claimed)
	}

	// Drop ownership: release the claim. State leaves CLAIMED; token is cleared.
	released, _, err := store.ReleaseProjectPatchQueueClaimWithEvent(ctx, sqlite.ProjectPatchQueueReleaseInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ClaimToken:            staleToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.release", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.release",
	})
	if err != nil {
		t.Fatalf("release patch queue claim: %v", err)
	}
	if released.State != sqlite.ProjectPatchQueueStateProposed || released.ClaimToken != "" || released.ClaimedBy != "" {
		t.Fatalf("release must clear live ownership, got %+v", released)
	}

	// The stale (released) token must not be usable to decide the now-PROPOSED item:
	// the must-be-CLAIMED state guard rejects it before the token is even consulted.
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		Decision:              "BLOCK",
		DecisionSummary:       "Stale released-token decision must fail closed.",
		ClaimToken:            staleToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "must be CLAIMED") {
		t.Fatalf("expected released-token decision to fail closed on must-be-CLAIMED, got %v", err)
	}

	// Likewise the stale token cannot re-release the item it no longer owns.
	if _, _, err := store.ReleaseProjectPatchQueueClaimWithEvent(ctx, sqlite.ProjectPatchQueueReleaseInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ClaimToken:            staleToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.release", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.release",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "cannot be released") {
		t.Fatalf("expected released-token re-release to fail closed on state guard, got %v", err)
	}

	// The item remains cleanly PROPOSED and re-claimable by a fresh claim minting a new token.
	reclaimed, _, err := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
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
		t.Fatalf("reclaim released patch queue item: %v", err)
	}
	if strings.TrimSpace(reclaimed.ClaimToken) == "" || strings.TrimSpace(reclaimed.ClaimToken) == staleToken {
		t.Fatalf("reclaim must mint a fresh token distinct from the released one, got %q (stale %q)", reclaimed.ClaimToken, staleToken)
	}
}

// CT-11: concurrent interleaving. Two authorized reviewers race to claim the same
// PROPOSED patch-queue item at the same instant. The fenced single-writer claim tx
// must serialize them so exactly one wins ownership and the loser is rejected fail-
// closed — the persisted item must advertise exactly one live owner with one token.
// This exercises the real concurrent race the single-actor claim canaries only imply.
func TestProjectPatchQueueConcurrentClaimYieldsSingleOwner(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-patch-queue-concurrent-claim"
		projectID   = "project-patch-queue-concurrent-claim"
		leadID      = "lead-agent"
		ownerID     = "owner-agent"
		reviewerA   = "reviewer-agent-a"
		reviewerB   = "reviewer-agent-b"
		repoID      = "repo-main"
		branchID    = "branch-concurrent-claim"
		scopeJSON   = `{"paths":["src/**"]}`
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerA, reviewerB})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertAgentWorkReadyProjectRepo(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerA, sqlite.ProjectRoleReviewer, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerB, sqlite.ProjectRoleReviewer, leadID)

	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\`+ownerID+`\`+branchID)
	reviewKey := "project." + projectID + ".branch." + branchID + ".review"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nReady for patch queue decision.",
		UpdatedBy:   ownerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	taskID := "task-" + branchID
	seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, ownerID, branchID, "agent/"+ownerID+"/"+branchID, scopeJSON)
	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
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
		t.Fatalf("register ready branch: %v", err)
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
		t.Fatalf("submit patch queue item: %v", err)
	}

	type claimResult struct {
		actorID string
		record  sqlite.ProjectPatchQueueItemRecord
		err     error
	}
	results := make([]claimResult, 2)
	racers := []string{reviewerA, reviewerB}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, racer := range racers {
		wg.Add(1)
		go func(idx int, actorID string) {
			defer wg.Done()
			<-start
			rec, _, claimErr := store.ClaimProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueClaimInput{
				WorkspaceID:           workspaceID,
				ProjectID:             projectID,
				QueueID:               item.QueueID,
				ItemID:                item.ItemID,
				LeaseSeconds:          900,
				ActorID:               actorID,
				ActorType:             "agent",
				PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", actorID),
				PromptContextSurface:  "project.patch_queue.claim",
			})
			results[idx] = claimResult{actorID: actorID, record: rec, err: claimErr}
		}(i, racer)
	}
	close(start)
	wg.Wait()

	var winners, losers []claimResult
	for _, r := range results {
		if r.err == nil {
			winners = append(winners, r)
			continue
		}
		if !errors.Is(r.err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(r.err.Error(), "already claimed") {
			t.Fatalf("loser of claim race must fail closed with already-claimed, got %v", r.err)
		}
		losers = append(losers, r)
	}
	if len(winners) != 1 || len(losers) != 1 {
		t.Fatalf("expected exactly one winner and one loser, got winners=%d losers=%d (%+v)", len(winners), len(losers), results)
	}
	winner := winners[0]
	if winner.record.State != sqlite.ProjectPatchQueueStateClaimed || strings.TrimSpace(winner.record.ClaimedBy) != winner.actorID || strings.TrimSpace(winner.record.ClaimToken) == "" {
		t.Fatalf("winner must hold a live CLAIMED token, got %+v", winner.record)
	}

	// The single durable owner must match the winner; the loser holds nothing.
	listed, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		BranchID:    branchID,
	})
	if err != nil {
		t.Fatalf("list patch queue items: %v", err)
	}
	var stored sqlite.ProjectPatchQueueItemRecord
	found := false
	for _, rec := range listed {
		if rec.QueueID == item.QueueID && rec.ItemID == item.ItemID {
			stored = rec
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected stored patch queue item %s/%s", item.QueueID, item.ItemID)
	}
	if stored.State != sqlite.ProjectPatchQueueStateClaimed {
		t.Fatalf("stored item must be CLAIMED after race, got %s", stored.State)
	}
	if strings.TrimSpace(stored.ClaimedBy) != winner.actorID || strings.TrimSpace(stored.ClaimToken) != strings.TrimSpace(winner.record.ClaimToken) {
		t.Fatalf("stored owner must be the race winner %s with its token, got claimed_by=%q token=%q", winner.actorID, stored.ClaimedBy, stored.ClaimToken)
	}
	if strings.TrimSpace(stored.ClaimedBy) == losers[0].actorID {
		t.Fatalf("loser %s must not own the item, got %+v", losers[0].actorID, stored)
	}
}
