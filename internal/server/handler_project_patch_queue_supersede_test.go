package server

import (
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestProjectPatchQueueSupersedeRPCBindsPromptContextToSuccessorItem(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID   = "ws-patch-queue-supersede-rpc"
		operatorID    = "operator-a"
		projectID     = "project-supersede-rpc"
		repoID        = "repo-main"
		workerID      = "worker-a"
		reviewerID    = "reviewer-a"
		branchID      = "branch-patch-queue-supersede-rpc"
		taskID        = "task-patch-queue-supersede-rpc"
		reviewDocKey  = "project.project-supersede-rpc.branch.worker.review"
		decisionKey   = "project.project-supersede-rpc.patch_queue.blocked"
		validationKey = "project.project-supersede-rpc.browser_smoke_validation"
	)
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	operatorCtx := testAuthContext(workspaceID, "human", operatorID)
	workerCtx := testAuthContext(workspaceID, "agent", workerID)
	reviewerCtx := testAuthContext(workspaceID, "agent", reviewerID)
	seedProjectRolesWorkspace(t, operatorCtx, store, workspaceID, operatorID, workerID, reviewerID)

	if _, rpcErr := h.projectCreate(operatorCtx, mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Patch Queue Supersede RPC",
		CreatedBy:   operatorID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.projectRoleAssign(operatorCtx, mustJSONRaw(projectRoleAssignParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     operatorID,
		AgentID:     reviewerID,
		RoleType:    sqlite.ProjectRoleReviewer,
		Summary:     "reviewer for supersede regression",
	})); rpcErr != nil {
		t.Fatalf("project.role.assign rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.projectRepositoryUpsert(operatorCtx, mustJSONRaw(projectRepositoryUpsertParams{
		WorkspaceID:      workspaceID,
		ProjectID:        projectID,
		ActorID:          operatorID,
		RepoID:           repoID,
		RemoteURL:        "git@github.com:ExampleOrg/project-supersede-rpc.git",
		RemoteKind:       sqlite.ProjectRepositoryRemoteKindGitHub,
		DefaultBranch:    "main",
		RepoStatus:       sqlite.ProjectRepositoryStatusReady,
		IsCanonical:      true,
		CreatedByAgentID: workerID,
	})); rpcErr != nil {
		t.Fatalf("project.repository.upsert rpc error: %+v", rpcErr)
	}
	checkoutResult, rpcErr := h.projectCheckoutRegister(workerCtx, mustJSONRaw(projectCheckoutRegisterParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      workerID,
		RepoID:       repoID,
		MachineID:    "developer-desktop",
		AgentID:      workerID,
		LocalPath:    `C:\fixtures\agents\worker-a\project-supersede-rpc`,
		CheckoutKind: sqlite.ProjectCheckoutKindClone,
		BranchName:   "agent/worker-a/project-supersede-rpc",
		BaseBranch:   "main",
		HeadSHA:      headSHA,
		BaseSHA:      baseSHA,
		DirtyState:   "clean",
		Status:       sqlite.ProjectCheckoutStatusActive,
	}))
	if rpcErr != nil {
		t.Fatalf("project.checkout.register rpc error: %+v", rpcErr)
	}
	checkout := checkoutResult.(map[string]any)["checkout"].(sqlite.ProjectCheckoutRecord)
	if err := store.UpsertWorkspaceDoc(workerCtx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewDocKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nReady for patch queue review.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	seedServerProjectBranchClaimForReady(t, workerCtx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, branchID, taskID, `{"paths":["src/**"]}`)
	branchResult, rpcErr := h.projectBranchRegister(workerCtx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        workerID,
		RepoID:         repoID,
		CheckoutID:     checkout.CheckoutID,
		AgentID:        workerID,
		BranchID:       branchID,
		BranchName:     "agent/worker-a/project-supersede-rpc",
		BaseBranch:     "main",
		BaseSHA:        baseSHA,
		HeadSHA:        headSHA,
		WriteScopeJSON: `{"paths":["src/**"]}`,
		ReviewDocKey:   reviewDocKey,
		ActiveTaskID:   taskID,
		ActiveClaimID:  taskID,
		Status:         sqlite.ProjectBranchStatusReadyForReview,
	}))
	if rpcErr != nil {
		t.Fatalf("project.branch.register rpc error: %+v", rpcErr)
	}
	branch := branchResult.(map[string]any)["branch"].(sqlite.ProjectBranchRecord)

	submitResult, rpcErr := h.projectPatchQueueSubmit(workerCtx, mustJSONRaw(projectPatchQueueSubmitParams{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		ActorID:                  workerID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		TaskID:                   taskID,
		SessionID:                "session-patch-queue-supersede-rpc",
		RunID:                    "run-patch-queue-supersede-rpc",
		AgentID:                  workerID,
		PrincipalType:            "agent",
		PrincipalID:              workerID,
		CapabilitySnapshotID:     "cap-patch-queue-supersede-rpc",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes:           map[string]string{},
		RepoLeaseID:              "lease-patch-queue-supersede-rpc",
		LeaseTerm:                7,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.submit rpc error: %+v", rpcErr)
	}
	oldItem := submitResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	claimResult, rpcErr := h.projectPatchQueueClaim(reviewerCtx, mustJSONRaw(projectPatchQueueClaimParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     reviewerID,
		QueueID:     oldItem.QueueID,
		ItemID:      oldItem.ItemID,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.claim rpc error: %+v", rpcErr)
	}
	claimed := claimResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	if err := store.UpsertWorkspaceDoc(reviewerCtx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      decisionKey,
		Title:       "Patch Queue Decision",
		Content:     "Blocked pending browser smoke evidence.",
		UpdatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("write decision doc: %v", err)
	}
	if _, rpcErr := h.projectPatchQueueDecision(reviewerCtx, mustJSONRaw(projectPatchQueueDecisionParams{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		ActorID:         reviewerID,
		QueueID:         oldItem.QueueID,
		ItemID:          oldItem.ItemID,
		Decision:        sqlite.ProjectPatchQueueStateBlocked,
		DecisionDocKey:  decisionKey,
		DecisionSummary: "Missing browser smoke evidence.",
		ClaimToken:      claimed.ClaimToken,
	})); rpcErr != nil {
		t.Fatalf("project.patch_queue.decision rpc error: %+v", rpcErr)
	}
	if err := store.UpsertWorkspaceDoc(reviewerCtx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      validationKey,
		Title:       "Fresh Browser Smoke Validation",
		Content: "Fresh same-head browser smoke passed.\n\n" +
			"queue_id: " + oldItem.QueueID + "\n" +
			"item_id: " + oldItem.ItemID + "\n" +
			"branch_id: " + branch.BranchID + "\n" +
			"head_sha: " + headSHA + "\n",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write validation doc: %v", err)
	}
	if _, err := store.WriteDB().ExecContext(reviewerCtx, `UPDATE workspace_docs SET updated_at = '2099-01-01T00:00:00Z' WHERE workspace_id = ? AND doc_key = ?`, workspaceID, validationKey); err != nil {
		t.Fatalf("force validation doc timestamp: %v", err)
	}

	freshItemID := oldItem.ItemID + "-fresh"
	supersedeResult, rpcErr := h.projectPatchQueueSupersede(reviewerCtx, mustJSONRaw(projectPatchQueueSupersedeParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        reviewerID,
		QueueID:        oldItem.QueueID,
		ItemID:         oldItem.ItemID,
		NewItemID:      freshItemID,
		EvidenceDocKey: validationKey,
		PrincipalType:  "agent",
		PrincipalID:    workerID,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.supersede rpc error: %+v", rpcErr)
	}
	freshItem := supersedeResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	if freshItem.ItemID != freshItemID ||
		freshItem.SupersedesItemID != oldItem.ItemID ||
		freshItem.RepoAuthorityMode != sqlite.ProjectPatchQueueAuthorityModeControlledQueue ||
		freshItem.ContextDigest == "" {
		t.Fatalf("unexpected supersede result: %+v", freshItem)
	}
	if freshItem.PrincipalType != "agent" || freshItem.PrincipalID != workerID {
		t.Fatalf("RPC must ignore principal-only supersede payload and preserve inherited binding principal, got %+v", freshItem)
	}
	event := mustRuntimeEvent(t, reviewerCtx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.patch_queue.submitted",
		EntityType:  "project_patch_queue_item",
		EntityID:    oldItem.QueueID + "/" + freshItemID,
	})
	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	if payload["item_id"] != freshItemID || payload["supersedes_item_id"] != oldItem.ItemID || payload["evidence_doc_key"] != validationKey {
		t.Fatalf("supersede event lost successor/provenance payload: %+v", payload)
	}
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("supersede event missing prompt_context_envelope: %+v", payload)
	}
	for key, want := range map[string]string{
		"surface":             "project.patch_queue.supersede",
		"item_id":             freshItemID,
		"new_item_id":         freshItemID,
		"supersedes_queue_id": oldItem.QueueID,
		"supersedes_item_id":  oldItem.ItemID,
		"evidence_doc_key":    validationKey,
		"actor_id":            reviewerID,
	} {
		if got, ok := envelope[key].(string); !ok || got != want {
			t.Fatalf("prompt_context_envelope[%s] = %v, want %q in %+v", key, envelope[key], want, envelope)
		}
	}
}

func TestProjectPatchQueueReviewTaskReconcileRPCRepairsReceiptAndDecisionContinuationConsumeRPC(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID  = "ws-patch-queue-reconcile-rpc"
		operatorID   = "operator-a"
		projectID    = "project-reconcile-rpc"
		repoID       = "repo-main"
		workerID     = "worker-a"
		integratorID = "integrator-a"
		observerID   = "observer-a"
		branchID     = "branch-patch-queue-reconcile-rpc"
		taskID       = "task-patch-queue-reconcile-rpc"
		reviewDocKey = "project.project-reconcile-rpc.branch.worker.review"
	)
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	operatorCtx := testAuthContext(workspaceID, "human", operatorID)
	workerCtx := testAuthContext(workspaceID, "agent", workerID)
	integratorCtx := testAuthContext(workspaceID, "agent", integratorID)
	observerCtx := testAuthContext(workspaceID, "agent", observerID)
	seedProjectRolesWorkspace(t, operatorCtx, store, workspaceID, operatorID, workerID, integratorID, observerID)

	if _, rpcErr := h.projectCreate(operatorCtx, mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Patch Queue Reconcile RPC",
		CreatedBy:   operatorID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.projectRoleAssign(operatorCtx, mustJSONRaw(projectRoleAssignParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     operatorID,
		AgentID:     integratorID,
		RoleType:    sqlite.ProjectRoleIntegrator,
		Summary:     "integrator for review task receipt repair",
	})); rpcErr != nil {
		t.Fatalf("project.role.assign integrator rpc error: %+v", rpcErr)
	}
	// The branch owner holds a claimable IMPLEMENTER role (production-faithful: they implemented the branch), so
	// the BLOCKED decision's revision continuation route-classifies satisfiable_now and materializes eagerly; the
	// consume RPC then idempotently REUSES that carrier (created=false) instead of minting a second one.
	if _, rpcErr := h.projectRoleAssign(operatorCtx, mustJSONRaw(projectRoleAssignParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        operatorID,
		AgentID:        workerID,
		RoleType:       sqlite.ProjectRoleImplementer,
		WriteScopeJSON: `{"paths":["**"]}`,
		Summary:        "implementer for the branch owner revision continuation",
	})); rpcErr != nil {
		t.Fatalf("project.role.assign implementer rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.projectRepositoryUpsert(operatorCtx, mustJSONRaw(projectRepositoryUpsertParams{
		WorkspaceID:      workspaceID,
		ProjectID:        projectID,
		ActorID:          operatorID,
		RepoID:           repoID,
		RemoteURL:        "git@github.com:ExampleOrg/project-reconcile-rpc.git",
		RemoteKind:       sqlite.ProjectRepositoryRemoteKindGitHub,
		DefaultBranch:    "main",
		RepoStatus:       sqlite.ProjectRepositoryStatusReady,
		IsCanonical:      true,
		CreatedByAgentID: workerID,
	})); rpcErr != nil {
		t.Fatalf("project.repository.upsert rpc error: %+v", rpcErr)
	}
	checkoutResult, rpcErr := h.projectCheckoutRegister(workerCtx, mustJSONRaw(projectCheckoutRegisterParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      workerID,
		RepoID:       repoID,
		MachineID:    "developer-desktop",
		AgentID:      workerID,
		LocalPath:    `C:\fixtures\agents\worker-a\project-reconcile-rpc`,
		CheckoutKind: sqlite.ProjectCheckoutKindClone,
		BranchName:   "agent/worker-a/project-reconcile-rpc",
		BaseBranch:   "main",
		HeadSHA:      headSHA,
		BaseSHA:      baseSHA,
		DirtyState:   "clean",
		Status:       sqlite.ProjectCheckoutStatusActive,
	}))
	if rpcErr != nil {
		t.Fatalf("project.checkout.register rpc error: %+v", rpcErr)
	}
	checkout := checkoutResult.(map[string]any)["checkout"].(sqlite.ProjectCheckoutRecord)
	if err := store.UpsertWorkspaceDoc(workerCtx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewDocKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nReady for patch queue review.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	seedServerProjectBranchClaimForReady(t, workerCtx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, branchID, taskID, `{"paths":["src/**"]}`)
	branchResult, rpcErr := h.projectBranchRegister(workerCtx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        workerID,
		RepoID:         repoID,
		CheckoutID:     checkout.CheckoutID,
		AgentID:        workerID,
		BranchID:       branchID,
		BranchName:     "agent/worker-a/project-reconcile-rpc",
		BaseBranch:     "main",
		BaseSHA:        baseSHA,
		HeadSHA:        headSHA,
		WriteScopeJSON: `{"paths":["src/**"]}`,
		ReviewDocKey:   reviewDocKey,
		ActiveTaskID:   taskID,
		ActiveClaimID:  taskID,
		Status:         sqlite.ProjectBranchStatusReadyForReview,
	}))
	if rpcErr != nil {
		t.Fatalf("project.branch.register rpc error: %+v", rpcErr)
	}
	branch := branchResult.(map[string]any)["branch"].(sqlite.ProjectBranchRecord)

	submitResult, rpcErr := h.projectPatchQueueSubmit(workerCtx, mustJSONRaw(projectPatchQueueSubmitParams{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		ActorID:                  workerID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		TaskID:                   taskID,
		SessionID:                "session-patch-queue-reconcile-rpc",
		RunID:                    "run-patch-queue-reconcile-rpc",
		AgentID:                  workerID,
		PrincipalType:            "agent",
		PrincipalID:              workerID,
		CapabilitySnapshotID:     "cap-patch-queue-reconcile-rpc",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes:           map[string]string{},
		RepoLeaseID:              "lease-patch-queue-reconcile-rpc",
		LeaseTerm:                7,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.submit rpc error: %+v", rpcErr)
	}
	item := submitResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	reviewTaskID := sqlite.ProjectPatchQueueReviewTaskID(item)
	if _, err := store.WriteDB().ExecContext(integratorCtx, `DELETE FROM runtime_events WHERE workspace_id = ? AND event_type = 'task.created' AND entity_type = 'task' AND entity_id = ?`, workspaceID, reviewTaskID); err != nil {
		t.Fatalf("remove task.created receipt to simulate missing review task event: %v", err)
	}

	reconcileResult, rpcErr := h.projectPatchQueueReviewTaskReconcile(integratorCtx, mustJSONRaw(projectPatchQueueReviewTaskReconcileParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     integratorID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.review_task.reconcile rpc error: %+v", rpcErr)
	}
	reconciled := reconcileResult.(map[string]any)
	status := reconciled["patch_queue_review_task"].(sqlite.TaskStatus)
	if status.TaskID != reviewTaskID || status.ProjectLane != "review" || !reconciled["repaired"].(bool) || strings.TrimSpace(reconciled["review_task_event_id"].(string)) == "" {
		t.Fatalf("unexpected reconcile result: %+v", reconciled)
	}

	claimResult, rpcErr := h.projectPatchQueueClaim(integratorCtx, mustJSONRaw(projectPatchQueueClaimParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      integratorID,
		QueueID:      item.QueueID,
		ItemID:       item.ItemID,
		LeaseSeconds: 900,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.claim rpc error: %+v", rpcErr)
	}
	claimed := claimResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	if _, rpcErr := h.projectPatchQueueDecision(integratorCtx, mustJSONRaw(projectPatchQueueDecisionParams{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		ActorID:         integratorID,
		QueueID:         claimed.QueueID,
		ItemID:          claimed.ItemID,
		Decision:        sqlite.ProjectPatchQueueStateBlocked,
		DecisionSummary: "Need a focused revision before integration.",
		ClaimToken:      claimed.ClaimToken,
	})); rpcErr != nil {
		t.Fatalf("project.patch_queue.decision rpc error: %+v", rpcErr)
	}
	continuations, err := store.ListProjectPatchQueueDecisionContinuations(integratorCtx, sqlite.ProjectPatchQueueDecisionContinuationFilter{
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
	consumeResult, rpcErr := h.projectPatchQueueDecisionContinuationConsume(integratorCtx, mustJSONRaw(projectPatchQueueDecisionContinuationConsumeParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     integratorID,
		OutboxID:    continuations[0].OutboxID,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.decision_continuation.consume rpc error: %+v", rpcErr)
	}
	consumed := consumeResult.(map[string]any)
	continuation := consumed["patch_queue_decision_continuation"].(sqlite.ProjectPatchQueueDecisionContinuationRecord)
	continuationTask := consumed["continuation_task"].(sqlite.TaskStatus)
	// Drain model: the BLOCKED decision eagerly materialized the revision carrier (the owner holds IMPLEMENTER), so
	// the consume RPC REUSES it (created=false) - still CONSUMED, still returning the same continuation task.
	if !consumed["consumed"].(bool) || consumed["created"].(bool) || continuation.State != "CONSUMED" || continuationTask.TaskID != continuation.ContinuationTaskID {
		t.Fatalf("unexpected decision continuation consume result: %+v", consumed)
	}

	if _, rpcErr := h.projectPatchQueueReviewTaskReconcile(observerCtx, mustJSONRaw(projectPatchQueueReviewTaskReconcileParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     observerID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
	})); rpcErr == nil {
		t.Fatalf("expected non-integrator reconcile to fail")
	}

	if _, err := store.WriteDB().ExecContext(integratorCtx, `UPDATE project_patch_queue_items SET state = 'REJECTED' WHERE workspace_id = ? AND queue_id = ? AND item_id = ?`, workspaceID, item.QueueID, item.ItemID); err != nil {
		t.Fatalf("force terminal patch queue item state: %v", err)
	}
	if _, rpcErr := h.projectPatchQueueReviewTaskReconcile(integratorCtx, mustJSONRaw(projectPatchQueueReviewTaskReconcileParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     integratorID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
	})); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "does not require a live review task receipt") {
		t.Fatalf("expected terminal reconcile to map to invalid params, got %+v", rpcErr)
	}
}
