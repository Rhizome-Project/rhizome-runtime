package server

import (
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/repoauthority"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestProjectRepositoryAndCheckoutRPCsUseAuthorityStorageAndPublishEvents(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-project-git-rpc"
		actorID     = "operator-a"
		projectID   = "project-git-rpc"
		repoID      = "repo-main"
		workerID    = "worker-a"
	)
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedProjectRolesWorkspace(t, ctx, store, workspaceID, actorID, workerID)
	if _, rpcErr := h.projectCreate(ctx, mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Git RPC",
		CreatedBy:   actorID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	repoResult, rpcErr := h.projectRepositoryUpsert(ctx, mustJSONRaw(projectRepositoryUpsertParams{
		WorkspaceID:            workspaceID,
		ProjectID:              projectID,
		ActorID:                actorID,
		RepoID:                 repoID,
		RemoteURL:              "git@github.com:ExampleOrg/project-git-rpc.git",
		RemoteKind:             sqlite.ProjectRepositoryRemoteKindGitHub,
		Owner:                  "ExampleOrg",
		Name:                   "project-git-rpc",
		DefaultBranch:          "main",
		IntegrationBranch:      "integration",
		CredentialVaultEntryID: "vault.github.mrdeveloper.ssh",
		RepoStatus:             sqlite.ProjectRepositoryStatusReady,
		IsCanonical:            true,
		CreatedByAgentID:       workerID,
	}))
	if rpcErr != nil {
		t.Fatalf("project.repository.upsert rpc error: %+v", rpcErr)
	}
	repo := repoResult.(map[string]any)["repository"].(sqlite.ProjectRepositoryRecord)
	if repo.RepoID != repoID || !repo.IsCanonical || repo.RepoStatus != sqlite.ProjectRepositoryStatusReady {
		t.Fatalf("project.repository.upsert returned unexpected repository: %+v", repo)
	}
	repoRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.repository.upserted",
		EntityType:  "project_repository",
		EntityID:    repoID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.repository.upserted"), repoRuntime, "project.repository.upserted")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.repository.changed"), repoRuntime, "project.repository.changed")
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, repoRuntime.PayloadJSON), "project.repository.upsert", workspaceID, "human", actorID, projectID, actorID)

	repoListResult, rpcErr := h.projectRepositoriesList(ctx, mustJSONRaw(projectRepositoriesListParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
	}))
	if rpcErr != nil {
		t.Fatalf("project.repositories.list rpc error: %+v", rpcErr)
	}
	repos := repoListResult.(map[string]any)["repositories"].([]sqlite.ProjectRepositoryRecord)
	if len(repos) != 1 || repos[0].RepoID != repoID {
		t.Fatalf("unexpected project.repositories.list payload: %+v", repoListResult)
	}

	checkoutResult, rpcErr := h.projectCheckoutRegister(ctx, mustJSONRaw(projectCheckoutRegisterParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      actorID,
		RepoID:       repoID,
		MachineID:    "developer-desktop",
		MachineLabel: "Example Workstation",
		OwnerUserID:  "developer",
		AgentID:      workerID,
		LocalPath:    `C:\fixtures\agents\worker-a\project-git-rpc`,
		CheckoutKind: sqlite.ProjectCheckoutKindClone,
		BranchName:   "agent/worker-a/project-git-rpc",
		BaseBranch:   "main",
		DirtyState:   "clean",
		Status:       sqlite.ProjectCheckoutStatusActive,
	}))
	if rpcErr != nil {
		t.Fatalf("project.checkout.register rpc error: %+v", rpcErr)
	}
	checkout := checkoutResult.(map[string]any)["checkout"].(sqlite.ProjectCheckoutRecord)
	if checkout.RepoID != repoID || checkout.AgentID != workerID || checkout.LocalPath == "" {
		t.Fatalf("project.checkout.register returned unexpected checkout: %+v", checkout)
	}
	checkoutRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.checkout.registered",
		EntityType:  "project_checkout",
		EntityID:    checkout.CheckoutID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.checkout.registered"), checkoutRuntime, "project.checkout.registered")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.checkout.changed"), checkoutRuntime, "project.checkout.changed")
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, checkoutRuntime.PayloadJSON), "project.checkout.register", workspaceID, "human", actorID, projectID, actorID)

	checkoutListResult, rpcErr := h.projectCheckoutsList(ctx, mustJSONRaw(projectCheckoutsListParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	}))
	if rpcErr != nil {
		t.Fatalf("project.checkouts.list rpc error: %+v", rpcErr)
	}
	checkouts := checkoutListResult.(map[string]any)["checkouts"].([]sqlite.ProjectCheckoutRecord)
	if len(checkouts) != 1 || checkouts[0].CheckoutID != checkout.CheckoutID || checkouts[0].DerivedStatus != sqlite.ProjectCheckoutStatusActive {
		t.Fatalf("unexpected project.checkouts.list payload: %+v", checkoutListResult)
	}

	branchResult, rpcErr := h.projectBranchRegister(ctx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        actorID,
		RepoID:         repoID,
		CheckoutID:     checkout.CheckoutID,
		AgentID:        workerID,
		BranchName:     "agent/worker-a/project-git-rpc",
		BranchKind:     sqlite.ProjectBranchKindFeature,
		BaseBranch:     "main",
		WriteScopeJSON: `{"paths":["web/**","api/**"]}`,
		Status:         sqlite.ProjectBranchStatusActive,
	}))
	if rpcErr != nil {
		t.Fatalf("project.branch.register rpc error: %+v", rpcErr)
	}
	branch := branchResult.(map[string]any)["branch"].(sqlite.ProjectBranchRecord)
	if branch.RepoID != repoID || branch.CheckoutID != checkout.CheckoutID || branch.AgentID != workerID || branch.BranchName == "" {
		t.Fatalf("project.branch.register returned unexpected branch: %+v", branch)
	}
	branchRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.branch.registered",
		EntityType:  "project_branch",
		EntityID:    branch.BranchID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.branch.registered"), branchRuntime, "project.branch.registered")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.branch.changed"), branchRuntime, "project.branch.changed")
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, branchRuntime.PayloadJSON), "project.branch.register", workspaceID, "human", actorID, projectID, actorID)

	branchListResult, rpcErr := h.projectBranchesList(ctx, mustJSONRaw(projectBranchesListParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		RepoID:      repoID,
		AgentID:     workerID,
	}))
	if rpcErr != nil {
		t.Fatalf("project.branches.list rpc error: %+v", rpcErr)
	}
	branches := branchListResult.(map[string]any)["branches"].([]sqlite.ProjectBranchRecord)
	if len(branches) != 1 || branches[0].BranchID != branch.BranchID {
		t.Fatalf("unexpected project.branches.list payload: %+v", branchListResult)
	}

	if _, rpcErr := h.projectRepositoryUpsert(ctx, mustJSONRaw(projectRepositoryUpsertParams{
		WorkspaceID:            workspaceID,
		ProjectID:              projectID,
		ActorID:                actorID,
		RepoID:                 "repo-secret",
		CredentialVaultEntryID: "-----BEGIN PRIVATE KEY-----",
	})); rpcErr == nil {
		t.Fatal("expected secret material credential reference to be rejected")
	}
}

func TestProjectRepositoryAndCheckoutListsRejectMissingProject(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-project-git-list-missing"
	const actorID = "operator-a"
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedProjectRolesWorkspace(t, ctx, store, workspaceID, actorID)

	for method, call := range map[string]func() (any, *RPCError){
		"project.repositories.list": func() (any, *RPCError) {
			return h.projectRepositoriesList(ctx, mustJSONRaw(projectRepositoriesListParams{
				WorkspaceID: workspaceID,
				ProjectID:   "missing-project",
			}))
		},
		"project.checkouts.list": func() (any, *RPCError) {
			return h.projectCheckoutsList(ctx, mustJSONRaw(projectCheckoutsListParams{
				WorkspaceID: workspaceID,
				ProjectID:   "missing-project",
			}))
		},
		"project.branches.list": func() (any, *RPCError) {
			return h.projectBranchesList(ctx, mustJSONRaw(projectBranchesListParams{
				WorkspaceID: workspaceID,
				ProjectID:   "missing-project",
			}))
		},
	} {
		result, rpcErr := call()
		if rpcErr == nil {
			t.Fatalf("expected %s to reject missing project", method)
		}
		if result != nil {
			t.Fatalf("expected no result for %s missing project, got %+v", method, result)
		}
	}
}

func TestProjectBranchRegisterReadyRequiresExplicitPatchQueueSubmit(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-project-branch-ready-auto-queue"
		actorID     = "operator-a"
		projectID   = "project-branch-ready-auto-queue"
		repoID      = "repo-main"
		workerID    = "worker-a"
		branchID    = "branch-ready"
		taskID      = "task-branch-ready-auto-queue"
		reviewKey   = "project.project-branch-ready-auto-queue.branch.branch-ready.review"
	)
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedProjectRolesWorkspace(t, ctx, store, workspaceID, actorID, workerID)
	if _, rpcErr := h.projectCreate(ctx, mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Branch Ready Auto Queue",
		CreatedBy:   actorID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.projectRepositoryUpsert(ctx, mustJSONRaw(projectRepositoryUpsertParams{
		WorkspaceID:      workspaceID,
		ProjectID:        projectID,
		ActorID:          actorID,
		RepoID:           repoID,
		RemoteURL:        "git@github.com:ExampleOrg/project-branch-ready-auto-queue.git",
		RemoteKind:       sqlite.ProjectRepositoryRemoteKindGitHub,
		DefaultBranch:    "main",
		RepoStatus:       sqlite.ProjectRepositoryStatusReady,
		IsCanonical:      true,
		CreatedByAgentID: workerID,
	})); rpcErr != nil {
		t.Fatalf("project.repository.upsert rpc error: %+v", rpcErr)
	}
	checkoutResult, rpcErr := h.projectCheckoutRegister(ctx, mustJSONRaw(projectCheckoutRegisterParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      actorID,
		RepoID:       repoID,
		MachineID:    "developer-desktop",
		AgentID:      workerID,
		LocalPath:    `C:\fixtures\agents\worker-a\project-branch-ready-auto-queue`,
		CheckoutKind: sqlite.ProjectCheckoutKindClone,
		BranchName:   "agent/worker-a/project-branch-ready-auto-queue",
		BaseBranch:   "main",
		Status:       sqlite.ProjectCheckoutStatusActive,
	}))
	if rpcErr != nil {
		t.Fatalf("project.checkout.register rpc error: %+v", rpcErr)
	}
	checkout := checkoutResult.(map[string]any)["checkout"].(sqlite.ProjectCheckoutRecord)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nReady for peer review.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	seedServerProjectBranchClaimForReady(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, branchID, taskID, `{"paths":["web/**"]}`)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)
	result, rpcErr := h.projectBranchRegister(ctx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        actorID,
		RepoID:         repoID,
		CheckoutID:     checkout.CheckoutID,
		AgentID:        workerID,
		BranchID:       branchID,
		BranchName:     "agent/worker-a/project-branch-ready-auto-queue",
		BranchKind:     sqlite.ProjectBranchKindFeature,
		BaseBranch:     "main",
		BaseSHA:        baseSHA,
		HeadSHA:        headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		ReviewDocKey:   reviewKey,
		ActiveTaskID:   taskID,
		ActiveClaimID:  taskID,
		Status:         sqlite.ProjectBranchStatusReadyForReview,
	}))
	if rpcErr != nil {
		t.Fatalf("project.branch.register ready rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	branch := payload["branch"].(sqlite.ProjectBranchRecord)
	if branch.Status != sqlite.ProjectBranchStatusReadyForReview ||
		branch.ReviewDocKey != reviewKey ||
		branch.HeadSHA != headSHA {
		t.Fatalf("unexpected READY_FOR_REVIEW branch receipt: %+v", branch)
	}
	if payload["mandatory_next_tool"] != "project_patch_queue_submit" ||
		payload["receipt_state"] != "branch_registry_ready_for_review" {
		t.Fatalf("expected explicit patch queue submit routing metadata, got %+v", payload)
	}
	if _, ok := payload["patch_queue_item"]; ok {
		t.Fatalf("project.branch.register must not return or create a patch queue item: %+v", payload)
	}
	if autoSubmitted, ok := payload["patch_queue_auto_submitted"].(bool); !ok || autoSubmitted {
		t.Fatalf("expected project.branch.register to report no patch queue auto-submit, got %+v", payload)
	}
	branchRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.branch.registered",
		EntityType:  "project_branch",
		EntityID:    branch.BranchID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.branch.registered"), branchRuntime, "project.branch.registered")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.branch.changed"), branchRuntime, "project.branch.changed")

	items, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		BranchID:    branch.BranchID,
	})
	if err != nil {
		t.Fatalf("list project patch queue items after READY_FOR_REVIEW: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("project.branch.register should not create patch queue items; got %+v", items)
	}
	queueEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.patch_queue.submitted",
	})
	if err != nil {
		t.Fatalf("list patch queue submitted events: %v", err)
	}
	if len(queueEvents) != 0 {
		t.Fatalf("project.branch.register should not emit project.patch_queue.submitted; got %+v", queueEvents)
	}

	submitResult, rpcErr := h.projectPatchQueueSubmit(ctx, mustJSONRaw(projectPatchQueueSubmitParams{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		ActorID:                  actorID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		ReviewDocKey:             reviewKey,
		TaskID:                   taskID,
		SessionID:                "session-branch-ready-auto-queue",
		RunID:                    "run-branch-ready-auto-queue",
		AgentID:                  workerID,
		PrincipalType:            "human",
		PrincipalID:              actorID,
		CapabilitySnapshotID:     "cap-branch-ready-auto-queue",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes:           map[string]string{},
		RepoLeaseID:              "lease-branch-ready-auto-queue",
		LeaseTerm:                7,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.submit rpc error: %+v", rpcErr)
	}
	submitPayload := submitResult.(map[string]any)
	item := submitPayload["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	reviewTask := submitPayload["patch_queue_review_task"].(sqlite.TaskStatus)
	if item.BranchID != branch.BranchID ||
		item.State != sqlite.ProjectPatchQueueStateProposed || item.RepoAuthorityMode != sqlite.ProjectPatchQueueAuthorityModeControlledQueue ||
		item.ContextDigest == "" || item.ReviewDocKey != reviewKey || item.HeadSHA != headSHA || item.AutoMerge {
		t.Fatalf("unexpected submitted patch queue item: branch=%+v item=%+v", branch, item)
	}
	if reviewTask.TaskID != projectPatchQueueReviewTaskID(item) ||
		reviewTask.ProjectID != projectID ||
		reviewTask.ProjectLane != "review" ||
		reviewTask.Status != "PENDING" ||
		reviewTask.RequiresProjectGate {
		t.Fatalf("unexpected patch queue review task: %+v", reviewTask)
	}
	queueRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.patch_queue.submitted",
		EntityType:  "project_patch_queue_item",
		EntityID:    item.QueueID + "/" + item.ItemID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.patch_queue.submitted"), queueRuntime, "project.patch_queue.submitted")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.patch_queue.changed"), queueRuntime, "project.patch_queue.changed")

	replayResult, rpcErr := h.projectBranchRegister(ctx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        actorID,
		RepoID:         repoID,
		CheckoutID:     checkout.CheckoutID,
		AgentID:        workerID,
		BranchID:       branch.BranchID,
		BranchName:     branch.BranchName,
		BranchKind:     sqlite.ProjectBranchKindFeature,
		BaseBranch:     "main",
		BaseSHA:        baseSHA,
		HeadSHA:        headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		ReviewDocKey:   reviewKey,
		ActiveTaskID:   taskID,
		ActiveClaimID:  taskID,
		Status:         sqlite.ProjectBranchStatusReadyForReview,
	}))
	if rpcErr != nil {
		t.Fatalf("project.branch.register ready replay rpc error: %+v", rpcErr)
	}
	replayPayload := replayResult.(map[string]any)
	if autoSubmitted, ok := replayPayload["patch_queue_auto_submitted"].(bool); !ok || autoSubmitted {
		t.Fatalf("expected replay to keep explicit patch queue submit separation, got %+v", replayPayload)
	}
	if created, ok := replayPayload["patch_queue_review_task_created"].(bool); !ok || created {
		t.Fatalf("expected replay not to create patch queue review task, got %+v", replayPayload)
	}
	if _, ok := replayPayload["patch_queue_item"]; ok {
		t.Fatalf("READY_FOR_REVIEW replay must not return patch queue item; got %+v", replayPayload)
	}
	items, err = store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		BranchID:    branch.BranchID,
	})
	if err != nil {
		t.Fatalf("list project patch queue items: %v", err)
	}
	if len(items) != 1 || items[0].QueueID != item.QueueID || items[0].ItemID != item.ItemID {
		t.Fatalf("expected exactly one live patch queue item after replay, got %+v", items)
	}
}

func TestProjectPatchQueueIntegrationRecordRPCWritesDurableReceipt(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-project-patch-queue-integration-rpc"
		actorID     = "operator-a"
		projectID   = "project-patch-queue-integration-rpc"
		repoID      = "repo-main"
		workerID    = "worker-a"
		reviewerID  = "reviewer-a"
		branchID    = "branch-ready"
		taskID      = "task-patch-queue-integration-rpc"
		reviewKey   = "project.project-patch-queue-integration-rpc.branch.branch-ready.review"
		decisionKey = "project.project-patch-queue-integration-rpc.patch_queue.branch-ready.decision"
	)
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	ctx := testAuthContext(workspaceID, "human", actorID)
	reviewerCtx := testAuthContext(workspaceID, "agent", reviewerID)
	seedProjectRolesWorkspace(t, ctx, store, workspaceID, actorID, workerID, reviewerID)
	if _, rpcErr := h.projectCreate(ctx, mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Patch Queue Integration RPC",
		CreatedBy:   actorID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               reviewerID,
		RoleType:              sqlite.ProjectRoleReviewer,
		ActorID:               actorID,
		ActorType:             "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "human", actorID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign reviewer role: %v", err)
	}
	if _, rpcErr := h.projectRepositoryUpsert(ctx, mustJSONRaw(projectRepositoryUpsertParams{
		WorkspaceID:      workspaceID,
		ProjectID:        projectID,
		ActorID:          actorID,
		RepoID:           repoID,
		RemoteURL:        "git@github.com:ExampleOrg/project-patch-queue-integration-rpc.git",
		RemoteKind:       sqlite.ProjectRepositoryRemoteKindGitHub,
		DefaultBranch:    "main",
		RepoStatus:       sqlite.ProjectRepositoryStatusReady,
		IsCanonical:      true,
		CreatedByAgentID: workerID,
	})); rpcErr != nil {
		t.Fatalf("project.repository.upsert rpc error: %+v", rpcErr)
	}
	checkoutResult, rpcErr := h.projectCheckoutRegister(ctx, mustJSONRaw(projectCheckoutRegisterParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      actorID,
		RepoID:       repoID,
		MachineID:    "developer-desktop",
		AgentID:      workerID,
		LocalPath:    `C:\fixtures\agents\worker-a\project-patch-queue-integration-rpc`,
		CheckoutKind: sqlite.ProjectCheckoutKindClone,
		BranchName:   "agent/worker-a/project-patch-queue-integration-rpc",
		BaseBranch:   "main",
		Status:       sqlite.ProjectCheckoutStatusActive,
	}))
	if rpcErr != nil {
		t.Fatalf("project.checkout.register rpc error: %+v", rpcErr)
	}
	checkout := checkoutResult.(map[string]any)["checkout"].(sqlite.ProjectCheckoutRecord)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nReady candidate.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	seedServerProjectBranchClaimForReady(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, branchID, taskID, `{"paths":["cmd/app.go"]}`)
	if _, rpcErr := h.projectBranchRegister(ctx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        actorID,
		RepoID:         repoID,
		CheckoutID:     checkout.CheckoutID,
		AgentID:        workerID,
		BranchID:       branchID,
		BranchName:     "agent/worker-a/project-patch-queue-integration-rpc",
		BranchKind:     sqlite.ProjectBranchKindFeature,
		BaseBranch:     "main",
		BaseSHA:        baseSHA,
		HeadSHA:        headSHA,
		WriteScopeJSON: `{"paths":["cmd/app.go"]}`,
		ReviewDocKey:   reviewKey,
		ActiveTaskID:   taskID,
		ActiveClaimID:  taskID,
		Status:         sqlite.ProjectBranchStatusReadyForReview,
	})); rpcErr != nil {
		t.Fatalf("project.branch.register rpc error: %+v", rpcErr)
	}
	submitResult, rpcErr := h.projectPatchQueueSubmit(ctx, mustJSONRaw(projectPatchQueueSubmitParams{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		ActorID:                  actorID,
		RepoID:                   repoID,
		BranchID:                 branchID,
		ReviewDocKey:             reviewKey,
		TaskID:                   taskID,
		SessionID:                "session-patch-queue-integration-rpc",
		RunID:                    "run-patch-queue-integration-rpc",
		AgentID:                  workerID,
		PrincipalType:            "human",
		PrincipalID:              actorID,
		CapabilitySnapshotID:     "cap-patch-queue-integration-rpc",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes:           map[string]string{"cmd/app.go": "sha256:cmd-app"},
		RepoLeaseID:              "lease-patch-queue-integration-rpc",
		LeaseTerm:                7,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.submit rpc error: %+v", rpcErr)
	}
	item := submitResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	claimResult, rpcErr := h.projectPatchQueueClaim(reviewerCtx, mustJSONRaw(projectPatchQueueClaimParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      reviewerID,
		QueueID:      item.QueueID,
		ItemID:       item.ItemID,
		LeaseSeconds: 600,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.claim rpc error: %+v", rpcErr)
	}
	claimed := claimResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      decisionKey,
		Title:       "Patch Queue Decision",
		Content:     "# Patch Queue Decision\n\nAccepted.",
		UpdatedBy:   reviewerID,
	}); err != nil {
		t.Fatalf("write decision doc: %v", err)
	}
	if _, rpcErr := h.projectPatchQueueDecision(reviewerCtx, mustJSONRaw(projectPatchQueueDecisionParams{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		ActorID:         reviewerID,
		QueueID:         item.QueueID,
		ItemID:          item.ItemID,
		Decision:        "ACCEPTED",
		DecisionDocKey:  decisionKey,
		DecisionSummary: "Accepted for integration.",
		ClaimToken:      claimed.ClaimToken,
	})); rpcErr != nil {
		t.Fatalf("project.patch_queue.decision rpc error: %+v", rpcErr)
	}

	result, rpcErr := h.projectPatchQueueIntegrationRecord(ctx, mustJSONRaw(projectPatchQueueIntegrationRecordParams{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ActorID:               actorID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		RepoID:                repoID,
		SourceBranchID:        branchID,
		SourceHeadSHA:         headSHA,
		TargetBranch:          "main",
		TargetHeadBefore:      baseSHA,
		TargetHeadAfter:       headSHA,
		RemoteTargetHeadAfter: headSHA,
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeIntegrated,
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		MergePerformed:        true,
		PushAttempted:         true,
		PushSucceeded:         true,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.integration_record rpc error: %+v", rpcErr)
	}
	integrated := result.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	if integrated.QueueID != item.QueueID || integrated.ItemID != item.ItemID || integrated.State != sqlite.ProjectPatchQueueStateIntegrated {
		t.Fatalf("unexpected integration record result: %+v", integrated)
	}
	event := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   sqlite.ProjectPatchQueueIntegratedEventType,
		EntityType:  "project_patch_queue_item",
		EntityID:    item.QueueID + "/" + item.ItemID,
	})
	if !strings.Contains(event.PayloadJSON, `"outcome":"integrated"`) || !strings.Contains(event.PayloadJSON, `"push_succeeded":"true"`) {
		t.Fatalf("integration receipt payload missing durable outcome: %s", event.PayloadJSON)
	}
}

func TestProjectPatchQueueSubmitAfterBlockedQueueCreatesExplicitRevisionItem(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID    = "ws-project-branch-ready-revision-queue"
		actorID        = "operator-a"
		projectID      = "project-branch-ready-revision-queue"
		repoID         = "repo-main"
		workerID       = "worker-a"
		branchID       = "branch-ready"
		taskID         = "task-branch-ready-revision-queue"
		revisionTaskID = "task-branch-ready-revision-queue-r2"
		reviewKey      = "project.project-branch-ready-revision-queue.branch.branch-ready.review"
		decisionKey    = "project.project-branch-ready-revision-queue.patchq.branch-ready.decision"
	)
	baseSHA := strings.Repeat("a", 40)
	firstHeadSHA := strings.Repeat("b", 40)
	revisedHeadSHA := strings.Repeat("c", 40)
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedProjectRolesWorkspace(t, ctx, store, workspaceID, actorID, workerID)
	if _, rpcErr := h.projectCreate(ctx, mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Branch Ready Revision Queue",
		CreatedBy:   actorID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.projectRepositoryUpsert(ctx, mustJSONRaw(projectRepositoryUpsertParams{
		WorkspaceID:      workspaceID,
		ProjectID:        projectID,
		ActorID:          actorID,
		RepoID:           repoID,
		RemoteURL:        "git@github.com:ExampleOrg/project-branch-ready-revision-queue.git",
		RemoteKind:       sqlite.ProjectRepositoryRemoteKindGitHub,
		DefaultBranch:    "main",
		RepoStatus:       sqlite.ProjectRepositoryStatusReady,
		IsCanonical:      true,
		CreatedByAgentID: workerID,
	})); rpcErr != nil {
		t.Fatalf("project.repository.upsert rpc error: %+v", rpcErr)
	}
	checkoutResult, rpcErr := h.projectCheckoutRegister(ctx, mustJSONRaw(projectCheckoutRegisterParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      actorID,
		RepoID:       repoID,
		MachineID:    "developer-desktop",
		AgentID:      workerID,
		LocalPath:    `C:\fixtures\agents\worker-a\project-branch-ready-revision-queue`,
		CheckoutKind: sqlite.ProjectCheckoutKindClone,
		BranchName:   "agent/worker-a/project-branch-ready-revision-queue",
		BaseBranch:   "main",
		Status:       sqlite.ProjectCheckoutStatusActive,
	}))
	if rpcErr != nil {
		t.Fatalf("project.checkout.register rpc error: %+v", rpcErr)
	}
	checkout := checkoutResult.(map[string]any)["checkout"].(sqlite.ProjectCheckoutRecord)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nInitial candidate.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	seedServerProjectBranchClaimForReady(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, branchID, taskID, `{"paths":["web/**"]}`)

	result, rpcErr := h.projectBranchRegister(ctx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        actorID,
		RepoID:         repoID,
		CheckoutID:     checkout.CheckoutID,
		AgentID:        workerID,
		BranchID:       branchID,
		BranchName:     "agent/worker-a/project-branch-ready-revision-queue",
		BranchKind:     sqlite.ProjectBranchKindFeature,
		BaseBranch:     "main",
		BaseSHA:        baseSHA,
		HeadSHA:        firstHeadSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		ReviewDocKey:   reviewKey,
		ActiveTaskID:   taskID,
		ActiveClaimID:  taskID,
		Status:         sqlite.ProjectBranchStatusReadyForReview,
	}))
	if rpcErr != nil {
		t.Fatalf("project.branch.register initial ready rpc error: %+v", rpcErr)
	}
	initialPayload := result.(map[string]any)
	if initialPayload["mandatory_next_tool"] != "project_patch_queue_submit" {
		t.Fatalf("expected READY_FOR_REVIEW to route to explicit patch queue submit, got %+v", initialPayload)
	}
	submitResult, rpcErr := h.projectPatchQueueSubmit(ctx, mustJSONRaw(projectPatchQueueSubmitParams{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		ActorID:                  actorID,
		RepoID:                   repoID,
		BranchID:                 branchID,
		ReviewDocKey:             reviewKey,
		TaskID:                   taskID,
		SessionID:                "session-branch-ready-revision-queue",
		RunID:                    "run-branch-ready-revision-queue",
		AgentID:                  workerID,
		PrincipalType:            "human",
		PrincipalID:              actorID,
		CapabilitySnapshotID:     "cap-branch-ready-revision-queue",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes:           map[string]string{},
		RepoLeaseID:              "lease-branch-ready-revision-queue",
		LeaseTerm:                7,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.submit initial rpc error: %+v", rpcErr)
	}
	firstItem := submitResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	claimResult, rpcErr := h.projectPatchQueueClaim(ctx, mustJSONRaw(projectPatchQueueClaimParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      actorID,
		QueueID:      firstItem.QueueID,
		ItemID:       firstItem.ItemID,
		LeaseSeconds: 600,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.claim rpc error: %+v", rpcErr)
	}
	claimed := claimResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      decisionKey,
		Title:       "Patch Queue Decision",
		Content:     "# Patch Queue Decision\n\nBlocked pending a concrete build fix.",
		UpdatedBy:   actorID,
	}); err != nil {
		t.Fatalf("write decision doc: %v", err)
	}
	if _, rpcErr := h.projectPatchQueueDecision(ctx, mustJSONRaw(projectPatchQueueDecisionParams{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		ActorID:         actorID,
		QueueID:         firstItem.QueueID,
		ItemID:          firstItem.ItemID,
		Decision:        "BLOCKED",
		DecisionDocKey:  decisionKey,
		DecisionSummary: "Blocked pending a concrete build fix.",
		ClaimToken:      claimed.ClaimToken,
	})); rpcErr != nil {
		t.Fatalf("project.patch_queue.decision rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.projectPatchQueueSubmit(ctx, mustJSONRaw(projectPatchQueueSubmitParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      actorID,
		RepoID:       repoID,
		BranchID:     branchID,
		ReviewDocKey: reviewKey,
	})); rpcErr == nil || !strings.Contains(rpcErr.Message, "same-head BLOCKED") {
		t.Fatalf("expected explicit same-head submit to require supersession evidence, got %+v", rpcErr)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nRevised candidate with build fix.",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("update review doc: %v", err)
	}
	seedServerProjectBranchClaimForReady(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, branchID, revisionTaskID, `{"paths":["web/**"]}`)

	revisionResult, rpcErr := h.projectBranchRegister(ctx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        actorID,
		RepoID:         repoID,
		CheckoutID:     checkout.CheckoutID,
		AgentID:        workerID,
		BranchID:       branchID,
		BranchName:     "agent/worker-a/project-branch-ready-revision-queue",
		BranchKind:     sqlite.ProjectBranchKindFeature,
		BaseBranch:     "main",
		BaseSHA:        baseSHA,
		HeadSHA:        revisedHeadSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		ReviewDocKey:   reviewKey,
		ActiveTaskID:   revisionTaskID,
		ActiveClaimID:  revisionTaskID,
		Status:         sqlite.ProjectBranchStatusReadyForReview,
	}))
	if rpcErr != nil {
		t.Fatalf("project.branch.register revised ready rpc error: %+v", rpcErr)
	}
	payload := revisionResult.(map[string]any)
	if payload["mandatory_next_tool"] != "project_patch_queue_submit" {
		t.Fatalf("expected revised READY_FOR_REVIEW to route to explicit patch queue submit, got %+v", payload)
	}
	revisionSubmitResult, rpcErr := h.projectPatchQueueSubmit(ctx, mustJSONRaw(projectPatchQueueSubmitParams{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		ActorID:                  actorID,
		RepoID:                   repoID,
		BranchID:                 branchID,
		ItemID:                   firstItem.ItemID + "-r2",
		ReviewDocKey:             reviewKey,
		TaskID:                   revisionTaskID,
		SessionID:                "session-branch-ready-revision-queue-r2",
		RunID:                    "run-branch-ready-revision-queue-r2",
		AgentID:                  workerID,
		PrincipalType:            "human",
		PrincipalID:              actorID,
		CapabilitySnapshotID:     "cap-branch-ready-revision-queue-r2",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes:           map[string]string{},
		RepoLeaseID:              "lease-branch-ready-revision-queue-r2",
		LeaseTerm:                8,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.submit revised rpc error: %+v", rpcErr)
	}
	revisionItem := revisionSubmitResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	if revisionItem.ItemID == firstItem.ItemID || !strings.HasSuffix(revisionItem.ItemID, "-r2") {
		t.Fatalf("expected explicit revised patch queue item id to advance from %s, got %+v", firstItem.ItemID, revisionItem)
	}
	if revisionItem.State != sqlite.ProjectPatchQueueStateProposed ||
		revisionItem.HeadSHA != revisedHeadSHA ||
		revisionItem.RepoAuthorityMode != sqlite.ProjectPatchQueueAuthorityModeControlledQueue ||
		revisionItem.ContextDigest == "" {
		t.Fatalf("unexpected revised patch queue item: %+v", revisionItem)
	}
	items, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		BranchID:    branchID,
	})
	if err != nil {
		t.Fatalf("list patch queue items: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected blocked original plus proposed revision item, got %+v", items)
	}
	states := map[string]string{}
	for _, item := range items {
		states[item.ItemID] = item.State
	}
	if states[firstItem.ItemID] != sqlite.ProjectPatchQueueStateBlocked || states[revisionItem.ItemID] != sqlite.ProjectPatchQueueStateProposed {
		t.Fatalf("unexpected patch queue item states after revision: %+v", states)
	}
}

func TestProjectPatchQueueSubmitRPCPublishesDurableCandidate(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-project-patch-queue-rpc"
		actorID     = "operator-a"
		projectID   = "project-patch-queue-rpc"
		repoID      = "repo-main"
		workerID    = "worker-a"
		branchID    = "branch-ready"
		taskID      = "task-patch-queue-rpc"
		reviewKey   = "project.project-patch-queue-rpc.branch.branch-ready.review"
	)
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	baseTreeHash := strings.Repeat("c", 40)
	ctx := testAuthContext(workspaceID, "human", actorID)
	agentCtx := testAuthContext(workspaceID, "agent", workerID)
	seedProjectRolesWorkspace(t, ctx, store, workspaceID, actorID, workerID)
	if _, rpcErr := h.projectCreate(ctx, mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Patch Queue RPC",
		CreatedBy:   actorID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               workerID,
		RoleType:              sqlite.ProjectRoleIntegrator,
		ActorID:               actorID,
		ActorType:             "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "human", actorID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign worker integrator role: %v", err)
	}
	if _, rpcErr := h.projectRepositoryUpsert(ctx, mustJSONRaw(projectRepositoryUpsertParams{
		WorkspaceID:      workspaceID,
		ProjectID:        projectID,
		ActorID:          actorID,
		RepoID:           repoID,
		RemoteURL:        "git@github.com:ExampleOrg/project-patch-queue-rpc.git",
		RemoteKind:       sqlite.ProjectRepositoryRemoteKindGitHub,
		DefaultBranch:    "main",
		RepoStatus:       sqlite.ProjectRepositoryStatusReady,
		IsCanonical:      true,
		CreatedByAgentID: workerID,
	})); rpcErr != nil {
		t.Fatalf("project.repository.upsert rpc error: %+v", rpcErr)
	}
	checkoutResult, rpcErr := h.projectCheckoutRegister(ctx, mustJSONRaw(projectCheckoutRegisterParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      actorID,
		RepoID:       repoID,
		MachineID:    "developer-desktop",
		AgentID:      workerID,
		LocalPath:    `C:\fixtures\agents\worker-a\project-patch-queue-rpc`,
		CheckoutKind: sqlite.ProjectCheckoutKindClone,
		BranchName:   "agent/worker-a/project-patch-queue-rpc",
		BaseBranch:   "main",
		Status:       sqlite.ProjectCheckoutStatusActive,
	}))
	if rpcErr != nil {
		t.Fatalf("project.checkout.register rpc error: %+v", rpcErr)
	}
	checkout := checkoutResult.(map[string]any)["checkout"].(sqlite.ProjectCheckoutRecord)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nRPC evidence",
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}
	seedServerProjectBranchClaimForReady(t, ctx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, branchID, taskID, `{"paths":["web/**"]}`)
	branchResult, rpcErr := h.projectBranchRegister(ctx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        actorID,
		RepoID:         repoID,
		CheckoutID:     checkout.CheckoutID,
		AgentID:        workerID,
		BranchID:       branchID,
		BranchName:     "agent/worker-a/project-patch-queue-rpc",
		BranchKind:     sqlite.ProjectBranchKindFeature,
		BaseBranch:     "main",
		BaseSHA:        baseSHA,
		HeadSHA:        headSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		ReviewDocKey:   reviewKey,
		ActiveTaskID:   taskID,
		ActiveClaimID:  taskID,
		Status:         sqlite.ProjectBranchStatusReadyForReview,
	}))
	if rpcErr != nil {
		t.Fatalf("project.branch.register ready rpc error: %+v", rpcErr)
	}
	branch := branchResult.(map[string]any)["branch"].(sqlite.ProjectBranchRecord)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)
	if _, rpcErr := h.projectPatchQueueSubmit(ctx, mustJSONRaw(projectPatchQueueSubmitParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     actorID,
		RepoID:      repoID,
		BranchID:    branch.BranchID,
		PathsetJSON: `{"paths":["api/**"]}`,
	})); rpcErr == nil || !strings.Contains(rpcErr.Message, "cannot widen") {
		t.Fatalf("expected widened project.patch_queue.submit pathset to fail, got %+v", rpcErr)
	}
	if _, rpcErr := h.projectPatchQueueSubmit(ctx, mustJSONRaw(projectPatchQueueSubmitParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     actorID,
		RepoID:      repoID,
		BranchID:    branch.BranchID,
		HeadSHA:     strings.Repeat("c", 40),
	})); rpcErr == nil || !strings.Contains(rpcErr.Message, "head_sha") {
		t.Fatalf("expected mismatched project.patch_queue.submit head_sha to fail, got %+v", rpcErr)
	}
	result, rpcErr := h.projectPatchQueueSubmit(ctx, mustJSONRaw(projectPatchQueueSubmitParams{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		ActorID:                  actorID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		ReviewDocKey:             reviewKey,
		RepoAuthorityMode:        sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		TaskID:                   taskID,
		SessionID:                "session-patch-queue-rpc",
		RunID:                    "run-patch-queue-rpc",
		AgentID:                  workerID,
		PrincipalType:            "agent",
		PrincipalID:              workerID,
		CapabilitySnapshotID:     "cap-patch-queue-rpc",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseTreeHash,
		BaseFileHashes:           map[string]string{},
		RepoLeaseID:              "lease-patch-queue-rpc",
		LeaseTerm:                7,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.submit rpc error: %+v", rpcErr)
	}
	item := result.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	if item.BranchID != branch.BranchID || item.State != sqlite.ProjectPatchQueueStateProposed || item.AutoMerge ||
		item.RepoAuthorityMode != sqlite.ProjectPatchQueueAuthorityModeControlledQueue || item.ContextDigest == "" {
		t.Fatalf("unexpected patch queue item: %+v", item)
	}
	if !strings.HasPrefix(item.QueueID, "patchq-") || !strings.HasPrefix(item.ItemID, "patchitem-") {
		t.Fatalf("expected default patch queue ids when omitted by RPC caller, got %+v", item)
	}
	if item.PrincipalType != "human" || item.PrincipalID != actorID {
		t.Fatalf("RPC must derive patch queue binding principal from auth context, got %+v", item)
	}
	runtime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.patch_queue.submitted",
		EntityType:  "project_patch_queue_item",
		EntityID:    item.QueueID + "/" + item.ItemID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.patch_queue.submitted"), runtime, "project.patch_queue.submitted")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.patch_queue.changed"), runtime, "project.patch_queue.changed")
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, runtime.PayloadJSON), "project.patch_queue.submit", workspaceID, "human", actorID, projectID, actorID)

	listResult, rpcErr := h.projectPatchQueueList(ctx, mustJSONRaw(projectPatchQueueListParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		BranchID:    branch.BranchID,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.list rpc error: %+v", rpcErr)
	}
	items := listResult.(map[string]any)["patch_queue_items"].([]sqlite.ProjectPatchQueueItemRecord)
	if len(items) != 1 || items[0].ItemID != item.ItemID {
		t.Fatalf("unexpected project.patch_queue.list payload: %+v", listResult)
	}
	claimResult, rpcErr := h.projectPatchQueueClaim(ctx, mustJSONRaw(projectPatchQueueClaimParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      actorID,
		QueueID:      item.QueueID,
		ItemID:       item.ItemID,
		LeaseSeconds: 600,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.claim rpc error: %+v", rpcErr)
	}
	claimed := claimResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	if claimed.State != sqlite.ProjectPatchQueueStateClaimed || claimed.ClaimToken == "" || claimed.ClaimedBy != actorID {
		t.Fatalf("unexpected claimed patch queue item: %+v", claimed)
	}
	claimRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.patch_queue.claimed",
		EntityType:  "project_patch_queue_item",
		EntityID:    item.QueueID + "/" + item.ItemID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.patch_queue.claimed"), claimRuntime, "project.patch_queue.claimed")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.patch_queue.changed"), claimRuntime, "project.patch_queue.changed")
	if _, rpcErr := h.projectPatchQueueDecision(ctx, mustJSONRaw(projectPatchQueueDecisionParams{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		ActorID:         actorID,
		QueueID:         item.QueueID,
		ItemID:          item.ItemID,
		Decision:        "ACCEPTED",
		DecisionSummary: "missing token should fail before a decision is recorded",
	})); rpcErr == nil || !strings.Contains(rpcErr.Message, "claim_token") {
		t.Fatalf("expected project.patch_queue.decision to require claim_token, got %+v", rpcErr)
	}
	releaseResult, rpcErr := h.projectPatchQueueRelease(ctx, mustJSONRaw(projectPatchQueueReleaseParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     actorID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ClaimToken:  claimed.ClaimToken,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.release rpc error: %+v", rpcErr)
	}
	released := releaseResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	if released.State != sqlite.ProjectPatchQueueStateProposed || released.ClaimToken != "" {
		t.Fatalf("unexpected released patch queue item: %+v", released)
	}
	claimResult, rpcErr = h.projectPatchQueueClaim(agentCtx, mustJSONRaw(projectPatchQueueClaimParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     workerID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.claim after release rpc error: %+v", rpcErr)
	}
	claimed = claimResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	mutationCh := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, mutationCh)

	boundResult, rpcErr := h.projectPatchQueueOperationBind(agentCtx, mustJSONRaw(projectPatchQueueOperationBindParams{
		WorkspaceID:       workspaceID,
		ProjectID:         projectID,
		ActorID:           workerID,
		QueueID:           item.QueueID,
		ItemID:            item.ItemID,
		OperationKind:     sqlite.ProjectPatchQueueOperationKindRepoPatchApply,
		MutationPathsJSON: `{"paths":["web/**"]}`,
		ClaimToken:        claimed.ClaimToken,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.operation_bind rpc error: %+v", rpcErr)
	}
	bound := boundResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	if bound.OperationID == "" || bound.OperationKind != sqlite.ProjectPatchQueueOperationKindRepoPatchApply || bound.OperationBoundBy != workerID {
		t.Fatalf("unexpected operation-bound patch queue item: %+v", bound)
	}
	operationRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.patch_queue.operation_bound",
		EntityType:  "project_patch_queue_item",
		EntityID:    item.QueueID + "/" + item.ItemID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, mutationCh, "project.patch_queue.operation_bound"), operationRuntime, "project.patch_queue.operation_bound")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, mutationCh, "project.patch_queue.changed"), operationRuntime, "project.patch_queue.changed")
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, operationRuntime.PayloadJSON), "project.patch_queue.operation_bind", workspaceID, "agent", workerID, projectID, workerID)

	candidateContent := "export const rpcPatchQueue = true;\n"
	candidateHash := repoauthority.PatchMaterializationContentDigest(candidateContent)
	casResult := repoauthority.EvaluateCASPatchApply(repoauthority.CASPatchApplyInput{
		Context: repoauthority.Context{
			Mode:        repoauthority.ModeControlledQueue,
			WorkspaceID: workspaceID,
			TaskID:      "task-patch-queue-rpc",
			SessionID:   "session-patch-queue-rpc",
			RunID:       "run-patch-queue-rpc",
			AgentID:     workerID,
			Principal: repoauthority.PrincipalRef{
				Type: "human",
				ID:   actorID,
			},
			CapabilitySnapshot: repoauthority.CapabilitySnapshotRef{
				ID:     "cap-patch-queue-rpc",
				Schema: "daemon_capability_snapshot.v1",
			},
			RepoRoot: checkout.LocalPath,
			Base: repoauthority.BaseIdentity{
				Ref:        "main",
				TreeHash:   baseTreeHash,
				FileHashes: map[string]string{},
			},
			Pathset: []string{"web/**"},
			Lease: repoauthority.LeaseRef{
				ID:   "lease-patch-queue-rpc",
				Term: 7,
			},
			PatchQueue: repoauthority.PatchQueueRef{
				QueueID: item.QueueID,
				ItemID:  item.ItemID,
			},
			Operation: repoauthority.OperationRef{
				ID:   bound.OperationID,
				Kind: bound.OperationKind,
			},
		},
		CurrentFileHashes:   map[string]string{},
		CandidateFileHashes: map[string]string{"web/app.js": candidateHash},
	})
	casResultPayload, rpcErr := h.projectPatchQueueCASRecord(agentCtx, mustJSONRaw(projectPatchQueueCASRecordParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     workerID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		CASResult:   casResult,
		TestEvidence: repoauthority.PatchQueueTestEvidence{
			Schema:       repoauthority.PatchQueueTestEvidenceSchemaVersion,
			Name:         "server-rpc",
			Command:      "go test ./internal/server -run TestProjectPatchQueueSubmitRPCPublishesDurableCandidate",
			Status:       repoauthority.PatchQueueTestStatusPassed,
			ExitCode:     0,
			OutputDigest: "sha256:" + strings.Repeat("7", 64),
		},
		ClaimToken: claimed.ClaimToken,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.cas_record rpc error: %+v", rpcErr)
	}
	casRecorded := casResultPayload.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	if !sqlite.ProjectPatchQueueCASEvidenceReady(casRecorded) ||
		casRecorded.CASRecordedBy != workerID ||
		casRecorded.CASStatus != repoauthority.CASPatchStatusApplied ||
		len(casRecorded.CASResult.Paths) != 1 ||
		casRecorded.CASResult.Paths[0].ChangeKind != repoauthority.CASPatchChangeAdd {
		t.Fatalf("unexpected CAS-recorded patch queue item: %+v", casRecorded)
	}
	casRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.patch_queue.cas_recorded",
		EntityType:  "project_patch_queue_item",
		EntityID:    item.QueueID + "/" + item.ItemID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, mutationCh, "project.patch_queue.cas_recorded"), casRuntime, "project.patch_queue.cas_recorded")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, mutationCh, "project.patch_queue.changed"), casRuntime, "project.patch_queue.changed")
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, casRuntime.PayloadJSON), "project.patch_queue.cas_record", workspaceID, "agent", workerID, projectID, workerID)

	materializedResult, rpcErr := h.projectPatchQueueMaterializationRecord(agentCtx, mustJSONRaw(projectPatchQueueMaterializationRecordParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     workerID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		Materialization: repoauthority.PatchMaterialization{
			Files: []repoauthority.PatchMaterializedFile{
				{Path: "web/app.js", Content: candidateContent},
			},
		},
		ClaimToken: claimed.ClaimToken,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.materialization_record rpc error: %+v", rpcErr)
	}
	materialized := materializedResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	if !sqlite.ProjectPatchQueueMaterializationReady(materialized) ||
		materialized.MaterializationRecordedBy != workerID ||
		len(materialized.Materialization.Files) != 1 ||
		materialized.Materialization.Files[0].ChangeKind != repoauthority.CASPatchChangeAdd ||
		materialized.Materialization.Files[0].ContentDigest != candidateHash {
		t.Fatalf("unexpected materialized patch queue item: %+v", materialized)
	}
	materializationRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.patch_queue.materialization_recorded",
		EntityType:  "project_patch_queue_item",
		EntityID:    item.QueueID + "/" + item.ItemID,
	})
	if strings.Contains(materializationRuntime.PayloadJSON, candidateContent) {
		t.Fatalf("materialization runtime event leaked raw content: %s", materializationRuntime.PayloadJSON)
	}
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, mutationCh, "project.patch_queue.materialization_recorded"), materializationRuntime, "project.patch_queue.materialization_recorded")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, mutationCh, "project.patch_queue.changed"), materializationRuntime, "project.patch_queue.changed")
	assertProjectRuntimePromptContext(t, decodeEventPayloadMap(t, materializationRuntime.PayloadJSON), "project.patch_queue.materialization_record", workspaceID, "agent", workerID, projectID, workerID)

	decisionKey := "project.project-patch-queue-rpc.patchq.decision"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      decisionKey,
		Title:       "Patch Queue RPC Decision",
		Content:     "# Patch Queue RPC Decision\n\nAccepted.\n\n" + serverAcceptedVisualPacketForPatchQueueTest(item, branch, "pass"),
		UpdatedBy:   workerID,
	}); err != nil {
		t.Fatalf("write decision doc: %v", err)
	}
	decisionResult, rpcErr := h.projectPatchQueueDecision(agentCtx, mustJSONRaw(projectPatchQueueDecisionParams{
		WorkspaceID:     workspaceID,
		ProjectID:       projectID,
		ActorID:         workerID,
		QueueID:         item.QueueID,
		ItemID:          item.ItemID,
		Decision:        "ACCEPTED",
		DecisionDocKey:  decisionKey,
		DecisionSummary: "Accepted after RPC integration evidence review.",
		ClaimToken:      claimed.ClaimToken,
	}))
	if rpcErr != nil {
		t.Fatalf("project.patch_queue.decision rpc error: %+v", rpcErr)
	}
	decided := decisionResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)
	if decided.State != sqlite.ProjectPatchQueueStateAccepted || decided.DecisionDocKey != decisionKey || decided.DecidedBy != workerID {
		t.Fatalf("unexpected decided patch queue item: %+v", decided)
	}
	decisionRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "project.patch_queue.accepted",
		EntityType:  "project_patch_queue_item",
		EntityID:    item.QueueID + "/" + item.ItemID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.patch_queue.accepted"), decisionRuntime, "project.patch_queue.accepted")
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "project.patch_queue.changed"), decisionRuntime, "project.patch_queue.changed")
}

func TestProjectRepositoryUpsertRequiresLeadForAgentPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-project-git-agent-lead"
		agentID     = "agent-lead-candidate"
		projectID   = "project-git-agent-lead"
		repoID      = "repo-main"
	)
	ctx := testAuthContext(workspaceID, "agent", agentID)
	seedProjectRolesWorkspace(t, ctx, store, workspaceID, agentID, agentID)
	if _, rpcErr := h.projectCreate(ctx, mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Git Agent Lead",
		CreatedBy:   agentID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}

	if _, rpcErr := h.projectRepositoryUpsert(ctx, mustJSONRaw(projectRepositoryUpsertParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     agentID,
		RepoID:      repoID,
		RemoteKind:  sqlite.ProjectRepositoryRemoteKindGitHub,
		RepoStatus:  sqlite.ProjectRepositoryStatusReady,
	})); rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected non-lead agent repo upsert to be permission denied, got %+v", rpcErr)
	}

	if _, rpcErr := h.projectLeadClaim(ctx, mustJSONRaw(projectLeadClaimParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      agentID,
		AgentID:      agentID,
		LeaseSeconds: 900,
		Summary:      "lead owns repo materialization",
	})); rpcErr != nil {
		t.Fatalf("project.lead.claim rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.projectRepositoryUpsert(ctx, mustJSONRaw(projectRepositoryUpsertParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     agentID,
		RepoID:      repoID,
		RemoteKind:  sqlite.ProjectRepositoryRemoteKindGitHub,
		RepoStatus:  sqlite.ProjectRepositoryStatusReady,
	})); rpcErr != nil {
		t.Fatalf("lead agent repo upsert rpc error: %+v", rpcErr)
	}
}

func TestProjectCheckoutRegisterScopesAgentPrincipal(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-project-git-agent-checkout"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		projectID   = "project-git-agent-checkout"
		repoID      = "repo-main"
	)
	leadCtx := testAuthContext(workspaceID, "agent", leadID)
	workerCtx := testAuthContext(workspaceID, "agent", workerID)
	seedProjectRolesWorkspace(t, leadCtx, store, workspaceID, leadID, leadID, workerID)
	if _, rpcErr := h.projectCreate(leadCtx, mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Git Agent Checkout",
		CreatedBy:   leadID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.projectLeadClaim(leadCtx, mustJSONRaw(projectLeadClaimParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      leadID,
		AgentID:      leadID,
		LeaseSeconds: 900,
		Summary:      "lead owns repo materialization",
	})); rpcErr != nil {
		t.Fatalf("project.lead.claim rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.projectRepositoryUpsert(leadCtx, mustJSONRaw(projectRepositoryUpsertParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     leadID,
		RepoID:      repoID,
		RemoteKind:  sqlite.ProjectRepositoryRemoteKindGitHub,
		RepoStatus:  sqlite.ProjectRepositoryStatusReady,
	})); rpcErr != nil {
		t.Fatalf("lead agent repo upsert rpc error: %+v", rpcErr)
	}

	checkoutResult, rpcErr := h.projectCheckoutRegister(workerCtx, mustJSONRaw(projectCheckoutRegisterParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      workerID,
		RepoID:       repoID,
		MachineID:    "developer-desktop",
		LocalPath:    `C:\fixtures\agents\worker-agent\agent-checkout`,
		CheckoutKind: sqlite.ProjectCheckoutKindClone,
		Status:       sqlite.ProjectCheckoutStatusActive,
	}))
	if rpcErr != nil {
		t.Fatalf("worker checkout register rpc error: %+v", rpcErr)
	}
	checkout := checkoutResult.(map[string]any)["checkout"].(sqlite.ProjectCheckoutRecord)
	if checkout.AgentID != workerID {
		t.Fatalf("agent principal checkout should default agent_id to actor, got %+v", checkout)
	}
	branchResult, rpcErr := h.projectBranchRegister(workerCtx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     workerID,
		RepoID:      repoID,
		CheckoutID:  checkout.CheckoutID,
		BranchName:  "agent/worker-agent/agent-checkout",
		Status:      sqlite.ProjectBranchStatusActive,
	}))
	if rpcErr != nil {
		t.Fatalf("worker branch register rpc error: %+v", rpcErr)
	}
	branch := branchResult.(map[string]any)["branch"].(sqlite.ProjectBranchRecord)
	if branch.AgentID != workerID {
		t.Fatalf("agent principal branch should default agent_id to actor, got %+v", branch)
	}

	if _, rpcErr := h.projectCheckoutRegister(workerCtx, mustJSONRaw(projectCheckoutRegisterParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     workerID,
		RepoID:      repoID,
		MachineID:   "developer-desktop",
		AgentID:     leadID,
		LocalPath:   `C:\fixtures\agents\lead-agent\borrowed-checkout`,
		Status:      sqlite.ProjectCheckoutStatusActive,
	})); rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected agent checkout for another agent to be permission denied, got %+v", rpcErr)
	}
	if _, rpcErr := h.projectBranchRegister(workerCtx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     workerID,
		RepoID:      repoID,
		AgentID:     leadID,
		BranchName:  "agent/lead-agent/borrowed-branch",
		Status:      sqlite.ProjectBranchStatusActive,
	})); rpcErr == nil || rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected agent branch for another agent to be permission denied, got %+v", rpcErr)
	}
	if _, rpcErr := h.projectBranchRegister(leadCtx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     leadID,
		RepoID:      repoID,
		BranchID:    branch.BranchID,
		CheckoutID:  branch.CheckoutID,
		AgentID:     workerID,
		BranchName:  branch.BranchName,
		Status:      sqlite.ProjectBranchStatusMerged,
	})); rpcErr == nil || rpcErr.Code == errCodePermissionDenied {
		t.Fatalf("expected cross-agent MERGED close to reach storage authority checks, got %+v", rpcErr)
	}
}

func TestProjectBranchRegisterAllowsIntegratorToMarkPeerBranchMerged(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID  = "ws-project-branch-integrator-merge"
		projectID    = "project-branch-integrator-merge"
		repoID       = "repo-main"
		reviewKey    = "project.project-branch-integrator-merge.branch.review"
		branchID     = "branch-worker-integrator-merge"
		taskID       = "task-branch-integrator-merge"
		leadID       = "lead-agent"
		workerID     = "worker-agent"
		integratorID = "integrator-agent"
		observerID   = "observer-agent"
	)
	baseSHA := strings.Repeat("a", 40)
	headSHA := strings.Repeat("b", 40)
	leadCtx := testAuthContext(workspaceID, "agent", leadID)
	workerCtx := testAuthContext(workspaceID, "agent", workerID)
	integratorCtx := testAuthContext(workspaceID, "agent", integratorID)
	observerCtx := testAuthContext(workspaceID, "agent", observerID)
	seedProjectRolesWorkspace(t, leadCtx, store, workspaceID, leadID, leadID, workerID, integratorID, observerID)
	if _, rpcErr := h.projectCreate(leadCtx, mustJSONRaw(projectCreateParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Branch Integrator Merge",
		CreatedBy:   leadID,
	})); rpcErr != nil {
		t.Fatalf("project.create rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.projectLeadClaim(leadCtx, mustJSONRaw(projectLeadClaimParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      leadID,
		AgentID:      leadID,
		LeaseSeconds: 900,
		Summary:      "lead owns coordination",
	})); rpcErr != nil {
		t.Fatalf("project.lead.claim rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.projectRepositoryUpsert(leadCtx, mustJSONRaw(projectRepositoryUpsertParams{
		WorkspaceID:       workspaceID,
		ProjectID:         projectID,
		ActorID:           leadID,
		RepoID:            repoID,
		RemoteURL:         "git@github.com:ExampleOrg/project-branch-integrator-merge.git",
		RemoteKind:        sqlite.ProjectRepositoryRemoteKindGitHub,
		DefaultBranch:     "main",
		IntegrationBranch: "main",
		RepoStatus:        sqlite.ProjectRepositoryStatusReady,
	})); rpcErr != nil {
		t.Fatalf("project.repository.upsert rpc error: %+v", rpcErr)
	}
	checkoutResult, rpcErr := h.projectCheckoutRegister(workerCtx, mustJSONRaw(projectCheckoutRegisterParams{
		WorkspaceID:  workspaceID,
		ProjectID:    projectID,
		ActorID:      workerID,
		RepoID:       repoID,
		MachineID:    "developer-desktop",
		LocalPath:    `C:\fixtures\agents\worker-agent\integrator-merge`,
		CheckoutKind: sqlite.ProjectCheckoutKindClone,
		Status:       sqlite.ProjectCheckoutStatusActive,
	}))
	if rpcErr != nil {
		t.Fatalf("worker checkout register rpc error: %+v", rpcErr)
	}
	checkout := checkoutResult.(map[string]any)["checkout"].(sqlite.ProjectCheckoutRecord)
	if err := store.UpsertWorkspaceDoc(integratorCtx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Worker Branch Review",
		Content:     "Review-ready branch evidence for integrator merge RPC coverage.",
		UpdatedBy:   workerID,
		PromptContextEnvelope: sqlite.BuildWorkspaceDocPromptContextEnvelope(
			"workspace.doc.put", "server_rpc", workspaceID, "agent", workerID,
		),
		PromptContextSurface: "workspace.doc.put",
	}); err != nil {
		t.Fatalf("seed review doc: %v", err)
	}
	seedServerProjectBranchClaimForReady(t, workerCtx, store, workspaceID, projectID, repoID, workerID, checkout.CheckoutID, branchID, taskID, `{"paths":["web/**"]}`)
	branchResult, rpcErr := h.projectBranchRegister(workerCtx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        workerID,
		RepoID:         repoID,
		CheckoutID:     checkout.CheckoutID,
		BranchID:       branchID,
		BranchName:     "agent/worker-agent/integrator-merge",
		BranchKind:     sqlite.ProjectBranchKindFeature,
		BaseBranch:     "main",
		HeadSHA:        headSHA,
		BaseSHA:        baseSHA,
		WriteScopeJSON: `{"paths":["web/**"]}`,
		ReviewDocKey:   reviewKey,
		ActiveTaskID:   taskID,
		ActiveClaimID:  taskID,
		Status:         sqlite.ProjectBranchStatusReadyForReview,
	}))
	if rpcErr != nil {
		t.Fatalf("worker branch register rpc error: %+v", rpcErr)
	}
	branch := branchResult.(map[string]any)["branch"].(sqlite.ProjectBranchRecord)
	if branchResult.(map[string]any)["mandatory_next_tool"] != "project_patch_queue_submit" {
		t.Fatalf("expected worker branch register to require explicit patch queue submit, got %+v", branchResult)
	}
	submitResult, rpcErr := h.projectPatchQueueSubmit(workerCtx, mustJSONRaw(projectPatchQueueSubmitParams{
		WorkspaceID:              workspaceID,
		ProjectID:                projectID,
		ActorID:                  workerID,
		RepoID:                   repoID,
		BranchID:                 branch.BranchID,
		ReviewDocKey:             reviewKey,
		TaskID:                   taskID,
		SessionID:                "session-branch-integrator-merge",
		RunID:                    "run-branch-integrator-merge",
		AgentID:                  workerID,
		PrincipalType:            "agent",
		PrincipalID:              workerID,
		CapabilitySnapshotID:     "cap-branch-integrator-merge",
		CapabilitySnapshotSchema: "daemon_capability_snapshot.v1",
		RepoRoot:                 checkout.LocalPath,
		BaseTreeHash:             baseSHA,
		BaseFileHashes:           map[string]string{},
		RepoLeaseID:              "lease-branch-integrator-merge",
		LeaseTerm:                7,
	}))
	if rpcErr != nil {
		t.Fatalf("worker patch queue submit rpc error: %+v", rpcErr)
	}
	item := submitResult.(map[string]any)["patch_queue_item"].(sqlite.ProjectPatchQueueItemRecord)

	if _, rpcErr := h.projectBranchRegister(observerCtx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        observerID,
		BranchID:       branch.BranchID,
		RepoID:         repoID,
		CheckoutID:     checkout.CheckoutID,
		AgentID:        workerID,
		BranchName:     branch.BranchName,
		BranchKind:     sqlite.ProjectBranchKindFeature,
		BaseBranch:     "main",
		WriteScopeJSON: branch.WriteScopeJSON,
		Status:         sqlite.ProjectBranchStatusMerged,
	})); rpcErr == nil {
		t.Fatal("expected unprivileged peer branch merge registration to fail")
	}
	if _, rpcErr := h.projectRoleAssign(leadCtx, mustJSONRaw(projectRoleAssignParams{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorID:     leadID,
		AgentID:     integratorID,
		RoleType:    sqlite.ProjectRoleIntegrator,
		Summary:     "integrates accepted peer branches",
	})); rpcErr != nil {
		t.Fatalf("project.role.assign integrator rpc error: %+v", rpcErr)
	}
	if _, rpcErr := h.projectBranchRegister(integratorCtx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        integratorID,
		BranchID:       branch.BranchID,
		RepoID:         repoID,
		CheckoutID:     checkout.CheckoutID,
		AgentID:        workerID,
		BranchName:     branch.BranchName,
		BranchKind:     sqlite.ProjectBranchKindFeature,
		BaseBranch:     "main",
		HeadSHA:        headSHA,
		BaseSHA:        baseSHA,
		WriteScopeJSON: branch.WriteScopeJSON,
		ReviewDocKey:   reviewKey,
		Status:         sqlite.ProjectBranchStatusMerged,
	})); rpcErr == nil {
		t.Fatal("expected integrator branch merge registration to wait for ACCEPTED patch queue evidence")
	}
	claim, _, err := store.ClaimProjectPatchQueueItemWithEvent(integratorCtx, sqlite.ProjectPatchQueueClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		ActorID:               integratorID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.claim", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.claim",
	})
	if err != nil {
		t.Fatalf("claim patch queue item: %v", err)
	}
	visualDecisionKey := "project." + projectID + ".patchq.merge_authority.visual_acceptance"
	if err := store.UpsertWorkspaceDoc(integratorCtx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      visualDecisionKey,
		Title:       "Patch Queue Merge Authority Visual Acceptance",
		Content:     serverAcceptedVisualPacketForPatchQueueTest(item, branch, "pass"),
		UpdatedBy:   integratorID,
	}); err != nil {
		t.Fatalf("write visual acceptance decision doc: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(integratorCtx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		Decision:              sqlite.ProjectPatchQueueStateAccepted,
		DecisionDocKey:        visualDecisionKey,
		DecisionSummary:       "Accepted for integration.",
		ClaimToken:            claim.ClaimToken,
		ActorID:               integratorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", integratorID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); err != nil {
		t.Fatalf("accept patch queue item: %v", err)
	}
	if _, rpcErr := h.projectBranchRegister(integratorCtx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        integratorID,
		BranchID:       branch.BranchID,
		RepoID:         repoID,
		CheckoutID:     checkout.CheckoutID,
		AgentID:        workerID,
		BranchName:     branch.BranchName,
		BranchKind:     sqlite.ProjectBranchKindFeature,
		BaseBranch:     "main",
		HeadSHA:        headSHA,
		BaseSHA:        baseSHA,
		WriteScopeJSON: branch.WriteScopeJSON,
		ReviewDocKey:   reviewKey,
		Status:         sqlite.ProjectBranchStatusMerged,
	})); rpcErr == nil || !strings.Contains(rpcErr.Message, "durable integrated patch queue receipt") {
		t.Fatalf("expected integrator branch merge registration to wait for integrated receipt, got %+v", rpcErr)
	}
	if _, rpcErr := h.projectPatchQueueIntegrationRecord(integratorCtx, mustJSONRaw(projectPatchQueueIntegrationRecordParams{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ActorID:               integratorID,
		QueueID:               item.QueueID,
		ItemID:                item.ItemID,
		RepoID:                repoID,
		SourceBranchID:        branch.BranchID,
		SourceHeadSHA:         headSHA,
		TargetBranch:          "main",
		TargetHeadBefore:      baseSHA,
		TargetHeadAfter:       headSHA,
		RemoteTargetHeadAfter: headSHA,
		Outcome:               sqlite.ProjectPatchQueueIntegrationOutcomeIntegrated,
		IntegrationMode:       sqlite.ProjectPatchQueueIntegrationModeDirectMerge,
		MergePerformed:        true,
		PushAttempted:         true,
		PushSucceeded:         true,
	})); rpcErr != nil {
		t.Fatalf("project.patch_queue.integration_record rpc error: %+v", rpcErr)
	}
	mergedResult, rpcErr := h.projectBranchRegister(integratorCtx, mustJSONRaw(projectBranchRegisterParams{
		WorkspaceID:    workspaceID,
		ProjectID:      projectID,
		ActorID:        integratorID,
		BranchID:       branch.BranchID,
		RepoID:         repoID,
		CheckoutID:     checkout.CheckoutID,
		AgentID:        workerID,
		BranchName:     branch.BranchName,
		BranchKind:     sqlite.ProjectBranchKindFeature,
		BaseBranch:     "main",
		HeadSHA:        headSHA,
		BaseSHA:        baseSHA,
		WriteScopeJSON: branch.WriteScopeJSON,
		ReviewDocKey:   reviewKey,
		Status:         sqlite.ProjectBranchStatusMerged,
	}))
	if rpcErr != nil {
		t.Fatalf("integrator should mark peer branch merged through RPC: %+v", rpcErr)
	}
	merged := mergedResult.(map[string]any)["branch"].(sqlite.ProjectBranchRecord)
	if merged.Status != sqlite.ProjectBranchStatusMerged || merged.AgentID != workerID || merged.UpdatedBy != integratorID {
		t.Fatalf("unexpected merged peer branch evidence: %+v", merged)
	}
}

func serverAcceptedVisualPacketForPatchQueueTest(item sqlite.ProjectPatchQueueItemRecord, branch sqlite.ProjectBranchRecord, verdict string) string {
	return strings.Join([]string{
		"schema: rhizome_visual_acceptance_v1",
		"visual_verdict: " + verdict,
		"product_intent:",
		"  acceptance_criteria: AC-server-rpc",
		"  core_user_promise: user can open the changed UI, complete the primary interaction, and inspect the resulting state.",
		"provenance:",
		"  queue_id: " + item.QueueID,
		"  item_id: " + item.ItemID,
		"  branch_id: " + branch.BranchID,
		"  branch_name: " + branch.BranchName,
		"  head_sha: " + item.HeadSHA,
		"  observed_url: http://127.0.0.1:51955/",
		"  validation_checkout: C:/fixtures/agents/worker-a/project-patch-queue-rpc",
		"viewport_matrix:",
		"  desktop: 1440x900",
		"  mobile: 390x844",
		"state_evidence:",
		"  initial_state: screenshot_path C:/tmp/rpc-initial.png",
		"  mobile_state: screenshot_path C:/tmp/rpc-mobile.png",
		"  primary_flow: screenshot_path C:/tmp/rpc-primary.png",
		"  result_state: screenshot_path C:/tmp/rpc-result.png",
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
