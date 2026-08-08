package sqlite_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestProjectBranchReadyRequiresSourceFidelityTrace(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-source-fidelity-ready"
		projectID   = "project-source-fidelity-ready"
		leadID      = "alpha"
		ownerID     = "beta"
		repoID      = "repo-main"
		reviewKey   = "project.project-source-fidelity-ready.branch.branch-ready.review"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/**"]}`)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\beta\source-fidelity-ready`)
	_, taskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, ownerID, "branch-ready", "agent/beta/source-fidelity-ready", `{"paths":["src/app.ts"]}`)
	writeProjectSourceRefsForTest(t, ctx, store, workspaceID, projectID, leadID, "run.clearpress.operator-spec")
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nReady for patch queue decision.",
		UpdatedBy:   ownerID,
	}); err != nil {
		t.Fatalf("write review doc: %v", err)
	}

	_, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              "branch-ready",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/beta/source-fidelity-ready",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               strings.Repeat("b", 40),
		WriteScopeJSON:        `{"paths":["src/app.ts"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) || !strings.Contains(err.Error(), "source_requirements_trace") {
		t.Fatalf("expected READY_FOR_REVIEW to require source requirements trace, got %v", err)
	}
}

func TestProjectBranchReadyAndPatchQueueSubmitBindSourceFidelity(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-source-fidelity-submit"
		projectID   = "project-source-fidelity-submit"
		leadID      = "alpha"
		ownerID     = "beta"
		repoID      = "repo-main"
		reviewKey   = "project.project-source-fidelity-submit.branch.branch-ready.review"
		sourceKey   = "run.clearpress.operator-spec"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/**"]}`)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\beta\source-fidelity-submit`)
	_, taskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, ownerID, "branch-ready", "agent/beta/source-fidelity-submit", `{"paths":["src/app.ts"]}`)
	writeProjectSourceRefsForTest(t, ctx, store, workspaceID, projectID, leadID, sourceKey)
	writeProjectSourceTraceForTest(t, ctx, store, workspaceID, projectID, leadID, sourceKey)
	writeSourceFidelityReviewDocForTest(t, ctx, store, workspaceID, projectID, reviewKey, ownerID)

	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              "branch-ready",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/beta/source-fidelity-submit",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               strings.Repeat("b", 40),
		WriteScopeJSON:        `{"paths":["src/app.ts"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register source-fidelity ready branch: %v", err)
	}
	beforeSubmit, err := store.ListProjectPatchQueueItems(ctx, sqlite.ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		BranchID:    branch.BranchID,
	})
	if err != nil {
		t.Fatalf("list patch queue before submit: %v", err)
	}
	if len(beforeSubmit) != 0 {
		t.Fatalf("READY_FOR_REVIEW branch registration should not create patch queue items, got %+v", beforeSubmit)
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
		t.Fatalf("submit source-fidelity branch to patch queue: %v", err)
	}
	if item.State != sqlite.ProjectPatchQueueStateProposed || item.ReviewDocKey != reviewKey {
		t.Fatalf("unexpected patch queue item: %+v", item)
	}
}

func TestProjectBranchReadyAcceptsInlineSourceRequirementsTrace(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-source-fidelity-inline"
		projectID   = "project-source-fidelity-inline"
		leadID      = "alpha"
		ownerID     = "beta"
		repoID      = "repo-main"
		reviewKey   = "project.project-source-fidelity-inline.branch.branch-ready.review"
		sourceKey   = "run.clearpress.operator-spec"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/**"]}`)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\beta\source-fidelity-inline`)
	_, taskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, ownerID, "branch-ready", "agent/beta/source-fidelity-inline", `{"paths":["src/app.ts"]}`)
	writeProjectSourceRefsForTest(t, ctx, store, workspaceID, projectID, leadID, sourceKey)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "project." + projectID + ".source_requirements_trace",
		Title:       "Source Requirements Trace",
		Content: "# Source Requirements Trace\n\n" +
			"rhizome_source_requirements_trace_v1:\n" +
			"  source_doc_keys:\n" +
			"    - " + sourceKey + "\n" +
			"  acceptance_critical_anchors:\n" +
			"    - Telegraph-like article editor\n" +
			"  acceptance_criteria_refs:\n" +
			"    - CP-01 article creation\n" +
			"  adjacent_wrong_products_non_goals:\n" +
			"    - marketing landing\n",
		UpdatedBy: leadID,
	}); err != nil {
		t.Fatalf("write inline source trace: %v", err)
	}
	writeSourceFidelityReviewDocForTest(t, ctx, store, workspaceID, projectID, reviewKey, ownerID)

	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              "branch-ready",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/beta/source-fidelity-inline",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               strings.Repeat("b", 40),
		WriteScopeJSON:        `{"paths":["src/app.ts"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register READY_FOR_REVIEW with inline source trace: %v", err)
	}
	if branch.Status != sqlite.ProjectBranchStatusReadyForReview || branch.ReviewDocKey != reviewKey {
		t.Fatalf("unexpected ready branch: %+v", branch)
	}
}

func TestProjectBranchReadyAcceptsSchemaFieldSourceRequirementsTrace(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-source-fidelity-schema-field"
		projectID   = "project-source-fidelity-schema-field"
		leadID      = "alpha"
		ownerID     = "beta"
		repoID      = "repo-main"
		reviewKey   = "project.project-source-fidelity-schema-field.branch.branch-ready.review"
		sourceKey   = "run.clearpress.operator-spec"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/**"]}`)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\beta\source-fidelity-schema-field`)
	_, taskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, ownerID, "branch-ready", "agent/beta/source-fidelity-schema-field", `{"paths":["src/app.ts"]}`)
	writeProjectSourceRefsForTest(t, ctx, store, workspaceID, projectID, leadID, sourceKey)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "project." + projectID + ".source_requirements_trace",
		Title:       "Source Requirements Trace",
		Content: "schema: rhizome_source_requirements_trace_v1\n" +
			"project_id: " + projectID + "\n" +
			"source_doc_keys:\n" +
			"  - " + sourceKey + "\n" +
			"  - project." + projectID + ".acceptance_criteria\n" +
			"source_fidelity_status: planning_preserved\n" +
			"acceptance_criteria_refs:\n" +
			"  - AC-01\n" +
			"acceptance_critical_anchors:\n" +
			"  - anchor: article_editor\n" +
			"    mapped_acceptance_ids: [AC-01]\n" +
			"    implementation_note: Preserve the writing-first editor identity.\n" +
			"per_source_anchors:\n" +
			"  " + sourceKey + ":\n" +
			"    - Telegraph-like article editor identity\n" +
			"  project." + projectID + ".acceptance_criteria:\n" +
			"    - AC-01 article creation acceptance\n" +
			"adjacent_wrong_products:\n" +
			"  - marketing landing\n" +
			"non_goals:\n" +
			"  - real OAuth\n",
		UpdatedBy: leadID,
	}); err != nil {
		t.Fatalf("write schema-field source trace: %v", err)
	}
	writeSourceFidelityReviewDocForTest(t, ctx, store, workspaceID, projectID, reviewKey, ownerID)

	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              "branch-ready",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/beta/source-fidelity-schema-field",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               strings.Repeat("b", 40),
		WriteScopeJSON:        `{"paths":["src/app.ts"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register READY_FOR_REVIEW with schema-field source trace: %v", err)
	}
	if branch.Status != sqlite.ProjectBranchStatusReadyForReview || branch.ReviewDocKey != reviewKey {
		t.Fatalf("unexpected ready branch: %+v", branch)
	}
}

func TestProjectBranchReadyRejectsMultiSourceTraceWithoutPerSourceAnchors(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-source-fidelity-multi-source"
		projectID   = "project-source-fidelity-multi-source"
		leadID      = "alpha"
		ownerID     = "beta"
		repoID      = "repo-main"
		reviewKey   = "project.project-source-fidelity-multi-source.branch.branch-ready.review"
		sourceKey   = "run.clearpress.operator-spec"
		patchKey    = "run.clearpress.operator-correction"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/**"]}`)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\beta\source-fidelity-multi-source`)
	_, taskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, ownerID, "branch-ready", "agent/beta/source-fidelity-multi-source", `{"paths":["src/app.ts"]}`)
	writeProjectSourceRefsForTest(t, ctx, store, workspaceID, projectID, leadID, sourceKey, patchKey)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "project." + projectID + ".source_requirements_trace",
		Title:       "Source Requirements Trace",
		Content: "schema: rhizome_source_requirements_trace_v1\n" +
			"project_id: " + projectID + "\n" +
			"source_doc_keys:\n" +
			"  - " + sourceKey + "\n" +
			"  - " + patchKey + "\n" +
			"acceptance_critical_anchors:\n" +
			"  - Telegraph-like article editor\n" +
			"  - public article route\n" +
			"acceptance_criteria_refs:\n" +
			"  - AC-01\n" +
			"adjacent_wrong_products:\n" +
			"  - marketing landing\n",
		UpdatedBy: leadID,
	}); err != nil {
		t.Fatalf("write weak multi-source trace: %v", err)
	}
	writeSourceFidelityReviewDocForTest(t, ctx, store, workspaceID, projectID, reviewKey, ownerID)

	_, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              "branch-ready",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/beta/source-fidelity-multi-source",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               strings.Repeat("b", 40),
		WriteScopeJSON:        `{"paths":["src/app.ts"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) || !strings.Contains(err.Error(), "source_requirements_trace") {
		t.Fatalf("expected READY_FOR_REVIEW to reject multi-source trace without per-source anchors, got %v", err)
	}
}

func TestProjectBranchReadyRejectsTraceWithoutSourceDocKeysField(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-source-fidelity-trace-binding"
		projectID   = "project-source-fidelity-trace-binding"
		leadID      = "alpha"
		ownerID     = "beta"
		repoID      = "repo-main"
		reviewKey   = "project.project-source-fidelity-trace-binding.branch.branch-ready.review"
		sourceKey   = "run.clearpress.operator-spec"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/**"]}`)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\beta\source-fidelity-trace-binding`)
	_, taskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, ownerID, "branch-ready", "agent/beta/source-fidelity-trace-binding", `{"paths":["src/app.ts"]}`)
	writeProjectSourceRefsForTest(t, ctx, store, workspaceID, projectID, leadID, sourceKey)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "project." + projectID + ".source_requirements_trace",
		Title:       "Source Requirements Trace",
		Content: "# Source Requirements Trace\n\n" +
			"rhizome_source_requirements_trace_v1:\n" +
			"  notes: mentions " + sourceKey + " but does not bind it as source_doc_keys\n" +
			"  acceptance_critical_anchors:\n" +
			"    - Telegraph-like article editor\n" +
			"  acceptance_criteria_refs:\n" +
			"    - CP-01 article creation\n" +
			"  adjacent_wrong_products_non_goals:\n" +
			"    - marketing landing\n",
		UpdatedBy: leadID,
	}); err != nil {
		t.Fatalf("write weak source trace: %v", err)
	}
	writeSourceFidelityReviewDocForTest(t, ctx, store, workspaceID, projectID, reviewKey, ownerID)

	_, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              "branch-ready",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/beta/source-fidelity-trace-binding",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               strings.Repeat("b", 40),
		WriteScopeJSON:        `{"paths":["src/app.ts"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) || !strings.Contains(err.Error(), "source_requirements_trace") {
		t.Fatalf("expected READY_FOR_REVIEW to reject trace without source_doc_keys binding, got %v", err)
	}
}

func TestProjectBranchReadyRejectsGenericSourceRequirementsTraceAnchors(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-source-fidelity-weak-anchors"
		projectID   = "project-source-fidelity-weak-anchors"
		leadID      = "alpha"
		ownerID     = "beta"
		repoID      = "repo-main"
		reviewKey   = "project.project-source-fidelity-weak-anchors.branch.branch-ready.review"
		sourceKey   = "run.clearpress.operator-spec"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/**"]}`)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\beta\source-fidelity-weak-anchors`)
	_, taskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, ownerID, "branch-ready", "agent/beta/source-fidelity-weak-anchors", `{"paths":["src/app.ts"]}`)
	writeProjectSourceRefsForTest(t, ctx, store, workspaceID, projectID, leadID, sourceKey)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "project." + projectID + ".source_requirements_trace",
		Title:       "Source Requirements Trace",
		Content: "```rhizome_source_requirements_trace_v1\n" +
			"source_doc_keys:\n" +
			"- " + sourceKey + "\n" +
			"acceptance_critical_anchors:\n" +
			"- primary flow\n" +
			"acceptance_criteria_refs:\n" +
			"- acceptance criteria\n" +
			"adjacent_wrong_products:\n" +
			"- adjacent wrong product\n" +
			"```\n",
		UpdatedBy: leadID,
	}); err != nil {
		t.Fatalf("write generic source trace: %v", err)
	}
	writeSourceFidelityReviewDocForTest(t, ctx, store, workspaceID, projectID, reviewKey, ownerID)

	_, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              "branch-ready",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/beta/source-fidelity-weak-anchors",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               strings.Repeat("b", 40),
		WriteScopeJSON:        `{"paths":["src/app.ts"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if !errors.Is(err, sqlite.ErrProjectBranchReviewEvidenceInvalid) || !strings.Contains(err.Error(), "source_requirements_trace") {
		t.Fatalf("expected READY_FOR_REVIEW to reject generic source anchors, got %v", err)
	}
}

func TestProjectPatchQueueAcceptedRequiresSourceFidelityVerdict(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-source-fidelity-accept"
		projectID   = "project-source-fidelity-accept"
		leadID      = "alpha"
		ownerID     = "beta"
		reviewerID  = "zeta"
		repoID      = "repo-main"
		reviewKey   = "project.project-source-fidelity-accept.branch.branch-ready.review"
		decisionKey = "project.project-source-fidelity-accept.patchq.branch-ready.decision"
		sourceKey   = "run.clearpress.operator-spec"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["src/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\beta\source-fidelity-accept`)
	_, taskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, ownerID, "branch-ready", "agent/beta/source-fidelity-accept", `{"paths":["src/app.ts"]}`)
	writeProjectSourceRefsForTest(t, ctx, store, workspaceID, projectID, leadID, sourceKey)
	writeProjectSourceTraceForTest(t, ctx, store, workspaceID, projectID, leadID, sourceKey)
	writeSourceFidelityReviewDocForTest(t, ctx, store, workspaceID, projectID, reviewKey, ownerID)

	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              "branch-ready",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/beta/source-fidelity-accept",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               strings.Repeat("b", 40),
		WriteScopeJSON:        `{"paths":["src/app.ts"]}`,
		ReviewDocKey:          reviewKey,
		Status:                sqlite.ProjectBranchStatusReadyForReview,
		ActorID:               ownerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", ownerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register source-fidelity ready branch: %v", err)
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
		t.Fatalf("submit source-fidelity branch to patch queue: %v", err)
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
		t.Fatalf("claim source-fidelity patch queue item: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              "ACCEPTED",
		DecisionSummary:       "Accepted after runnable artifact review.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "source-fidelity") {
		t.Fatalf("expected ACCEPTED decision to require source-fidelity verdict, got %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      decisionKey,
		Title:       "Patch Queue Source Fidelity Decision",
		Content: "```rhizome_spec_fidelity_review_v1\n" +
			"source_fidelity_status: passed\n" +
			"checked_source_doc_keys: [" + sourceKey + "]\n" +
			"notes: Candidate matches the non-droppable article editor anchors.\n" +
			"```\n",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write source-fidelity decision doc: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nReady for patch queue decision, but missing source fidelity binding.",
		UpdatedBy:   ownerID,
	}); err != nil {
		t.Fatalf("overwrite review doc without source-fidelity binding: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              "ACCEPTED",
		DecisionDocKey:        decisionKey,
		DecisionSummary:       "Accepted with source_fidelity_status: passed.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "Source Artifact Fidelity") {
		t.Fatalf("expected ACCEPTED decision to require source-bound review packet, got %v", err)
	}
	writeSourceFidelityReviewDocForTest(t, ctx, store, workspaceID, projectID, reviewKey, ownerID)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      decisionKey,
		Title:       "Patch Queue Source Fidelity Decision",
		Content: "```rhizome_spec_fidelity_review_v1\n" +
			"source_fidelity_status: pass_pending\n" +
			"checked_source_doc_keys: [" + sourceKey + "]\n" +
			"notes: Candidate is not actually accepted yet.\n" +
			"```\n",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write weak source-fidelity decision doc: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              "ACCEPTED",
		DecisionDocKey:        decisionKey,
		DecisionSummary:       "Accepted with source_fidelity_status: passable.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "source-fidelity") {
		t.Fatalf("expected weak source-fidelity status to be rejected, got %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      decisionKey,
		Title:       "Patch Queue Source Fidelity Decision",
		Content: "```rhizome_spec_fidelity_review_v1\n" +
			"source_fidelity_status: passed\n" +
			"notes: Candidate mentions " + sourceKey + " but omits checked_source_doc_keys.\n" +
			"```\n",
		UpdatedBy: reviewerID,
	}); err != nil {
		t.Fatalf("write missing checked-source decision doc: %v", err)
	}
	if _, _, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              "ACCEPTED",
		DecisionDocKey:        decisionKey,
		DecisionSummary:       "Accepted with source_fidelity_status: passed.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	}); !errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(err.Error(), "checked_source_doc_keys") {
		t.Fatalf("expected accepted decision to require exact checked_source_doc_keys, got %v", err)
	}
	decided, event, err := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              "ACCEPTED",
		DecisionDocKey:        decisionKey,
		DecisionSummary:       "Accepted with source_fidelity_status: passed.",
		CheckedSourceDocKeys:  []string{sourceKey},
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if err != nil {
		t.Fatalf("accept source-fidelity patch queue item: %v", err)
	}
	if decided.State != sqlite.ProjectPatchQueueStateAccepted || decided.DecisionDocKey != decisionKey {
		t.Fatalf("unexpected accepted source-fidelity decision: %+v", decided)
	}
	if !strings.Contains(event.PayloadJSON, `"checked_source_doc_keys":["`+sourceKey+`"]`) {
		t.Fatalf("decision event payload missing checked source doc keys: %s", event.PayloadJSON)
	}
}

func writeProjectSourceRefsForTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, actorID string, sourceDocKeys ...string) {
	t.Helper()
	docKey := "project." + projectID + ".source_refs"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Project Source Refs",
		Content: "```rhizome_source_refs_v1\n" +
			"project_id: " + projectID + "\n" +
			"source_doc_keys:\n" +
			"- " + strings.Join(sourceDocKeys, "\n- ") + "\n" +
			"```\n",
		UpdatedBy: actorID,
	}); err != nil {
		t.Fatalf("write source refs: %v", err)
	}
}

func writeProjectSourceTraceForTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, actorID string, sourceDocKeys ...string) {
	t.Helper()
	docKey := "project." + projectID + ".source_requirements_trace"
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Source Requirements Trace",
		Content: "```rhizome_source_requirements_trace_v1\n" +
			"source_doc_keys:\n" +
			"- " + strings.Join(sourceDocKeys, "\n- ") + "\n" +
			"acceptance_critical_anchors:\n" +
			"- Telegraph-like article editor\n" +
			"- auth/profile\n" +
			"acceptance_criteria_refs:\n" +
			"- AC-01 article creation\n" +
			"adjacent_wrong_products:\n" +
			"- marketing landing\n" +
			"- brief generator\n" +
			"```\n",
		UpdatedBy: actorID,
	}); err != nil {
		t.Fatalf("write source trace: %v", err)
	}
}

func writeSourceFidelityReviewDocForTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, reviewKey, actorID string) {
	t.Helper()
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content: "# Branch Review Packet\n\n" +
			"## Source Artifact Fidelity\n\n" +
			"- source_refs_doc_key: project." + projectID + ".source_refs\n" +
			"- source_requirements_trace_doc_key: project." + projectID + ".source_requirements_trace\n" +
			"- reviewer_expectation: block SPEC_DRIFT if the candidate satisfies only an adjacent compressed product flow.\n",
		UpdatedBy: actorID,
	}); err != nil {
		t.Fatalf("write source-fidelity review doc: %v", err)
	}
}
