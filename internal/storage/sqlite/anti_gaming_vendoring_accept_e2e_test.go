package sqlite_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	sqlite "github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// TestProjectPatchQueueAcceptedBlocksVendoredInterpreter is the end-to-end "proven to prevent"
// proof: a candidate whose head (919f156) vendors github.com/yuin/gopher-lua, submitted and
// claimed through the real accept path with the NO-VENDORING opt-in declared, MUST be BLOCKED at
// ACCEPT. The repo points at the real managed-remote so the gate reads ground-truth go.mod.
func TestProjectPatchQueueAcceptedBlocksVendoredInterpreter(t *testing.T) {
	t.Parallel()

	if _, lookErr := exec.LookPath("git"); lookErr != nil {
		t.Skip("git is required for repository-backed patch-queue tests")
	}
	gitDir, vendoredSHA := createVendoredInterpreterRepoForAcceptTest(t)
	remoteURL := "file:///" + filepath.ToSlash(gitDir)

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-no-vendoring-accept"
		projectID   = "project-no-vendoring-accept"
		leadID      = "alpha"
		ownerID     = "beta"
		reviewerID  = "zeta"
		repoID      = "repo-lua"
	)
	noVendoringDocKey := "project." + projectID + ".no_vendoring_interpreter"

	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, ownerID, reviewerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	if _, _, repoErr := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		RemoteURL:             remoteURL,
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindUnknown,
		DefaultBranch:         "main",
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		IsCanonical:           true,
		CreatedByAgentID:      leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.repository.upsert",
	}); repoErr != nil {
		t.Fatalf("upsert repo: %v", repoErr)
	}
	openProjectImplementationPhaseForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, ownerID, leadID, `{"paths":["internal/**"]}`)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, projectID, reviewerID, sqlite.ProjectRoleReviewer, leadID)
	checkout := registerCheckoutForBranchTest(t, ctx, store, workspaceID, projectID, repoID, ownerID, `C:\fixtures\agents\beta\no-vendoring-accept`)
	_, taskID := seedReservedReadyBranchBindingForTest(t, ctx, store, workspaceID, projectID, repoID, checkout.CheckoutID, ownerID, "branch-vendored", "agent/beta/no-vendoring-accept", `{"paths":["internal/runner/runner.go"]}`)

	// Opt-in: declare NO-VENDORING required for this project.
	if docErr := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      noVendoringDocKey,
		Title:       "No-Vendoring Interpreter Policy",
		Content:     "The product interpreter must be roster-built; zero third-party interpreter/runtime dependencies.",
		UpdatedBy:   leadID,
	}); docErr != nil {
		t.Fatalf("seed no-vendoring config doc: %v", docErr)
	}

	reviewKey := "project." + projectID + ".branch.branch-vendored.review"
	if docErr := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      reviewKey,
		Title:       "Branch Review Packet",
		Content:     "# Branch Review Packet\n\nBackend-only change under internal/runner; ready for patch queue decision.",
		UpdatedBy:   ownerID,
	}); docErr != nil {
		t.Fatalf("seed review doc: %v", docErr)
	}

	branch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		CheckoutID:            checkout.CheckoutID,
		AgentID:               ownerID,
		BranchID:              "branch-vendored",
		ActiveTaskID:          taskID,
		ActiveClaimID:         taskID,
		BranchName:            "agent/beta/no-vendoring-accept",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		BaseSHA:               strings.Repeat("a", 40),
		HeadSHA:               vendoredSHA,
		WriteScopeJSON:        `{"paths":["internal/runner/runner.go"]}`,
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
		t.Fatalf("submit: %v", err)
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
		t.Fatalf("claim: %v", err)
	}
	_, _, decErr := store.DecideProjectPatchQueueItemWithEvent(ctx, sqlite.ProjectPatchQueueDecisionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		QueueID:               claimed.QueueID,
		ItemID:                claimed.ItemID,
		Decision:              "ACCEPTED",
		DecisionSummary:       "Accepted after review.",
		ClaimToken:            claimed.ClaimToken,
		ActorID:               reviewerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.patch_queue.decision", "server_rpc", workspaceID, "agent", reviewerID),
		PromptContextSurface:  "project.patch_queue.decision",
	})
	if !errors.Is(decErr, sqlite.ErrProjectPatchQueueInvalid) || !strings.Contains(strings.ToLower(decErr.Error()), "vendor") {
		t.Fatalf("expected ACCEPT to be BLOCKED by NO-VENDORING (vendored gopher-lua at head 919f156), got: %v", decErr)
	}
}

func createVendoredInterpreterRepoForAcceptTest(t *testing.T) (gitDir, vendoredSHA string) {
	t.Helper()
	repo := filepath.Join(t.TempDir(), "interpreter-fixture")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatalf("create fixture repo: %v", err)
	}
	runGitForAcceptFixture(t, repo, "init")
	runGitForAcceptFixture(t, repo, "config", "user.name", "Rhizome Test")
	runGitForAcceptFixture(t, repo, "config", "user.email", "test@example.invalid")
	goMod := "module example.invalid/interpreter\n\ngo 1.24\n\nrequire github.com/yuin/gopher-lua v1.1.1\n"
	if err := os.WriteFile(filepath.Join(repo, "go.mod"), []byte(goMod), 0o600); err != nil {
		t.Fatalf("write fixture go.mod: %v", err)
	}
	runGitForAcceptFixture(t, repo, "add", "go.mod")
	runGitForAcceptFixture(t, repo, "commit", "-m", "add prohibited interpreter dependency")
	vendoredSHA = strings.TrimSpace(runGitForAcceptFixture(t, repo, "rev-parse", "HEAD"))
	return filepath.Join(repo, ".git"), vendoredSHA
}

func runGitForAcceptFixture(t *testing.T, repo string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = repo
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}
