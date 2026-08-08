package sqlite_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestProjectRepositoryAndCheckoutRegistry(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-git-registry"
		projectID   = "project-git-registry"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "Project Git Registry",
		CreatedBy:   leadID,
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:            workspaceID,
		ProjectID:              projectID,
		RepoID:                 "repo-secret-material",
		RemoteKind:             sqlite.ProjectRepositoryRemoteKindGitHub,
		CredentialVaultEntryID: "-----BEGIN PRIVATE KEY-----",
		RepoStatus:             sqlite.ProjectRepositoryStatusReady,
		ActorID:                leadID,
		ActorType:              "agent",
		PromptContextEnvelope:  sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:   "project.repository.upsert",
	}); !errors.Is(err, sqlite.ErrProjectCredentialVaultReferenceInvalid) {
		t.Fatalf("expected secret material credential reference to be rejected, got %v", err)
	}

	repo, repoEvent, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:            workspaceID,
		ProjectID:              projectID,
		RepoID:                 repoID,
		RemoteURL:              "git@github.com:ExampleOrg/project-git-registry.git",
		RemoteKind:             sqlite.ProjectRepositoryRemoteKindGitHub,
		Owner:                  "ExampleOrg",
		Name:                   "project-git-registry",
		DefaultBranch:          "main",
		IntegrationBranch:      "integration",
		CredentialVaultEntryID: "vault.github.mrdeveloper.ssh",
		RepoStatus:             sqlite.ProjectRepositoryStatusReady,
		IsCanonical:            true,
		CreatedByAgentID:       leadID,
		ActorID:                leadID,
		ActorType:              "agent",
		PromptContextEnvelope:  sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:   "project.repository.upsert",
	})
	if err != nil {
		t.Fatalf("upsert project repository: %v", err)
	}
	if repoEvent.EventType != "project.repository.upserted" || repo.RepoID != repoID || !repo.IsCanonical || repo.RepoStatus != sqlite.ProjectRepositoryStatusReady {
		t.Fatalf("unexpected repository upsert repo=%+v event=%+v", repo, repoEvent)
	}
	profile, err := store.GetProjectProfile(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get project profile after repository upsert: %v", err)
	}
	if !profile.RepoRequired || profile.RepoStatus != sqlite.ProjectRepoStatusReady || profile.RepoURL != repo.RemoteURL || profile.RepoDefaultBranch != "main" {
		t.Fatalf("canonical repository should sync profile repo fields, got %+v", profile)
	}
	repos, err := store.ListProjectRepositories(ctx, sqlite.ProjectRepositoryListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
	})
	if err != nil {
		t.Fatalf("list project repositories: %v", err)
	}
	if len(repos) != 1 || repos[0].RepoID != repoID || repos[0].CredentialVaultEntryID != "vault.github.mrdeveloper.ssh" {
		t.Fatalf("unexpected repository list %+v", repos)
	}

	referenceAt := time.Now().UTC()
	oldLastSeen := referenceAt.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	activeCheckout, activeEvent, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		MachineLabel:          "Example Workstation",
		OwnerUserID:           "developer",
		AgentID:               workerID,
		LocalPath:             `C:\fixtures\agents\worker-agent\project-git-registry`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		BranchName:            "agent/worker-agent/project-git-registry",
		BaseBranch:            "main",
		DirtyState:            "clean",
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register active checkout: %v", err)
	}
	if activeEvent.EventType != "project.checkout.registered" || activeCheckout.AgentID != workerID || activeCheckout.Status != sqlite.ProjectCheckoutStatusActive {
		t.Fatalf("unexpected active checkout checkout=%+v event=%+v", activeCheckout, activeEvent)
	}
	reusedCheckout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		OwnerUserID:           "developer",
		AgentID:               workerID,
		LocalPath:             activeCheckout.LocalPath,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		BranchName:            "agent/worker-agent/reused",
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register duplicate path checkout: %v", err)
	}
	if reusedCheckout.CheckoutID != activeCheckout.CheckoutID || reusedCheckout.BranchName != "agent/worker-agent/reused" {
		t.Fatalf("expected duplicate active path to update same checkout, got original=%+v reused=%+v", activeCheckout, reusedCheckout)
	}
	staleCheckout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		CheckoutID:            "checkout-stale",
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		MachineID:             "vps-main",
		MachineLabel:          "Main VPS",
		AgentID:               leadID,
		LocalPath:             "/srv/rhizome/project-git-registry",
		CheckoutKind:          sqlite.ProjectCheckoutKindIntegration,
		BranchName:            "integration",
		Status:                sqlite.ProjectCheckoutStatusActive,
		LastSeenAt:            oldLastSeen,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register stale checkout: %v", err)
	}
	checkouts, err := store.ListProjectCheckouts(ctx, sqlite.ProjectCheckoutListFilter{
		WorkspaceID:        workspaceID,
		ProjectID:          projectID,
		StaleAfterSeconds:  60,
		ReferenceTimestamp: referenceAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("list project checkouts: %v", err)
	}
	if len(checkouts) != 2 {
		t.Fatalf("expected two checkouts, got %+v", checkouts)
	}
	if got := findProjectCheckoutForTest(t, checkouts, reusedCheckout.CheckoutID); got.DerivedStatus != sqlite.ProjectCheckoutStatusActive {
		t.Fatalf("expected current checkout to derive ACTIVE, got %+v", got)
	}
	if got := findProjectCheckoutForTest(t, checkouts, staleCheckout.CheckoutID); got.DerivedStatus != sqlite.ProjectCheckoutStatusStale {
		t.Fatalf("expected old checkout to derive STALE, got %+v", got)
	}

	coordination, err := store.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get project coordination: %v", err)
	}
	if len(coordination.Repositories) != 1 || coordination.Repositories[0].RepoID != repoID {
		t.Fatalf("coordination should include repositories, got %+v", coordination.Repositories)
	}
	if len(coordination.Checkouts) != 2 {
		t.Fatalf("coordination should include checkout registry, got %+v", coordination.Checkouts)
	}
	if coordination.SnapshotAt == "" || coordination.CoordinationVersion == "" || coordination.LatestEventID == "" {
		t.Fatalf("coordination should carry freshness envelope, got %+v", coordination)
	}
}

func TestProjectRepositoryUpsertFallsBackToActiveProjectRoleWhenNoFreshLead(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-git-role-fallback"
		projectID   = "project-git-role-fallback"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		outsiderID  = "outsider-agent"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, outsiderID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	leadRole, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               leadID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		Summary:               "temporary git registry lead",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.claim",
	})
	if err != nil {
		t.Fatalf("claim project lead: %v", err)
	}
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectID, workerID, leadID, `{"paths":["**"]}`)
	if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                "repo-worker-while-lead-active",
		RemoteURL:             "file:///C:/fixtures/agents/worker/project-remotes/lead-active.git",
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindLocal,
		DefaultBranch:         "main",
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		IsCanonical:           true,
		CreatedByAgentID:      workerID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.repository.upsert",
	}); !errors.Is(err, sqlite.ErrProjectLeadMismatch) {
		t.Fatalf("expected active-role non-lead to remain blocked while fresh lead exists, got %v", err)
	}
	if _, _, err := store.ReleaseProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadReleaseInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RoleID:                leadRole.RoleID,
		ActorID:               leadID,
		ActorType:             "agent",
		LeaseToken:            leadRole.LeaseToken,
		Summary:               "release lead so active role fallback can recover repository evidence",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.release", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.lead.release",
	}); err != nil {
		t.Fatalf("release project lead: %v", err)
	}

	if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                "repo-worker-github",
		RemoteURL:             "git@github.com:ExampleOrg/recovered.git",
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindGitHub,
		DefaultBranch:         "main",
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		IsCanonical:           true,
		CreatedByAgentID:      workerID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.repository.upsert",
	}); !errors.Is(err, sqlite.ErrProjectLeadRequired) {
		t.Fatalf("expected active-role fallback to reject non-local repository replacement, got %v", err)
	}

	if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                "repo-worker-recovery",
		RemoteURL:             "file:///C:/fixtures/agents/worker/project-remotes/recovered.git",
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindLocal,
		DefaultBranch:         "main",
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		IsCanonical:           true,
		CreatedByAgentID:      workerID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.repository.upsert",
	}); err != nil {
		t.Fatalf("active project role should recover repository evidence when no fresh lead exists: %v", err)
	}

	if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                "repo-worker-recovery",
		RemoteURL:             "file:///C:/fixtures/agents/worker/project-remotes/recovered-v2.git",
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindLocal,
		DefaultBranch:         "main",
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		IsCanonical:           true,
		CreatedByAgentID:      workerID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.repository.upsert",
	}); err != nil {
		t.Fatalf("active project role should recover the same canonical repo id when no fresh lead exists: %v", err)
	}

	if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                "repo-worker-unrelated",
		RemoteURL:             "file:///C:/fixtures/agents/worker/project-remotes/unrelated.git",
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindLocal,
		DefaultBranch:         "main",
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		IsCanonical:           true,
		CreatedByAgentID:      workerID,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.repository.upsert",
	}); !errors.Is(err, sqlite.ErrProjectLeadRequired) {
		t.Fatalf("expected active-role fallback to reject unrelated canonical repo replacement, got %v", err)
	}

	if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                "repo-outsider",
		RemoteURL:             "file:///C:/fixtures/agents/outsider/project-remotes/recovered.git",
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindLocal,
		DefaultBranch:         "main",
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		IsCanonical:           true,
		CreatedByAgentID:      outsiderID,
		ActorID:               outsiderID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", outsiderID),
		PromptContextSurface:  "project.repository.upsert",
	}); !errors.Is(err, sqlite.ErrProjectLeadRequired) {
		t.Fatalf("expected outsider without active project role to remain blocked, got %v", err)
	}
}

func TestProjectGitStoreTreatsRegisteredAgentActorAsAgent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-git-actor-type"
		projectID   = "project-git-actor-type"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-main"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectID, leadID)

	if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                "repo-agent-actor-lie",
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindGitHub,
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		ActorID:               leadID,
		ActorType:             "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "human", leadID),
		PromptContextSurface:  "project.repository.upsert",
	}); !errors.Is(err, sqlite.ErrProjectLeadRequired) {
		t.Fatalf("expected registered agent actor with human actor_type to still require project lead, got %v", err)
	}

	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectID, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectID, repoID, leadID)
	if _, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               leadID,
		LocalPath:             `C:\fixtures\agents\worker-agent\forged-agent-type`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               workerID,
		ActorType:             "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "human", workerID),
		PromptContextSurface:  "project.checkout.register",
	}); !errors.Is(err, sqlite.ErrProjectLeadMismatch) {
		t.Fatalf("expected registered agent actor with human actor_type to be limited to its own checkout, got %v", err)
	}
}

func TestProjectRepositoryAndCheckoutRegistryRejectsCrossProjectScope(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-git-scope"
		projectA    = "project-git-scope-a"
		projectB    = "project-git-scope-b"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		repoID      = "repo-shared-id"
		repoB       = "repo-project-b"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectA, leadID)
	createProjectForGitTest(t, ctx, store, workspaceID, projectB, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectA, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectB, leadID)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectA, repoID, leadID)

	if _, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectB,
		RepoID:                repoID,
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindGitHub,
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.repository.upsert",
	}); !errors.Is(err, sqlite.ErrProjectRepositoryScopeMismatch) {
		t.Fatalf("expected repo_id collision across projects to be rejected, got %v", err)
	}

	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectB, repoB, leadID)
	activeCheckout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectA,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             `C:\fixtures\agents\worker-agent\shared-path`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register project A checkout: %v", err)
	}

	if _, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		CheckoutID:            activeCheckout.CheckoutID,
		WorkspaceID:           workspaceID,
		ProjectID:             projectA,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               leadID,
		LocalPath:             `C:\fixtures\agents\lead-agent\same-project-hijack-by-id`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.checkout.register",
	}); !errors.Is(err, sqlite.ErrProjectCheckoutScopeMismatch) {
		t.Fatalf("expected agent to be unable to hijack another agent checkout_id, got %v", err)
	}

	if _, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectA,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               leadID,
		LocalPath:             activeCheckout.LocalPath,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               leadID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", leadID),
		PromptContextSurface:  "project.checkout.register",
	}); !errors.Is(err, sqlite.ErrProjectCheckoutScopeMismatch) {
		t.Fatalf("expected agent to be unable to hijack another agent checkout path, got %v", err)
	}

	if _, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectB,
		RepoID:                repoB,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             activeCheckout.LocalPath,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	}); !errors.Is(err, sqlite.ErrProjectCheckoutScopeMismatch) {
		t.Fatalf("expected active path collision across projects to be rejected, got %v", err)
	}

	blockedCheckout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		CheckoutID:            "checkout-blocked-live-path",
		WorkspaceID:           workspaceID,
		ProjectID:             projectA,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             `C:\fixtures\agents\worker-agent\blocked-live-path`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		Status:                sqlite.ProjectCheckoutStatusBlocked,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register blocked checkout: %v", err)
	}
	if _, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectB,
		RepoID:                repoB,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             blockedCheckout.LocalPath,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	}); !errors.Is(err, sqlite.ErrProjectCheckoutScopeMismatch) {
		t.Fatalf("expected blocked live path collision across projects to be rejected, got %v", err)
	}

	if _, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		CheckoutID:            activeCheckout.CheckoutID,
		WorkspaceID:           workspaceID,
		ProjectID:             projectB,
		RepoID:                repoB,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             `C:\fixtures\agents\worker-agent\project-b`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	}); !errors.Is(err, sqlite.ErrProjectCheckoutScopeMismatch) {
		t.Fatalf("expected checkout_id collision across projects to be rejected, got %v", err)
	}
}

func TestProjectCheckoutRegistryValidatesActiveReferences(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID = "ws-project-git-active-refs"
		projectA    = "project-git-active-refs-a"
		projectB    = "project-git-active-refs-b"
		leadID      = "lead-agent"
		workerID    = "worker-agent"
		otherID     = "other-agent"
		repoID      = "repo-active-refs"
	)
	seedAgentWorkWorkspace(t, ctx, store, workspaceID, []string{leadID, workerID, otherID})
	createProjectForGitTest(t, ctx, store, workspaceID, projectA, leadID)
	createProjectForGitTest(t, ctx, store, workspaceID, projectB, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectA, leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, projectB, leadID)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectA, workerID, leadID, `{"paths":["src/**","tests/**"]}`)
	assignProjectImplementerRoleForGitTest(t, ctx, store, workspaceID, projectA, otherID, leadID, `{"paths":["docs/**"]}`)
	upsertRepositoryForGitTest(t, ctx, store, workspaceID, projectA, repoID, leadID)
	createProjectImplementationTask(t, ctx, store, workspaceID, projectB, "task-other-project")
	if _, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectA,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             `C:\fixtures\agents\worker-agent\active-ref-wrong-task`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActiveTaskID:          "task-other-project",
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	}); !errors.Is(err, sqlite.ErrProjectCheckoutActiveReferenceInvalid) {
		t.Fatalf("expected active_task_id from another project to be rejected, got %v", err)
	}

	createProjectImplementationTask(t, ctx, store, workspaceID, projectA, "task-unclaimed")
	if _, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectA,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             `C:\fixtures\agents\worker-agent\active-ref-unclaimed-task`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActiveTaskID:          "task-unclaimed",
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	}); !errors.Is(err, sqlite.ErrProjectCheckoutActiveReferenceInvalid) {
		t.Fatalf("expected active_task_id without active_claim_id to be rejected, got %v", err)
	}

	createProjectImplementationTask(t, ctx, store, workspaceID, projectA, "task-claimed-by-other")
	otherCheckout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectA,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               otherID,
		LocalPath:             `C:\fixtures\agents\other-agent\active-ref-owned-task`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		BranchName:            "agent/other-agent/active-ref-owned-task",
		DirtyState:            "clean",
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               otherID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", otherID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register other checkout: %v", err)
	}
	otherBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectA,
		RepoID:                repoID,
		CheckoutID:            otherCheckout.CheckoutID,
		AgentID:               otherID,
		BranchName:            "agent/other-agent/active-ref-owned-task",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["docs/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               otherID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", otherID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register other branch: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                "task-claimed-by-other",
		AgentID:               otherID,
		RepoID:                repoID,
		CheckoutID:            otherCheckout.CheckoutID,
		BranchID:              otherBranch.BranchID,
		WriteScopeJSON:        `{"paths":["docs/**"]}`,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claimed by another agent",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, "task-claimed-by-other", otherID),
	}); err != nil {
		t.Fatalf("claim task by other agent: %v", err)
	}
	if _, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectA,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             `C:\fixtures\agents\worker-agent\active-ref-wrong-claim`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActiveTaskID:          "task-claimed-by-other",
		ActiveClaimID:         "task-claimed-by-other",
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	}); !errors.Is(err, sqlite.ErrProjectCheckoutActiveReferenceInvalid) {
		t.Fatalf("expected active_claim_id owned by another agent to be rejected, got %v", err)
	}

	createProjectImplementationTask(t, ctx, store, workspaceID, projectA, "task-claimed-agentless")
	workerCheckout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectA,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             `C:\fixtures\agents\worker-agent\active-ref-agentless-owned`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		BranchName:            "agent/worker-agent/active-ref-agentless-owned",
		DirtyState:            "clean",
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register worker checkout: %v", err)
	}
	workerBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectA,
		RepoID:                repoID,
		CheckoutID:            workerCheckout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/active-ref-agentless-owned",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register worker branch: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                "task-claimed-agentless",
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            workerCheckout.CheckoutID,
		BranchID:              workerBranch.BranchID,
		WriteScopeJSON:        `{"paths":["src/**"]}`,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claimed by worker for agentless checkout rejection",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, "task-claimed-agentless", workerID),
	}); err != nil {
		t.Fatalf("claim task for agentless checkout rejection: %v", err)
	}
	if _, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectA,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		LocalPath:             `C:\fixtures\agents\worker-agent\active-ref-agentless`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActiveTaskID:          "task-claimed-agentless",
		ActiveClaimID:         "task-claimed-agentless",
		ActorID:               "developer",
		ActorType:             "human",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "human", "developer"),
		PromptContextSurface:  "project.checkout.register",
	}); !errors.Is(err, sqlite.ErrProjectCheckoutActiveReferenceInvalid) {
		t.Fatalf("expected active_claim_id without checkout agent_id to be rejected, got %v", err)
	}

	createProjectImplementationTask(t, ctx, store, workspaceID, projectA, "task-released-by-worker")
	releasedCheckout, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectA,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             `C:\fixtures\agents\worker-agent\active-ref-release-owned`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		BranchName:            "agent/worker-agent/active-ref-release-owned",
		DirtyState:            "clean",
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	})
	if err != nil {
		t.Fatalf("register released checkout: %v", err)
	}
	releasedBranch, _, err := store.RegisterProjectBranchWithEvent(ctx, sqlite.ProjectBranchRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectA,
		RepoID:                repoID,
		CheckoutID:            releasedCheckout.CheckoutID,
		AgentID:               workerID,
		BranchName:            "agent/worker-agent/active-ref-release-owned",
		BranchKind:            sqlite.ProjectBranchKindFeature,
		BaseBranch:            "main",
		WriteScopeJSON:        `{"paths":["tests/**"]}`,
		Status:                sqlite.ProjectBranchStatusReserved,
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.branch.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.branch.register",
	})
	if err != nil {
		t.Fatalf("register released branch: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID:           workspaceID,
		TaskID:                "task-released-by-worker",
		AgentID:               workerID,
		RepoID:                repoID,
		CheckoutID:            releasedCheckout.CheckoutID,
		BranchID:              releasedBranch.BranchID,
		WriteScopeJSON:        `{"paths":["tests/**"]}`,
		CoordinationMode:      sqlite.CoordinationModeTrustFirst,
		Summary:               "claimed then released",
		PromptContextEnvelope: taskClaimPromptEnvelopeForGitTest(workspaceID, "task-released-by-worker", workerID),
	}); err != nil {
		t.Fatalf("claim task by worker: %v", err)
	}
	if err := store.ReleaseTaskClaim(ctx, sqlite.TaskReleaseInput{
		WorkspaceID:           workspaceID,
		TaskID:                "task-released-by-worker",
		AgentID:               workerID,
		Reason:                "released for active-ref validation",
		PromptContextEnvelope: taskReleasePromptEnvelopeForGitTest(workspaceID, "task-released-by-worker", workerID),
	}); err != nil {
		t.Fatalf("release task by worker: %v", err)
	}
	if _, _, err := store.RegisterProjectCheckoutWithEvent(ctx, sqlite.ProjectCheckoutRegisterInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectA,
		RepoID:                repoID,
		MachineID:             "developer-desktop",
		AgentID:               workerID,
		LocalPath:             `C:\fixtures\agents\worker-agent\active-ref-released-claim`,
		CheckoutKind:          sqlite.ProjectCheckoutKindClone,
		Status:                sqlite.ProjectCheckoutStatusActive,
		ActiveTaskID:          "task-released-by-worker",
		ActiveClaimID:         "task-released-by-worker",
		ActorID:               workerID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.checkout.register", "server_rpc", workspaceID, "agent", workerID),
		PromptContextSurface:  "project.checkout.register",
	}); !errors.Is(err, sqlite.ErrProjectCheckoutActiveReferenceInvalid) {
		t.Fatalf("expected released active_claim_id to be rejected, got %v", err)
	}
}

func createProjectForGitTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, createdBy string) {
	t.Helper()
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       projectID,
		CreatedBy:   createdBy,
	}); err != nil {
		t.Fatalf("create project %s: %v", projectID, err)
	}
	designDocID := "project." + projectID + ".design"
	implementationPlanDocID := "project." + projectID + ".plan"
	if _, _, err := store.UpsertProjectProfileWithEvent(ctx, sqlite.ProjectProfileUpdateInput{
		WorkspaceID:             workspaceID,
		ProjectID:               projectID,
		DesignDocID:             &designDocID,
		ImplementationPlanDocID: &implementationPlanDocID,
		ActorID:                 createdBy,
		ActorType:               "agent",
		PromptContextEnvelope:   sqlite.BuildProjectPromptContextEnvelope("project.profile.update", "server_rpc", workspaceID, "agent", createdBy),
		PromptContextSurface:    "project.profile.update",
	}); err != nil {
		t.Fatalf("seed project docs for %s: %v", projectID, err)
	}
}

func claimProjectLeadForGitTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, agentID string) {
	t.Helper()
	if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               agentID,
		ActorID:               agentID,
		ActorType:             "agent",
		LeaseSeconds:          900,
		Summary:               "git registry test lead",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", agentID),
		PromptContextSurface:  "project.lead.claim",
	}); err != nil {
		t.Fatalf("claim project lead for %s: %v", projectID, err)
	}
}

func assignProjectImplementerRoleForGitTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, agentID, actorID, scopeJSON string) {
	t.Helper()
	if strings.TrimSpace(scopeJSON) == "" {
		t.Fatalf("assign project implementer role for %s: scopeJSON is required", agentID)
	}
	if _, _, err := store.AssignProjectRoleWithEvent(ctx, sqlite.ProjectRoleAssignInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		AgentID:               agentID,
		RoleType:              sqlite.ProjectRoleImplementer,
		WriteScopeJSON:        scopeJSON,
		Summary:               "git registry test implementer",
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.role.assign", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.role.assign",
	}); err != nil {
		t.Fatalf("assign project implementer role for %s: %v", agentID, err)
	}
}

func upsertRepositoryForGitTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, repoID, actorID string) sqlite.ProjectRepositoryRecord {
	t.Helper()
	repo, _, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		RepoID:                repoID,
		RemoteURL:             "git@github.com:ExampleOrg/" + projectID + ".git",
		RemoteKind:            sqlite.ProjectRepositoryRemoteKindGitHub,
		DefaultBranch:         "main",
		RepoStatus:            sqlite.ProjectRepositoryStatusReady,
		IsCanonical:           true,
		CreatedByAgentID:      actorID,
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.repository.upsert", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.repository.upsert",
	})
	if err != nil {
		t.Fatalf("upsert repository %s for %s: %v", repoID, projectID, err)
	}
	openProjectImplementationPhaseForGitTest(t, ctx, store, workspaceID, projectID, actorID)
	return repo
}

func openProjectImplementationPhaseForGitTest(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, projectID, actorID string) {
	t.Helper()
	profile, err := store.GetProjectProfile(ctx, workspaceID, projectID)
	if err != nil {
		t.Fatalf("get project profile for %s: %v", projectID, err)
	}
	if profile.CurrentPhase == sqlite.ProjectPhaseImplementation ||
		profile.CurrentPhase == sqlite.ProjectPhaseReview ||
		profile.CurrentPhase == sqlite.ProjectPhaseIntegration ||
		profile.CurrentPhase == sqlite.ProjectPhaseValidation {
		return
	}
	if _, ok, err := store.GetActiveProjectStrategicLead(ctx, workspaceID, projectID); err != nil {
		t.Fatalf("get active project lead for %s: %v", projectID, err)
	} else if !ok {
		return
	}
	if _, _, _, err := store.TransitionProjectPhaseWithEvent(ctx, sqlite.ProjectPhaseTransitionInput{
		WorkspaceID:           workspaceID,
		ProjectID:             projectID,
		ToPhase:               sqlite.ProjectPhaseImplementation,
		Reason:                "git/branch registry test project is ready for implementation admission.",
		ActorID:               actorID,
		ActorType:             "agent",
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.phase.transition", "server_rpc", workspaceID, "agent", actorID),
		PromptContextSurface:  "project.phase.transition",
	}); err != nil {
		t.Fatalf("transition project %s to implementation: %v", projectID, err)
	}
}

func taskClaimPromptEnvelopeForGitTest(workspaceID, taskID, agentID string) map[string]any {
	envelope := sqlite.BuildTaskPromptContextEnvelope("agent.task.claim", "server_rpc", workspaceID, "agent", agentID)
	envelope["actor_agent_id"] = agentID
	envelope["agent_id"] = agentID
	envelope["task_id"] = taskID
	envelope["claim_status"] = "CLAIMED"
	return envelope
}

func taskReleasePromptEnvelopeForGitTest(workspaceID, taskID, agentID string) map[string]any {
	envelope := sqlite.BuildTaskPromptContextEnvelope("agent.task.release", "server_rpc", workspaceID, "agent", agentID)
	envelope["actor_agent_id"] = agentID
	envelope["agent_id"] = agentID
	envelope["task_id"] = taskID
	envelope["claim_status"] = "RELEASED"
	return envelope
}

func findProjectCheckoutForTest(t *testing.T, checkouts []sqlite.ProjectCheckoutRecord, checkoutID string) sqlite.ProjectCheckoutRecord {
	t.Helper()
	for _, checkout := range checkouts {
		if checkout.CheckoutID == checkoutID {
			return checkout
		}
	}
	t.Fatalf("checkout %q not found in %+v", checkoutID, checkouts)
	return sqlite.ProjectCheckoutRecord{}
}
