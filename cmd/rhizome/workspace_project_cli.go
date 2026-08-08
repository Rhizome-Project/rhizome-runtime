package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func runWorkspaceProject(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace project subcommand")
	}
	switch args[0] {
	case "repository":
		return runWorkspaceProjectRepository(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace project subcommand: %s", args[0])
	}
}

func runWorkspaceProjectRepository(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace project repository subcommand")
	}
	switch args[0] {
	case "upsert":
		return runWorkspaceProjectRepositoryUpsert(args[1:])
	case "list":
		return runWorkspaceProjectRepositoryList(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace project repository subcommand: %s", args[0])
	}
}

func runWorkspaceProjectRepositoryUpsert(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace project repository upsert", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	projectID := fs.String("project-id", "", "Project identifier")
	actorID := fs.String("actor-id", "", "Human/operator actor recording the repository")
	repoID := fs.String("repo-id", "", "Repository record identifier")
	remoteURL := fs.String("remote-url", "", "Repository remote URL")
	remoteKind := fs.String("remote-kind", sqlite.ProjectRepositoryRemoteKindUnknown, "Remote kind: local|github|gitlab|unknown")
	owner := fs.String("owner", "", "Repository owner or namespace")
	name := fs.String("name", "", "Repository name")
	defaultBranch := fs.String("default-branch", "main", "Default branch")
	integrationBranch := fs.String("integration-branch", "", "Integration branch")
	credentialVaultEntryID := fs.String("credential-vault-entry-id", "", "Vault entry id for credentials")
	repoStatus := fs.String("repo-status", sqlite.ProjectRepositoryStatusReady, "Repository status: MISSING|REQUESTED|CREATED|READY|BROKEN|ARCHIVED")
	canonical := fs.Bool("canonical", false, "Mark repository as canonical for the project")
	createdByAgentID := fs.String("created-by-agent-id", "", "Agent that originally requested or materialized the repo")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*workspaceID) == "" {
		return errors.New("--workspace-id is required")
	}
	if strings.TrimSpace(*projectID) == "" {
		return errors.New("--project-id is required")
	}
	if strings.TrimSpace(*actorID) == "" {
		return errors.New("--actor-id is required")
	}

	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	repo, event, err := store.UpsertProjectRepositoryWithEvent(ctx, sqlite.ProjectRepositoryUpsertInput{
		WorkspaceID:            *workspaceID,
		ProjectID:              *projectID,
		ActorID:                *actorID,
		ActorType:              "human",
		RepoID:                 *repoID,
		RemoteURL:              *remoteURL,
		RemoteKind:             *remoteKind,
		Owner:                  *owner,
		Name:                   *name,
		DefaultBranch:          *defaultBranch,
		IntegrationBranch:      *integrationBranch,
		CredentialVaultEntryID: *credentialVaultEntryID,
		RepoStatus:             *repoStatus,
		IsCanonical:            *canonical,
		CreatedByAgentID:       *createdByAgentID,
		PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope(
			"cli.workspace.project.repository.upsert",
			"cli_local",
			strings.TrimSpace(*workspaceID),
			"human",
			strings.TrimSpace(*actorID),
		),
		PromptContextSurface: "cli.workspace.project.repository.upsert",
	})
	if err != nil {
		return fmt.Errorf("upsert project repository: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":         traceID,
		"workspace_id":     strings.TrimSpace(*workspaceID),
		"project_id":       strings.TrimSpace(*projectID),
		"repository":       repo,
		"runtime_event_id": event.EventID,
	})
}

func runWorkspaceProjectRepositoryList(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace project repository list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	projectID := fs.String("project-id", "", "Project identifier")
	includeArchived := fs.Bool("include-archived", false, "Include archived repository records")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*workspaceID) == "" {
		return errors.New("--workspace-id is required")
	}
	if strings.TrimSpace(*projectID) == "" {
		return errors.New("--project-id is required")
	}

	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	repositories, err := store.ListProjectRepositories(ctx, sqlite.ProjectRepositoryListFilter{
		WorkspaceID:     *workspaceID,
		ProjectID:       *projectID,
		IncludeArchived: *includeArchived,
	})
	if err != nil {
		return fmt.Errorf("list project repositories: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"project_id":   strings.TrimSpace(*projectID),
		"repositories": repositories,
		"count":        len(repositories),
	})
}
