package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func runWorkspace(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace subcommand")
	}

	switch args[0] {
	case "create":
		return runWorkspaceCreate(args[1:])
	case "status":
		return runWorkspaceStatus(args[1:])
	case "search":
		return runWorkspaceSearch(args[1:])
	case "memory":
		return runWorkspaceMemory(args[1:])
	case "events":
		return runWorkspaceEvents(args[1:])
	case "authority":
		return runWorkspaceAuthority(args[1:])
	case "ops":
		return runWorkspaceOps(args[1:])
	case "claim":
		return runWorkspaceClaim(args[1:])
	case "execution":
		return runWorkspaceExecution(args[1:])
	case "policy":
		return runWorkspacePolicy(args[1:])
	case "project":
		return runWorkspaceProject(args[1:])
	case "doc":
		return runWorkspaceDoc(args[1:])
	case "task":
		return runWorkspaceTask(args[1:])
	case "artifact":
		return runWorkspaceArtifact(args[1:])
	case "compaction":
		return runWorkspaceCompaction(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace subcommand: %s", args[0])
	}
}

func runWorkspaceCreate(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	title := fs.String("title", "", "Workspace title")
	description := fs.String("description", "", "Workspace description")
	createdBy := fs.String("created-by", "", "Workspace initiator")
	status := fs.String("status", model.WorkspaceStatusActive, "Workspace status: ACTIVE|PAUSED|ARCHIVED")
	if err := fs.Parse(args); err != nil {
		return err
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
	workspacePassword := strings.TrimSpace(os.Getenv("RHIZOME_WORKSPACE_PASSWORD"))
	if workspacePassword == "" {
		return errors.New("RHIZOME_WORKSPACE_PASSWORD is required when creating a workspace")
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID:       *workspaceID,
		Title:             *title,
		Description:       *description,
		CreatedBy:         *createdBy,
		Status:            *status,
		WorkspacePassword: workspacePassword,
	}); err != nil {
		return fmt.Errorf("create workspace: %w", err)
	}

	record, err := store.GetWorkspace(ctx, *workspaceID)
	if err != nil {
		return fmt.Errorf("get workspace: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":  traceID,
		"workspace": record,
	})
}

func runWorkspaceStatus(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace status", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	updatesLimit := fs.Int("updates-limit", 10, "Number of recent updates to include")
	if err := fs.Parse(args); err != nil {
		return err
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
	snapshot, err := store.GetWorkspaceSnapshot(ctx, *workspaceID, *updatesLimit)
	if err != nil {
		return fmt.Errorf("get workspace snapshot: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"generated_at": time.Now().UTC().Format(time.RFC3339Nano),
		"snapshot":     snapshot,
	})
}

func runWorkspaceSearch(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace search", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	query := fs.String("query", "", "Search query")
	entityType := fs.String("type", "", "Optional entity type filter: doc|task|update|tool|artifact")
	limit := fs.Int("limit", 20, "Maximum number of results")
	if err := fs.Parse(args); err != nil {
		return err
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
	results, err := store.SearchWorkspace(ctx, sqlite.WorkspaceSearchFilter{
		WorkspaceID: *workspaceID,
		Query:       *query,
		EntityType:  *entityType,
		Limit:       *limit,
	})
	if err != nil {
		return fmt.Errorf("search workspace: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"query":        strings.TrimSpace(*query),
		"results":      results,
	})
}

func runWorkspaceMemory(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace memory subcommand")
	}
	switch args[0] {
	case "write":
		return runWorkspaceMemoryWrite(args[1:])
	case "list":
		return runWorkspaceMemoryList(args[1:])
	case "search":
		return runWorkspaceMemorySearch(args[1:])
	case "remove":
		return runWorkspaceMemoryRemove(args[1:])
	case "restore":
		return runWorkspaceMemoryRestore(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace memory subcommand: %s", args[0])
	}
}

func runWorkspaceMemoryWrite(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace memory write", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	memoryID := fs.String("memory-id", "", "Optional stable memory identifier")
	memoryType := fs.String("memory-type", "NOTE", "Memory type")
	title := fs.String("title", "", "Optional memory title")
	body := fs.String("body", "", "Inline memory body")
	bodyFile := fs.String("file", "", "Path to memory body file")
	summary := fs.String("summary", "", "Optional short summary")
	agentID := fs.String("agent-id", "", "Optional agent attribution")
	sessionID := fs.String("session-id", "", "Optional session identifier")
	taskID := fs.String("task-id", "", "Optional task identifier")
	sourceKind := fs.String("source-kind", "manual", "Memory source kind")
	sourceID := fs.String("source-id", "", "Optional source identifier")
	tags := fs.String("tags", "", "Comma separated tags")
	importance := fs.Float64("importance", 0, "Importance weight in 0..1")
	confidence := fs.Float64("confidence", 0, "Confidence weight in 0..1")
	if err := fs.Parse(args); err != nil {
		return err
	}

	bodyText, err := readTextValue(*body, *bodyFile)
	if err != nil {
		return err
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
	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		MemoryID:    *memoryID,
		WorkspaceID: *workspaceID,
		MemoryType:  *memoryType,
		Title:       *title,
		Body:        bodyText,
		Summary:     *summary,
		AgentID:     *agentID,
		SessionID:   *sessionID,
		TaskID:      *taskID,
		SourceKind:  *sourceKind,
		SourceID:    *sourceID,
		Tags:        parseCSV(*tags),
		Importance:  *importance,
		Confidence:  *confidence,
	})
	if err != nil {
		return fmt.Errorf("record workspace memory: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"memory":   record,
		"status":   "RECORDED",
	})
}

func runWorkspaceMemoryList(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace memory list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	memoryType := fs.String("memory-type", "", "Optional memory type filter")
	agentID := fs.String("agent-id", "", "Optional agent filter")
	sessionID := fs.String("session-id", "", "Optional session filter")
	taskID := fs.String("task-id", "", "Optional task filter")
	sourceKind := fs.String("source-kind", "", "Optional source kind filter")
	includeArchived := fs.Bool("include-archived", false, "Include archived workspace memories")
	limit := fs.Int("limit", 20, "Maximum memories to list")
	if err := fs.Parse(args); err != nil {
		return err
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
	items, err := store.ListWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID:     *workspaceID,
		MemoryType:      *memoryType,
		AgentID:         *agentID,
		SessionID:       *sessionID,
		TaskID:          *taskID,
		SourceKind:      *sourceKind,
		IncludeArchived: *includeArchived,
		Limit:           *limit,
	})
	if err != nil {
		return fmt.Errorf("list workspace memory: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"items":        items,
		"count":        len(items),
	})
}

func runWorkspaceMemorySearch(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace memory search", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	query := fs.String("query", "", "Search query")
	memoryType := fs.String("memory-type", "", "Optional memory type filter")
	agentID := fs.String("agent-id", "", "Optional agent filter")
	sessionID := fs.String("session-id", "", "Optional session filter")
	taskID := fs.String("task-id", "", "Optional task filter")
	sourceKind := fs.String("source-kind", "", "Optional source kind filter")
	includeArchived := fs.Bool("include-archived", false, "Include archived workspace memories")
	limit := fs.Int("limit", 20, "Maximum results")
	if err := fs.Parse(args); err != nil {
		return err
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
	items, err := store.SearchWorkspaceMemory(ctx, sqlite.WorkspaceMemoryFilter{
		WorkspaceID:     *workspaceID,
		Query:           *query,
		MemoryType:      *memoryType,
		AgentID:         *agentID,
		SessionID:       *sessionID,
		TaskID:          *taskID,
		SourceKind:      *sourceKind,
		IncludeArchived: *includeArchived,
		Limit:           *limit,
	})
	if err != nil {
		return fmt.Errorf("search workspace memory: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"query":        strings.TrimSpace(*query),
		"items":        items,
		"count":        len(items),
	})
}

func runWorkspaceMemoryRemove(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace memory remove", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	memoryID := fs.String("memory-id", "", "Memory identifier to archive")
	removedBy := fs.String("removed-by", "", "Actor archiving the memory")
	reason := fs.String("reason", "", "Optional tombstone reason")
	if err := fs.Parse(args); err != nil {
		return err
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
	record, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: *workspaceID,
		MemoryID:    *memoryID,
		ArchivedBy:  *removedBy,
		Reason:      *reason,
	})
	if err != nil {
		return fmt.Errorf("archive workspace memory: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"memory":   record,
		"status":   "ARCHIVED",
	})
}

func runWorkspaceMemoryRestore(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace memory restore", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	memoryID := fs.String("memory-id", "", "Memory identifier to restore")
	restoredBy := fs.String("restored-by", "", "Actor restoring the memory")
	if err := fs.Parse(args); err != nil {
		return err
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
	record, err := store.RestoreWorkspaceMemory(ctx, sqlite.WorkspaceMemoryRestoreInput{
		WorkspaceID: *workspaceID,
		MemoryID:    *memoryID,
		RestoredBy:  *restoredBy,
	})
	if err != nil {
		return fmt.Errorf("restore workspace memory: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"memory":   record,
		"status":   "RESTORED",
	})
}

func runWorkspaceDoc(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace doc subcommand")
	}
	switch args[0] {
	case "put":
		return runWorkspaceDocPut(args[1:])
	case "get":
		return runWorkspaceDocGet(args[1:])
	case "history":
		return runWorkspaceDocHistory(args[1:])
	case "archive":
		return runWorkspaceDocArchive(args[1:])
	case "delete":
		return runWorkspaceDocDelete(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace doc subcommand: %s", args[0])
	}
}

func runWorkspaceDocPut(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace doc put", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	docKey := fs.String("doc-key", "", "Stable document key")
	title := fs.String("title", "", "Document title")
	updatedBy := fs.String("updated-by", "", "Document updater")
	content := fs.String("content", "", "Inline document content")
	filePath := fs.String("file", "", "Path to document content file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	body, err := readTextValue(*content, *filePath)
	if err != nil {
		return err
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
	if _, _, err := store.UpsertWorkspaceDocWithEffects(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID:           *workspaceID,
		DocKey:                *docKey,
		Title:                 *title,
		Content:               body,
		UpdatedBy:             *updatedBy,
		PromptContextEnvelope: cliWorkspaceDocPromptContextEnvelope("cli.workspace.doc.put", *workspaceID, *updatedBy),
		PromptContextSurface:  "cli.workspace.doc.put",
	}); err != nil {
		return fmt.Errorf("upsert workspace doc: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"doc_key":      strings.TrimSpace(*docKey),
		"status":       "UPDATED",
	})
}

func runWorkspaceDocGet(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace doc get", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	docKey := fs.String("doc-key", "", "Stable document key")
	if err := fs.Parse(args); err != nil {
		return err
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
	record, err := store.GetWorkspaceDoc(ctx, *workspaceID, *docKey)
	if err != nil {
		return fmt.Errorf("get workspace doc: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"doc":      record,
	})
}

func runWorkspaceDocHistory(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace doc history", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	docKey := fs.String("doc-key", "", "Stable document key")
	limit := fs.Int("limit", 20, "Maximum number of revisions")
	if err := fs.Parse(args); err != nil {
		return err
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
	records, err := store.ListWorkspaceDocRevisions(ctx, *workspaceID, *docKey, *limit)
	if err != nil {
		return fmt.Errorf("list workspace doc revisions: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"doc_key":      strings.TrimSpace(*docKey),
		"revisions":    records,
	})
}

func runWorkspaceDocArchive(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace doc archive", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	docKey := fs.String("doc-key", "", "Stable document key")
	archivedBy := fs.String("archived-by", "", "Actor archiving the document")
	if err := fs.Parse(args); err != nil {
		return err
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
	if _, _, err := store.ArchiveWorkspaceDocWithEffectsAndPromptContextSurface(
		ctx,
		*workspaceID,
		*docKey,
		*archivedBy,
		cliWorkspaceDocPromptContextEnvelope("cli.workspace.doc.archive", *workspaceID, *archivedBy),
		"cli.workspace.doc.archive",
	); err != nil {
		return fmt.Errorf("archive workspace doc: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"doc_key":      strings.TrimSpace(*docKey),
		"status":       "ARCHIVED",
	})
}

func runWorkspaceDocDelete(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace doc delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	docKey := fs.String("doc-key", "", "Stable document key")
	deletedBy := fs.String("deleted-by", "", "Actor deleting the document")
	if err := fs.Parse(args); err != nil {
		return err
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
	if _, _, err := store.DeleteWorkspaceDocWithEffectsAndPromptContextSurface(
		ctx,
		*workspaceID,
		*docKey,
		*deletedBy,
		cliWorkspaceDocPromptContextEnvelope("cli.workspace.doc.delete", *workspaceID, *deletedBy),
		"cli.workspace.doc.delete",
	); err != nil {
		return fmt.Errorf("delete workspace doc: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"doc_key":      strings.TrimSpace(*docKey),
		"status":       "DELETED",
	})
}

func cliWorkspaceDocPromptContextEnvelope(surface, workspaceID, actorID string) map[string]any {
	return sqlite.BuildWorkspaceDocPromptContextEnvelope(surface, "cli_local", workspaceID, "operator", strings.TrimSpace(actorID))
}

func runWorkspaceTask(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace task subcommand")
	}
	switch args[0] {
	case "attach":
		return runWorkspaceTaskAttach(args[1:])
	case "link":
		return runWorkspaceTaskLink(args[1:])
	case "links":
		return runWorkspaceTaskLinks(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace task subcommand: %s", args[0])
	}
}

func runWorkspaceTaskAttach(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace task attach", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	taskID := fs.String("task-id", "", "Task identifier")
	linkedBy := fs.String("linked-by", "", "Actor attaching the task")
	if err := fs.Parse(args); err != nil {
		return err
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
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: *workspaceID,
		TaskID:      *taskID,
		LinkedBy:    *linkedBy,
	}); err != nil {
		return fmt.Errorf("attach task to workspace: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"task_id":      strings.TrimSpace(*taskID),
		"status":       "ATTACHED",
	})
}

func runWorkspaceTaskLink(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace task link", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	fromTaskID := fs.String("from-task-id", "", "Source task identifier")
	toTaskID := fs.String("to-task-id", "", "Target task identifier")
	linkType := fs.String("link-type", model.TaskLinkRelatesTo, "Link type: BLOCKS|RELATES_TO|SUBTASK_OF")
	createdBy := fs.String("created-by", "", "Actor creating the link")
	if err := fs.Parse(args); err != nil {
		return err
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
	if err := store.AddWorkspaceTaskLink(ctx, sqlite.WorkspaceTaskLinkInput{
		WorkspaceID: *workspaceID,
		FromTaskID:  *fromTaskID,
		ToTaskID:    *toTaskID,
		LinkType:    *linkType,
		CreatedBy:   *createdBy,
	}); err != nil {
		return fmt.Errorf("link workspace tasks: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"from_task_id": strings.TrimSpace(*fromTaskID),
		"to_task_id":   strings.TrimSpace(*toTaskID),
		"link_type":    strings.ToUpper(strings.TrimSpace(*linkType)),
		"status":       "LINKED",
	})
}

func runWorkspaceTaskLinks(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace task links", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	taskID := fs.String("task-id", "", "Optional task identifier filter")
	linkType := fs.String("link-type", "", "Optional link type filter")
	limit := fs.Int("limit", 50, "Maximum links to return")
	if err := fs.Parse(args); err != nil {
		return err
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
	records, err := store.ListWorkspaceTaskLinks(ctx, sqlite.WorkspaceTaskLinkFilter{
		WorkspaceID: *workspaceID,
		TaskID:      *taskID,
		LinkType:    *linkType,
		Limit:       *limit,
	})
	if err != nil {
		return fmt.Errorf("list workspace task links: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"task_id":      strings.TrimSpace(*taskID),
		"links":        records,
	})
}

func runWorkspaceArtifact(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace artifact subcommand")
	}
	switch args[0] {
	case "put":
		return runWorkspaceArtifactPut(args[1:])
	case "list":
		return runWorkspaceArtifactList(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace artifact subcommand: %s", args[0])
	}
}

func runWorkspaceArtifactPut(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace artifact put", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	artifactID := fs.String("artifact-id", "", "Artifact identifier")
	taskID := fs.String("task-id", "", "Optional linked task identifier")
	updateID := fs.String("update-id", "", "Optional linked agent update identifier")
	title := fs.String("title", "", "Artifact title")
	artifactRef := fs.String("ref", "", "Artifact reference path or URL")
	kind := fs.String("kind", "reference", "Artifact kind")
	contentType := fs.String("content-type", "application/octet-stream", "Artifact content type")
	createdBy := fs.String("created-by", "", "Actor recording the artifact")
	metadata := fs.String("metadata", "", "Inline metadata JSON")
	metadataFile := fs.String("metadata-file", "", "Path to metadata JSON file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	metadataBody, err := readTextValue(*metadata, *metadataFile)
	if err != nil {
		return err
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
	promptContextEnvelope, err := cliWorkspaceArtifactPromptContextEnvelope(ctx, store, "cli.workspace.artifact.put", *workspaceID, *createdBy)
	if err != nil {
		return err
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:            *artifactID,
		WorkspaceID:           *workspaceID,
		TaskID:                *taskID,
		UpdateID:              *updateID,
		Title:                 *title,
		ArtifactRef:           *artifactRef,
		Kind:                  *kind,
		ContentType:           *contentType,
		CreatedBy:             *createdBy,
		MetadataJSON:          metadataBody,
		PromptContextEnvelope: promptContextEnvelope,
		PromptContextSurface:  "cli.workspace.artifact.put",
	}); err != nil {
		return fmt.Errorf("create workspace artifact: %w", err)
	}

	records, err := store.ListWorkspaceArtifacts(ctx, sqlite.WorkspaceArtifactFilter{
		WorkspaceID: *workspaceID,
		UpdateID:    *updateID,
		TaskID:      *taskID,
		Limit:       1,
	})
	if err != nil {
		return fmt.Errorf("list workspace artifacts after create: %w", err)
	}
	if len(records) == 0 {
		return errors.New("artifact created but not found in workspace listing")
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"artifact": records[0],
	})
}

func runWorkspaceArtifactList(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace artifact list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	taskID := fs.String("task-id", "", "Optional task filter")
	updateID := fs.String("update-id", "", "Optional update filter")
	limit := fs.Int("limit", 20, "Maximum artifacts to list")
	if err := fs.Parse(args); err != nil {
		return err
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
	records, err := store.ListWorkspaceArtifacts(ctx, sqlite.WorkspaceArtifactFilter{
		WorkspaceID: *workspaceID,
		TaskID:      *taskID,
		UpdateID:    *updateID,
		Limit:       *limit,
	})
	if err != nil {
		return fmt.Errorf("list workspace artifacts: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"artifacts":    records,
	})
}

func cliWorkspaceArtifactPromptContextEnvelope(ctx context.Context, store *sqlite.Store, surface, workspaceID, actorID string) (map[string]any, error) {
	actorID = strings.TrimSpace(actorID)
	workspaceID = strings.TrimSpace(workspaceID)
	principalType := "operator"
	if actorID != "" && workspaceID != "" {
		if _, err := store.GetAgent(ctx, workspaceID, actorID); err == nil {
			principalType = "agent"
		} else if !errors.Is(err, sqlite.ErrAgentNotFound) {
			return nil, fmt.Errorf("resolve workspace artifact prompt context actor: %w", err)
		}
	}
	return sqlite.BuildWorkspaceArtifactPromptContextEnvelope(surface, "cli_local", workspaceID, principalType, actorID), nil
}

func runWorkspaceCompaction(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace compaction subcommand")
	}
	switch args[0] {
	case "candidates":
		return runWorkspaceCompactionCandidates(args[1:])
	case "snapshots":
		return runWorkspaceCompactionSnapshots(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace compaction subcommand: %s", args[0])
	}
}

func runWorkspaceCompactionCandidates(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace compaction candidates", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	agentID := fs.String("agent-id", "", "Optional agent filter")
	activeOnly := fs.Bool("active-only", true, "Restrict to active sessions")
	minMessages := fs.Int("min-messages", model.DefaultSessionCompactionMinMessages, "Minimum canonical session message count")
	minTokens := fs.Int("min-tokens", model.DefaultSessionCompactionMinTokens, "Minimum canonical total token count")
	limit := fs.Int("limit", 20, "Maximum candidates to list")
	if err := fs.Parse(args); err != nil {
		return err
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
	items, err := store.ListSessionCompactionCandidates(ctx, sqlite.SessionCompactionFilter{
		WorkspaceID: *workspaceID,
		AgentID:     *agentID,
		ActiveOnly:  *activeOnly,
		MinMessages: *minMessages,
		MinTokens:   *minTokens,
		Limit:       *limit,
	})
	if err != nil {
		return fmt.Errorf("list workspace compaction candidates: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"agent_id":     strings.TrimSpace(*agentID),
		"active_only":  *activeOnly,
		"min_messages": *minMessages,
		"min_tokens":   *minTokens,
		"items":        items,
		"count":        len(items),
	})
}

func runWorkspaceCompactionSnapshots(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace compaction snapshots", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	sessionID := fs.String("session-id", "", "Optional session filter")
	agentID := fs.String("agent-id", "", "Optional agent filter")
	limit := fs.Int("limit", 20, "Maximum snapshots to list")
	if err := fs.Parse(args); err != nil {
		return err
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
	items, err := store.ListSessionCompactionSnapshots(ctx, sqlite.SessionCompactionSnapshotFilter{
		WorkspaceID: *workspaceID,
		SessionID:   *sessionID,
		AgentID:     *agentID,
		Limit:       *limit,
	})
	if err != nil {
		return fmt.Errorf("list workspace compaction snapshots: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"session_id":   strings.TrimSpace(*sessionID),
		"agent_id":     strings.TrimSpace(*agentID),
		"items":        items,
		"count":        len(items),
	})
}

func printWorkspaceUsage(out *os.File) {
	fmt.Fprintln(out, "Workspace commands:")
	fmt.Fprintln(out, "  RHIZOME_WORKSPACE_PASSWORD=<secret> rhizome workspace create --workspace-id <id> --title <title> --created-by <user> [--description text] [--status ACTIVE]")
	fmt.Fprintln(out, "  rhizome workspace status --workspace-id <id> [--updates-limit 10]")
	fmt.Fprintln(out, "  rhizome workspace search --workspace-id <id> --query <text> [--type doc|task|update|tool|artifact|memory|claim] [--limit 20]")
	fmt.Fprintln(out, "  rhizome workspace events list --workspace-id <id> [--event-type session.blocked] [--entity-type agent_session] [--entity-id <id>] [--agent-id <id>] [--session-id <id>] [--task-id <id>] [--limit 50]")
	fmt.Fprintln(out, "  rhizome workspace events replay --workspace-id <id> [--agent-id <id>] [--session-id <id>] [--task-id <id>] [--limit 500] [--include-events]")
	fmt.Fprintln(out, "  rhizome workspace events evaluate --workspace-id <id> [--agent-id <id>] [--session-id <id>] [--task-id <id>] [--limit 500]")
	fmt.Fprintln(out, "  rhizome workspace authority status --workspace-id <id> [--scope workspace]")
	fmt.Fprintln(out, "  rhizome workspace authority ensure-local --workspace-id <id> [--scope workspace] [--actor-type operator|system] [--actor-id <actor>]")
	fmt.Fprintln(out, "  rhizome workspace authority force-break --workspace-id <id> [--scope workspace] [--actor-type operator|system] [--actor-id <actor>]")
	fmt.Fprintln(out, "  rhizome workspace authority maintain-once")
	fmt.Fprintln(out, "  rhizome workspace ops upsert --workspace-id <id> --queue-key <key> --title <title> [--queue-type BLOCKER] [--summary text] [--details text|--file path] [--assigned-to <actor>] [--urgency NORMAL] [--source-kind session_event] [--source-id <id>] [--task-id <task>] [--session-id <id>] [--agent-id <id>] [--keep-session-active]")
	fmt.Fprintln(out, "  rhizome workspace ops list --workspace-id <id> [--queue-type BLOCKER] [--status OPEN] [--assigned-to <actor>] [--session-id <id>] [--task-id <task>] [--agent-id <id>] [--limit 50]")
	fmt.Fprintln(out, "  rhizome workspace ops resolve --workspace-id <id> (--queue-id <id>|--queue-key <key>) --resolved-by <actor> [--status RESOLVED] [--resolution text]")
	fmt.Fprintln(out, "  rhizome workspace ops escalate --workspace-id <id> (--queue-id <id>|--queue-key <key>) --escalated-by <actor> [--reason text] [--assigned-to <actor>] [--urgency HIGH] [--due-at RFC3339]")
	fmt.Fprintln(out, "  rhizome workspace claim write --workspace-id <id> --subject <text> (--body text|--file path) [--claim-id <id>] [--claim-type FACT] [--status ACTIVE] [--summary text] [--confidence 0.8] [--source-kind manual] [--source-id <id>] [--memory-id <id>] [--task-id <task>] [--session-id <id>] [--agent-id <id>] [--supersedes-claim-id <id>] [--conflicts-claim-id <id>] [--evidence a,b] [--tags a,b]")
	fmt.Fprintln(out, "  rhizome workspace claim list --workspace-id <id> [--claim-type FACT] [--status ACTIVE] [--agent-id <id>] [--session-id <id>] [--task-id <task>] [--source-kind manual] [--include-archived] [--limit 20]")
	fmt.Fprintln(out, "  rhizome workspace claim search --workspace-id <id> --query <text> [--claim-type FACT] [--status ACTIVE] [--agent-id <id>] [--session-id <id>] [--task-id <task>] [--source-kind manual] [--include-archived] [--limit 20]")
	fmt.Fprintln(out, "  rhizome workspace claim review --workspace-id <id> --claim-id <id> --actor-id <actor> [--reason text] [--due-at RFC3339] [--assigned-to <actor>]")
	fmt.Fprintln(out, "  rhizome workspace claim confirm --workspace-id <id> --claim-id <id> --actor-id <actor> [--reason text]")
	fmt.Fprintln(out, "  rhizome workspace claim dispute --workspace-id <id> --claim-id <id> --actor-id <actor> [--reason text] [--due-at RFC3339] [--assigned-to <actor>] [--conflicts-claim-id <id>]")
	fmt.Fprintln(out, "  rhizome workspace claim supersede --workspace-id <id> --claim-id <id> --actor-id <actor> --superseding-claim-id <id> [--reason text]")
	fmt.Fprintln(out, "  rhizome workspace claim stale --workspace-id <id> --claim-id <id> --actor-id <actor> [--reason text] [--due-at RFC3339] [--assigned-to <actor>]")
	fmt.Fprintln(out, "  rhizome workspace claim escalate --workspace-id <id> --claim-id <id> --actor-id <actor> [--reason text] [--assigned-to <actor>] [--urgency HIGH] [--due-at RFC3339]")
	fmt.Fprintln(out, "  rhizome workspace claim archive --workspace-id <id> --claim-id <id> --archived-by <actor> [--reason text]")
	fmt.Fprintln(out, "  rhizome workspace execution run write --workspace-id <id> --title <title> [--run-id <id>] [--task-id <task>] [--session-id <id>] [--agent-id <id>] [--summary text] [--status PLANNED] [--outcome text]")
	fmt.Fprintln(out, "  rhizome workspace execution run list --workspace-id <id> [--status ACTIVE] [--task-id <task>] [--session-id <id>] [--agent-id <id>] [--limit 20]")
	fmt.Fprintln(out, "  rhizome workspace execution run get --workspace-id <id> --run-id <id>")
	fmt.Fprintln(out, "  rhizome workspace execution step write --workspace-id <id> --run-id <id> --title <title> [--step-id <id>] [--parent-step-id <id>] [--phase PLAN] [--summary text] [--status PENDING] [--sort-order 0] [--evidence a,b] [--verification json|--verification-file path]")
	fmt.Fprintln(out, "  rhizome workspace policy put --workspace-id <id> --subject-type <type> --created-by <actor> [--policy-id <id>] [--subject-id <id|*>] [--capability <name|*>] [--tool-id <id|*>] [--effect ALLOW] [--reason text]")
	fmt.Fprintln(out, "  rhizome workspace policy list --workspace-id <id> [--subject-type <type>] [--subject-id <id>] [--capability <name>] [--tool-id <id>] [--limit 50]")
	fmt.Fprintln(out, "  rhizome workspace policy check --workspace-id <id> --subject-type <type> --subject-id <id> --capability <name> [--tool-id <id>]")
	fmt.Fprintln(out, "  rhizome workspace project repository upsert --workspace-id <id> --project-id <id> --actor-id <actor> --repo-id <id> [--remote-url url] [--remote-kind local|github|gitlab|unknown] [--repo-status READY] [--canonical]")
	fmt.Fprintln(out, "  rhizome workspace project repository list --workspace-id <id> --project-id <id> [--include-archived]")
	fmt.Fprintln(out, "  rhizome workspace memory write --workspace-id <id> [--memory-id <id>] [--memory-type NOTE] [--title <title>] (--body text|--file path) [--summary text] [--agent-id <id>] [--session-id <id>] [--task-id <id>] [--source-kind manual] [--source-id <id>] [--tags a,b] [--importance 0.7] [--confidence 0.9]")
	fmt.Fprintln(out, "  rhizome workspace memory list --workspace-id <id> [--memory-type NOTE] [--agent-id <id>] [--session-id <id>] [--task-id <id>] [--source-kind compaction] [--include-archived] [--limit 20]")
	fmt.Fprintln(out, "  rhizome workspace memory search --workspace-id <id> --query <text> [--memory-type NOTE] [--agent-id <id>] [--session-id <id>] [--task-id <id>] [--source-kind manual] [--include-archived] [--limit 20]")
	fmt.Fprintln(out, "  rhizome workspace memory remove --workspace-id <id> --memory-id <id> --removed-by <actor> [--reason <text>]")
	fmt.Fprintln(out, "  rhizome workspace memory restore --workspace-id <id> --memory-id <id> --restored-by <actor>")
	fmt.Fprintln(out, "  rhizome workspace doc put --workspace-id <id> --doc-key <key> --title <title> --updated-by <actor> [--content text|--file path]")
	fmt.Fprintln(out, "  rhizome workspace doc get --workspace-id <id> --doc-key <key>")
	fmt.Fprintln(out, "  rhizome workspace doc history --workspace-id <id> --doc-key <key> [--limit 20]")
	fmt.Fprintln(out, "  rhizome workspace doc archive --workspace-id <id> --doc-key <key> --archived-by <actor>")
	fmt.Fprintln(out, "  rhizome workspace doc delete --workspace-id <id> --doc-key <key> --deleted-by <actor>")
	fmt.Fprintln(out, "  rhizome workspace task attach --workspace-id <id> --task-id <task> --linked-by <actor>")
	fmt.Fprintln(out, "  rhizome workspace task link --workspace-id <id> --from-task-id <task> --to-task-id <task> [--link-type RELATES_TO] --created-by <actor>")
	fmt.Fprintln(out, "  rhizome workspace task links --workspace-id <id> [--task-id <task>] [--link-type BLOCKS] [--limit 50]")
	fmt.Fprintln(out, "  rhizome workspace artifact put --workspace-id <id> --title <title> --ref <path|url> --created-by <actor> [--artifact-id <id>] [--task-id <task>] [--update-id <update>] [--kind reference] [--content-type mime] [--metadata json|--metadata-file path]")
	fmt.Fprintln(out, "  rhizome workspace artifact list --workspace-id <id> [--task-id <task>] [--update-id <update>] [--limit 20]")
	fmt.Fprintln(out, "  rhizome workspace compaction candidates --workspace-id <id> [--agent-id <id>] [--active-only=true] [--min-messages 12] [--min-tokens 12000] [--limit 20]")
	fmt.Fprintln(out, "  rhizome workspace compaction snapshots --workspace-id <id> [--session-id <id>] [--agent-id <id>] [--limit 20]")
}

func readTextValue(inline, filePath string) (string, error) {
	if strings.TrimSpace(inline) != "" {
		return inline, nil
	}
	if strings.TrimSpace(filePath) == "" {
		return "", nil
	}
	raw, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("read file %s: %w", filePath, err)
	}
	return string(raw), nil
}
