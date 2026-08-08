package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func runWorkspaceEvents(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace events subcommand")
	}
	switch args[0] {
	case "list":
		return runWorkspaceEventsList(args[1:])
	case "replay":
		return runWorkspaceEventsReplay(args[1:])
	case "evaluate":
		return runWorkspaceEventsEvaluate(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace events subcommand: %s", args[0])
	}
}

func runWorkspaceEventsList(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace events list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	eventType := fs.String("event-type", "", "Optional event type filter")
	entityType := fs.String("entity-type", "", "Optional entity type filter")
	entityID := fs.String("entity-id", "", "Optional entity identifier filter")
	agentID := fs.String("agent-id", "", "Optional agent filter")
	sessionID := fs.String("session-id", "", "Optional session filter")
	taskID := fs.String("task-id", "", "Optional task filter")
	limit := fs.Int("limit", 50, "Maximum events to list")
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
	items, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: *workspaceID,
		EventType:   *eventType,
		EntityType:  *entityType,
		EntityID:    *entityID,
		AgentID:     *agentID,
		SessionID:   *sessionID,
		TaskID:      *taskID,
		Limit:       *limit,
	})
	if err != nil {
		return fmt.Errorf("list runtime events: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"items":        items,
		"count":        len(items),
	})
}

func runWorkspaceEventsReplay(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace events replay", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	agentID := fs.String("agent-id", "", "Optional agent filter")
	sessionID := fs.String("session-id", "", "Optional session filter")
	taskID := fs.String("task-id", "", "Optional task filter")
	limit := fs.Int("limit", 500, "Maximum events to replay")
	includeEvents := fs.Bool("include-events", false, "Include raw runtime events in the report")
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
	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: *workspaceID,
		AgentID:     *agentID,
		SessionID:   *sessionID,
		TaskID:      *taskID,
		Limit:       *limit,
	})
	if err != nil {
		return fmt.Errorf("replay runtime journal: %w", err)
	}
	if !*includeEvents {
		report.Events = nil
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"report":       report,
	})
}

func runWorkspaceEventsEvaluate(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace events evaluate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	agentID := fs.String("agent-id", "", "Optional agent filter")
	sessionID := fs.String("session-id", "", "Optional session filter")
	taskID := fs.String("task-id", "", "Optional task filter")
	limit := fs.Int("limit", 500, "Maximum events to replay")
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
	report, err := store.ReplayRuntimeJournal(ctx, sqlite.RuntimeReplayFilter{
		WorkspaceID: *workspaceID,
		AgentID:     *agentID,
		SessionID:   *sessionID,
		TaskID:      *taskID,
		Limit:       *limit,
	})
	if err != nil {
		return fmt.Errorf("evaluate runtime journal: %w", err)
	}
	report.Events = nil
	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"metrics":      report.Metrics,
		"evaluation":   report.Evaluation,
		"counts": map[string]int{
			"sessions":       len(report.Sessions),
			"queues":         len(report.Queues),
			"claims":         len(report.Claims),
			"execution_runs": len(report.ExecutionRuns),
		},
	})
}

func runWorkspaceOps(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace ops subcommand")
	}
	switch args[0] {
	case "upsert":
		return runWorkspaceOpsUpsert(args[1:])
	case "list":
		return runWorkspaceOpsList(args[1:])
	case "resolve":
		return runWorkspaceOpsResolve(args[1:])
	case "escalate":
		return runWorkspaceOpsEscalate(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace ops subcommand: %s", args[0])
	}
}

func isOperatorQueueItemNotFoundCLI(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "operator queue item not found")
}

func lookupCLIQueueMutationTargets(ctx context.Context, store *sqlite.Store, workspaceID, queueID, queueKey string) (*sqlite.OperatorQueueRecord, *sqlite.OperatorQueueRecord, error) {
	if store == nil {
		return nil, nil, nil
	}
	workspaceID = strings.TrimSpace(workspaceID)
	queueID = strings.TrimSpace(queueID)
	queueKey = strings.TrimSpace(queueKey)
	var byID *sqlite.OperatorQueueRecord
	if queueID != "" {
		record, err := store.GetOperatorQueueItem(ctx, workspaceID, queueID, "")
		if err != nil {
			if !isOperatorQueueItemNotFoundCLI(err) {
				return nil, nil, err
			}
		} else {
			recordCopy := record
			byID = &recordCopy
		}
	}
	var byKey *sqlite.OperatorQueueRecord
	if queueKey != "" {
		record, err := store.GetOperatorQueueItem(ctx, workspaceID, "", queueKey)
		if err != nil {
			if !isOperatorQueueItemNotFoundCLI(err) {
				return nil, nil, err
			}
		} else {
			recordCopy := record
			byKey = &recordCopy
		}
	}
	return byID, byKey, nil
}

func requireCLIQueueBaseVersionForAdvancedRevision(ctx context.Context, store *sqlite.Store, workspaceID, queueID, queueKey string, currentRevision int64, currentUpdatedAt, commandLabel string) error {
	queueID = strings.TrimSpace(queueID)
	queueKey = strings.TrimSpace(queueKey)
	byID, byKey, err := lookupCLIQueueMutationTargets(ctx, store, workspaceID, queueID, queueKey)
	if err != nil {
		return err
	}
	if byID != nil && queueKey != "" && byID.QueueKey != queueKey {
		return fmt.Errorf("%s requires queue_id and queue_key to refer to the same queue item", commandLabel)
	}
	if byKey != nil && queueID != "" && byKey.QueueID != queueID {
		return fmt.Errorf("%s requires queue_id and queue_key to refer to the same queue item", commandLabel)
	}
	if currentRevision > 0 || strings.TrimSpace(currentUpdatedAt) != "" {
		return nil
	}
	queue := byID
	if queue == nil {
		queue = byKey
	}
	if queue == nil || queue.Revision <= 1 {
		return nil
	}
	return fmt.Errorf("%s requires current_revision (preferred) or current_updated_at once queue revision has advanced beyond its initial create", commandLabel)
}

func runWorkspaceOpsUpsert(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace ops upsert", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	queueID := fs.String("queue-id", "", "Optional stable queue id")
	queueKey := fs.String("queue-key", "", "Stable queue key")
	queueType := fs.String("queue-type", "BLOCKER", "Queue type")
	title := fs.String("title", "", "Queue title")
	summary := fs.String("summary", "", "Optional summary")
	details := fs.String("details", "", "Inline details")
	detailsFile := fs.String("file", "", "Path to details file")
	assignedTo := fs.String("assigned-to", "", "Optional assignee")
	urgency := fs.String("urgency", "NORMAL", "Urgency")
	sourceKind := fs.String("source-kind", "", "Optional source kind")
	sourceID := fs.String("source-id", "", "Optional source id")
	taskID := fs.String("task-id", "", "Optional task id")
	sessionID := fs.String("session-id", "", "Optional session id")
	agentID := fs.String("agent-id", "", "Optional agent id")
	keepSessionActive := fs.Bool("keep-session-active", false, "Whether peers should keep sessions active")
	dueAt := fs.String("due-at", "", "Optional due timestamp")
	currentRevision := fs.Int64("current-revision", 0, "Optional optimistic-concurrency queue revision")
	currentUpdatedAt := fs.String("current-updated-at", "", "Optional optimistic-concurrency token from the current queue revision")
	if err := fs.Parse(args); err != nil {
		return err
	}

	detailsText, err := readTextValue(*details, *detailsFile)
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
	if err := requireCLIQueueBaseVersionForAdvancedRevision(ctx, store, *workspaceID, *queueID, *queueKey, *currentRevision, *currentUpdatedAt, "workspace ops upsert"); err != nil {
		return err
	}
	record, err := store.UpsertOperatorQueueItem(ctx, sqlite.OperatorQueueUpsertInput{
		QueueID:                 *queueID,
		WorkspaceID:             *workspaceID,
		QueueKey:                *queueKey,
		QueueType:               *queueType,
		Title:                   *title,
		Summary:                 *summary,
		Details:                 detailsText,
		AssignedTo:              *assignedTo,
		Urgency:                 *urgency,
		SourceKind:              *sourceKind,
		SourceID:                *sourceID,
		TaskID:                  *taskID,
		SessionID:               *sessionID,
		AgentID:                 *agentID,
		KeepSessionActive:       *keepSessionActive,
		DueAt:                   *dueAt,
		RequireCurrentRevision:  *currentRevision,
		RequireCurrentUpdatedAt: *currentUpdatedAt,
		PromptContextEnvelope:   cliOperatorQueuePromptContextEnvelope("cli.workspace.ops.upsert", *workspaceID),
	})
	if err != nil {
		return fmt.Errorf("upsert operator queue item: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"item":     record,
		"status":   "UPSERTED",
	})
}

func runWorkspaceOpsList(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace ops list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	queueType := fs.String("queue-type", "", "Optional queue type filter")
	status := fs.String("status", "", "Optional status filter")
	assignedTo := fs.String("assigned-to", "", "Optional assignee filter")
	sessionID := fs.String("session-id", "", "Optional session filter")
	taskID := fs.String("task-id", "", "Optional task filter")
	agentID := fs.String("agent-id", "", "Optional agent filter")
	limit := fs.Int("limit", 50, "Maximum items to list")
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
	items, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: *workspaceID,
		QueueType:   *queueType,
		Status:      *status,
		AssignedTo:  *assignedTo,
		SessionID:   *sessionID,
		TaskID:      *taskID,
		AgentID:     *agentID,
		Limit:       *limit,
	})
	if err != nil {
		return fmt.Errorf("list operator queue items: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"items":        items,
		"count":        len(items),
	})
}

func runWorkspaceOpsResolve(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace ops resolve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	queueID := fs.String("queue-id", "", "Queue id")
	queueKey := fs.String("queue-key", "", "Queue key")
	status := fs.String("status", "RESOLVED", "Resolution status")
	resolvedBy := fs.String("resolved-by", "", "Actor resolving the queue item")
	resolution := fs.String("resolution", "", "Resolution note")
	currentRevision := fs.Int64("current-revision", 0, "Optional optimistic-concurrency queue revision")
	currentUpdatedAt := fs.String("current-updated-at", "", "Optional optimistic-concurrency token from the current queue revision")
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
	if err := requireCLIQueueBaseVersionForAdvancedRevision(ctx, store, *workspaceID, *queueID, *queueKey, *currentRevision, *currentUpdatedAt, "workspace ops resolve"); err != nil {
		return err
	}
	record, err := store.ResolveOperatorQueueItem(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID:             *workspaceID,
		QueueID:                 *queueID,
		QueueKey:                *queueKey,
		Status:                  *status,
		ResolvedBy:              *resolvedBy,
		Resolution:              *resolution,
		RequireCurrentRevision:  *currentRevision,
		RequireCurrentUpdatedAt: *currentUpdatedAt,
		PromptContextEnvelope:   cliOperatorQueuePromptContextEnvelope("cli.workspace.ops.resolve", *workspaceID),
	})
	if err != nil {
		return fmt.Errorf("resolve operator queue item: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"item":     record,
		"status":   record.Status,
	})
}

func runWorkspaceOpsEscalate(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace ops escalate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	queueID := fs.String("queue-id", "", "Queue id")
	queueKey := fs.String("queue-key", "", "Queue key")
	escalatedBy := fs.String("escalated-by", "", "Actor escalating the queue item")
	reason := fs.String("reason", "", "Optional escalation reason")
	assignedTo := fs.String("assigned-to", "", "Optional new assignee")
	urgency := fs.String("urgency", "", "Optional new urgency")
	dueAt := fs.String("due-at", "", "Optional RFC3339 due timestamp")
	currentRevision := fs.Int64("current-revision", 0, "Optional optimistic-concurrency queue revision")
	currentUpdatedAt := fs.String("current-updated-at", "", "Optional optimistic-concurrency token from the current queue revision")
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
	if err := requireCLIQueueBaseVersionForAdvancedRevision(ctx, store, *workspaceID, *queueID, *queueKey, *currentRevision, *currentUpdatedAt, "workspace ops escalate"); err != nil {
		return err
	}
	record, err := store.EscalateOperatorQueueItem(ctx, sqlite.OperatorQueueEscalateInput{
		WorkspaceID:             *workspaceID,
		QueueID:                 *queueID,
		QueueKey:                *queueKey,
		EscalatedBy:             *escalatedBy,
		Reason:                  *reason,
		AssignedTo:              *assignedTo,
		Urgency:                 *urgency,
		DueAt:                   *dueAt,
		RequireCurrentRevision:  *currentRevision,
		RequireCurrentUpdatedAt: *currentUpdatedAt,
		PromptContextEnvelope:   cliOperatorQueuePromptContextEnvelope("cli.workspace.ops.escalate", *workspaceID),
	})
	if err != nil {
		return fmt.Errorf("escalate operator queue item: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"item":     record,
		"status":   record.Status,
	})
}

func runWorkspaceClaim(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace claim subcommand")
	}
	switch args[0] {
	case "write":
		return runWorkspaceClaimWrite(args[1:])
	case "list":
		return runWorkspaceClaimList(args[1:])
	case "search":
		return runWorkspaceClaimSearch(args[1:])
	case "review":
		return runWorkspaceClaimReview(args[1:])
	case "confirm":
		return runWorkspaceClaimConfirm(args[1:])
	case "dispute":
		return runWorkspaceClaimDispute(args[1:])
	case "supersede":
		return runWorkspaceClaimSupersede(args[1:])
	case "stale":
		return runWorkspaceClaimStale(args[1:])
	case "escalate":
		return runWorkspaceClaimEscalate(args[1:])
	case "archive":
		return runWorkspaceClaimArchive(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace claim subcommand: %s", args[0])
	}
}

func runWorkspaceClaimWrite(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace claim write", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	claimID := fs.String("claim-id", "", "Optional claim id")
	claimType := fs.String("claim-type", "FACT", "Claim type")
	status := fs.String("status", "ACTIVE", "Claim status")
	subject := fs.String("subject", "", "Claim subject")
	body := fs.String("body", "", "Inline claim body")
	bodyFile := fs.String("file", "", "Path to claim body file")
	summary := fs.String("summary", "", "Optional summary")
	confidence := fs.Float64("confidence", 0, "Confidence in 0..1")
	sourceKind := fs.String("source-kind", "manual", "Source kind")
	sourceID := fs.String("source-id", "", "Optional source id")
	memoryID := fs.String("memory-id", "", "Optional linked memory id")
	taskID := fs.String("task-id", "", "Optional task id")
	sessionID := fs.String("session-id", "", "Optional session id")
	agentID := fs.String("agent-id", "", "Optional agent id")
	supersedesClaimID := fs.String("supersedes-claim-id", "", "Optional superseded claim id")
	conflictsClaimID := fs.String("conflicts-claim-id", "", "Optional conflicting claim id")
	evidence := fs.String("evidence", "", "Comma separated evidence references")
	tags := fs.String("tags", "", "Comma separated tags")
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
	record, _, _, err := store.RecordKnowledgeClaimWithAuthorityEffects(ctx, sqlite.KnowledgeClaimInput{
		ClaimID:           *claimID,
		WorkspaceID:       *workspaceID,
		ClaimType:         *claimType,
		Status:            *status,
		Subject:           *subject,
		Body:              bodyText,
		Summary:           *summary,
		Confidence:        *confidence,
		SourceKind:        *sourceKind,
		SourceID:          *sourceID,
		MemoryID:          *memoryID,
		TaskID:            *taskID,
		SessionID:         *sessionID,
		AgentID:           *agentID,
		SupersedesClaimID: *supersedesClaimID,
		ConflictsClaimID:  *conflictsClaimID,
		Evidence:          parseCSV(*evidence),
		Tags:              parseCSV(*tags),
	})
	if err != nil {
		return fmt.Errorf("record knowledge claim: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{"trace_id": traceID, "claim": record, "status": "RECORDED"})
}

func runWorkspaceClaimList(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace claim list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	claimType := fs.String("claim-type", "", "Optional claim type filter")
	status := fs.String("status", "", "Optional status filter")
	agentID := fs.String("agent-id", "", "Optional agent filter")
	sessionID := fs.String("session-id", "", "Optional session filter")
	taskID := fs.String("task-id", "", "Optional task filter")
	memoryID := fs.String("memory-id", "", "Optional memory filter")
	sourceKind := fs.String("source-kind", "", "Optional source kind filter")
	includeArchived := fs.Bool("include-archived", false, "Include archived claims")
	limit := fs.Int("limit", 20, "Maximum claims to list")
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
	items, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID:     *workspaceID,
		ClaimType:       *claimType,
		Status:          *status,
		AgentID:         *agentID,
		SessionID:       *sessionID,
		TaskID:          *taskID,
		MemoryID:        *memoryID,
		SourceKind:      *sourceKind,
		IncludeArchived: *includeArchived,
		Limit:           *limit,
	})
	if err != nil {
		return fmt.Errorf("list knowledge claims: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{"trace_id": traceID, "workspace_id": strings.TrimSpace(*workspaceID), "items": items, "count": len(items)})
}

func runWorkspaceClaimSearch(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace claim search", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	query := fs.String("query", "", "Search query")
	claimType := fs.String("claim-type", "", "Optional claim type filter")
	status := fs.String("status", "", "Optional status filter")
	agentID := fs.String("agent-id", "", "Optional agent filter")
	sessionID := fs.String("session-id", "", "Optional session filter")
	taskID := fs.String("task-id", "", "Optional task filter")
	memoryID := fs.String("memory-id", "", "Optional memory filter")
	sourceKind := fs.String("source-kind", "", "Optional source kind filter")
	includeArchived := fs.Bool("include-archived", false, "Include archived claims")
	limit := fs.Int("limit", 20, "Maximum claims to list")
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
	items, err := store.SearchKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID:     *workspaceID,
		Query:           *query,
		ClaimType:       *claimType,
		Status:          *status,
		AgentID:         *agentID,
		SessionID:       *sessionID,
		TaskID:          *taskID,
		MemoryID:        *memoryID,
		SourceKind:      *sourceKind,
		IncludeArchived: *includeArchived,
		Limit:           *limit,
	})
	if err != nil {
		return fmt.Errorf("search knowledge claims: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{"trace_id": traceID, "workspace_id": strings.TrimSpace(*workspaceID), "query": strings.TrimSpace(*query), "items": items, "count": len(items)})
}

func runWorkspaceClaimArchive(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace claim archive", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	claimID := fs.String("claim-id", "", "Claim identifier")
	archivedBy := fs.String("archived-by", "", "Actor archiving the claim")
	reason := fs.String("reason", "", "Optional archive reason")
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
	record, err := store.ArchiveKnowledgeClaim(ctx, sqlite.KnowledgeClaimArchiveInput{
		WorkspaceID: *workspaceID,
		ClaimID:     *claimID,
		ArchivedBy:  *archivedBy,
		Reason:      *reason,
	})
	if err != nil {
		return fmt.Errorf("archive knowledge claim: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{"trace_id": traceID, "claim": record, "status": "ARCHIVED"})
}

func runWorkspaceClaimReview(args []string) error {
	return runWorkspaceClaimLifecycleAction("review", args)
}

func runWorkspaceClaimConfirm(args []string) error {
	return runWorkspaceClaimLifecycleAction("confirm", args)
}

func runWorkspaceClaimDispute(args []string) error {
	return runWorkspaceClaimLifecycleAction("dispute", args)
}

func runWorkspaceClaimSupersede(args []string) error {
	return runWorkspaceClaimLifecycleAction("supersede", args)
}

func runWorkspaceClaimStale(args []string) error {
	return runWorkspaceClaimLifecycleAction("stale", args)
}

func runWorkspaceClaimEscalate(args []string) error {
	return runWorkspaceClaimLifecycleAction("escalate", args)
}

func runWorkspaceClaimLifecycleAction(action string, args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace claim "+action, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	claimID := fs.String("claim-id", "", "Claim identifier")
	actorID := fs.String("actor-id", "", "Actor performing the lifecycle change")
	reason := fs.String("reason", "", "Optional lifecycle reason")
	dueAt := fs.String("due-at", "", "Optional RFC3339 review due timestamp")
	assignedTo := fs.String("assigned-to", "", "Optional assignee for review follow-up")
	urgency := fs.String("urgency", "", "Optional urgency for review follow-up")
	supersedingClaimID := fs.String("superseding-claim-id", "", "Optional superseding claim id")
	conflictsClaimID := fs.String("conflicts-claim-id", "", "Optional conflicting claim id")
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

	input := sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID:        *workspaceID,
		ClaimID:            *claimID,
		ActorID:            *actorID,
		Reason:             *reason,
		ReviewDueAt:        *dueAt,
		AssignedTo:         *assignedTo,
		Urgency:            *urgency,
		SupersedingClaimID: *supersedingClaimID,
		ConflictsClaimID:   *conflictsClaimID,
	}
	var record sqlite.KnowledgeClaimRecord
	var queue *sqlite.OperatorQueueRecord
	switch action {
	case "review":
		record, err = store.RequestKnowledgeClaimReview(ctx, input)
	case "confirm":
		record, err = store.ConfirmKnowledgeClaim(ctx, input)
	case "dispute":
		record, err = store.DisputeKnowledgeClaim(ctx, input)
	case "supersede":
		record, err = store.SupersedeKnowledgeClaim(ctx, input)
	case "stale":
		record, err = store.MarkKnowledgeClaimStale(ctx, input)
	case "escalate":
		var escalated sqlite.KnowledgeClaimReviewEscalationRecord
		escalated, err = store.EscalateKnowledgeClaimReview(ctx, input)
		if err == nil {
			record = escalated.Claim
			queue = &escalated.Queue
		}
	default:
		err = fmt.Errorf("unsupported claim lifecycle action: %s", action)
	}
	if err != nil {
		return fmt.Errorf("%s knowledge claim: %w", action, err)
	}
	payload := map[string]any{"trace_id": traceID, "claim": record, "status": record.Status}
	if queue != nil {
		payload["queue"] = queue
	}
	return writeJSON(os.Stdout, payload)
}

func runWorkspaceExecution(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace execution subcommand")
	}
	switch args[0] {
	case "run":
		return runWorkspaceExecutionRun(args[1:])
	case "step":
		return runWorkspaceExecutionStep(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace execution subcommand: %s", args[0])
	}
}

func runWorkspaceExecutionRun(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace execution run subcommand")
	}
	switch args[0] {
	case "write":
		return runWorkspaceExecutionRunWrite(args[1:])
	case "list":
		return runWorkspaceExecutionRunList(args[1:])
	case "get":
		return runWorkspaceExecutionRunGet(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace execution run subcommand: %s", args[0])
	}
}

func runWorkspaceExecutionRunWrite(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace execution run write", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	runID := fs.String("run-id", "", "Optional run id")
	taskID := fs.String("task-id", "", "Optional task id")
	sessionID := fs.String("session-id", "", "Optional session id")
	agentID := fs.String("agent-id", "", "Optional agent id")
	title := fs.String("title", "", "Run title")
	summary := fs.String("summary", "", "Optional run summary")
	status := fs.String("status", "PLANNED", "Run status")
	outcome := fs.String("outcome", "", "Optional run outcome")
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
	record, _, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		RunID:       *runID,
		WorkspaceID: *workspaceID,
		TaskID:      *taskID,
		SessionID:   *sessionID,
		AgentID:     *agentID,
		Title:       *title,
		Summary:     *summary,
		Status:      *status,
		Outcome:     *outcome,
		PromptContextEnvelope: cliExecutionPromptContextEnvelope(
			"cli.workspace.execution.run.write",
			*workspaceID,
		),
	})
	if err != nil {
		return fmt.Errorf("upsert execution run: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{"trace_id": traceID, "run": record, "status": "RECORDED"})
}

func runWorkspaceExecutionRunList(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace execution run list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	status := fs.String("status", "", "Optional run status filter")
	taskID := fs.String("task-id", "", "Optional task filter")
	sessionID := fs.String("session-id", "", "Optional session filter")
	agentID := fs.String("agent-id", "", "Optional agent filter")
	limit := fs.Int("limit", 20, "Maximum runs to list")
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
	items, err := store.ListExecutionRuns(ctx, sqlite.ExecutionRunFilter{
		WorkspaceID: *workspaceID,
		Status:      *status,
		TaskID:      *taskID,
		SessionID:   *sessionID,
		AgentID:     *agentID,
		Limit:       *limit,
	})
	if err != nil {
		return fmt.Errorf("list execution runs: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{"trace_id": traceID, "workspace_id": strings.TrimSpace(*workspaceID), "items": items, "count": len(items)})
}

func runWorkspaceExecutionRunGet(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace execution run get", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	runID := fs.String("run-id", "", "Run identifier")
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
	detail, err := store.GetExecutionRun(ctx, *workspaceID, *runID)
	if err != nil {
		return fmt.Errorf("get execution run: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{"trace_id": traceID, "detail": detail})
}

func runWorkspaceExecutionStep(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace execution step subcommand")
	}
	switch args[0] {
	case "write":
		return runWorkspaceExecutionStepWrite(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace execution step subcommand: %s", args[0])
	}
}

func runWorkspaceExecutionStepWrite(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace execution step write", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	stepID := fs.String("step-id", "", "Optional step id")
	runID := fs.String("run-id", "", "Run identifier")
	parentStepID := fs.String("parent-step-id", "", "Optional parent step id")
	phase := fs.String("phase", "PLAN", "Step phase")
	title := fs.String("title", "", "Step title")
	summary := fs.String("summary", "", "Optional step summary")
	status := fs.String("status", "PENDING", "Step status")
	sortOrder := fs.Int("sort-order", 0, "Step sort order")
	evidence := fs.String("evidence", "", "Comma separated evidence references")
	verification := fs.String("verification", "", "Inline verification JSON")
	verificationFile := fs.String("verification-file", "", "Path to verification JSON file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	evidenceProvided := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "evidence" {
			evidenceProvided = true
		}
	})

	verificationBody, err := readTextValue(*verification, *verificationFile)
	if err != nil {
		return err
	}
	var verificationJSON map[string]any
	if strings.TrimSpace(verificationBody) != "" {
		verificationJSON = map[string]any{}
		if err := json.Unmarshal([]byte(verificationBody), &verificationJSON); err != nil {
			return fmt.Errorf("decode verification json: %w", err)
		}
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
	var evidenceRefs []string
	if evidenceProvided {
		evidenceRefs = parseCSV(*evidence)
	}
	record, _, err := store.RecordExecutionStepWithEvent(ctx, sqlite.ExecutionStepInput{
		StepID:       *stepID,
		RunID:        *runID,
		WorkspaceID:  *workspaceID,
		ParentStepID: *parentStepID,
		Phase:        *phase,
		Title:        *title,
		Summary:      *summary,
		Status:       *status,
		SortOrder:    *sortOrder,
		Evidence:     evidenceRefs,
		Verification: verificationJSON,
		PromptContextEnvelope: cliExecutionPromptContextEnvelope(
			"cli.workspace.execution.step.write",
			*workspaceID,
		),
	})
	if err != nil {
		return fmt.Errorf("record execution step: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{"trace_id": traceID, "step": record, "status": "RECORDED"})
}

func cliExecutionPromptContextEnvelope(surface, workspaceID string) map[string]any {
	return sqlite.BuildExecutionPromptContextEnvelope(surface, "cli_local", workspaceID, "operator", "local_cli")
}

func cliOperatorQueuePromptContextEnvelope(surface, workspaceID string) map[string]any {
	return sqlite.BuildOperatorQueuePromptContextEnvelope(surface, "cli_local", workspaceID, "operator", "local_cli")
}

func runWorkspacePolicy(args []string) error {
	if len(args) < 1 {
		printWorkspaceUsage(os.Stderr)
		return errors.New("missing workspace policy subcommand")
	}
	switch args[0] {
	case "put":
		return runWorkspacePolicyPut(args[1:])
	case "list":
		return runWorkspacePolicyList(args[1:])
	case "check":
		return runWorkspacePolicyCheck(args[1:])
	default:
		printWorkspaceUsage(os.Stderr)
		return fmt.Errorf("unknown workspace policy subcommand: %s", args[0])
	}
}

func runWorkspacePolicyPut(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace policy put", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	policyID := fs.String("policy-id", "", "Optional policy id")
	subjectType := fs.String("subject-type", "", "Subject type")
	subjectID := fs.String("subject-id", "", "Subject identifier")
	capability := fs.String("capability", "", "Capability name")
	toolID := fs.String("tool-id", "", "Tool identifier")
	effect := fs.String("effect", "ALLOW", "Policy effect")
	reason := fs.String("reason", "", "Optional reason")
	createdBy := fs.String("created-by", "", "Actor creating the policy")
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
	record, err := store.PutCapabilityPolicy(ctx, sqlite.CapabilityPolicyInput{
		PolicyID:              *policyID,
		WorkspaceID:           *workspaceID,
		SubjectType:           *subjectType,
		SubjectID:             *subjectID,
		Capability:            *capability,
		ToolID:                *toolID,
		Effect:                *effect,
		Reason:                *reason,
		CreatedBy:             *createdBy,
		PromptContextEnvelope: cliCapabilityPolicyPromptContextEnvelope(*workspaceID, *createdBy),
		PromptContextSurface:  "cli.workspace.policy.put",
	})
	if err != nil {
		return fmt.Errorf("put capability policy: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{"trace_id": traceID, "policy": record, "status": "RECORDED"})
}

func cliCapabilityPolicyPromptContextEnvelope(workspaceID, principalID string) map[string]any {
	return sqlite.BuildCapabilityPolicyPromptContextEnvelope("cli.workspace.policy.put", "cli_local", workspaceID, "operator", principalID)
}

func runWorkspacePolicyList(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace policy list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	subjectType := fs.String("subject-type", "", "Optional subject type filter")
	subjectID := fs.String("subject-id", "", "Optional subject id filter")
	capability := fs.String("capability", "", "Optional capability filter")
	toolID := fs.String("tool-id", "", "Optional tool filter")
	limit := fs.Int("limit", 50, "Maximum policies to list")
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
	items, err := store.ListCapabilityPolicies(ctx, sqlite.CapabilityPolicyFilter{
		WorkspaceID: *workspaceID,
		SubjectType: *subjectType,
		SubjectID:   *subjectID,
		Capability:  *capability,
		ToolID:      *toolID,
		Limit:       *limit,
	})
	if err != nil {
		return fmt.Errorf("list capability policies: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{"trace_id": traceID, "workspace_id": strings.TrimSpace(*workspaceID), "items": items, "count": len(items)})
}

func runWorkspacePolicyCheck(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("workspace policy check", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	subjectType := fs.String("subject-type", "", "Subject type")
	subjectID := fs.String("subject-id", "", "Subject id")
	capability := fs.String("capability", "", "Capability")
	toolID := fs.String("tool-id", "", "Optional tool id")
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
	check, err := store.CheckCapabilityPolicy(ctx, sqlite.CapabilityCheckInput{
		WorkspaceID: *workspaceID,
		SubjectType: *subjectType,
		SubjectID:   *subjectID,
		Capability:  *capability,
		ToolID:      *toolID,
	})
	if err != nil {
		return fmt.Errorf("check capability policy: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{"trace_id": traceID, "check": check})
}
