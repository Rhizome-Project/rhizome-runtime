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
	"github.com/Rhizome-Project/rhizome-runtime/internal/repoauthority"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func runApproval(args []string) error {
	if len(args) < 1 {
		printApprovalUsage(os.Stderr)
		return errors.New("missing approval subcommand")
	}

	switch args[0] {
	case "list":
		return runApprovalList(args[1:])
	case "decide":
		return runApprovalDecide(args[1:])
	case "patch-queue-enable":
		return runApprovalPatchQueueEnable(args[1:])
	default:
		printApprovalUsage(os.Stderr)
		return fmt.Errorf("unknown approval subcommand: %s", args[0])
	}
}

func runApprovalList(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("approval list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	status := fs.String("status", "", "Filter by approval status")
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

	approvals, err := store.ListApprovalRequests(ctx, *status)
	if err != nil {
		return fmt.Errorf("list approvals: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"count":     len(approvals),
		"approvals": approvals,
		"trace_id":  traceID,
	})
}

func runApprovalDecide(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("approval decide", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	approvalID := fs.String("approval-id", "", "Approval identifier")
	decision := fs.String("decision", "", "Decision: approve|reject")
	actor := fs.String("actor", "", "Actor user id")
	note := fs.String("note", "", "Decision note")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*approvalID) == "" {
		return errors.New("--approval-id is required")
	}
	if strings.TrimSpace(*decision) == "" {
		return errors.New("--decision is required")
	}
	if strings.TrimSpace(*actor) == "" {
		return errors.New("--actor is required")
	}
	if err := ensureApprovalActorAuthorized(strings.TrimSpace(*actor)); err != nil {
		return err
	}

	var targetStatus string
	switch strings.ToLower(strings.TrimSpace(*decision)) {
	case "approve":
		targetStatus = model.ApprovalStatusApproved
	case "reject":
		targetStatus = model.ApprovalStatusRejected
	default:
		return fmt.Errorf("unsupported --decision: %s", *decision)
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

	if err := store.DecideApproval(ctx, sqlite.ApprovalDecisionInput{
		ApprovalID:   strings.TrimSpace(*approvalID),
		NewStatus:    targetStatus,
		DecidedBy:    strings.TrimSpace(*actor),
		DecisionNote: strings.TrimSpace(*note),
	}); err != nil {
		return fmt.Errorf("decide approval: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"approval_id": strings.TrimSpace(*approvalID),
		"status":      targetStatus,
		"actor":       strings.TrimSpace(*actor),
		"trace_id":    traceID,
	})
}

func runApprovalPatchQueueEnable(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("approval patch-queue-enable", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	projectID := fs.String("project-id", "", "Project identifier")
	queueID := fs.String("queue-id", "", "Patch queue id")
	itemID := fs.String("item-id", "", "Patch queue item id")
	actor := fs.String("actor", "", "Operator user id")
	reason := fs.String("reason", "", "Operator enablement reason")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*workspaceID) == "" {
		return errors.New("--workspace-id is required")
	}
	if strings.TrimSpace(*projectID) == "" {
		return errors.New("--project-id is required")
	}
	if strings.TrimSpace(*queueID) == "" {
		return errors.New("--queue-id is required")
	}
	if strings.TrimSpace(*itemID) == "" {
		return errors.New("--item-id is required")
	}
	claimToken := strings.TrimSpace(os.Getenv("RHIZOME_PATCH_QUEUE_CLAIM_TOKEN"))
	if claimToken == "" {
		return errors.New("RHIZOME_PATCH_QUEUE_CLAIM_TOKEN is required")
	}
	if strings.TrimSpace(*actor) == "" {
		return errors.New("--actor is required")
	}
	if err := ensurePatchQueueOperatorEnablementActorAuthorized(strings.TrimSpace(*actor)); err != nil {
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

	item, event, err := store.RecordProjectPatchQueueOperatorEnablementWithEvent(ctx, sqlite.ProjectPatchQueueOperatorEnablementRecordInput{
		WorkspaceID: strings.TrimSpace(*workspaceID),
		ProjectID:   strings.TrimSpace(*projectID),
		QueueID:     strings.TrimSpace(*queueID),
		ItemID:      strings.TrimSpace(*itemID),
		OperatorEnablement: repoauthority.PatchQueueOperatorEnablement{
			Enabled: true,
			Reason:  strings.TrimSpace(*reason),
		},
		ClaimToken:            claimToken,
		ActorID:               strings.TrimSpace(*actor),
		ActorType:             "human",
		PromptContextEnvelope: cliProjectPromptContextEnvelope("cli.project.patch_queue.operator_enablement_record", strings.TrimSpace(*workspaceID), strings.TrimSpace(*actor)),
		PromptContextSurface:  "cli.project.patch_queue.operator_enablement_record",
	})
	if err != nil {
		return fmt.Errorf("record patch queue operator enablement: %w", err)
	}

	return writeJSON(os.Stdout, approvalPatchQueueEnableSuccessPayload(
		traceID,
		strings.TrimSpace(*workspaceID),
		strings.TrimSpace(*projectID),
		strings.TrimSpace(*queueID),
		strings.TrimSpace(*itemID),
		strings.TrimSpace(*actor),
		item,
		event,
	))
}

func approvalPatchQueueEnableSuccessPayload(traceID, workspaceID, projectID, queueID, itemID, actor string, item sqlite.ProjectPatchQueueItemRecord, event sqlite.RuntimeEventRecord) map[string]any {
	return map[string]any{
		"trace_id":     traceID,
		"workspace_id": workspaceID,
		"project_id":   projectID,
		"queue_id":     queueID,
		"item_id":      itemID,
		"actor":        actor,
		"item":         sqlite.RedactProjectPatchQueueItemClaimToken(item),
		"event":        event,
	}
}

func printApprovalUsage(out *os.File) {
	fmt.Fprintln(out, "Approval commands:")
	fmt.Fprintln(out, "  rhizome approval list [--status <status>]")
	fmt.Fprintln(out, "  rhizome approval decide --approval-id <id> --decision approve|reject --actor <id> [--note text]")
	fmt.Fprintln(out, "  RHIZOME_PATCH_QUEUE_CLAIM_TOKEN=<token> rhizome approval patch-queue-enable --workspace-id <id> --project-id <id> --queue-id <id> --item-id <id> --actor <id> [--reason text]")
}

func ensureApprovalActorAuthorized(actor string) error {
	operators := parseOperatorIDs(os.Getenv("RHIZOME_OPERATOR_IDS"))
	if len(operators) == 0 {
		operators = map[string]struct{}{
			"operator-1": {},
		}
	}

	if _, ok := operators[strings.TrimSpace(actor)]; !ok {
		return fmt.Errorf("approval_action_forbidden: actor %q is not in RHIZOME_OPERATOR_IDS", actor)
	}
	return nil
}

func ensurePatchQueueOperatorEnablementActorAuthorized(actor string) error {
	operators := parseOperatorIDs(os.Getenv("RHIZOME_OPERATOR_IDS"))
	if len(operators) == 0 {
		return errors.New("approval_action_forbidden: RHIZOME_OPERATOR_IDS is required for patch queue operator enablement")
	}
	if _, ok := operators[strings.TrimSpace(actor)]; !ok {
		return fmt.Errorf("approval_action_forbidden: actor %q is not in RHIZOME_OPERATOR_IDS", actor)
	}
	return nil
}

func parseOperatorIDs(raw string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		id := strings.TrimSpace(part)
		if id == "" {
			continue
		}
		out[id] = struct{}{}
	}
	return out
}

func cliProjectPromptContextEnvelope(surface, workspaceID, principalID string) map[string]any {
	return sqlite.BuildProjectPromptContextEnvelope(surface, "cli_local", workspaceID, "human", principalID)
}
