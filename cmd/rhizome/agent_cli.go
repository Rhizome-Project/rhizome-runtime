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

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type agentProtocol struct {
	Name        string   `json:"name"`
	Version     string   `json:"version"`
	Purpose     string   `json:"purpose"`
	Commands    []string `json:"commands"`
	HumanFacing bool     `json:"human_facing"`
}

func runAgent(args []string) error {
	if len(args) < 1 {
		printAgentUsage(os.Stderr)
		return errors.New("missing agent subcommand")
	}

	switch args[0] {
	case "register":
		return runAgentRegister(args[1:])
	case "heartbeat":
		return runAgentHeartbeat(args[1:])
	case "bootstrap":
		return runAgentBootstrap(args[1:])
	case "update":
		return runAgentUpdate(args[1:])
	case "session":
		return runAgentSession(args[1:])
	case "task":
		return runAgentTask(args[1:])
	case "run-internal":
		return runAgentInternal(args[1:])
	case "run-team":
		return runAgentRunTeam(args[1:])
	case "run":
		return runAgentRun(args[1:])
	case "create":
		return runAgentCreate(args[1:])
	case "list":
		return runAgentList(args[1:])
	case "show":
		return runAgentShow(args[1:])
	case "delete":
		return runAgentDelete(args[1:])
	default:
		printAgentUsage(os.Stderr)
		return fmt.Errorf("unknown agent subcommand: %s", args[0])
	}
}

func runAgentRegister(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("agent register", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	agentID := fs.String("agent-id", "", "Agent identifier")
	ownerUserID := fs.String("owner-user-id", "", "Human owner identifier")
	displayName := fs.String("display-name", "", "Agent display name")
	role := fs.String("role", "", "Agent role/profile (defaults on first register; omitted re-register preserves current value)")
	status := fs.String("status", "", "Agent status (defaults on first register; omitted re-register preserves current value)")
	protocolVersion := fs.String("protocol-version", "", "Protocol version (defaults on first register; omitted re-register preserves current value)")
	capabilities := fs.String("capabilities", "", "Comma separated capabilities")
	summary := fs.String("summary", "", "Current agent summary")
	if err := fs.Parse(args); err != nil {
		return err
	}
	setFlags := map[string]bool{}
	fs.Visit(func(f *flag.Flag) {
		setFlags[f.Name] = true
	})

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
	var capabilitiesValue *[]string
	if setFlags["capabilities"] {
		parsed := parseCSV(*capabilities)
		if strings.TrimSpace(*capabilities) == "" {
			parsed = []string{}
		}
		capabilitiesValue = &parsed
	}
	agent, err := store.RegisterAgentPreservingOmitted(ctx, sqlite.AgentRegisterPatchInput{
		WorkspaceID:     *workspaceID,
		AgentID:         *agentID,
		OwnerUserID:     cliOptionalString(setFlags, "owner-user-id", *ownerUserID),
		DisplayName:     cliOptionalString(setFlags, "display-name", *displayName),
		Role:            cliOptionalString(setFlags, "role", *role),
		Status:          cliOptionalString(setFlags, "status", *status),
		ProtocolVersion: cliOptionalString(setFlags, "protocol-version", *protocolVersion),
		Capabilities:    capabilitiesValue,
		Summary:         cliOptionalString(setFlags, "summary", *summary),
	})
	if err != nil {
		return fmt.Errorf("register agent: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"agent":    agent,
	})
}

func cliOptionalString(setFlags map[string]bool, key, value string) *string {
	if !setFlags[key] {
		return nil
	}
	trimmed := strings.TrimSpace(value)
	return &trimmed
}

func runAgentHeartbeat(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("agent heartbeat", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	agentID := fs.String("agent-id", "", "Agent identifier")
	status := fs.String("status", model.AgentStatusActive, "Agent status")
	summary := fs.String("summary", "", "Current agent summary")
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
	if err := store.RecordAgentHeartbeat(ctx, sqlite.AgentHeartbeatInput{
		WorkspaceID: *workspaceID,
		AgentID:     *agentID,
		Status:      *status,
		Summary:     *summary,
	}); err != nil {
		return fmt.Errorf("record agent heartbeat: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"agent_id":     strings.TrimSpace(*agentID),
		"status":       strings.TrimSpace(*status),
		"recorded_at":  time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func runAgentBootstrap(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("agent bootstrap", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	agentID := fs.String("agent-id", "", "Agent identifier")
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
	agent, err := store.GetAgent(ctx, *workspaceID, *agentID)
	if err != nil {
		return fmt.Errorf("get agent: %w", err)
	}
	snapshot, err := store.GetWorkspaceSnapshot(ctx, *workspaceID, *updatesLimit)
	if err != nil {
		return fmt.Errorf("get workspace snapshot: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"generated_at": time.Now().UTC().Format(time.RFC3339Nano),
		"agent":        agent,
		"protocols":    listAgentProtocols(),
		"snapshot":     snapshot,
	})
}

func runAgentUpdate(args []string) error {
	if len(args) < 1 {
		printAgentUsage(os.Stderr)
		return errors.New("missing agent update subcommand")
	}
	if args[0] != "post" {
		printAgentUsage(os.Stderr)
		return fmt.Errorf("unknown agent update subcommand: %s", args[0])
	}
	return runAgentUpdatePost(args[1:])
}

func runAgentUpdatePost(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("agent update post", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	agentID := fs.String("agent-id", "", "Agent identifier")
	updateType := fs.String("type", "", "Update type")
	summary := fs.String("summary", "", "Short update summary")
	payload := fs.String("payload", "", "Inline payload json or text")
	payloadFile := fs.String("payload-file", "", "Path to payload file")
	payloadSchema := fs.String("payload-schema", "", "Optional payload schema name (v1)")
	payloadStatus := fs.String("payload-status", "", "Structured payload status")
	nextAction := fs.String("next-action", "", "Structured payload next action")
	taskIDs := fs.String("task-ids", "", "Comma separated task ids for structured payload")
	docKeys := fs.String("doc-keys", "", "Comma separated doc keys for structured payload")
	links := fs.String("links", "", "Comma separated links for structured payload")
	artifactRefs := fs.String("artifact-refs", "", "Comma separated artifact refs or path@kind@content_type tuples")
	blockedOn := fs.String("blocked-on", "", "Comma separated kind:detail blockers for structured payload")
	ownerUserID := fs.String("owner-user-id", "", "Structured payload target human owner user id")
	humanReason := fs.String("human-reason", "", "Structured payload human reason")
	ownerAction := fs.String("owner-action", "", "Structured payload owner action")
	notes := fs.String("notes", "", "Structured payload notes")
	requiresHuman := fs.Bool("requires-human", false, "Whether the update requires human action")
	if err := fs.Parse(args); err != nil {
		return err
	}

	body, err := readTextValue(*payload, *payloadFile)
	if err != nil {
		return err
	}
	if strings.TrimSpace(*payloadSchema) != "" {
		body, err = resolveAgentUpdatePayload(strings.TrimSpace(*payloadSchema), body, structuredUpdatePayloadInput{
			Status:      *payloadStatus,
			NextAction:  *nextAction,
			TaskIDs:     parseCSV(*taskIDs),
			DocKeys:     parseCSV(*docKeys),
			Links:       parseCSV(*links),
			Artifacts:   parseArtifactRefs(*artifactRefs),
			BlockedOn:   parseBlockedRefs(*blockedOn),
			OwnerUserID: *ownerUserID,
			HumanReason: *humanReason,
			OwnerAction: *ownerAction,
			Notes:       *notes,
		})
		if err != nil {
			return err
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
	if _, err := store.RecordAgentUpdateWithEvent(ctx, sqlite.AgentUpdateInput{
		WorkspaceID:           *workspaceID,
		AgentID:               *agentID,
		UpdateType:            *updateType,
		Summary:               *summary,
		PayloadJSON:           body,
		RequiresHuman:         *requiresHuman,
		PromptContextEnvelope: cliAgentUpdatePromptContextEnvelope("cli.agent.update.post", *workspaceID, *agentID),
		PromptContextSurface:  "cli.agent.update.post",
	}); err != nil {
		return fmt.Errorf("record agent update: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":       traceID,
		"workspace_id":   strings.TrimSpace(*workspaceID),
		"agent_id":       strings.TrimSpace(*agentID),
		"update_type":    strings.TrimSpace(*updateType),
		"requires_human": *requiresHuman,
		"status":         "RECORDED",
	})
}

func cliAgentUpdatePromptContextEnvelope(surface, workspaceID, agentID string) map[string]any {
	return sqlite.BuildAgentUpdatePromptContextEnvelope(surface, "cli_local", workspaceID, "agent", strings.TrimSpace(agentID))
}

func runAgentTask(args []string) error {
	if len(args) < 1 {
		printAgentUsage(os.Stderr)
		return errors.New("missing agent task subcommand")
	}
	switch args[0] {
	case "claim":
		return runAgentTaskClaim(args[1:])
	case "release":
		return runAgentTaskRelease(args[1:])
	default:
		printAgentUsage(os.Stderr)
		return fmt.Errorf("unknown agent task subcommand: %s", args[0])
	}
}

func runAgentTaskClaim(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("agent task claim", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	agentID := fs.String("agent-id", "", "Agent identifier")
	taskID := fs.String("task-id", "", "Task identifier")
	summary := fs.String("summary", "", "Claim summary")
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
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: *workspaceID,
		TaskID:      *taskID,
		AgentID:     *agentID,
		Summary:     *summary,
	}); err != nil {
		return fmt.Errorf("claim task: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"task_id":      strings.TrimSpace(*taskID),
		"agent_id":     strings.TrimSpace(*agentID),
		"status":       model.TaskClaimStatusClaimed,
	})
}

func runAgentTaskRelease(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("agent task release", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	agentID := fs.String("agent-id", "", "Agent identifier")
	taskID := fs.String("task-id", "", "Task identifier")
	reason := fs.String("reason", "", "Release reason")
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
	if err := store.ReleaseTaskClaim(ctx, sqlite.TaskReleaseInput{
		WorkspaceID: *workspaceID,
		TaskID:      *taskID,
		AgentID:     *agentID,
		Reason:      *reason,
	}); err != nil {
		return fmt.Errorf("release task claim: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"task_id":      strings.TrimSpace(*taskID),
		"agent_id":     strings.TrimSpace(*agentID),
		"status":       model.TaskClaimStatusReleased,
	})
}

func listAgentProtocols() []agentProtocol {
	return []agentProtocol{
		{
			Name:        "bootstrap.snapshot",
			Version:     "v1",
			Purpose:     "Load machine-readable workspace context for an agent entering the system",
			Commands:    []string{"rhizome agent bootstrap"},
			HumanFacing: false,
		},
		{
			Name:        "workspace.doc.put",
			Version:     "v1",
			Purpose:     "Persist shared working memory and decisions",
			Commands:    []string{"rhizome workspace doc put", "rhizome workspace doc get", "rhizome workspace doc history"},
			HumanFacing: false,
		},
		{
			Name:        "agent.heartbeat",
			Version:     "v1",
			Purpose:     "Refresh presence, status and current focus",
			Commands:    []string{"rhizome agent heartbeat"},
			HumanFacing: false,
		},
		{
			Name:        "agent.update.post",
			Version:     "v1",
			Purpose:     "Publish progress, blockers, questions and human escalation metadata",
			Commands:    []string{"rhizome agent update post"},
			HumanFacing: false,
		},
		{
			Name:        "session.coordination",
			Version:     "v1",
			Purpose:     "Declare active session ownership, blockers, decisions, keepalive state, and explicit session closure",
			Commands:    []string{"rhizome agent session start", "rhizome agent session status", "rhizome agent session blocked", "rhizome agent session decision-needed", "rhizome agent session keepalive", "rhizome agent session end", "rhizome agent session sync-queue", "rhizome agent session list"},
			HumanFacing: false,
		},
		{
			Name:        "agent.task.claim",
			Version:     "v1",
			Purpose:     "Take a workspace task into active ownership",
			Commands:    []string{"rhizome agent task claim", "rhizome agent task release"},
			HumanFacing: false,
		},
		{
			Name:        "tool.register",
			Version:     "v1",
			Purpose:     "Register, inspect, invoke, and clean up workspace tools with machine-readable ownership and access metadata",
			Commands:    []string{"rhizome tool register", "rhizome tool invoke", "rhizome tool status", "rhizome tool list", "rhizome tool remove"},
			HumanFacing: false,
		},
		{
			Name:        "workspace.artifact",
			Version:     "v1",
			Purpose:     "Persist artifact references linked to tasks and updates for later agent retrieval",
			Commands:    []string{"rhizome workspace artifact put", "rhizome workspace artifact list"},
			HumanFacing: false,
		},
		{
			Name:        "workspace.search",
			Version:     "v1",
			Purpose:     "Search shared docs, tasks, updates, tools, artifacts, and durable memory without loading the full workspace every time",
			Commands:    []string{"rhizome workspace search"},
			HumanFacing: false,
		},
		{
			Name:        "workspace.memory",
			Version:     "v1",
			Purpose:     "Persist and retrieve durable workspace-scoped memory from canonical runtime truth",
			Commands:    []string{"rhizome workspace memory write", "rhizome workspace memory list", "rhizome workspace memory search"},
			HumanFacing: false,
		},
		{
			Name:        "workspace.compaction",
			Version:     "v1",
			Purpose:     "Inspect canonical session-ledger compaction candidates and snapshots based on current prompt tokens and runtime state",
			Commands:    []string{"rhizome workspace compaction candidates", "rhizome workspace compaction snapshots"},
			HumanFacing: false,
		},
		{
			Name:        "workspace.task.link",
			Version:     "v1",
			Purpose:     "Create and inspect lightweight task relationships such as blocks, relates_to, and subtask_of",
			Commands:    []string{"rhizome workspace task link", "rhizome workspace task links"},
			HumanFacing: false,
		},
		{
			Name:        "task.template",
			Version:     "v1",
			Purpose:     "Discover valid task templates before creating new coordination or execution work items",
			Commands:    []string{"rhizome task template list"},
			HumanFacing: false,
		},
		{
			Name:        "task.hydrate",
			Version:     "v1",
			Purpose:     "Compile a dense task context bundle for stateless bridges and heavyweight provider calls",
			Commands:    []string{"rhizome task hydrate"},
			HumanFacing: false,
		},
	}
}

type structuredUpdatePayloadInput struct {
	Status      string
	NextAction  string
	TaskIDs     []string
	DocKeys     []string
	Links       []string
	Artifacts   []model.AgentUpdateArtifactRef
	BlockedOn   []model.AgentUpdateBlockedRef
	OwnerUserID string
	HumanReason string
	OwnerAction string
	Notes       string
}

func resolveAgentUpdatePayload(schemaName, raw string, input structuredUpdatePayloadInput) (string, error) {
	switch strings.ToLower(strings.TrimSpace(schemaName)) {
	case "v1":
		if strings.TrimSpace(raw) != "" && hasStructuredUpdateFields(input) {
			return "", errors.New("payload schema v1 cannot mix --payload/--payload-file with structured payload flags")
		}
		if strings.TrimSpace(raw) != "" {
			return model.ParseAndNormalizeAgentUpdatePayloadV1(raw)
		}
		payload := model.AgentUpdatePayloadV1{
			Status:      strings.TrimSpace(input.Status),
			NextAction:  strings.TrimSpace(input.NextAction),
			TaskIDs:     input.TaskIDs,
			DocKeys:     input.DocKeys,
			Links:       input.Links,
			Artifacts:   input.Artifacts,
			BlockedOn:   input.BlockedOn,
			OwnerUserID: strings.TrimSpace(input.OwnerUserID),
			HumanReason: strings.TrimSpace(input.HumanReason),
			OwnerAction: strings.TrimSpace(input.OwnerAction),
			Notes:       strings.TrimSpace(input.Notes),
		}
		payload.Normalize()
		if err := payload.Validate(); err != nil {
			return "", err
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return "", fmt.Errorf("encode payload schema v1: %w", err)
		}
		return string(encoded), nil
	default:
		return "", fmt.Errorf("unsupported payload schema: %s", schemaName)
	}
}

func hasStructuredUpdateFields(input structuredUpdatePayloadInput) bool {
	return strings.TrimSpace(input.Status) != "" ||
		strings.TrimSpace(input.NextAction) != "" ||
		len(input.TaskIDs) > 0 ||
		len(input.DocKeys) > 0 ||
		len(input.Links) > 0 ||
		len(input.Artifacts) > 0 ||
		len(input.BlockedOn) > 0 ||
		strings.TrimSpace(input.OwnerUserID) != "" ||
		strings.TrimSpace(input.HumanReason) != "" ||
		strings.TrimSpace(input.OwnerAction) != "" ||
		strings.TrimSpace(input.Notes) != ""
}

func parseArtifactRefs(raw string) []model.AgentUpdateArtifactRef {
	items := parseCSV(raw)
	out := make([]model.AgentUpdateArtifactRef, 0, len(items))
	for _, item := range items {
		parts := strings.Split(item, "@")
		ref := model.AgentUpdateArtifactRef{Ref: strings.TrimSpace(parts[0])}
		if len(parts) > 1 {
			ref.Kind = strings.TrimSpace(parts[1])
		}
		if len(parts) > 2 {
			ref.ContentType = strings.TrimSpace(parts[2])
		}
		if ref.Ref == "" {
			continue
		}
		out = append(out, ref)
	}
	return out
}

func parseBlockedRefs(raw string) []model.AgentUpdateBlockedRef {
	items := parseCSV(raw)
	out := make([]model.AgentUpdateBlockedRef, 0, len(items))
	for _, item := range items {
		parts := strings.SplitN(item, ":", 2)
		entry := model.AgentUpdateBlockedRef{}
		if len(parts) > 0 {
			entry.Kind = strings.TrimSpace(parts[0])
		}
		if len(parts) > 1 {
			entry.Detail = strings.TrimSpace(parts[1])
		}
		if entry.Kind == "" && entry.Detail == "" {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func parseCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func printAgentUsage(out *os.File) {
	fmt.Fprintln(out, "Agent commands:")
	fmt.Fprintln(out, "  rhizome agent register --workspace-id <id> --agent-id <id> [--owner-user-id <user>] [--display-name <name>] [--role generalist] [--capabilities a,b]")
	fmt.Fprintln(out, "    owner/display are required on first registration; omitted re-register preserves current values")
	fmt.Fprintln(out, "  rhizome agent heartbeat --workspace-id <id> --agent-id <id> [--status ACTIVE] [--summary text]")
	fmt.Fprintln(out, "  rhizome agent bootstrap --workspace-id <id> --agent-id <id> [--updates-limit 10]")
	fmt.Fprintln(out, "  rhizome agent update post --workspace-id <id> --agent-id <id> --type <kind> --summary <text> [--payload json|text] [--payload-file path] [--payload-schema v1 --payload-status in_progress --task-ids a,b --doc-keys x,y --blocked-on auth:Need token --owner-user-id developer --owner-action \"Provide token\"] [--requires-human]")
	fmt.Fprintln(out, "  rhizome agent session <start|status|blocked|decision-needed|keepalive|end> --workspace-id <id> --session-id <id> --agent-id <id> --summary <text> [--task-id <task>] [--owner-scope text] [--blocked-on kind:detail] [--decision-needed-from actor] [--decision-type type] [--keep-session-active true|false] [--handoff-to agent] [--doc-keys a,b] [--artifact-refs ref@kind@content_type]")
	fmt.Fprintln(out, "  rhizome agent session sync-queue --workspace-id <id> --session-id <id>")
	fmt.Fprintln(out, "  rhizome agent session takeover --workspace-id <id> --session-id <id> --takeover-agent-id <agent> --summary <text> [--successor-session-id <id>] [--successor-summary <text>]")
	fmt.Fprintln(out, "  rhizome agent session list --workspace-id <id> [--active-only=true] [--limit 20]")
	fmt.Fprintln(out, "  rhizome agent task claim --workspace-id <id> --agent-id <id> --task-id <task> [--summary text]")
	fmt.Fprintln(out, "  rhizome agent task release --workspace-id <id> --agent-id <id> --task-id <task> [--reason text]")
	fmt.Fprintln(out, "  rhizome agent run-internal --agent-id <id> --workspace-id <id> --task <text> [--task-file path] [--max-iterations 50] [--model model] [--format json]")
	fmt.Fprintln(out, "  rhizome agent run-team --config <path> --task <text> [--task-file path] [--format json]")
	fmt.Fprintln(out, "  rhizome agent run <agent-id> --task <text> [--task-file path] [--max-iterations N] [--model model] [--provider claude|openai] [--format json]")
	fmt.Fprintln(out, "  rhizome agent create --name <name> --workspace-id <id> [--provider claude] [--model model] [--prompt text] [--prompt-file path] [--tools a,b] [--max-iterations 50]")
	fmt.Fprintln(out, "  rhizome agent list [--workspace-id <id>]")
	fmt.Fprintln(out, "  rhizome agent show <agent-id> | --id <agent-id>")
	fmt.Fprintln(out, "  rhizome agent delete <agent-id> [--force]")
}
