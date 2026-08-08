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

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func runTask(args []string) error {
	if len(args) < 1 {
		printTaskUsage(os.Stderr)
		return errors.New("missing task subcommand")
	}

	switch args[0] {
	case "submit":
		return runTaskSubmit(args[1:])
	case "status":
		return runTaskStatus(args[1:])
	case "graph":
		return runTaskGraph(args[1:])
	case "hydrate":
		return runTaskHydrate(args[1:])
	case "close":
		return runTaskClose(args[1:])
	case "template":
		return runTaskTemplate(args[1:])
	case "run":
		return runTaskRun(args[1:])
	default:
		printTaskUsage(os.Stderr)
		return fmt.Errorf("unknown task subcommand: %s", args[0])
	}
}

func runTaskSubmit(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("task submit", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	taskID := fs.String("task-id", "", "Task identifier")
	owner := fs.String("owner-user-id", "", "Task owner user id")
	priority := fs.String("priority", "normal", "Task priority: low|normal|high|critical")
	title := fs.String("title", "", "Task title")
	description := fs.String("description", "", "Task description")
	taskKind := fs.String("kind", model.TaskKindExecution, "Task kind: EXECUTION|COORDINATION")
	taskTemplate := fs.String("template", "generic", "Task template name")
	projectID := fs.String("project-id", "", "Project identifier for project-linked tasks")
	projectLane := fs.String("project-lane", "", "Project lane: strategy|implementation|review|synthesis|...")
	requiresProjectGate := fs.Bool("requires-project-gate", false, "Require project admission gate for implementation claims")
	graphFile := fs.String("graph-file", "", "Path to DAG graph JSON file")
	workspaceID := fs.String("workspace-id", "", "Workspace identifier for task lifecycle/runtime event")
	linkedBy := fs.String("linked-by", "", "Actor attaching the task to the workspace (defaults to owner-user-id)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	taskIDValue := strings.TrimSpace(*taskID)
	ownerID := strings.TrimSpace(*owner)
	workspaceIDValue := strings.TrimSpace(*workspaceID)
	if taskIDValue == "" {
		return errors.New("--task-id is required")
	}
	if ownerID == "" {
		return errors.New("--owner-user-id is required")
	}
	if workspaceIDValue == "" {
		return errors.New("--workspace-id is required")
	}

	graph, err := loadGraph(*graphFile, *taskKind)
	if err != nil {
		return err
	}
	resolvedTaskKind := strings.ToUpper(strings.TrimSpace(*taskKind))
	templateName := strings.ToLower(strings.TrimSpace(*taskTemplate))
	if templateName == "" {
		templateName = model.TaskTemplateGeneric
	}
	if _, ok := model.LookupTaskTemplate(templateName); !ok {
		return fmt.Errorf("invalid task template: %s", templateName)
	}
	if !model.ValidTaskTemplateForKind(templateName, resolvedTaskKind) {
		return fmt.Errorf("task template %s does not support task kind %s", templateName, resolvedTaskKind)
	}

	graph = dag.NormalizeGraph(graph)
	if err := dag.ValidateGraph(graph); err != nil {
		return fmt.Errorf("invalid graph: %w", err)
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
	if _, err := store.GetWorkspace(ctx, workspaceIDValue); err != nil {
		return fmt.Errorf("validate workspace: %w", err)
	}
	attachActor := strings.TrimSpace(*linkedBy)
	if attachActor == "" {
		attachActor = ownerID
	}
	payloadJSON, err := marshalCLITaskLifecyclePayload("task.submit", workspaceIDValue, attachActor, map[string]any{
		"workspace_id":          workspaceIDValue,
		"task_id":               taskIDValue,
		"title":                 strings.TrimSpace(*title),
		"description":           strings.TrimSpace(*description),
		"priority":              strings.TrimSpace(*priority),
		"task_kind":             resolvedTaskKind,
		"task_template":         templateName,
		"project_id":            strings.TrimSpace(*projectID),
		"project_lane":          strings.TrimSpace(*projectLane),
		"requires_project_gate": *requiresProjectGate,
		"owner_user_id":         ownerID,
		"linked_by":             attachActor,
		"status":                model.TaskStatusPending,
		"summary":               "Task created: " + taskLifecycleSummaryTitle(taskIDValue, *title),
		"node_count":            len(graph.Nodes),
		"workspace_link":        true,
	})
	if err != nil {
		return fmt.Errorf("build task lifecycle prompt context: %w", err)
	}

	runtimeEvent, err := store.CreateTaskWithGraphAndWorkspaceEvent(ctx, sqlite.TaskCreateInput{
		TaskID:              taskIDValue,
		OwnerUserID:         ownerID,
		Priority:            *priority,
		Title:               *title,
		Description:         *description,
		TaskKind:            resolvedTaskKind,
		TaskTemplate:        templateName,
		ProjectID:           *projectID,
		ProjectLane:         *projectLane,
		RequiresProjectGate: *requiresProjectGate,
	}, graph, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceIDValue,
		TaskID:      taskIDValue,
		LinkedBy:    attachActor,
	}, sqlite.RuntimeEventInput{
		DedupKey:    "task:" + taskIDValue + ":created",
		WorkspaceID: workspaceIDValue,
		EventType:   "task.created",
		EntityType:  "task",
		EntityID:    taskIDValue,
		ActorType:   "operator",
		ActorID:     attachActor,
		TaskID:      taskIDValue,
		PayloadJSON: payloadJSON,
	})
	if err != nil {
		return fmt.Errorf("create task: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"task_id":               taskIDValue,
		"task_kind":             resolvedTaskKind,
		"task_template":         templateName,
		"project_id":            strings.TrimSpace(*projectID),
		"project_lane":          strings.TrimSpace(*projectLane),
		"requires_project_gate": *requiresProjectGate,
		"status":                model.TaskStatusPending,
		"node_count":            len(graph.Nodes),
		"workspace_id":          workspaceIDValue,
		"runtime_event_id":      runtimeEvent.EventID,
		"trace_id":              traceID,
	})
}

func runTaskTemplate(args []string) error {
	if len(args) < 1 || args[0] != "list" {
		printTaskUsage(os.Stderr)
		return errors.New("missing task template subcommand")
	}

	traceID := newTraceID()
	return writeJSON(os.Stdout, map[string]any{
		"trace_id":  traceID,
		"templates": model.ListTaskTemplates(),
	})
}

func runTaskStatus(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("task status", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	taskID := fs.String("task-id", "", "Task identifier")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*taskID) == "" {
		return errors.New("--task-id is required")
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

	status, err := store.GetTaskStatus(ctx, "", *taskID)
	if err != nil {
		if errors.Is(err, sqlite.ErrTaskNotFound) {
			return fmt.Errorf("task not found: %s", strings.TrimSpace(*taskID))
		}
		return fmt.Errorf("get task status: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"task":     status,
	})
}

func runTaskGraph(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("task graph", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	taskID := fs.String("task-id", "", "Task identifier")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*taskID) == "" {
		return errors.New("--task-id is required")
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

	status, err := store.GetTaskStatus(ctx, "", *taskID)
	if err != nil {
		if errors.Is(err, sqlite.ErrTaskNotFound) {
			return fmt.Errorf("task not found: %s", strings.TrimSpace(*taskID))
		}
		return fmt.Errorf("get task graph: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"task_id":  status.TaskID,
		"nodes":    status.Nodes,
		"trace_id": traceID,
	})
}

func runTaskHydrate(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("task hydrate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	taskID := fs.String("task-id", "", "Task identifier")
	workspaceID := fs.String("workspace-id", "", "Optional workspace identifier override")
	docKeys := fs.String("doc-keys", "", "Optional comma separated doc keys to include")
	allDocs := fs.Bool("all-docs", true, "Include all workspace docs in the hydration bundle")
	updatesLimit := fs.Int("updates-limit", 20, "Maximum number of related updates")
	artifactLimit := fs.Int("artifacts-limit", 20, "Maximum number of task artifacts")
	relatedTaskLimit := fs.Int("related-task-limit", 10, "Maximum number of related tasks")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*taskID) == "" {
		return errors.New("--task-id is required")
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
	selectedDocKeys := parseCSV(*docKeys)
	includeAllDocs := *allDocs
	if len(selectedDocKeys) > 0 {
		includeAllDocs = false
	}
	bundle, err := store.GetTaskHydrationBundle(ctx, sqlite.TaskHydrationFilter{
		TaskID:           *taskID,
		WorkspaceID:      *workspaceID,
		DocKeys:          selectedDocKeys,
		IncludeAllDocs:   includeAllDocs,
		UpdatesLimit:     *updatesLimit,
		ArtifactLimit:    *artifactLimit,
		RelatedTaskLimit: *relatedTaskLimit,
	})
	if err != nil {
		return fmt.Errorf("hydrate task context: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"bundle":   bundle,
	})
}

func runTaskClose(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("task close", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	taskID := fs.String("task-id", "", "Task identifier")
	workspaceID := fs.String("workspace-id", "", "Workspace identifier for task lifecycle/runtime event")
	resolution := fs.String("resolution", model.TaskStatusResolved, "Close status: RESOLVED|FAILED|CANCELLED")
	reason := fs.String("reason", "", "Close reason")
	actorID := fs.String("actor-id", "operator", "Actor closing the task")
	if err := fs.Parse(args); err != nil {
		return err
	}
	taskIDValue := strings.TrimSpace(*taskID)
	workspaceIDValue := strings.TrimSpace(*workspaceID)
	actorIDValue := strings.TrimSpace(*actorID)
	if actorIDValue == "" {
		actorIDValue = "operator"
	}
	resolutionValue := strings.ToUpper(strings.TrimSpace(*resolution))
	if resolutionValue == "" {
		resolutionValue = model.TaskStatusResolved
	}
	if taskIDValue == "" {
		return errors.New("--task-id is required")
	}
	if workspaceIDValue == "" {
		return errors.New("--workspace-id is required")
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
	if _, err := store.GetWorkspace(ctx, workspaceIDValue); err != nil {
		return fmt.Errorf("validate workspace: %w", err)
	}
	payloadJSON, err := marshalCLITaskLifecyclePayload("task.close", workspaceIDValue, actorIDValue, map[string]any{
		"workspace_id": workspaceIDValue,
		"task_id":      taskIDValue,
		"resolution":   resolutionValue,
		"reason":       strings.TrimSpace(*reason),
		"status":       resolutionValue,
		"actor_id":     actorIDValue,
		"summary":      "Task closed: " + taskLifecycleSummaryTitle(taskIDValue, ""),
	})
	if err != nil {
		return fmt.Errorf("build task close prompt context: %w", err)
	}
	runtimeEvent, changed, err := store.CloseTaskWithRuntimeEvent(ctx, sqlite.TaskCloseInput{
		WorkspaceID: workspaceIDValue,
		TaskID:      taskIDValue,
		ActorID:     actorIDValue,
		Resolution:  resolutionValue,
		Reason:      *reason,
	}, sqlite.RuntimeEventInput{
		DedupKey:    "task:" + taskIDValue + ":closed:" + resolutionValue,
		WorkspaceID: workspaceIDValue,
		EventType:   "task.closed",
		EntityType:  "task",
		EntityID:    taskIDValue,
		ActorType:   "operator",
		ActorID:     actorIDValue,
		TaskID:      taskIDValue,
		PayloadJSON: payloadJSON,
	})
	if err != nil {
		return fmt.Errorf("close task: %w", err)
	}

	status, err := store.GetTaskStatus(ctx, workspaceIDValue, taskIDValue)
	if err != nil {
		return fmt.Errorf("get closed task status: %w", err)
	}
	out := map[string]any{
		"trace_id":              traceID,
		"task":                  status,
		"runtime_event_changed": changed,
	}
	if changed {
		out["runtime_event_id"] = runtimeEvent.EventID
	}
	return writeJSON(os.Stdout, out)
}

func runTaskRun(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("task run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	taskID := fs.String("task-id", "", "Task identifier")
	wait := fs.Bool("wait", false, "Wait until task reaches terminal status")
	timeoutSec := fs.Int("timeout-sec", 120, "Wait timeout in seconds when --wait is enabled")
	pollMS := fs.Int("poll-ms", 500, "Polling interval in milliseconds")
	maxNodes := fs.Int("max-nodes", 10, "Maximum nodes processed per tick")
	nodeTimeoutSec := fs.Int("node-timeout-sec", 120, "Executor node timeout in seconds")
	format := fs.String("format", "json", "Output format: json|jsonl")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*taskID) == "" {
		return errors.New("--task-id is required")
	}
	if *timeoutSec <= 0 {
		return errors.New("--timeout-sec must be positive")
	}
	if *pollMS <= 0 {
		return errors.New("--poll-ms must be positive")
	}
	if *maxNodes <= 0 {
		return errors.New("--max-nodes must be positive")
	}
	if *nodeTimeoutSec <= 0 {
		return errors.New("--node-timeout-sec must be positive")
	}
	outputFormat, err := normalizeOutputFormat(*format)
	if err != nil {
		return err
	}

	cfg := app.LoadConfig()
	store, err := openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	taskStatus, err := store.GetTaskStatus(ctx, "", *taskID)
	if err != nil {
		if errors.Is(err, sqlite.ErrTaskNotFound) {
			return fmt.Errorf("task not found: %s", strings.TrimSpace(*taskID))
		}
		return fmt.Errorf("get task status: %w", err)
	}
	if taskStatus.TaskKind == model.TaskKindCoordination {
		return fmt.Errorf("task %s is coordination-only; use `rhizome task close` instead of runtime execution", strings.TrimSpace(*taskID))
	}
	workspaceID, err := store.ResolveSingleTaskWorkspace(ctx, *taskID)
	if err != nil {
		switch {
		case errors.Is(err, sqlite.ErrWorkspaceTaskAbsent):
			return fmt.Errorf("task %s is not attached to an execution workspace; attach it to exactly one workspace before runtime execution", strings.TrimSpace(*taskID))
		case errors.Is(err, sqlite.ErrTaskWorkspaceAmbiguous):
			return fmt.Errorf("task %s is attached to multiple workspaces; resolve execution ownership before runtime execution", strings.TrimSpace(*taskID))
		default:
			return fmt.Errorf("resolve execution workspace: %w", err)
		}
	}
	if workspaceID == "" {
		return fmt.Errorf("task %s has no execution workspace", strings.TrimSpace(*taskID))
	}

	runner, err := newDaemonRunner(store, cfg, *maxNodes, *nodeTimeoutSec, "task-run")
	if err != nil {
		return err
	}

	totalProcessed := 0
	runTick := func() error {
		runCtx, runCancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer runCancel()
		processed, err := runner.RunOnce(runCtx)
		if err != nil {
			return err
		}
		totalProcessed += processed
		return nil
	}

	if !*wait {
		if err := runTick(); err != nil {
			return fmt.Errorf("task run tick: %w", err)
		}
		latest, err := store.GetTaskStatus(context.Background(), "", *taskID)
		if err != nil {
			return fmt.Errorf("get task status after run: %w", err)
		}
		if outputFormat == outputFormatJSONL {
			return writeJSONLine(os.Stdout, map[string]any{
				"event":           "task_run_result",
				"task_id":         strings.TrimSpace(*taskID),
				"processed_nodes": totalProcessed,
				"status":          latest.Status,
				"wait":            false,
				"trace_id":        traceID,
				"ts":              time.Now().UTC().Format(time.RFC3339Nano),
			})
		}
		return writeJSON(os.Stdout, map[string]any{
			"task_id":         strings.TrimSpace(*taskID),
			"processed_nodes": totalProcessed,
			"status":          latest.Status,
			"wait":            false,
			"trace_id":        traceID,
		})
	}

	deadline := time.Now().Add(time.Duration(*timeoutSec) * time.Second)
	for {
		if err := runTick(); err != nil {
			return fmt.Errorf("task run tick: %w", err)
		}

		latest, err := store.GetTaskStatus(context.Background(), "", *taskID)
		if err != nil {
			return fmt.Errorf("get task status while waiting: %w", err)
		}
		taskStatus = latest
		if outputFormat == outputFormatJSONL {
			if err := writeJSONLine(os.Stdout, map[string]any{
				"event":           "task_wait_tick",
				"task_id":         strings.TrimSpace(*taskID),
				"processed_nodes": totalProcessed,
				"status":          taskStatus.Status,
				"wait":            true,
				"trace_id":        traceID,
				"ts":              time.Now().UTC().Format(time.RFC3339Nano),
			}); err != nil {
				return err
			}
		}

		if isTerminalTaskStatus(taskStatus.Status) {
			if outputFormat == outputFormatJSONL {
				return writeJSONLine(os.Stdout, map[string]any{
					"event":           "task_run_result",
					"task_id":         strings.TrimSpace(*taskID),
					"processed_nodes": totalProcessed,
					"status":          taskStatus.Status,
					"wait":            true,
					"trace_id":        traceID,
					"ts":              time.Now().UTC().Format(time.RFC3339Nano),
				})
			}
			return writeJSON(os.Stdout, map[string]any{
				"task_id":         strings.TrimSpace(*taskID),
				"processed_nodes": totalProcessed,
				"status":          taskStatus.Status,
				"wait":            true,
				"trace_id":        traceID,
			})
		}

		if time.Now().After(deadline) {
			return fmt.Errorf(
				"task run wait timeout after %ds: task=%s status=%s processed_nodes=%d",
				*timeoutSec,
				strings.TrimSpace(*taskID),
				taskStatus.Status,
				totalProcessed,
			)
		}

		time.Sleep(time.Duration(*pollMS) * time.Millisecond)
	}
}

func loadGraph(graphFile string, taskKind string) (dag.Graph, error) {
	if strings.TrimSpace(graphFile) == "" {
		if strings.EqualFold(strings.TrimSpace(taskKind), model.TaskKindCoordination) {
			return dag.Graph{
				Nodes: []dag.NodeSpec{
					{NodeID: "coordination", Type: "coordination_manual"},
				},
			}, nil
		}
		return dag.DefaultGraph(), nil
	}

	raw, err := os.ReadFile(graphFile)
	if err != nil {
		return dag.Graph{}, fmt.Errorf("read graph file: %w", err)
	}

	var graph dag.Graph
	if err := json.Unmarshal(raw, &graph); err != nil {
		return dag.Graph{}, fmt.Errorf("decode graph json: %w", err)
	}

	return graph, nil
}

func marshalCLITaskLifecyclePayload(surface, workspaceID, actorID string, payload map[string]any) (string, error) {
	withEnvelope, err := sqlite.AttachTaskPromptContextEnvelope(
		payload,
		sqlite.BuildTaskPromptContextEnvelope(surface, "cli_local", workspaceID, "operator", actorID),
	)
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(withEnvelope)
	if err != nil {
		return "", fmt.Errorf("encode task lifecycle payload: %w", err)
	}
	return string(raw), nil
}

func taskLifecycleSummaryTitle(taskID, title string) string {
	if trimmed := strings.TrimSpace(title); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(taskID)
}

func printTaskUsage(out *os.File) {
	fmt.Fprintln(out, "Task commands:")
	fmt.Fprintln(out, "  rhizome task submit --task-id <id> --owner-user-id <user> --workspace-id <id> [--title text] [--description text] [--kind EXECUTION|COORDINATION] [--template generic] [--priority normal] [--project-id id] [--project-lane lane] [--requires-project-gate] [--graph-file path.json] [--linked-by <actor>]")
	fmt.Fprintln(out, "  rhizome task status --task-id <id>")
	fmt.Fprintln(out, "  rhizome task graph --task-id <id>")
	fmt.Fprintln(out, "  rhizome task hydrate --task-id <id> [--workspace-id <id>] [--doc-keys a,b] [--all-docs true] [--updates-limit 20] [--artifacts-limit 20] [--related-task-limit 10]")
	fmt.Fprintln(out, "  rhizome task close --task-id <id> --workspace-id <id> [--resolution RESOLVED|FAILED|CANCELLED] [--reason text] [--actor-id operator]")
	fmt.Fprintln(out, "  rhizome task template list")
	fmt.Fprintln(out, "  rhizome task run --task-id <id> [--wait] [--timeout-sec 120] [--poll-ms 500] [--format json|jsonl]")
}

func isTerminalTaskStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case model.TaskStatusResolved, model.TaskStatusFailed, model.TaskStatusCancelled:
		return true
	default:
		return false
	}
}
