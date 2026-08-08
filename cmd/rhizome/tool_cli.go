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

type toolInvokePayload struct {
	ToolID        string   `json:"tool_id"`
	TaskID        string   `json:"task_id,omitempty"`
	Prompt        string   `json:"prompt"`
	Provider      string   `json:"provider,omitempty"`
	Model         string   `json:"model,omitempty"`
	DocKeys       []string `json:"doc_keys,omitempty"`
	ArtifactRefs  []string `json:"artifact_refs,omitempty"`
	Instructions  []string `json:"instructions,omitempty"`
	Constraints   []string `json:"constraints,omitempty"`
	DesiredOutput string   `json:"desired_output,omitempty"`
	ArtifactTitle string   `json:"artifact_title,omitempty"`
	ArtifactKind  string   `json:"artifact_kind,omitempty"`
	ContentType   string   `json:"content_type,omitempty"`
	DocKey        string   `json:"doc_key,omitempty"`
	DocTitle      string   `json:"doc_title,omitempty"`
}

func runTool(args []string) error {
	if len(args) < 1 {
		printToolUsage(os.Stderr)
		return errors.New("missing tool subcommand")
	}

	switch args[0] {
	case "register":
		return runToolRegister(args[1:])
	case "invoke":
		return runToolInvoke(args[1:])
	case "status":
		return runToolStatus(args[1:])
	case "list":
		return runToolList(args[1:])
	case "remove":
		return runToolRemove(args[1:])
	default:
		printToolUsage(os.Stderr)
		return fmt.Errorf("unknown tool subcommand: %s", args[0])
	}
}

func runToolInvoke(args []string) error {
	traceID := newTraceID()
	positionalToolID := ""
	if len(args) > 0 && !strings.HasPrefix(strings.TrimSpace(args[0]), "-") {
		positionalToolID = strings.TrimSpace(args[0])
		args = args[1:]
	}

	fs := flag.NewFlagSet("tool invoke", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	agentID := fs.String("agent-id", "", "Agent identifier issuing the request")
	toolIDFlag := fs.String("tool-id", "", "Optional tool identifier override")
	taskID := fs.String("task-id", "", "Optional task identifier")
	prompt := fs.String("prompt", "", "Inline tool prompt")
	promptFile := fs.String("prompt-file", "", "Path to a prompt file")
	summary := fs.String("summary", "", "Optional agent update summary")
	updateType := fs.String("type", "", "Optional agent update type override")
	provider := fs.String("provider", "", "Optional provider hint")
	modelName := fs.String("model", "", "Optional model hint")
	docKeys := fs.String("doc-keys", "", "Comma separated doc keys to include in hydration")
	artifactRefs := fs.String("artifact-refs", "", "Comma separated artifact refs to include")
	instructions := fs.String("instructions", "", "Comma separated execution instructions")
	constraints := fs.String("constraints", "", "Comma separated constraints")
	desiredOutput := fs.String("desired-output", "", "Preferred response shape")
	artifactTitle := fs.String("artifact-title", "", "Title for the generated artifact")
	artifactKind := fs.String("artifact-kind", "", "Kind for the generated artifact")
	contentType := fs.String("content-type", "", "Content type for the generated artifact")
	docKey := fs.String("doc-key", "", "Optional workspace doc key for persisting the response")
	docTitle := fs.String("doc-title", "", "Workspace doc title when --doc-key is set")
	dryRun := fs.Bool("dry-run", false, "Print the generated payload without recording an agent update")
	if err := fs.Parse(args); err != nil {
		return err
	}

	toolID := strings.TrimSpace(positionalToolID)
	if toolID == "" {
		toolID = strings.TrimSpace(*toolIDFlag)
	}
	if toolID == "" {
		return errors.New("--tool-id or positional <tool-id> is required")
	}
	if sqlite.IsRemovedWorkspaceToolID(toolID) {
		return fmt.Errorf("workspace tool %q has been removed from Rhizome; register a supported workspace tool instead", toolID)
	}
	if !*dryRun && strings.TrimSpace(*agentID) == "" {
		return errors.New("--agent-id is required")
	}

	promptBody, err := readTextValue(*prompt, *promptFile)
	if err != nil {
		return err
	}

	request := toolInvokePayload{
		ToolID:        toolID,
		TaskID:        strings.TrimSpace(*taskID),
		Prompt:        promptBody,
		Provider:      strings.TrimSpace(*provider),
		Model:         strings.TrimSpace(*modelName),
		DocKeys:       parseCSV(*docKeys),
		ArtifactRefs:  parseCSV(*artifactRefs),
		Instructions:  parseCSV(*instructions),
		Constraints:   parseCSV(*constraints),
		DesiredOutput: strings.TrimSpace(*desiredOutput),
		ArtifactTitle: strings.TrimSpace(*artifactTitle),
		ArtifactKind:  strings.TrimSpace(*artifactKind),
		ContentType:   strings.TrimSpace(*contentType),
		DocKey:        strings.TrimSpace(*docKey),
		DocTitle:      strings.TrimSpace(*docTitle),
	}
	request.Normalize()
	rawPayload, err := request.MarshalJSONNormalized()
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

	record, err := store.GetWorkspaceTool(ctx, *workspaceID, toolID)
	if err != nil {
		return fmt.Errorf("get workspace tool: %w", err)
	}
	if record.Status != model.ToolStatusActive {
		return fmt.Errorf("tool %s is %s and cannot be invoked", toolID, record.Status)
	}

	resolvedUpdateType := resolveToolInvokeUpdateType(toolID, *updateType)
	resolvedSummary := strings.TrimSpace(*summary)
	if resolvedSummary == "" {
		resolvedSummary = defaultToolInvokeSummary(record, request.TaskID)
	}

	if *dryRun {
		return writeJSON(os.Stdout, map[string]any{
			"trace_id":    traceID,
			"tool":        record,
			"update_type": resolvedUpdateType,
			"summary":     resolvedSummary,
			"payload":     jsonRaw(rawPayload),
			"dry_run":     true,
		})
	}

	updateID := nextCLIID("agent_update")
	if _, err := store.RecordAgentUpdateWithEvent(ctx, sqlite.AgentUpdateInput{
		UpdateID:              updateID,
		WorkspaceID:           *workspaceID,
		AgentID:               *agentID,
		UpdateType:            resolvedUpdateType,
		Summary:               resolvedSummary,
		PayloadJSON:           rawPayload,
		PromptContextEnvelope: cliAgentUpdatePromptContextEnvelope("cli.tool.invoke.update", *workspaceID, *agentID),
		PromptContextSurface:  "cli.tool.invoke.update",
	}); err != nil {
		return fmt.Errorf("record tool invoke update: %w", err)
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"update_id":    updateID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"agent_id":     strings.TrimSpace(*agentID),
		"tool_id":      toolID,
		"update_type":  resolvedUpdateType,
		"summary":      resolvedSummary,
		"status":       "RECORDED",
	})
}

func runToolRegister(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("tool register", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	toolID := fs.String("tool-id", "", "Tool identifier")
	displayName := fs.String("display-name", "", "Tool display name")
	description := fs.String("description", "", "Tool description")
	ownerUserID := fs.String("owner-user-id", "", "Owner user identifier")
	ownerAgentID := fs.String("owner-agent-id", "", "Optional owner agent identifier")
	kind := fs.String("kind", model.ToolKindOther, "Tool kind")
	status := fs.String("status", model.ToolStatusActive, "Tool status")
	version := fs.String("version", "", "Tool version")
	accessLevel := fs.String("access-level", model.ToolAccessWorkspace, "Tool access level")
	endpoint := fs.String("endpoint", "", "Optional endpoint or command ref")
	capabilities := fs.String("capabilities", "", "Comma separated capabilities")
	manifest := fs.String("manifest", "", "Inline manifest JSON")
	manifestFile := fs.String("manifest-file", "", "Path to manifest JSON file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	manifestBody, err := readTextValue(*manifest, *manifestFile)
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
	if err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID:  *workspaceID,
		ToolID:       *toolID,
		DisplayName:  *displayName,
		Description:  *description,
		OwnerUserID:  *ownerUserID,
		OwnerAgentID: *ownerAgentID,
		Kind:         strings.ToUpper(strings.TrimSpace(*kind)),
		Status:       strings.ToUpper(strings.TrimSpace(*status)),
		Version:      *version,
		AccessLevel:  strings.ToUpper(strings.TrimSpace(*accessLevel)),
		Endpoint:     *endpoint,
		Capabilities: parseCSV(*capabilities),
		ManifestJSON: manifestBody,
		PromptContextEnvelope: cliToolRegistryPromptContextEnvelope(
			"cli.tool.register",
			*workspaceID,
			*ownerUserID,
		),
		PromptContextSurface:       "cli.tool.register",
		PromptContextPrincipalType: "operator",
		PromptContextPrincipalID:   strings.TrimSpace(*ownerUserID),
	}); err != nil {
		return fmt.Errorf("register workspace tool: %w", err)
	}

	record, err := store.GetWorkspaceTool(ctx, *workspaceID, *toolID)
	if err != nil {
		return fmt.Errorf("get workspace tool: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"tool":     record,
	})
}

func runToolStatus(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("tool status", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	toolID := fs.String("tool-id", "", "Tool identifier")
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
	record, err := store.GetWorkspaceTool(ctx, *workspaceID, *toolID)
	if err != nil {
		return fmt.Errorf("get workspace tool: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id": traceID,
		"tool":     record,
	})
}

func runToolList(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("tool list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	workspaceID := fs.String("workspace-id", "", "Workspace identifier")
	status := fs.String("status", "", "Optional tool status filter")
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
	records, err := store.ListWorkspaceTools(ctx, sqlite.WorkspaceToolFilter{
		WorkspaceID: *workspaceID,
		Status:      strings.ToUpper(strings.TrimSpace(*status)),
	})
	if err != nil {
		return fmt.Errorf("list workspace tools: %w", err)
	}
	return writeJSON(os.Stdout, map[string]any{
		"trace_id":     traceID,
		"workspace_id": strings.TrimSpace(*workspaceID),
		"tools":        records,
	})
}

func printToolUsage(out *os.File) {
	fmt.Fprintln(out, "Tool commands:")
	fmt.Fprintln(out, "  rhizome tool register --workspace-id <id> --tool-id <id> --display-name <name> --owner-user-id <user> [--owner-agent-id <agent>] [--description text] [--kind OTHER] [--status ACTIVE] [--version v1] [--access-level WORKSPACE] [--endpoint ref] [--capabilities a,b] [--manifest json|--manifest-file path]")
	fmt.Fprintln(out, "  rhizome tool invoke <tool-id>|--tool-id <id> --workspace-id <id> --agent-id <id> [--task-id <task>] [--prompt text|--prompt-file path] [--provider <name>] [--model <name>] [--doc-keys a,b] [--artifact-refs ref1,ref2] [--instructions rule1,rule2] [--constraints c1,c2] [--desired-output design+code] [--artifact-title title] [--artifact-kind llm_response] [--content-type text/markdown] [--doc-key key --doc-title title] [--summary text] [--type <update-type>] [--dry-run]")
	fmt.Fprintln(out, "  rhizome tool status --workspace-id <id> --tool-id <id>")
	fmt.Fprintln(out, "  rhizome tool list --workspace-id <id> [--status ACTIVE]")
	fmt.Fprintln(out, "  rhizome tool remove --workspace-id <id> --tool-id <id> [--removed-by <actor>]")
}

func resolveToolInvokeUpdateType(toolID, override string) string {
	if strings.TrimSpace(override) != "" {
		return strings.TrimSpace(override)
	}
	return "tool_call_requested"
}

func (p *toolInvokePayload) Normalize() {
	p.ToolID = strings.TrimSpace(p.ToolID)
	p.TaskID = strings.TrimSpace(p.TaskID)
	p.Prompt = strings.TrimSpace(p.Prompt)
	p.Provider = strings.TrimSpace(p.Provider)
	p.Model = strings.TrimSpace(p.Model)
	p.DocKeys = normalizeToolInvokeStrings(p.DocKeys)
	p.ArtifactRefs = normalizeToolInvokeStrings(p.ArtifactRefs)
	p.Instructions = normalizeToolInvokeStrings(p.Instructions)
	p.Constraints = normalizeToolInvokeStrings(p.Constraints)
	p.DesiredOutput = strings.TrimSpace(p.DesiredOutput)
	p.ArtifactTitle = strings.TrimSpace(p.ArtifactTitle)
	p.ArtifactKind = strings.TrimSpace(p.ArtifactKind)
	p.ContentType = strings.TrimSpace(p.ContentType)
	p.DocKey = strings.TrimSpace(p.DocKey)
	p.DocTitle = strings.TrimSpace(p.DocTitle)
}

func (p toolInvokePayload) Validate() error {
	if strings.TrimSpace(p.ToolID) == "" {
		return errors.New("tool_id is required")
	}
	if strings.TrimSpace(p.Prompt) == "" {
		return errors.New("prompt is required")
	}
	if strings.TrimSpace(p.DocKey) != "" && strings.TrimSpace(p.DocTitle) == "" {
		return errors.New("doc_title is required when doc_key is set")
	}
	return nil
}

func (p toolInvokePayload) MarshalJSONNormalized() (string, error) {
	payload := p
	payload.Normalize()
	if err := payload.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode tool invoke payload: %w", err)
	}
	return string(raw), nil
}

func normalizeToolInvokeStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func defaultToolInvokeSummary(tool sqlite.WorkspaceToolRecord, taskID string) string {
	label := strings.TrimSpace(tool.DisplayName)
	if label == "" {
		label = strings.TrimSpace(tool.ToolID)
	}
	if strings.TrimSpace(taskID) == "" {
		return "Invoke " + label
	}
	return "Invoke " + label + " for " + strings.TrimSpace(taskID)
}

type jsonRaw string

func (r jsonRaw) MarshalJSON() ([]byte, error) {
	return []byte(r), nil
}

func cliToolRegistryPromptContextEnvelope(surface, workspaceID, principalID string) map[string]any {
	return sqlite.BuildToolRegistryPromptContextEnvelope(surface, "cli_local", workspaceID, "operator", cliToolRegistryPrincipalID(principalID))
}

func cliToolRegistryPrincipalID(principalID string) string {
	principalID = strings.TrimSpace(principalID)
	if principalID == "" {
		return "cli"
	}
	return principalID
}
