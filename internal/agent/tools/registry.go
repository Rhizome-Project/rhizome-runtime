package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Tool defines the interface for an agent tool.
type Tool interface {
	Name() string
	Description() string
	Schema() Schema
	Execute(ctx context.Context, input json.RawMessage) (string, error)
}

// Capability tiers for built-in tools.
const (
	TierSafeLocal = "safe_local"
	TierHighRisk  = "high_risk"
)

// BuiltinConfig provides the configuration context to built-in tools during registration.
type BuiltinConfig struct {
	WorkspaceDir string
	AllowedTiers []string
	RepoMutation RepoMutationPolicy
}

const DirectRepoMutationDeniedReason = "program_b.raw_repo_mutation_disabled"

type RepoMutationPolicy struct {
	RequireAuthority bool
	DisableDirect    bool
	Authority        RepoMutationAuthorityContext
	RecordDeny       func(MutationDenyRecord)
}

type RepoMutationAuthorityContext struct {
	RepoLeaseID      string
	LeaseTerm        int64
	PatchQueueID     string
	PatchQueueItemID string
	OperationID      string
}

type MutationDenyRecord struct {
	ToolName       string
	TargetPath     string
	ReasonCode     string
	MissingContext []string
}

// ResolveAndVerifyPath computes the absolute path given the WorkspaceDir and a target path,
// and ensures it does not escape the WorkspaceDir constraints due to path traversal.
func (c BuiltinConfig) ResolveAndVerifyPath(target string) (string, error) {
	baseDir := c.WorkspaceDir
	if baseDir == "" {
		baseDir, _ = os.Getwd()
	}
	baseDir = filepath.Clean(baseDir)

	var resolved string
	if filepath.IsAbs(target) {
		resolved = filepath.Clean(target)
	} else {
		resolved = filepath.Clean(filepath.Join(baseDir, target))
	}

	if !strings.HasPrefix(resolved, baseDir) {
		return "", fmt.Errorf("path %q escapes workspace boundary", target)
	}
	return resolved, nil
}

// EnsureTier checks if a required tier is present in allowed tiers.
func (c BuiltinConfig) EnsureTier(requiredTier string) error {
	for _, t := range c.AllowedTiers {
		if t == requiredTier || t == "system_admin" || t == "autonomous" {
			return nil
		}
	}
	return fmt.Errorf("tool requires capability tier %q", requiredTier)
}

func (c BuiltinConfig) EnsureDirectMutationAllowed(toolName, targetPath string) error {
	policy := c.RepoMutation
	if !policy.DisableDirect && !policy.RequireAuthority {
		return nil
	}

	missing := policy.MissingContext()
	if policy.DisableDirect || len(missing) > 0 {
		record := MutationDenyRecord{
			ToolName:       strings.TrimSpace(toolName),
			TargetPath:     strings.TrimSpace(targetPath),
			ReasonCode:     DirectRepoMutationDeniedReason,
			MissingContext: missing,
		}
		if policy.RecordDeny != nil {
			policy.RecordDeny(record)
		}
		if policy.DisableDirect {
			return fmt.Errorf("%s: direct %s is disabled; use repo authority patch flow", DirectRepoMutationDeniedReason, toolName)
		}
		return fmt.Errorf("%s: direct %s requires repo authority context: %s", DirectRepoMutationDeniedReason, toolName, strings.Join(missing, ", "))
	}
	return nil
}

func (c BuiltinConfig) EnsureShellCommandReadOnly(toolName, command string) error {
	policy := c.RepoMutation
	if !policy.DisableDirect && !policy.RequireAuthority {
		return nil
	}

	indicator, mutating := shellMutationIndicator(command)
	if compoundIndicator, compound := shellCompoundCommandIndicator(command); compound {
		indicator = compoundIndicator
		mutating = true
	}
	if !mutating {
		if allowed, readOnlyIndicator := shellReadOnlyCommandAllowed(command); allowed {
			_ = readOnlyIndicator
			return nil
		}
		indicator = "shell_command_not_read_only_allowlisted"
	} else {
		indicator = strings.TrimSpace(indicator)
	}
	if indicator == "" {
		return nil
	}

	missing := policy.MissingContext()
	record := MutationDenyRecord{
		ToolName:       strings.TrimSpace(toolName),
		TargetPath:     truncateMutationDenyTarget(command),
		ReasonCode:     DirectRepoMutationDeniedReason,
		MissingContext: missing,
	}
	if policy.RecordDeny != nil {
		policy.RecordDeny(record)
	}
	return fmt.Errorf("%s: raw shell mutation is disabled (%s); use repo authority patch flow", DirectRepoMutationDeniedReason, indicator)
}

func (p RepoMutationPolicy) MissingContext() []string {
	if !p.RequireAuthority {
		return nil
	}
	var missing []string
	if strings.TrimSpace(p.Authority.RepoLeaseID) == "" {
		missing = append(missing, "repo_lease_id")
	}
	if p.Authority.LeaseTerm <= 0 {
		missing = append(missing, "lease_term")
	}
	if strings.TrimSpace(p.Authority.PatchQueueID) == "" {
		missing = append(missing, "patch_queue_id")
	}
	if strings.TrimSpace(p.Authority.PatchQueueItemID) == "" {
		missing = append(missing, "patch_queue_item_id")
	}
	return missing
}

func shellCompoundCommandIndicator(command string) (string, bool) {
	trimmed := strings.TrimSpace(command)
	if trimmed == "" {
		return "", false
	}
	compoundOperators := map[string]string{
		"\r": "shell_compound_newline",
		"\n": "shell_compound_newline",
		";":  "shell_compound_separator",
		"&&": "shell_compound_and",
		"||": "shell_compound_or",
		"|":  "shell_compound_pipe",
		"$(": "shell_compound_subshell",
		"`":  "shell_compound_backtick",
		"(":  "shell_compound_group",
		")":  "shell_compound_group",
		"{":  "shell_compound_group",
		"}":  "shell_compound_group",
		"<":  "shell_compound_redirect",
	}
	for operator, indicator := range compoundOperators {
		if strings.Contains(trimmed, operator) {
			return indicator, true
		}
	}
	if strings.Contains(trimmed, "&") {
		return "shell_compound_background", true
	}
	return "", false
}

func shellMutationIndicator(command string) (string, bool) {
	trimmed := strings.TrimSpace(command)
	if trimmed == "" {
		return "", false
	}
	if strings.Contains(trimmed, ">") {
		return "shell_redirect", true
	}

	for _, field := range shellCommandFields(trimmed) {
		if indicator, ok := shellMutationCommands()[field]; ok {
			return indicator, true
		}
	}
	return "", false
}

func shellReadOnlyCommandAllowed(command string) (bool, string) {
	fields := shellCommandFields(command)
	if len(fields) == 0 {
		return false, ""
	}
	switch fields[0] {
	case "get-content", "gc", "type", "cat", "select-string", "dir", "ls", "pwd", "echo":
		return true, fields[0]
	case "git":
		return shellGitReadOnlyCommandAllowed(fields)
	default:
		return false, fields[0]
	}
}

func shellGitReadOnlyCommandAllowed(fields []string) (bool, string) {
	if len(fields) < 2 {
		return false, "git"
	}
	subcommand := fields[1]
	switch subcommand {
	case "status", "diff", "log", "show", "rev-parse":
	default:
		return false, "git_" + subcommand
	}
	for _, field := range fields[2:] {
		if indicator, denied := shellGitReadOnlyOptionDenyIndicator(field); denied {
			return false, indicator
		}
	}
	return true, "git_" + subcommand
}

func shellGitReadOnlyOptionDenyIndicator(field string) (string, bool) {
	field = strings.TrimSpace(strings.ToLower(field))
	switch {
	case field == "":
		return "", false
	case field == "--output" || strings.HasPrefix(field, "--output=") || field == "-o" || strings.HasPrefix(field, "-o"):
		return "git_output", true
	case field == "--ext-diff" || strings.HasPrefix(field, "--ext-diff=") ||
		field == "--external-diff" || strings.HasPrefix(field, "--external-diff="):
		return "git_external_diff", true
	case field == "--textconv" || strings.HasPrefix(field, "--textconv="):
		return "git_textconv", true
	case field == "--paginate" || field == "--pager" || strings.HasPrefix(field, "--pager="):
		return "git_pager", true
	case field == "--help" || field == "-h":
		return "git_help_pager", true
	case field == "-c" || strings.HasPrefix(field, "-c") ||
		field == "--config-env" || strings.HasPrefix(field, "--config-env="):
		return "git_config_override", true
	case field == "--exec-path" || strings.HasPrefix(field, "--exec-path="):
		return "git_exec_path", true
	case field == "--git-dir" || strings.HasPrefix(field, "--git-dir=") ||
		field == "--work-tree" || strings.HasPrefix(field, "--work-tree="):
		return "git_repo_scope_override", true
	default:
		return "", false
	}
}

func shellCommandFields(command string) []string {
	replacer := strings.NewReplacer(
		"\t", " ",
		"\r", " ",
		"\n", " ",
		";", " ",
		"|", " ",
		"&", " ",
		"(", " ",
		")", " ",
		"{", " ",
		"}", " ",
		"[", " ",
		"]", " ",
		"\"", " ",
		"'", " ",
		"`", " ",
		",", " ",
	)
	rawFields := strings.Fields(replacer.Replace(strings.ToLower(command)))
	fields := make([]string, 0, len(rawFields))
	for _, field := range rawFields {
		field = strings.TrimSpace(field)
		field = strings.TrimPrefix(field, "./")
		field = strings.TrimSuffix(field, ".exe")
		if field != "" {
			fields = append(fields, field)
		}
	}
	return fields
}

func shellMutationCommands() map[string]string {
	return map[string]string{
		"set-content":   "powershell_set_content",
		"add-content":   "powershell_add_content",
		"out-file":      "powershell_out_file",
		"new-item":      "powershell_new_item",
		"remove-item":   "powershell_remove_item",
		"move-item":     "powershell_move_item",
		"rename-item":   "powershell_rename_item",
		"copy-item":     "powershell_copy_item",
		"clear-content": "powershell_clear_content",
		"sc":            "powershell_set_content_alias",
		"ac":            "powershell_add_content_alias",
		"ni":            "powershell_new_item_alias",
		"rm":            "remove",
		"mv":            "move",
		"cp":            "copy",
		"del":           "windows_delete",
		"erase":         "windows_delete",
		"rd":            "windows_remove_directory",
		"rmdir":         "remove_directory",
		"mkdir":         "make_directory",
		"md":            "windows_make_directory",
		"move":          "windows_move",
		"ren":           "windows_rename",
		"rename":        "windows_rename",
		"copy":          "windows_copy",
		"xcopy":         "windows_copy",
		"robocopy":      "windows_copy",
		"rm.exe":        "remove",
		"mv.exe":        "move",
		"touch":         "touch",
		"tee":           "tee_write",
		"tee-object":    "powershell_tee_object",
		"write":         "write_command",
	}
}

func truncateMutationDenyTarget(value string) string {
	value = strings.TrimSpace(value)
	const maxLen = 240
	if len(value) <= maxLen {
		return value
	}
	return value[:maxLen] + "..."
}

// WithCapabilityTier wraps a tool with a capability tier check.
func WithCapabilityTier(t Tool, requiredTier string, cfg BuiltinConfig) Tool {
	return &capabilityWrapper{
		Tool:         t,
		cfg:          cfg,
		requiredTier: requiredTier,
	}
}

type capabilityWrapper struct {
	Tool
	cfg          BuiltinConfig
	requiredTier string
}

func (w *capabilityWrapper) Execute(ctx context.Context, input json.RawMessage) (string, error) {
	if err := w.cfg.EnsureTier(w.requiredTier); err != nil {
		return fmt.Sprintf("Permission Denied: %v", err), nil
	}
	return w.Tool.Execute(ctx, input)
}

// Schema represents a JSON Schema object for tool input parameters.
type Schema struct {
	Type       string              `json:"type"`
	Properties map[string]Property `json:"properties,omitempty"`
	Required   []string            `json:"required,omitempty"`
}

// Property represents a JSON Schema property.
type Property struct {
	Type        string    `json:"type"`
	Description string    `json:"description,omitempty"`
	Enum        []string  `json:"enum,omitempty"`
	Default     any       `json:"default,omitempty"`
	Items       *Property `json:"items,omitempty"`
}

// Registry holds registered tools with ordered access and O(1) lookup.
type Registry struct {
	mu     sync.RWMutex
	tools  []Tool
	byName map[string]Tool
}

// NewRegistry creates a new empty tool registry.
func NewRegistry() *Registry {
	return &Registry{
		tools:  make([]Tool, 0),
		byName: make(map[string]Tool),
	}
}

// Register adds a tool to the registry. Returns error if a tool with the same name exists.
func (r *Registry) Register(tool Tool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := tool.Name()
	if _, exists := r.byName[name]; exists {
		return fmt.Errorf("tool already registered: %s", name)
	}
	r.tools = append(r.tools, tool)
	r.byName[name] = tool
	return nil
}

// Get returns a tool by name.
func (r *Registry) Get(name string) (Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.byName[name]
	return t, ok
}

// List returns all registered tools in registration order.
func (r *Registry) List() []Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Tool, len(r.tools))
	copy(out, r.tools)
	return out
}

// Names returns all registered tool names in registration order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, len(r.tools))
	for i, t := range r.tools {
		names[i] = t.Name()
	}
	return names
}

// SchemaToClaudeFormat produces the tool definition in Claude API format.
func SchemaToClaudeFormat(name, description string, schema Schema) map[string]any {
	return map[string]any{
		"name":        name,
		"description": description,
		"input_schema": map[string]any{
			"type":       schema.Type,
			"properties": schema.Properties,
			"required":   schema.Required,
		},
	}
}
