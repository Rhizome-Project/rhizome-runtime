package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Agent struct {
	LLM                   ChatLLM
	Workdir               string
	Client                *RhizomeClient
	WorkspaceID           string
	AgentID               string
	OwnerUserID           string
	CoordinationMode      string
	Anatomy               AgentAnatomyConfig
	DisableLocalShell     bool
	DisableLocalMutation  bool
	DisableLocalMemory    bool
	MemoryService         *AgentMemoryService
	WorkspaceDocDraftGate func() WorkspaceDocDraftGateState
	RuntimeBinding        func() AgentRuntimeBinding
	DisabledToolNames     []string
	registry              *ToolRegistry
	dynamicTools          []Tool
	toolBundleDiscovery   ToolBundleDiscoveryReport
}

type AgentRuntimeBinding struct {
	TaskID                   string
	ProjectID                string
	ClaimRepoID              string
	ClaimCheckoutID          string
	ClaimBranchID            string
	ClaimWriteScopeJSON      string
	SessionID                string
	RunID                    string
	CapabilitySnapshotID     string
	CapabilitySnapshotSchema string
}

type AgentTaskContext struct {
	Mode                     string
	WorkspaceID              string
	AgentID                  string
	SessionID                string
	RunID                    string
	CapabilitySnapshotID     string
	CapabilitySnapshotSchema string
	Summary                  string
	SpecPack                 string
	Task                     *WorkspaceTaskRecord
	Hydration                *TaskHydrationBundle
	Snapshot                 *WorkspaceSnapshot
	Message                  *MessageRecord
	Packet                   *AgentWorkPacket
	AdvisorySignals          []string
	ReflectionSignals        []ReflectionSignal
	ToolLoopLimit            int
}

// Init registers tools and loads personality.
func (a *Agent) Init() {
	a.SetDynamicTools(nil)
}

func (a *Agent) registerCoreTools(registry *ToolRegistry) {
	if registry == nil {
		return
	}
	if !a.DisableLocalShell {
		shellTool := NewShellTool(a.Workdir)
		if a.Client != nil && a.WorkspaceID != "" && a.AgentID != "" {
			shellTool.WithProjectCheckoutGuard(a.Client, a.WorkspaceID, a.AgentID, a.RuntimeBinding)
		}
		registry.Register(shellTool)
	}
	readTool := NewReadFileTool(a.Workdir)
	if a.Client != nil && a.WorkspaceID != "" && a.AgentID != "" {
		readTool.WithProjectCheckoutGuard(a.Client, a.WorkspaceID, a.AgentID, a.RuntimeBinding)
	}
	registry.Register(readTool)
	listTool := NewListDirectoryTool(a.Workdir)
	if a.Client != nil && a.WorkspaceID != "" && a.AgentID != "" {
		listTool.WithProjectCheckoutGuard(a.Client, a.WorkspaceID, a.AgentID, a.RuntimeBinding)
	}
	registry.Register(listTool)
	if !a.DisableLocalMutation {
		writeTool := NewWriteFileTool(a.Workdir)
		if a.Client != nil && a.WorkspaceID != "" && a.AgentID != "" {
			writeTool.WithProjectCheckoutGuard(a.Client, a.WorkspaceID, a.AgentID, a.RuntimeBinding)
		}
		registry.Register(writeTool)
	}
	registry.Register(NewToolBundleRegistryTool(a.Workdir, !a.DisableLocalMutation, func() ToolBundleDiscoveryReport {
		return a.CaptureInstalledToolBundleDiscovery()
	}, func() ToolBundleDiscoveryReport {
		return a.RefreshInstalledToolBundles()
	}))
	if !a.DisableLocalMemory {
		registry.Register(NewMemoryReadTool(a.Workdir))
		if !a.DisableLocalMutation {
			registry.Register(NewMemoryWriteTool(a.Workdir))
			registry.Register(NewDailyNoteAppendTool(a.Workdir))
		}
	}
	if a.MemoryService != nil {
		registry.Register(NewMemorySearchTool(a.MemoryService, a.Client, a.WorkspaceID))
		if !a.DisableLocalMemory {
			registry.Register(NewMemoryReinforceTool(a.Client, a.WorkspaceID))
		}
	}
	if a.Client != nil && a.WorkspaceID != "" && a.AgentID != "" {
		registry.Register(NewWorkspaceDocGetTool(a.Client, a.WorkspaceID).WithRuntimeBinding(a.RuntimeBinding))
		registry.Register(NewWorkspaceDocPutTool(a.Client, a.WorkspaceID, a.AgentID).WithDraftGate(a.WorkspaceDocDraftGate))
		registry.Register(NewProjectBootstrapTool(a.Client, a.WorkspaceID, a.AgentID, a.OwnerUserID).WithRuntimeBinding(a.RuntimeBinding))
		registry.Register(NewProjectRepoRegisterTool(a.Client, a.WorkspaceID, a.AgentID, a.OwnerUserID).WithRuntimeBinding(a.RuntimeBinding))
		registry.Register(NewProjectRepoMaterializeTool(a.Client, a.WorkspaceID, a.AgentID, a.OwnerUserID, a.Workdir).WithRuntimeBinding(a.RuntimeBinding))
		registry.Register(NewProjectRoleAssignTool(a.Client, a.WorkspaceID, a.AgentID).WithRuntimeBinding(a.RuntimeBinding))
		registry.Register(NewProjectPhaseTransitionTool(a.Client, a.WorkspaceID, a.AgentID).WithCoordinationMode(a.CoordinationMode).WithRuntimeBinding(a.RuntimeBinding))
		if projectGovernanceToolsEnabled() {
			registry.Register(NewProjectGovernanceTool(a.Client, a.WorkspaceID, a.AgentID).WithRuntimeBinding(a.RuntimeBinding))
		}
		registry.Register(NewProjectCheckoutMaterializeTool(a.Client, a.WorkspaceID, a.AgentID, a.OwnerUserID, a.Workdir).WithRuntimeBinding(a.RuntimeBinding))
		registry.Register(NewProjectCheckoutRegisterTool(a.Client, a.WorkspaceID, a.AgentID, a.OwnerUserID, a.Workdir).WithRuntimeBinding(a.RuntimeBinding))
		registry.Register(NewProjectBranchCommitTool(a.Client, a.WorkspaceID, a.AgentID, a.OwnerUserID, a.Workdir).WithCoordinationMode(a.CoordinationMode).WithRuntimeBinding(a.RuntimeBinding))
		registry.Register(NewSideEffectResolveTool(a.Client, a.WorkspaceID, a.AgentID, a.OwnerUserID))
		registry.Register(NewProjectBranchReviewReadyTool(a.Client, a.WorkspaceID, a.AgentID, a.Workdir).WithRuntimeBinding(a.RuntimeBinding))
		registry.Register(NewProjectPatchQueueSubmitTool(a.Client, a.WorkspaceID, a.AgentID).WithWorkdir(a.Workdir).WithRuntimeBinding(a.RuntimeBinding))
		registry.Register(NewProjectPatchQueueListTool(a.Client, a.WorkspaceID, a.AgentID).WithRuntimeBinding(a.RuntimeBinding))
		registry.Register(NewProjectPatchQueueLifecycleTool(a.Client, a.WorkspaceID, a.AgentID).WithRuntimeBinding(a.RuntimeBinding))
		registry.Register(NewProjectPatchQueueCASTool(a.Client, a.WorkspaceID, a.AgentID, a.Workdir).WithRuntimeBinding(a.RuntimeBinding))
		registry.Register(NewProjectPatchQueueMaterializeTool(a.Client, a.WorkspaceID, a.AgentID, a.Workdir).WithRuntimeBinding(a.RuntimeBinding))
		registry.Register(NewProjectPatchQueueIntegrateTool(a.Client, a.WorkspaceID, a.AgentID, a.OwnerUserID, a.Workdir).WithCoordinationMode(a.CoordinationMode).WithRuntimeBinding(a.RuntimeBinding))
		registry.Register(NewProjectPatchQueueFollowupTool(a.Client, a.WorkspaceID, a.AgentID, a.OwnerUserID).WithRuntimeBinding(a.RuntimeBinding))
		RegisterServiceVentureTools(registry, a.Client, a.WorkspaceID, a.AgentID)
		RegisterBudgetTools(registry, a.Client, a.WorkspaceID, a.AgentID)
		registry.Register(NewTaskSubmitTool(a.Client, a.WorkspaceID, a.AgentID, a.OwnerUserID).WithCoordinationMode(a.CoordinationMode).WithRuntimeBinding(a.RuntimeBinding))

		registry.Register(NewTensionAttachTool(a.Client, a.WorkspaceID, a.AgentID))
		registry.Register(NewTensionDetachTool(a.Client, a.WorkspaceID, a.AgentID))
		registry.Register(NewTensionLifecycleTool(a.Client, a.WorkspaceID, a.AgentID))

		registry.Register(NewCoalitionOfferTool(a.Client, a.WorkspaceID, a.AgentID))
		registry.Register(NewCoalitionLeaveTool(a.Client, a.WorkspaceID, a.AgentID))
		registry.Register(NewCoalitionSeekTool(a.Client, a.WorkspaceID, a.AgentID))
		registry.Register(NewCoalitionInviteTool(a.Client, a.WorkspaceID, a.AgentID))
		registry.Register(NewCoalitionKickTool(a.Client, a.WorkspaceID, a.AgentID))
		registry.Register(NewCoalitionStatusTool(a.Client, a.WorkspaceID))
		registry.Register(NewAgentRequestTool(a.Client, a.WorkspaceID, a.AgentID))

		registry.Register(NewReviewerRouteTool(a.Client, a.WorkspaceID, a.AgentID))
		registry.Register(NewReviewerScarcityTool(a.Client, a.WorkspaceID, a.AgentID))

		registry.Register(NewMemoryCoherenceReadTool(a.Client, a.WorkspaceID, a.AgentID))
		registry.Register(NewMemoryPromotionReadTool(a.Client, a.WorkspaceID))
	}
}

func (a *Agent) registerBaseTools(registry *ToolRegistry) {
	if registry == nil {
		return
	}
	a.registerCoreTools(registry)
	a.toolBundleDiscovery = RegisterInstalledToolBundles(registry, a.Workdir)
}

func (a *Agent) baseToolNames() map[string]struct{} {
	registry := NewToolRegistry()
	a.registerBaseTools(registry)
	names := make(map[string]struct{}, len(registry.tools))
	for name := range registry.tools {
		names[name] = struct{}{}
	}
	return names
}

func validateWorkspaceToolCollisions(baseNames map[string]struct{}, records []WorkspaceToolRecord) error {
	seen := make(map[string]string, len(records))
	for _, record := range records {
		toolID := strings.TrimSpace(record.ToolID)
		if !containsAlphaNumeric(toolID) {
			return fmt.Errorf("workspace tool %q has no usable identifier after sanitization", toolID)
		}
		name := sanitizeWorkspaceFunctionName(toolID)
		if _, ok := baseNames[name]; ok {
			return fmt.Errorf("workspace tool %q collides with reserved tool name %q", toolID, name)
		}
		if existing, ok := seen[name]; ok {
			return fmt.Errorf("workspace tools %q and %q collide after sanitization as %q", existing, toolID, name)
		}
		seen[name] = toolID
	}
	return nil
}

func validateMCPToolCollisions(baseNames map[string]struct{}, records []MCPToolRecord) error {
	seen := make(map[string]string, len(records))
	for _, record := range records {
		serverID := strings.TrimSpace(record.ServerID)
		toolName := strings.TrimSpace(record.ToolName)
		source := serverID + "/" + toolName
		if !containsAlphaNumeric(serverID) || !containsAlphaNumeric(toolName) {
			return fmt.Errorf("mcp tool %q has no usable identifier after sanitization", source)
		}
		name := sanitizeMCPFunctionName(record.ServerID, record.ToolName)
		if _, ok := baseNames[name]; ok {
			return fmt.Errorf("mcp tool %q collides with reserved tool name %q", source, name)
		}
		if existing, ok := seen[name]; ok {
			return fmt.Errorf("mcp tools %q and %q collide after sanitization as %q", existing, source, name)
		}
		seen[name] = source
	}
	return nil
}

func containsAlphaNumeric(value string) bool {
	for _, r := range strings.TrimSpace(value) {
		switch {
		case r >= 'a' && r <= 'z':
			return true
		case r >= 'A' && r <= 'Z':
			return true
		case r >= '0' && r <= '9':
			return true
		}
	}
	return false
}

func (a *Agent) SetDynamicTools(extra []Tool) {
	if a == nil {
		return
	}
	a.dynamicTools = append([]Tool(nil), extra...)
	registry := NewToolRegistry()
	a.registerCoreTools(registry)
	for _, tool := range extra {
		registry.Register(tool)
	}
	a.toolBundleDiscovery = RegisterInstalledToolBundles(registry, a.Workdir)
	a.registry = a.applyToolFilters(registry)
}

// roleBannedToolNames returns the set of tools that are trimmed for a given
// agent anatomy preset (role). The trim is conservative and safety-first: a
// tool is banned only when the role's documented duties clearly exclude it.
// This is the single registry/role boundary, so a banned tool appears NEITHER
// in the backend tool JSON (ToolRegistry.Definitions) NOR in the capability
// snapshot / prompt projection (capabilitySnapshotToolNames reads a.registry),
// and tool execution of a banned tool is rejected because the registry never
// holds it.
//
// reviewer_qa / ui_ux_reality_critic: these roles seek falsifying evidence and
// produce provisional critique and lane review decisions. The operating
// contract states reviewers "do not merge", do not own implementation
// branches, and inspect candidates as non-canonical evidence. So the
// write/integrate/apply tools below are excluded by the role contract:
//   - write_file: raw local source mutation (implementation work)
//   - project_branch_commit: creates/pushes git commits on an owned
//     implementation branch (implementation work)
//   - project_patch_queue_integrate: merges accepted candidates into the
//     canonical baseline (integration/lead duty, not reviewer)
//   - project_repo_materialize: seeds a canonical git repo (setup/lead duty)
//
// Deliberately KEPT for reviewers (in doubt, keep the tool):
//   - read_file / list_directory / workspace_doc_get: inspection
//   - project_checkout_materialize: the contract explicitly directs reviewers
//     to use validation checkouts (omit branch_name) for QA/smoke/UX
//   - project_branch_review_ready, reviewer_route, reviewer_scarcity,
//     project_patch_queue_lifecycle (reviewer_advisory/accept/reject/block),
//     project_patch_queue_cas_record / _materialize: the contract notes a
//     reviewer can record lane decisions and these read-only evidence tools
//     may be needed by reviewer/integrator overlap.
func roleBannedToolNames(preset string) map[string]struct{} {
	switch normalizeAgentAnatomyPreset(preset) {
	case "reviewer_qa", "ui_ux_reality_critic":
		return map[string]struct{}{
			"write_file":                    {},
			"project_branch_commit":         {},
			"project_patch_queue_integrate": {},
			"project_repo_materialize":      {},
		}
	default:
		return nil
	}
}

// applyRoleToolDiet trims role-banned tools from the registry at the role
// boundary. When no trim applies it returns the registry unchanged so behavior
// is byte-identical to the pre-diet path for non-reviewer roles.
func (a *Agent) applyRoleToolDiet(registry *ToolRegistry) *ToolRegistry {
	if a == nil || registry == nil {
		return registry
	}
	banned := roleBannedToolNames(a.Anatomy.Preset)
	if len(banned) == 0 {
		return registry
	}
	return registry.Filter(func(name string) bool {
		_, isBanned := banned[strings.TrimSpace(name)]
		return !isBanned
	})
}

func (a *Agent) applyRuntimeToolDenylist(registry *ToolRegistry) *ToolRegistry {
	if a == nil || registry == nil {
		return registry
	}
	disabled := runtimeDisabledToolNameItems(a.DisabledToolNames)
	if len(disabled) == 0 {
		return registry
	}
	return registry.Filter(func(name string) bool {
		_, blocked := capabilityDisabledToolNameMatch(name, disabled)
		return !blocked
	})
}

func (a *Agent) applyToolFilters(registry *ToolRegistry) *ToolRegistry {
	registry = a.applyRuntimeToolDenylist(registry)
	return a.applyRoleToolDiet(registry)
}

func runtimeDisabledToolNameItems(names []string) []CapabilityDisabledToolName {
	normalized := uniqueTrimmedCSVStrings(names)
	if len(normalized) == 0 {
		return nil
	}
	items := make([]CapabilityDisabledToolName, 0, len(normalized))
	for _, name := range normalized {
		items = append(items, CapabilityDisabledToolName{
			Name:       name,
			ReasonCode: "operator_harness.tool_disabled",
		})
	}
	return items
}

func (a *Agent) RefreshInstalledToolBundles() ToolBundleDiscoveryReport {
	if a == nil {
		return ToolBundleDiscoveryReport{}
	}
	a.SetDynamicTools(a.dynamicTools)
	return a.toolBundleDiscovery
}

func (a *Agent) CaptureInstalledToolBundleDiscovery() ToolBundleDiscoveryReport {
	if a == nil {
		return ToolBundleDiscoveryReport{}
	}
	registry := NewToolRegistry()
	a.registerCoreTools(registry)
	for _, tool := range a.dynamicTools {
		registry.Register(tool)
	}
	return RegisterInstalledToolBundles(registry, a.Workdir)
}

func (a *Agent) RefreshRhizomeMCPTools(ctx context.Context, client *RhizomeClient, workspaceID string) error {
	if a == nil || client == nil || strings.TrimSpace(workspaceID) == "" {
		return nil
	}
	tools, err := client.ListMCPTools(ctx, workspaceID)
	if err != nil {
		return err
	}
	if err := validateMCPToolCollisions(a.baseToolNames(), tools); err != nil {
		a.SetDynamicTools(nil)
		return err
	}
	extra := make([]Tool, 0, len(tools))
	for _, record := range tools {
		extra = append(extra, NewRhizomeMCPTool(client, workspaceID, record))
	}
	a.SetDynamicTools(extra)
	return nil
}

func (a *Agent) RefreshRhizomeWorkspaceTools(ctx context.Context, client *RhizomeClient, workspaceID, agentID string, options ...WorkspaceToolOption) error {
	if a == nil || client == nil || strings.TrimSpace(workspaceID) == "" {
		return nil
	}
	records, err := client.ListWorkspaceTools(ctx, workspaceID)
	if err != nil {
		return err
	}
	candidates := make([]WorkspaceToolRecord, 0, len(records))
	for _, record := range records {
		if !strings.EqualFold(strings.TrimSpace(record.Status), "ACTIVE") {
			continue
		}
		if !workspaceToolHasUsableSchema(record) {
			continue
		}
		candidates = append(candidates, record)
	}
	if err := validateWorkspaceToolCollisions(a.baseToolNames(), candidates); err != nil {
		a.SetDynamicTools(nil)
		return err
	}
	extra := make([]Tool, 0, len(candidates))
	for _, record := range candidates {
		extra = append(extra, NewRhizomeWorkspaceTool(client, workspaceID, agentID, record, options...))
	}
	if len(extra) == 0 {
		return a.RefreshRhizomeMCPTools(ctx, client, workspaceID)
	}
	a.SetDynamicTools(extra)
	return nil
}

// ExecuteTask runs the default tool loop for a given task.
func (a *Agent) ExecuteTask(ctx context.Context, task string) (string, error) {
	return a.ExecuteTaskWithHooks(ctx, task, AgentTaskContext{}, nil, nil)
}

func (a *Agent) ExecuteTaskWithHooks(ctx context.Context, task string, taskCtx AgentTaskContext, executor ToolLoopExecutor, observer ToolLoopObserver) (string, error) {
	if err := validateDaemonPromptSpecPack(taskCtx); err != nil {
		return "", err
	}
	systemPrompt := a.buildSystemPrompt(taskCtx)
	userPrompt := a.buildUserPrompt(task, taskCtx)

	messages := []Message{
		{Role: "system", Content: systemPrompt},
		{Role: "user", Content: userPrompt},
	}

	if taskCtx.ToolLoopLimit > 0 {
		return RunToolLoopWithLimit(ctx, a.LLM, a.registry, messages, executor, observer, taskCtx.ToolLoopLimit)
	}
	return RunToolLoopWithHooks(ctx, a.LLM, a.registry, messages, executor, observer)
}

func validateDaemonPromptSpecPack(taskCtx AgentTaskContext) error {
	if !strings.EqualFold(strings.TrimSpace(taskCtx.Mode), "daemon") {
		return nil
	}
	section, ok := leadingDaemonCapabilityProjectionSection(taskCtx.SpecPack)
	required := []string{
		"## Active Capability Snapshot",
	}
	missing := make([]string, 0, len(required))
	if !ok {
		missing = append(missing, "leading ## Active Capability Snapshot section")
	}
	for _, marker := range required {
		if !strings.Contains(section, marker) {
			missing = append(missing, marker)
		}
	}
	if snapshotID := daemonCapabilityProjectionField(section, "snapshot_id"); snapshotID == "" {
		missing = append(missing, "non-empty snapshot_id")
	} else if !strings.HasPrefix(snapshotID, "cap_") {
		missing = append(missing, "snapshot_id with cap_ prefix")
	}
	if source := daemonCapabilityProjectionField(section, "projection_source"); source != "agent.runtime_capability_snapshot" {
		missing = append(missing, "projection_source: agent.runtime_capability_snapshot")
	}
	if contract := daemonCapabilityProjectionField(section, "projection_contract"); contract != "active_capability_snapshot_projection.v1" {
		missing = append(missing, "projection_contract: active_capability_snapshot_projection.v1")
	}
	digests := daemonCapabilityProjectionFieldValues(section, "projection_digest")
	if len(digests) != 1 {
		missing = append(missing, "exactly one projection_digest")
	} else if digests[0] == "" {
		missing = append(missing, "projection_digest")
	} else if digests[0] != daemonCapabilityProjectionDigest(section) {
		missing = append(missing, "projection_digest matches rendered section")
	}
	snapshotKind := daemonCapabilityProjectionField(section, "snapshot_kind")
	if snapshotKind != "boot" && snapshotKind != "run" {
		missing = append(missing, "snapshot_kind boot|run")
	}
	if schema := daemonCapabilityProjectionField(section, "schema"); schema != daemonCapabilitySnapshotSchema {
		missing = append(missing, "schema: "+daemonCapabilitySnapshotSchema)
	}
	if contract := daemonCapabilityProjectionField(section, "prompt_contract"); contract != "prompt_capabilities.v1" {
		missing = append(missing, "prompt_contract: prompt_capabilities.v1")
	}
	enabled := daemonCapabilityProjectionField(section, "enabled_tools")
	if enabled == "" {
		missing = append(missing, "non-empty enabled_tools")
	} else {
		enabledTools := promptProjectionCSVValues(enabled)
		if !containsPromptProjectionValue(enabledTools, "read_file") && !containsPromptProjectionValue(enabledTools, "list_directory") {
			missing = append(missing, "enabled_tools read_file|list_directory")
		}
		disabledTools := daemonCapabilityProjectionDisabledToolNames(section)
		for _, name := range enabledTools {
			if _, disabled := capabilityDisabledToolNameMatch(name, disabledTools); disabled {
				missing = append(missing, "enabled_tools excludes disabled tools")
				break
			}
		}
	}
	if !strings.Contains(section, "- disabled_tools:") {
		missing = append(missing, "disabled_tools section")
	}
	if !strings.Contains(section, "executor.subprocess: disabled (executor.operation_ledger_required)") {
		missing = append(missing, "executor disabled reason")
	}
	if !strings.Contains(section, "- surface_states:") {
		missing = append(missing, "surface_states section")
	}
	if !strings.Contains(section, "executor: disabled") || !strings.Contains(section, "mcp: disabled") {
		missing = append(missing, "disabled executor/mcp surface states")
	}
	if !strings.Contains(section, "- hard_rules:") || !strings.Contains(section, "Only use enabled tools listed in this capability snapshot.") {
		missing = append(missing, "capability hard rule")
	}
	maxToolIterations := daemonCapabilityProjectionField(section, "max_tool_iterations")
	if maxToolIterations == "" {
		missing = append(missing, "budget_ceilings.max_tool_iterations")
	} else if parsed, err := strconv.Atoi(maxToolIterations); err != nil || parsed <= 0 {
		missing = append(missing, "positive budget_ceilings.max_tool_iterations")
	}
	if len(missing) > 0 {
		return fmt.Errorf("daemon prompt spec pack missing active capability snapshot projection markers: %s", strings.Join(missing, ", "))
	}
	return nil
}

func leadingDaemonCapabilityProjectionSection(specPack string) (string, bool) {
	specPack = strings.TrimSpace(specPack)
	if !strings.HasPrefix(specPack, "## Active Capability Snapshot") {
		return "", false
	}
	lines := strings.Split(specPack, "\n")
	end := len(lines)
	for idx := 1; idx < len(lines); idx++ {
		if strings.HasPrefix(strings.TrimSpace(lines[idx]), "## ") {
			end = idx
			break
		}
	}
	return strings.TrimSpace(strings.Join(lines[:end], "\n")), true
}

func daemonCapabilityProjectionField(section, name string) string {
	values := daemonCapabilityProjectionFieldValues(section, name)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func daemonCapabilityProjectionFieldValues(section, name string) []string {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	prefix := name + ":"
	values := []string{}
	for _, line := range strings.Split(section, "\n") {
		trimmed := strings.TrimSpace(line)
		trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))
		if strings.HasPrefix(trimmed, prefix) {
			values = append(values, strings.TrimSpace(strings.TrimPrefix(trimmed, prefix)))
		}
	}
	return values
}

func promptProjectionCSVValues(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" || value == "(none)" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" || strings.HasPrefix(item, "...") {
			continue
		}
		out = append(out, item)
	}
	return out
}

func containsPromptProjectionValue(items []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, item := range items {
		if strings.TrimSpace(item) == want {
			return true
		}
	}
	return false
}

func daemonCapabilityProjectionDisabledToolNames(section string) []CapabilityDisabledToolName {
	lines := strings.Split(section, "\n")
	disabled := make([]CapabilityDisabledToolName, 0)
	inDisabledTools := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "- disabled_tools:" {
			inDisabledTools = true
			continue
		}
		if !inDisabledTools {
			continue
		}
		if !strings.HasPrefix(line, "  - ") {
			if strings.HasPrefix(trimmed, "- ") {
				break
			}
			continue
		}
		item := strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))
		name, rest, ok := strings.Cut(item, ": disabled (")
		if !ok {
			continue
		}
		reason := strings.TrimSuffix(strings.TrimSpace(rest), ")")
		name = strings.TrimSpace(name)
		if name == "" || reason == "" {
			continue
		}
		disabled = append(disabled, CapabilityDisabledToolName{Name: name, ReasonCode: reason})
	}
	return disabled
}

func (a *Agent) buildSystemPrompt(taskCtx AgentTaskContext) string {
	var b strings.Builder

	soul := LoadWorkspaceFile(a.Workdir, "SOUL.md")
	if soul != "" {
		b.WriteString(soul)
		b.WriteString("\n\n")
	}
	if taskCtx.Mode != "daemon" {
		agentProfile := LoadWorkspaceFile(a.Workdir, "AGENT.md")
		if agentProfile != "" {
			b.WriteString(agentProfile)
			b.WriteString("\n\n")
		}
		toolProfile := LoadWorkspaceFile(a.Workdir, "TOOLS.md")
		if toolProfile != "" {
			b.WriteString(toolProfile)
			b.WriteString("\n\n")
		}
	}

	memCtx := ""
	if !a.DisableLocalMemory && taskCtx.Mode != "daemon" {
		memCtx = LoadMemoryContext(a.Workdir)
	}
	if memCtx != "" {
		b.WriteString(memCtx)
		b.WriteString("\n\n")
	}

	if strings.TrimSpace(taskCtx.SpecPack) != "" {
		b.WriteString(taskCtx.SpecPack)
		b.WriteString("\n\n")
	}
	if section := buildInstalledToolBundlePromptSectionWithReportAndSuites(a.registry, a.toolBundleDiscovery, toolBundleSuiteReadinessForAgent(a)); section != "" {
		b.WriteString(section)
		b.WriteString("\n\n")
	}

	if taskCtx.Mode == "tui" {
		b.WriteString("## Interactive Session Contract\n")
		b.WriteString("- The local operator is speaking to you directly in a terminal chat.\n")
		b.WriteString("- Answer directly instead of narrating hidden process.\n")
		b.WriteString("- Preserve conversational context across turns.\n")
		b.WriteString("- Use tools when they materially improve the answer or let you verify the workspace.\n\n")
	}

	b.WriteString("## Current Context\n")
	b.WriteString(fmt.Sprintf("- Time: %s\n", time.Now().Format("2006-01-02 15:04 (Monday)")))
	b.WriteString(fmt.Sprintf("- Local Workspace: %s\n", a.Workdir))
	b.WriteString(fmt.Sprintf("- Mode: %s\n", firstNonEmpty(taskCtx.Mode, "oneshot")))
	if taskCtx.WorkspaceID != "" {
		b.WriteString(fmt.Sprintf("- Rhizome Workspace: %s\n", taskCtx.WorkspaceID))
	}
	if taskCtx.AgentID != "" {
		b.WriteString(fmt.Sprintf("- Agent ID: %s\n", taskCtx.AgentID))
	}
	if taskCtx.SessionID != "" {
		b.WriteString(fmt.Sprintf("- Session ID: %s\n", taskCtx.SessionID))
	}
	if taskCtx.RunID != "" {
		b.WriteString(fmt.Sprintf("- Run ID: %s\n", taskCtx.RunID))
	}
	if taskCtx.CapabilitySnapshotID != "" {
		b.WriteString(fmt.Sprintf("- Capability Snapshot ID: %s\n", taskCtx.CapabilitySnapshotID))
	}
	if taskCtx.CapabilitySnapshotSchema != "" {
		b.WriteString(fmt.Sprintf("- Capability Snapshot Schema: %s\n", taskCtx.CapabilitySnapshotSchema))
	}
	if taskCtx.Task != nil && taskCtx.Task.TaskID != "" {
		b.WriteString(fmt.Sprintf("- Active Task: %s\n", taskCtx.Task.TaskID))
	}
	if strings.TrimSpace(taskCtx.Summary) != "" {
		b.WriteString(fmt.Sprintf("- Runtime Summary: %s\n", strings.TrimSpace(taskCtx.Summary)))
	}

	if len(taskCtx.AdvisorySignals) > 0 {
		b.WriteString("\n## 🚨 Watchdog Advisory Signals 🚨\n")
		b.WriteString("The Shadow Monitor has observed your recent actions and generated the following interventions. You MUST read these carefully and adjust your behavior to avoid looping or drifting:\n")
		for _, sig := range taskCtx.AdvisorySignals {
			b.WriteString(fmt.Sprintf("- %s\n", sig))
		}
	}

	writeReflectionChannelSection(&b, taskCtx.ReflectionSignals)

	return strings.TrimSpace(b.String())
}

// writeReflectionChannelSection renders the Layer B reflection channel: the
// agent's own heartbeat-authored insight, kept visually and semantically separate
// from the watchdog advisory ring so external intervention and self-reflection
// are not confused. Absent entirely when the channel is empty (cap=0 fallback).
func writeReflectionChannelSection(b *strings.Builder, signals []ReflectionSignal) {
	if len(signals) == 0 {
		return
	}
	b.WriteString("\n## Reflection Channel (self-authored insight)\n")
	b.WriteString("These are observations your own reflection/sensing heartbeats recorded. Treat them as advisory context, distinct from the watchdog interventions above:\n")
	for _, sig := range signals {
		text := strings.TrimSpace(sig.Text)
		if text == "" {
			continue
		}
		kind := reflectionKindCanonical(sig.Kind)
		line := fmt.Sprintf("- [%s]", kind)
		if source := strings.TrimSpace(sig.Source); source != "" {
			line += " " + source + ":"
		}
		line += " " + text
		if sig.Count > 1 {
			line += fmt.Sprintf(" (x%d)", sig.Count)
		}
		b.WriteString(line + "\n")
	}
}

func (a *Agent) buildUserPrompt(task string, taskCtx AgentTaskContext) string {
	if taskCtx.Task == nil && taskCtx.Message == nil && taskCtx.Hydration == nil && taskCtx.Snapshot == nil {
		return task
	}

	var b strings.Builder
	b.WriteString("# Objective\n")
	b.WriteString(strings.TrimSpace(task))
	b.WriteString("\n")

	if taskCtx.Task != nil {
		b.WriteString("\n# Task Context\n")
		b.WriteString(fmt.Sprintf("- Task ID: %s\n", taskCtx.Task.TaskID))
		if taskCtx.Task.Title != "" {
			b.WriteString(fmt.Sprintf("- Title: %s\n", taskCtx.Task.Title))
		}
		if taskCtx.Task.Description != "" {
			b.WriteString("- Description:\n")
			b.WriteString(taskCtx.Task.Description)
			b.WriteString("\n")
		}
		b.WriteString(fmt.Sprintf("- Status: %s\n", taskCtx.Task.Status))
		b.WriteString(fmt.Sprintf("- Priority: %s\n", taskCtx.Task.Priority))
		b.WriteString(fmt.Sprintf("- Claim Agent: %s\n", pointerValue(taskCtx.Task.ClaimAgentID)))
		b.WriteString(fmt.Sprintf("- Claim Status: %s\n", pointerValue(taskCtx.Task.ClaimStatus)))
		appendProjectClaimAdmissionPrompt(&b, taskCtx.Task, "- ")
		if taskCtx.Task.ClaimSummary != nil && strings.TrimSpace(*taskCtx.Task.ClaimSummary) != "" {
			b.WriteString(fmt.Sprintf("- Existing Claim Summary: %s\n", strings.TrimSpace(*taskCtx.Task.ClaimSummary)))
		}
	}

	if taskCtx.Message != nil {
		b.WriteString("\n# Trigger Message\n")
		b.WriteString(fmt.Sprintf("- Message ID: %s\n", taskCtx.Message.MessageID))
		b.WriteString(fmt.Sprintf("- From: %s\n", firstNonEmpty(taskCtx.Message.FromAgentID, "unknown")))
		b.WriteString(fmt.Sprintf("- Channel: %s\n", firstNonEmpty(taskCtx.Message.Channel, "default")))
		b.WriteString("- Content:\n")
		b.WriteString(strings.TrimSpace(taskCtx.Message.Content))
		b.WriteString("\n")
		if strings.TrimSpace(taskCtx.Message.MetadataJSON) != "" {
			b.WriteString("- Metadata JSON:\n")
			b.WriteString(strings.TrimSpace(taskCtx.Message.MetadataJSON))
			b.WriteString("\n")
		}
	}

	if taskCtx.Hydration != nil {
		b.WriteString("\n# Hydrated Task Context\n")
		b.WriteString(fmt.Sprintf("- Related Tasks: %d\n", len(taskCtx.Hydration.RelatedTasks)))
		b.WriteString(fmt.Sprintf("- Related Artifacts: %d\n", len(taskCtx.Hydration.Artifacts)))
		b.WriteString(fmt.Sprintf("- Related Updates: %d\n", len(taskCtx.Hydration.Updates)))
		if len(taskCtx.Hydration.Updates) > 0 {
			b.WriteString("- Recent Task Updates:\n")
			for _, update := range taskCtx.Hydration.Updates {
				b.WriteString(fmt.Sprintf("  - %s [%s] %s\n", firstNonEmpty(update.AgentName, update.AgentID), update.UpdateType, oneLine(update.Summary)))
			}
		}
		if len(taskCtx.Hydration.Artifacts) > 0 {
			b.WriteString("- Task Artifacts:\n")
			for _, artifact := range taskCtx.Hydration.Artifacts {
				b.WriteString(fmt.Sprintf("  - %s | %s | %s\n", firstNonEmpty(artifact.Title, artifact.ArtifactID), artifact.Kind, artifact.ArtifactRef))
			}
		}
	}

	if taskCtx.Snapshot != nil {
		b.WriteString("\n# Shared Coordination Snapshot\n")
		b.WriteString(fmt.Sprintf("- Recent Updates: %d\n", len(taskCtx.Snapshot.RecentUpdates)))
		b.WriteString(fmt.Sprintf("- Recent Messages: %d\n", len(taskCtx.Snapshot.RecentMessages)))
		if len(taskCtx.Snapshot.Sessions) > 0 {
			b.WriteString("- Active Sessions:\n")
			for _, session := range taskCtx.Snapshot.Sessions {
				b.WriteString(fmt.Sprintf("  - %s | %s | %s | %s\n", session.SessionID, session.AgentID, session.Status, session.Summary))
			}
		}
		if len(taskCtx.Snapshot.RecentMessages) > 0 {
			b.WriteString("- Message Details:\n")
			for _, message := range taskCtx.Snapshot.RecentMessages {
				target := firstNonEmpty(message.ToAgentID, "broadcast")
				b.WriteString(fmt.Sprintf("  - %s -> %s | %s\n", firstNonEmpty(message.FromAgentID, "unknown"), target, oneLine(message.Content)))
			}
		}
		if len(taskCtx.Snapshot.RecentUpdates) > 0 {
			b.WriteString("- Update Details:\n")
			for _, update := range taskCtx.Snapshot.RecentUpdates {
				b.WriteString(fmt.Sprintf("  - %s [%s] %s\n", firstNonEmpty(update.AgentName, update.AgentID), update.UpdateType, update.Summary))
			}
		}
	}

	if taskCtx.Packet != nil {
		trustFirst := runtimeTrustFirst(RuntimeConfig{CoordinationMode: a.CoordinationMode})
		b.WriteString("\n# Transition Semantics\n")
		b.WriteString(fmt.Sprintf("- Coordination State: %s\n", firstNonEmpty(taskCtx.Packet.CoordinationState, "unknown")))
		if len(taskCtx.Packet.Blockers) > 0 {
			if trustFirst {
				b.WriteString("- Advisory Blockers Detected:\n")
			} else {
				b.WriteString("- Hard Blockers Detected:\n")
			}
			for _, blocker := range taskCtx.Packet.Blockers {
				b.WriteString(fmt.Sprintf("  - [%s] %s\n", blocker.Kind, blocker.Detail))
			}
			if trustFirst {
				b.WriteString("  > [!NOTE] In trust_first mode, treat these as fresh evidence to reason around. Try a bounded alternate path, self-repair, or coordination update before declaring a blocker.\n")
			} else {
				b.WriteString("  > [!WARNING] You are blocked by hard system state. Do not brute force. Create a structural tension or seek a different approach.\n")
			}
		}
		if taskCtx.Packet.Unblock != nil {
			b.WriteString(fmt.Sprintf("- Unblock Potential: %s\n", taskCtx.Packet.Unblock.Summary))
		}
		if taskCtx.Packet.Handoff != nil {
			b.WriteString(fmt.Sprintf("- Passive Handoff Wait: %s\n", taskCtx.Packet.Handoff.Summary))
			if taskCtx.Packet.HandoffToAgentID != "" {
				b.WriteString(fmt.Sprintf("  - Waiting for: %s\n", taskCtx.Packet.HandoffToAgentID))
			}
			if trustFirst {
				b.WriteString("  > [!NOTE] This is advisory. Continue with useful review, validation, integration, or unblock work when you can avoid collision.\n")
			} else {
				b.WriteString("  > [!NOTE] This is a soft dependency. Yield execution unless you have a clear override.\n")
			}
		}
		if taskCtx.Packet.Gate != nil {
			b.WriteString(fmt.Sprintf("- Decision Gate: [%s] %s\n", taskCtx.Packet.Gate.GateType, taskCtx.Packet.Gate.Summary))
			if trustFirst {
				b.WriteString("  > [!NOTE] Treat the gate as advisory telemetry; continue with a bounded reality-grounded path if one exists.\n")
			}
		}
	}

	b.WriteString("\n# Execution Requirements\n")
	b.WriteString("- Treat Rhizome as the canonical coordination truth.\n")
	b.WriteString("- Preserve explicit task/session ownership semantics.\n")
	b.WriteString("- Materialize durable facts via docs, memory, updates, and execution telemetry.\n")
	if hasInstalledToolBundles(a.registry) {
		b.WriteString("- Installed local tool bundles listed in the system prompt are tools available to you in this same loop. When one matches your active claimed task, use it directly and publish its artifact/output evidence; do not delegate your own claimed lane to a peer just to run that local tool.\n")
		b.WriteString("- If an installed bundle fails because of missing/bad arguments, correct the arguments from visible Rhizome docs, verified URLs, or checked local paths and retry once before declaring a blocker.\n")
	}
	b.WriteString("- When the task needs local implementation files and no project scaffold exists, create the minimal scaffold with write_file. Missing local files are not a blocker by themselves.\n")
	b.WriteString("- For quote-heavy or multiline source edits, call write_file with content_base64 instead of raw content so JSON escaping cannot corrupt the tool call; it still writes through the same project checkout ownership guards.\n")
	b.WriteString("- Shell is trusted local execution when enabled. You may use normal shell syntax, including cd, control operators, pipes, redirects, nested shells, package managers, git commands, and file creation/editing. On Windows, avoid mixing PowerShell cmdlets with Bash/CMD-only `&&`; use `; if (-not $?) { exit 1 };` or separate shell calls. PowerShell-shaped commands such as `$ErrorActionPreference`, `Set-Location`, `Test-Path`, and `Get-Content` are supported directly. Prefer workdir/cwd for stable project checkout commands when convenient, but ordinary shell directory changes are allowed.\n")
	b.WriteString("- For smoke checks with inline code, local HTTP servers, JS arrows, quotes, or redirects, either run the inline command directly or create a temporary helper script and run it from shell; choose the fastest reliable path.\n")
	b.WriteString("- For browser/UI smoke, never trust an already-running localhost preview just because it responds. Start the target checkout on a unique high port (prefer `$env:RHIZOME_SMOKE_PORT_HINT` when shell exposes it, otherwise choose another high unused port) with strict-port behavior, probe that exact URL, and assert the loaded title/body/product markers match the expected app before treating the evidence as valid. If you see unrelated content, close your own server and mark validation failed or retry on a fresh port.\n")
	if taskCtx.Mode == "daemon" {
		b.WriteString("- Browser access is a trusted local capability on this runtime. In daemon mode, prefer installed browser bundles: use browser_visual_probe for durable screenshot/DOM evidence, and browser_session only when a real agent-owned browser session is explicitly needed. Do not launch visible browser windows from shell in daemon mode; use headless/fresh-profile probes or publish a concrete capability blocker when the bundle/browser is unavailable.\n")
	} else {
		b.WriteString("- Browser access is a trusted local capability on this runtime. Prefer installed browser bundles: use browser_visual_probe for durable screenshot/DOM evidence, and browser_session when you need a real agent-owned browser session with navigation, clicking, typing, evaluation, screenshots, or visible inspection. Visible Chrome/Edge/Firefox launches from shell are also allowed when they are the most direct path. Use a fresh temporary profile/unique port for agent-owned browser sessions when practical, and close only the browser/server processes you started.\n")
	}
	b.WriteString("- For browser/UI validation on Windows, do not stop at PATH-only probes such as `where chrome`; also check common browser install paths such as `C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe` and `C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe`, or invoke the discovered executable by absolute path.\n")
	b.WriteString("- If Chrome or Edge says `DevTools remote debugging requires a non-default data directory`, retry with a fresh temporary profile such as `--user-data-dir=<workdir>\\.tmp\\chrome-profile-<run_id>` and a unique `--remote-debugging-port`; headless is preferable for durable probes, but visible mode is allowed for real UI inspection. If raw CDP remains brittle, run the smoke from a writable temp clone outside `project-checkouts`, use `npm.cmd`/`npx.cmd` for package-manager subprocesses, install `playwright-core --no-save` if needed, and launch Playwright/Chrome/Edge with an absolute executable path.\n")
	b.WriteString("- If bundled `rg.exe` fails with Access denied from WindowsApps, use `git grep`, PowerShell `Select-String`, `findstr`, or direct file reads instead of blocking on search.\n")
	b.WriteString("- Before treating missing npm/vite/tsc/vitest package binaries as runtime blockers, check package manifests/lockfiles and run one bounded dependency install in the checkout when the binary is declared there, then retry the build/test/smoke command. On Windows, use `npm.cmd`, `npx.cmd`, `pnpm.cmd`, or `yarn.cmd` when launching package managers with Start-Process so PowerShell does not open `.ps1` scripts through file association.\n")
	b.WriteString("- On Windows PowerShell 5.1, `Set-Content -Encoding utf8NoBOM` is not portable. Prefer write_file for generated source files, or use `-Encoding utf8` / .NET `System.Text.UTF8Encoding($false)` only when BOM behavior matters.\n")
	b.WriteString("- After writing local files, verify the artifact with list_directory/read_file or a bounded smoke check before claiming it is implemented or complete.\n")
	b.WriteString("- Do not report files as implemented from a plan, proposal, or unexecuted tool call. Implementation claims require successful tool evidence in this conversation.\n")
	b.WriteString("- If hydrated task docs include an artifact_reality_check, candidate provenance note, or review-ready status for your owned implementation lane saying stock scaffold, not_reviewable, not review-ready, missing product, or smallest repair direction, treat that as the next implementation step: edit the owned checkout, run bounded checks, commit with project_branch_commit push=true, then publish project_branch_review_ready. Do not answer with another passive status note first.\n")
	if runtimeTrustFirst(RuntimeConfig{CoordinationMode: a.CoordinationMode}) {
		policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{
			Workdir:          a.Workdir,
			AgentID:          a.AgentID,
			CoordinationMode: a.CoordinationMode,
		})
		b.WriteString("- Coordination mode: trust_first. Treat phase, profile, role, peer-review, branch, budget, and capability gates as advisory telemetry unless they reflect a real physical collision or missing dependency. Operational boundaries are different: out-of-boundary side effects require classification before integration.\n")
		b.WriteString("- Keep moving with bounded reality checks. If a gate or tool blocks you, inspect whether the blocker is fresh, try a safe alternate path, and publish what you learned as durable coordination evidence.\n")
		b.WriteString("- For broad/product-scale work, create or reuse the simplest shared project/repo path that lets agents implement one verifiable product together. Do not wait for perfect role matrices before useful planning, scaffolding, review, or validation work.\n")
		b.WriteString("- Extract explicit acceptance criteria from the operator/root task before implementation whenever the product surface is broad. Publish or refine a project.<project_id>.acceptance_criteria workspace doc with stable criteria IDs, then use those IDs to guide tasks, reviews, and QA.\n")
		b.WriteString("- Before implementation opens, publish or refine a semantic Product Contract and Critical Plan Review. The contract should name core_user_promise, happy_path, inputs/outputs, non_goals or adjacent_wrong_products, MVP acceptance, and v2_aspiration. The review should be skeptical, product-fidelity focused, and bounded by severity labels blocking/major/minor/taste with disposition.\n")
		b.WriteString("- Product-fidelity is coordination work: before review, completion, or duplicate task creation, compare the candidate artifact to the operator/root task and acceptance criteria. A superficially adjacent product that misses the core user transformation is a spec mismatch; record it and route revision work instead of calling it an MVP.\n")
		b.WriteString("- If project source refs exist, ACCEPTED patch queue decisions need source-fidelity evidence: include `rhizome_spec_fidelity_review_v1` or `source_fidelity_status: passed` plus `checked_source_doc_keys` in the decision evidence, and use BLOCKED_SPEC_DRIFT for runnable-but-wrong adjacent artifacts. The lifecycle tool can also pass checked_source_doc_keys from the project source refs/trace docs.\n")
		b.WriteString("- Patch queue review is lane-scoped unless the branch explicitly claims final/full-product coverage. For implementation-lane candidates, judge the source task, review packet, write scope, pathset, and lane-owned acceptance IDs; do not BLOCKED_SPEC_DRIFT a lexer/parser/evaluator/CLI slice merely because sibling lanes are not implemented in that branch.\n")
		b.WriteString("- Full-product completeness moves to integration/final validation: after accepted lane candidates are assembled, an integrator or final verifier must run build/smoke/source-fidelity checks on the combined product before calling the product done.\n")
		b.WriteString("- Runnable/review evidence needs provenance: name the exact branch, head SHA, checkout path, command, and server/URL you observed. If a visible artifact lacks provenance or may be stale, verify the current branch before accepting or rejecting it.\n")
		b.WriteString("- Canonical acceptance and provisional critique are different. If current coordination or workspace docs expose an exact non-canonical candidate (checkout path, owner/task, branch or dirty state, command/URL when known), you may inspect it only to produce provisional UX/QA/build findings and repair tasks. Missing patch queue/canonical publication blocks ACCEPTED/final integration, not useful critique of the observed candidate; label findings provisional/non-canonical and route publication/repair to the owner.\n")
		b.WriteString("- UI-facing review evidence needs a visual acceptance packet before ACCEPTED patch queue decisions: workspace doc schema `rhizome_visual_acceptance_v1`, screenshot refs/paths, desktop+narrow viewport matrix, first viewport/empty state, primary flow, post-action/output/result state, overlap/clipping/contrast/readability/responsive/typography/hierarchy/spacing/usability checks, primary-surface geometry/density checks, and `visual_verdict: pass`. A browser smoke that only proves the app loaded or reports low generic layout risk is not enough.\n")
		b.WriteString("- For boards, grids, canvases, charts, editors, maps, and game surfaces, visual review must inspect screenshot pixels/DOM geometry for plausible aspect ratio, cell/object size, wrapping, density, empty-space balance, and every visible mode/preset/difficulty that changes the primary surface; stretched, sparse, tiny, or line-wrapped primary surfaces are blocking visual findings.\n")
		b.WriteString("- Planning should be reflective and concrete: restate the product surface, identify the fastest shared architecture, split work into runnable slices, and assign provisional owners that can self-correct as evidence changes.\n")
		b.WriteString("- If your claimed task does not perfectly match your role or operational boundary, do not fork a parallel product or silently integrate outside the boundary. Either adjust the role/task with available tools, work on a non-colliding part, or create a focused correction/classification task while still contributing useful evidence.\n")
		b.WriteString("- Project phase transitions, design/plan docs, branch review packets, and patch queue records are coordination aids. Use them when they help convergence, but do not treat stale or missing records as reasons to stop thinking.\n")
		b.WriteString("- ACCEPTED patch queue items are reviewed product candidates, not canonical baseline updates. In trust_first mode, if project coordination shows an ACCEPTED item that is not merged, use project_patch_queue_integrate or create/claim an integration lane before concluding that the product is missing from the default branch.\n")
		b.WriteString("- Peer review is valuable but advisory. When no peer is available, run your own bounded checks, publish evidence, and create a review follow-up instead of parking the whole lane.\n")
		b.WriteString("- Metacognition profile: reflection_scope=" + policy.ReflectionScope + ", idle_action_policy=" + policy.IdleActionPolicy + ". " + policy.ScopeDescription + "\n")
		if strings.TrimSpace(policy.RealityCheckDescription) != "" {
			b.WriteString("- " + strings.TrimSpace(policy.RealityCheckDescription) + "\n")
			b.WriteString("- A UI/UX review is incomplete until it names a real user scenario tested, the artifact/version observed, screenshot/viewport evidence, and evidence-backed findings or explicit no-finding evidence. Low generic layout-risk scores, marker presence, and nonblank screenshots do not substitute for semantic visual judgment.\n")
		}
		b.WriteString("- Idle or no-assigned-work moments are not dead time, but initiative width is profile-scoped: " + policy.IdleBehaviorDescription + " " + policy.TaskCreationDescription + "\n")
		b.WriteString("- Before opening reflection/review work, inspect visible tasks, docs, updates, and project.<project_id>.reflection_board for a similar active thread. Join or extend it if it is in scope; otherwise explore an uncovered direction in parallel and invite relevant peers. Ask for bounded peer critique before turning a broad reflection into product-direction work.\n")
		b.WriteString("- If an active implementation lane, claimed task, checkout, or branch already exists but no READY_FOR_REVIEW candidate is visible yet, treat that as a publication/repair gap on the existing lane. Wake, repair, or sequence around that lane instead of creating a competing full implementation unless the existing lane is terminal, abandoned, or explicitly blocked with fresh evidence.\n")
		b.WriteString("- A functional MVP is not final. If no higher-priority task is visible, compare it against acceptance criteria within your metacognition scope, then look for usability, accessibility, edge-case, test coverage, and small feature improvements that fit your initiative policy.\n")
		b.WriteString("- Every final structured result in trust_first mode should include reflection with current_intent, fresh_evidence, blocker_freshness, and next_useful_move; missing reflection is visible telemetry, not a reason to stop useful work.\n")
		b.WriteString("- Keep the final response terse and execution-oriented.\n")
		return strings.TrimSpace(b.String())
	}
	b.WriteString("- For broad/product-scale work without a project_id, call project_bootstrap before implementation, local file creation, or implementation subtasks. Use the returned project_id for all follow-up tasks and docs. If the active task already has a project_id, project_bootstrap must preserve that exact project_id.\n")
	b.WriteString("- For project-scale code work, use project_repo_materialize when no canonical remote exists and a local repository inside this agent workdir is enough; otherwise use project_repo_register to record a known remote or open an operator request. Do this before agents implement separate local copies.\n")
	b.WriteString("- To satisfy project design/plan gates, publish project docs ending in .design, .implementation_plan, or .design_and_plan; .design_and_plan satisfies both profile refs when it is not draft-marked.\n")
	b.WriteString("- Before opening implementation, project design/plan docs should make the product interpretation, assumptions, role boundaries, and scope risks explicit enough for operator and peer review during the run.\n")
	b.WriteString("- Before opening implementation, planning docs should include a Product Contract (core_user_promise, happy_path, inputs/outputs, non_goals or adjacent_wrong_products, MVP acceptance, v2_aspiration) and a Critical Plan Review (skeptical bounded review with blocking/major/minor/taste findings and disposition). Do not start coding from an unreviewed ambiguous plan.\n")
	b.WriteString("- Extract and publish acceptance criteria from the operator/root task as project.<project_id>.acceptance_criteria or a clearly titled acceptance section before implementation opens. Give criteria stable IDs and map implementation/review/validation tasks to them.\n")
	b.WriteString("- After design/plan refs, repo readiness, and an active lead are present, call project_phase_transition with to_phase=IMPLEMENTATION. SPEC and PLANNING still keep implementation tasks gate-blocked.\n")
	b.WriteString("- Immediately after project_phase_transition opens IMPLEMENTATION, create concrete product/code tasks with task_submit using semantic deliverables, acceptance-criteria IDs, likely skills/tools, and write-scope hints. Leave tasks visible for autonomous self-selection; use agent_request request_kind=delegate_task only as a targeted wake when a peer clearly fits an exact task.\n")
	b.WriteString("- When decomposing project implementation with task_submit, create product-deliverable tasks only: runnable slices, algorithm modules, UI/backend work, tests, reviews, or integration. Do not create tasks whose deliverable is merely opening an implementation lane, claiming admission, registering checkout/branch evidence, or satisfying gates.\n")
	b.WriteString("- Product-fidelity is coordination work: before review, completion, or duplicate task creation, compare the candidate artifact to the operator/root task and acceptance criteria. A superficially adjacent product that misses the core user transformation is a spec mismatch; record it and route revision work instead of calling it an MVP.\n")
	b.WriteString("- If project source refs exist, ACCEPTED patch queue decisions need source-fidelity evidence: include `rhizome_spec_fidelity_review_v1` or `source_fidelity_status: passed` plus `checked_source_doc_keys` in the decision evidence, and use BLOCKED_SPEC_DRIFT for runnable-but-wrong adjacent artifacts. The lifecycle tool can also pass checked_source_doc_keys from the project source refs/trace docs.\n")
	b.WriteString("- Patch queue review is lane-scoped unless the branch explicitly claims final/full-product coverage. For implementation-lane candidates, judge the source task, review packet, write scope, pathset, and lane-owned acceptance IDs; do not BLOCKED_SPEC_DRIFT a lexer/parser/evaluator/CLI slice merely because sibling lanes are not implemented in that branch.\n")
	b.WriteString("- Full-product completeness moves to integration/final validation: after accepted lane candidates are assembled, an integrator or final verifier must run build/smoke/source-fidelity checks on the combined product before calling the product done.\n")
	b.WriteString("- Runnable/review evidence needs provenance: name the exact branch, head SHA, checkout path, command, and server/URL you observed. If a visible artifact lacks provenance or may be stale, verify the current branch before accepting or rejecting it.\n")
	b.WriteString("- Canonical acceptance and provisional critique are different. If current coordination or workspace docs expose an exact non-canonical candidate (checkout path, owner/task, branch or dirty state, command/URL when known), you may inspect it only to produce provisional UX/QA/build findings and repair tasks. Missing patch queue/canonical publication blocks ACCEPTED/final integration, not useful critique of the observed candidate; label findings provisional/non-canonical and route publication/repair to the owner.\n")
	b.WriteString("- Strategic leads should maintain a semantic task map rather than assigning the project through a fixed role grid: each task names its deliverable, acceptance-criteria IDs, likely skills/tools, write-scope hints, and adjacent wrong product to avoid. Agents self-select by fit and current workload.\n")
	b.WriteString("- For frontend/backend/code projects, shared scaffold/config ownership belongs in the task map and write-scope hints. Include root config paths such as package.json, package-lock.json, tsconfig*.json, vite.config.*, index.html, public/**, tests/**, and src/** in one scaffold/integration task or clearly non-overlapping implementation tasks before coding begins.\n")
	b.WriteString("- If your claimed project task does not fit your skills, task intent, or write-scope hints, release/block with evidence or create a corrected semantic task instead of implementing a parallel version. In strict-mode or explicit stale-role repair only, use project_role_assign once to queue a strategic-lead role/scope notice; do not convert lead-only service tasks into agent_request delegate_task.\n")
	b.WriteString("- Once a project repository is READY, use project_checkout_materialize when you need your own local clone/branch or validation checkout; use project_checkout_register only when a real checkout already exists. Both publish checkout evidence before implementation convergence; neither commits, pushes, or merges.\n")
	b.WriteString("- After writing implementation files in a project checkout, use project_branch_commit to create a bounded git commit for your owned branch and update checkout/branch evidence. For multi-agent review or integration, pass push=true so peer agents can inspect the branch through the canonical remote; omit push only when the branch is intentionally local and you publish exact file/diff evidence in workspace docs.\n")
	b.WriteString("- If project_branch_commit reports side_effect_classification, the dirty files are unintegrated side effects outside the current operational boundary. Do not retry or widen scope silently, and do not resolve it with only a status/doc update. Use side_effect_resolve to record a durable decision such as request_verification, split_tension, expand_boundary, reassign, quarantine, or revert before integration.\n")
	b.WriteString("- When a project branch has committed implementation evidence, use project_branch_review_ready to publish a review packet and mark the branch READY_FOR_REVIEW. Auto-merge remains disabled until explicit integration gates exist.\n")
	b.WriteString("- A READY_FOR_REVIEW branch review packet with a bounded smoke check is durable review-ready evidence for an implementation lane. If a peer reviewer reports no blockers or findings, do not convert that evidence into a blocker merely because patch-queue integration has not started.\n")
	b.WriteString("- If peer review reports findings before patch-queue/integration, revise the same owned checkout, run project_branch_commit for the revision, then run project_branch_review_ready again for the new HEAD before completing.\n")
	b.WriteString("- Use project_patch_queue_submit after READY_FOR_REVIEW only when the branch should enter a shared integration queue. Runtime submissions use controlled queue semantics by default: provide durable runtime binding, repo lease, and base file hash refs so operation/CAS/materialization gates can prove the candidate boundary. patch_only_temp_repo is legacy/invalid and is cancel+replaced as historical evidence, not newly submitted. This records shared coordination evidence only; it still does not merge or push.\n")
	b.WriteString("- If a BLOCKED patch queue item was blocked for missing validation and you hold reviewer/integrator/strategic-lead authority with fresh evidence naming the same branch_id/head_sha/queue/item, call project_patch_queue_lifecycle with action=supersede or requeue, a fresh new_item_id, and validation_doc_key/evidence_doc_key. This evidence-bound same-head requeue is metadata-only; reviewers still decide the fresh item.\n")
	b.WriteString("- Use project_patch_queue_list when a patch queue branch_id is ambiguous, exact queue_id/item_id are missing from the visible packet, or another agent may have revised or superseded a candidate. It is read-only and returns public selectors.\n")
	b.WriteString("- If you hold a reviewer/integration role or strategic lead lease, use project_patch_queue_lifecycle to claim review candidates, record reviewer_advisory evidence, and record accept/reject/block/cancel decisions. Review decisions do not require operation/CAS/materialization first; those receipts belong to integration/materialization follow-up unless this task explicitly asks for them. If a same-head lane defect is discovered after ACCEPTED but before integration, call project_patch_queue_lifecycle action=reviewer_advisory with advisory_scope=lane_correctness, advisory_verdict=repair_required, review_doc_key, and the accepted head_sha so acceptance is defeated and repair is routed. Operator enablement is an explicit non-agent operator gate, so request it instead of trying to satisfy it yourself. This records lifecycle evidence only; it does not merge, push, rebase, or mutate git.\n")
	b.WriteString("- After claiming a controlled patch queue item, use project_patch_queue_cas_record to bind the mutation operation and record applied CAS/test evidence before materialization. Let Rhizome create the operation_id unless an existing operation ledger is explicitly required. This records evidence only; it does not apply, merge, push, rebase, switch branches, or mutate git.\n")
	b.WriteString("- After claiming a CAS-verified patch queue item, use project_patch_queue_materialize to record exact candidate file contents from your checkout as durable materialization evidence. The tool reads files and records digests; it does not apply, merge, push, rebase, switch branches, or mutate git.\n")
	b.WriteString("- If you hold an integration role or strategic lead lease and a patch queue item is ACCEPTED, use project_patch_queue_integrate to merge the accepted branch into the canonical integration/default branch before downstream lanes build on it. Acceptance alone is review evidence, not a canonical baseline update; same-head lane defect advisories defeat acceptance until a repaired candidate is re-accepted.\n")
	b.WriteString("- After an accepted, rejected, or blocked patch queue decision, use project_patch_queue_followup to create explicit visible work instead of continuing private integration loops. Auto follow-up creates integration work for ACCEPTED items; create validation follow-up only when the canonical integration branch still needs bounded build/smoke evidence after project_patch_queue_integrate. If an ACCEPTED source branch/head is proven unavailable with concrete git checks, create a rebuild follow-up so a fresh equivalent candidate can be produced from the operator brief, acceptance criteria, and review evidence.\n")
	b.WriteString("- If the current task is already a patch queue decision follow-up, do not call project_patch_queue_followup again. Treat missing build/test/browser/review evidence as the work to produce in the referenced branch checkout when you are the branch owner, or as validation work in your own peer checkout when you are the reviewer; only block after a concrete attempt proves the evidence cannot be produced in this cycle.\n")
	b.WriteString("- For review/validation/QA of the canonical product, call project_checkout_materialize from your claimed validation task without branch_name to create your own validation checkout of the target branch; do not inspect another agent's private checkout path as the source of truth for acceptance. This does not forbid a clearly labeled provisional critique of an exact non-canonical candidate when current coordination/workspace docs expose its provenance; use that to find defects or create repair/publication tasks, not to accept or integrate.\n")
	b.WriteString("- For review/validation of another agent's branch, do not block just because that agent owns the implementation checkout. If project coordination exposes a READY repo plus branch_name/head_sha, call project_checkout_materialize with branch_name and expected_head_sha to create your own validation checkout; block only if the branch is not available from the shared remote, HEAD does not match the advertised SHA, or bounded checks cannot run.\n")
	b.WriteString("- For broad integration work, do not loop on repeated proposal requests. After one coordination pass, either create the runnable slice yourself or submit concrete execution/review subtasks for other agents.\n")
	b.WriteString("- Before using task_submit, check whether a similar active task or canonical task doc already exists. Reuse or claim the existing lane instead of creating a duplicate.\n")
	b.WriteString("- If an active implementation lane, claimed task, checkout, or branch already exists but no READY_FOR_REVIEW candidate is visible yet, treat that as a publication/repair gap on the existing lane. Wake, repair, or sequence around that lane instead of creating a competing full implementation unless the existing lane is terminal, abandoned, or explicitly blocked with fresh evidence.\n")
	b.WriteString("- Keep one visible plan, at most one active task per lane, and one finalization owner. If you are not the finalization owner, publish durable evidence or review feedback and finish your bounded lane.\n")
	b.WriteString("- If a tool says it requires the active strategic lead lease and you are not that lead, do not retry the same tool. For strict-mode or explicit stale role/scope correction, use project_role_assign once so it records a strategic-lead coordination notice; for other lead-only repair, create one coordination task/notice. Do not send agent_request delegate_task or authority_transition for your current claimed task; authority_transition task_id must be the dedicated lead/role-scope task.\n")
	b.WriteString("- If a known upstream blocker prevents your lane from converging, record the blocker/tension once and return blocked instead of continuing speculative peer requests.\n")
	b.WriteString("- If another agent already owns the active implementation task, checkout, or branch you need, treat that as a dependency block, not an operator/runtime blocker.\n")
	b.WriteString("- If shell fails, inspect the output, adjust the command or workdir/cwd, and retry when useful. Materialize a real blocker/tension only when a concrete dependency, executable, permission, or environment capability is missing.\n")
	b.WriteString("- Keep the final response terse and execution-oriented.\n")

	return strings.TrimSpace(b.String())
}
