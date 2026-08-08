package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	goruntime "runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	daemonCapabilitySnapshotSchema                 = "daemon_capability_snapshot.v1"
	daemonPromptCapabilityEvidenceContract         = "daemon_prompt_capability_evidence.v1"
	daemonPromptCompilerStatusConverged            = "daemon_converged"
	daemonPromptCompilerConvergenceAccepted        = "daemon_prompt_compiler_converged"
	daemonPromptCompilerDeploymentEvidenceAccepted = "accepted_for_daemon_prompt_compiler_convergence"
	daemonPromptCapabilityProjectionSource         = "agent.runtime_capability_snapshot"
	daemonPromptCapabilityProjectionContract       = "active_capability_snapshot_projection.v1"
	daemonPromptCapabilityPromptContract           = "prompt_capabilities.v1"
)

var capabilitySnapshotBinaryFingerprint = struct {
	once sync.Once
	path string
	sha  string
}{}

type DaemonCapabilitySnapshot struct {
	Schema             string                       `json:"schema"`
	SnapshotID         string                       `json:"snapshot_id"`
	SnapshotKind       string                       `json:"snapshot_kind"`
	GeneratedAt        string                       `json:"generated_at"`
	Generator          CapabilitySnapshotGenerator  `json:"generator"`
	Identity           CapabilitySnapshotIdentity   `json:"identity"`
	Authority          CapabilitySnapshotAuthority  `json:"authority"`
	Binding            CapabilitySnapshotBinding    `json:"binding,omitempty"`
	Status             CapabilitySnapshotStatus     `json:"status"`
	Surfaces           map[string]CapabilitySurface `json:"surfaces"`
	PromptContract     CapabilityPromptContract     `json:"prompt_contract"`
	SourceFingerprints map[string]string            `json:"source_fingerprints,omitempty"`
	Notes              []string                     `json:"notes,omitempty"`
}

type CapabilitySnapshotGenerator struct {
	Component       string `json:"component"`
	RuntimeMode     string `json:"runtime_mode"`
	BinaryPath      string `json:"binary_path,omitempty"`
	BinarySHA256    string `json:"binary_sha256,omitempty"`
	ProtocolVersion string `json:"protocol_version"`
	BuildVersion    string `json:"build_version,omitempty"`
}

type CapabilitySnapshotIdentity struct {
	PrincipalType             string   `json:"principal_type"`
	PrincipalID               string   `json:"principal_id"`
	WorkspaceID               string   `json:"workspace_id"`
	AgentID                   string   `json:"agent_id"`
	OwnerUserID               string   `json:"owner_user_id,omitempty"`
	DisplayName               string   `json:"display_name,omitempty"`
	Role                      string   `json:"role,omitempty"`
	AgentCapabilitiesDeclared []string `json:"agent_capabilities_declared,omitempty"`
	IdentitySource            string   `json:"identity_source"`
	IdentityBindingHash       string   `json:"identity_binding_hash"`
}

type CapabilitySnapshotAuthority struct {
	AuthorityScope string `json:"authority_scope"`
	AuthorityTerm  string `json:"authority_term,omitempty"`
	ClaimTerm      string `json:"claim_term,omitempty"`
	PolicyEpoch    string `json:"policy_epoch,omitempty"`
	Source         string `json:"source"`
}

type CapabilitySnapshotBinding struct {
	TaskID               string `json:"task_id,omitempty"`
	SessionID            string `json:"session_id,omitempty"`
	RunID                string `json:"run_id,omitempty"`
	WorkPacketID         string `json:"work_packet_id,omitempty"`
	BootstrapSnapshotRef string `json:"bootstrap_snapshot_ref,omitempty"`
}

type CapabilitySnapshotStatus struct {
	Overall             string   `json:"overall"`
	DisabledReasonCodes []string `json:"disabled_reason_codes,omitempty"`
	DegradedReasonCodes []string `json:"degraded_reason_codes,omitempty"`
}

type CapabilitySurface struct {
	SurfaceID                string                     `json:"surface_id"`
	SurfaceKind              string                     `json:"surface_kind"`
	Status                   string                     `json:"status"`
	AutonomyState            string                     `json:"autonomy_state,omitempty"`
	CapabilityTier           string                     `json:"capability_tier"`
	PromptVisible            bool                       `json:"prompt_visible"`
	ToolVisible              bool                       `json:"tool_visible"`
	MutationAllowed          bool                       `json:"mutation_allowed"`
	RequiresPolicyAllow      bool                       `json:"requires_policy_allow"`
	RequiresOperatorApproval bool                       `json:"requires_operator_approval"`
	DisabledReasons          []CapabilityDisabledReason `json:"disabled_reasons,omitempty"`
	IdentityBinding          CapabilitySurfaceIdentity  `json:"identity_binding"`
	Policy                   CapabilitySurfacePolicy    `json:"policy"`
	Limits                   CapabilitySurfaceLimits    `json:"limits"`
	Evidence                 CapabilitySurfaceEvidence  `json:"evidence"`
	Metadata                 map[string]any             `json:"metadata,omitempty"`
}

type CapabilityDisabledReason struct {
	Code        string `json:"code"`
	Severity    string `json:"severity"`
	Message     string `json:"message"`
	Source      string `json:"source"`
	Recoverable bool   `json:"recoverable"`
}

type CapabilitySurfaceIdentity struct {
	WorkspaceID      string `json:"workspace_id"`
	AgentID          string `json:"agent_id"`
	PrincipalType    string `json:"principal_type"`
	PrincipalID      string `json:"principal_id"`
	OwnerUserID      string `json:"owner_user_id,omitempty"`
	ToolOwnerAgentID string `json:"tool_owner_agent_id,omitempty"`
	RegisteredBy     string `json:"registered_by,omitempty"`
}

type CapabilitySurfacePolicy struct {
	RequestedCapability string `json:"requested_capability,omitempty"`
	PolicyVerdict       string `json:"policy_verdict"`
	PolicyID            string `json:"policy_id,omitempty"`
	PolicyEpoch         string `json:"policy_epoch,omitempty"`
	PolicyEnvelope      string `json:"policy_envelope,omitempty"`
}

type CapabilitySurfaceLimits struct {
	TimeoutSec       int `json:"timeout_sec,omitempty"`
	MaxOutputBytes   int `json:"max_output_bytes,omitempty"`
	MaxCallsPerCycle int `json:"max_calls_per_cycle,omitempty"`
	RetryCeiling     int `json:"retry_ceiling,omitempty"`
}

type CapabilitySurfaceEvidence struct {
	ProbeStatus string `json:"probe_status"`
	ProbeError  string `json:"probe_error,omitempty"`
	SourceRef   string `json:"source_ref,omitempty"`
}

type CapabilityPromptContract struct {
	ContractID             string                       `json:"contract_id"`
	SnapshotID             string                       `json:"snapshot_id"`
	MustInclude            []string                     `json:"must_include"`
	EnabledToolNames       []string                     `json:"enabled_tool_names,omitempty"`
	DisabledToolNames      []CapabilityDisabledToolName `json:"disabled_tool_names,omitempty"`
	InspectionOnlySurfaces []string                     `json:"inspection_only_surfaces,omitempty"`
	BudgetSummary          CapabilityBudgetSummary      `json:"budget_summary"`
}

type CapabilityDisabledToolName struct {
	Name       string `json:"name"`
	ReasonCode string `json:"reason_code"`
}

type CapabilityBudgetSummary struct {
	MaxToolIterations   int `json:"max_tool_iterations"`
	MaxShellTimeoutSec  int `json:"max_shell_timeout_sec"`
	MaxPromptDocChars   int `json:"max_prompt_doc_chars"`
	MaxPromptSpecChars  int `json:"max_prompt_spec_chars"`
	MaxSmokeCyclesAgent int `json:"max_smoke_cycles_per_agent"`
	MaxSmokeCyclesTask  int `json:"max_smoke_cycles_per_task"`
}

func (r *Runtime) captureStartupCapabilitySnapshot() (DaemonCapabilitySnapshot, string, error) {
	snapshot := buildDaemonBootCapabilitySnapshot(r.cfg, r.agent, time.Now().UTC())
	path, err := r.persistCapabilitySnapshot(snapshot)
	if err != nil {
		return DaemonCapabilitySnapshot{}, "", err
	}
	r.mu.Lock()
	r.startupCapabilitySnapshotID = snapshot.SnapshotID
	r.startupCapabilitySnapshotPath = path
	r.scratch.CapabilityBootSnapshotID = snapshot.SnapshotID
	r.scratch.CapabilityBootSnapshotPath = path
	r.scratch.ActiveCapabilitySnapshotID = snapshot.SnapshotID
	r.scratch.ActiveCapabilitySnapshotPath = path
	r.mu.Unlock()
	return snapshot, path, nil
}

func (r *Runtime) captureRunCapabilitySnapshot(task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, work *AgentWorkPacket) (DaemonCapabilitySnapshot, string, error) {
	r.mu.Lock()
	bootID := r.startupCapabilitySnapshotID
	r.mu.Unlock()
	snapshot := buildDaemonRunCapabilitySnapshot(r.cfg, r.agent, bootID, task, session, runID, work, time.Now().UTC())
	if strings.EqualFold(snapshot.Status.Overall, "blocked") {
		return DaemonCapabilitySnapshot{}, "", fmt.Errorf("capability snapshot blocked for run %s: %s", strings.TrimSpace(runID), strings.Join(snapshot.Status.DisabledReasonCodes, ","))
	}
	path, err := r.persistCapabilitySnapshot(snapshot)
	if err != nil {
		return DaemonCapabilitySnapshot{}, "", err
	}
	r.mu.Lock()
	r.activeCapabilitySnapshotID = snapshot.SnapshotID
	r.activeCapabilitySnapshotPath = path
	r.scratch.ActiveCapabilitySnapshotID = snapshot.SnapshotID
	r.scratch.ActiveCapabilitySnapshotPath = path
	r.mu.Unlock()
	return snapshot, path, nil
}

func (r *Runtime) persistCapabilitySnapshot(snapshot DaemonCapabilitySnapshot) (string, error) {
	if r == nil || strings.TrimSpace(snapshot.SnapshotID) == "" || strings.TrimSpace(r.cfg.Workdir) == "" {
		return "", nil
	}
	path := filepath.Join(r.cfg.Workdir, ".rhizome", "capability_snapshots", snapshot.SnapshotID+".json")
	raw, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode capability snapshot: %w", err)
	}
	if err := atomicWriteFile(path, append(raw, '\n'), 0o600); err != nil {
		return "", fmt.Errorf("persist capability snapshot %s: %w", snapshot.SnapshotID, err)
	}
	return path, nil
}

func (r *Runtime) activeCapabilitySnapshotPromptProjection(fallback DaemonCapabilitySnapshot) string {
	if snapshot, ok := r.activeCapabilitySnapshotFromScratch(); ok {
		return renderDaemonCapabilityPromptProjection(snapshot)
	}
	return renderDaemonCapabilityPromptProjection(fallback)
}

func (r *Runtime) activeCapabilitySnapshotFromScratch() (DaemonCapabilitySnapshot, bool) {
	if r == nil {
		return DaemonCapabilitySnapshot{}, false
	}
	r.mu.Lock()
	snapshotID := firstNonEmpty(strings.TrimSpace(r.scratch.ActiveCapabilitySnapshotID), strings.TrimSpace(r.activeCapabilitySnapshotID))
	path := firstNonEmpty(strings.TrimSpace(r.scratch.ActiveCapabilitySnapshotPath), strings.TrimSpace(r.activeCapabilitySnapshotPath))
	r.mu.Unlock()
	if path == "" {
		return DaemonCapabilitySnapshot{}, false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return DaemonCapabilitySnapshot{}, false
	}
	var snapshot DaemonCapabilitySnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return DaemonCapabilitySnapshot{}, false
	}
	if strings.TrimSpace(snapshot.SnapshotID) == "" {
		return DaemonCapabilitySnapshot{}, false
	}
	if snapshotID != "" && strings.TrimSpace(snapshot.SnapshotID) != snapshotID {
		return DaemonCapabilitySnapshot{}, false
	}
	return snapshot, true
}

func buildDaemonBootCapabilitySnapshot(cfg RuntimeConfig, agent *Agent, generatedAt time.Time) DaemonCapabilitySnapshot {
	cfg.ApplyDefaults()
	snapshotID := daemonBootCapabilitySnapshotID(cfg, generatedAt)
	return buildDaemonCapabilitySnapshot(cfg, agent, "boot", snapshotID, generatedAt, CapabilitySnapshotBinding{}, "")
}

func buildDaemonRunCapabilitySnapshot(cfg RuntimeConfig, agent *Agent, bootSnapshotID string, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, work *AgentWorkPacket, generatedAt time.Time) DaemonCapabilitySnapshot {
	cfg.ApplyDefaults()
	binding := CapabilitySnapshotBinding{
		TaskID:               strings.TrimSpace(task.TaskID),
		SessionID:            strings.TrimSpace(session.SessionID),
		RunID:                strings.TrimSpace(runID),
		WorkPacketID:         capabilityWorkPacketID(work),
		BootstrapSnapshotRef: strings.TrimSpace(bootSnapshotID),
	}
	snapshotID := daemonRunCapabilitySnapshotID(cfg, task, session, runID)
	return buildDaemonCapabilitySnapshot(cfg, agent, "run", snapshotID, generatedAt, binding, bootSnapshotID)
}

func buildDaemonCapabilitySnapshot(cfg RuntimeConfig, agent *Agent, kind, snapshotID string, generatedAt time.Time, binding CapabilitySnapshotBinding, bootSnapshotID string) DaemonCapabilitySnapshot {
	cfg.ApplyDefaults()
	registeredToolNames := capabilitySnapshotToolNames(agent)
	disabledTools := capabilityDisabledToolNames(cfg, registeredToolNames)
	toolNames := capabilityEnabledToolNames(registeredToolNames, disabledTools)
	status := capabilitySnapshotStatus(cfg, kind, binding)
	promptContract := CapabilityPromptContract{
		ContractID:             "prompt_capabilities.v1",
		SnapshotID:             snapshotID,
		MustInclude:            capabilityPromptMustInclude(),
		EnabledToolNames:       toolNames,
		DisabledToolNames:      disabledTools,
		InspectionOnlySurfaces: []string{"workspace_tools", "bridges", "ui"},
		BudgetSummary: CapabilityBudgetSummary{
			MaxToolIterations:   cfg.MaxToolLoopIterations,
			MaxShellTimeoutSec:  60,
			MaxPromptDocChars:   cfg.MaxPromptDocChars,
			MaxPromptSpecChars:  cfg.MaxPromptSpecChars,
			MaxSmokeCyclesAgent: cfg.MaxSmokeCyclesPerAgent,
			MaxSmokeCyclesTask:  cfg.MaxSmokeCyclesPerTask,
		},
	}
	return DaemonCapabilitySnapshot{
		Schema:       daemonCapabilitySnapshotSchema,
		SnapshotID:   snapshotID,
		SnapshotKind: strings.TrimSpace(kind),
		GeneratedAt:  generatedAt.UTC().Format(time.RFC3339Nano),
		Generator:    capabilitySnapshotGenerator(cfg),
		Identity: CapabilitySnapshotIdentity{
			PrincipalType:             "agent",
			PrincipalID:               strings.TrimSpace(cfg.AgentID),
			WorkspaceID:               strings.TrimSpace(cfg.WorkspaceID),
			AgentID:                   strings.TrimSpace(cfg.AgentID),
			OwnerUserID:               strings.TrimSpace(cfg.OwnerUserID),
			DisplayName:               strings.TrimSpace(cfg.DisplayName),
			Role:                      strings.TrimSpace(cfg.Role),
			AgentCapabilitiesDeclared: capabilitySnapshotDeclaredCapabilities(cfg),
			IdentitySource:            capabilityIdentitySource(cfg),
			IdentityBindingHash:       capabilityIdentityBindingHash(cfg, bootSnapshotID),
		},
		Authority: CapabilitySnapshotAuthority{
			AuthorityScope: "workspace",
			Source:         "local_agent_state",
		},
		Binding:        binding,
		Status:         status,
		Surfaces:       capabilitySurfaces(cfg, agent, toolNames),
		PromptContract: promptContract,
		SourceFingerprints: map[string]string{
			"config_hash":            "sha256:" + sha256Hex(strings.Join(capabilityConfigFingerprintParts(cfg), "|")),
			"tool_registry_hash":     "sha256:" + sha256Hex(strings.Join(toolNames, "|")),
			"raw_tool_registry_hash": "sha256:" + sha256Hex(strings.Join(registeredToolNames, "|")),
			"prompt_contract_hash":   "sha256:" + sha256Hex(strings.Join(capabilityPromptMustInclude(), "|")),
		},
		Notes: capabilitySnapshotNotes(cfg),
	}
}

func daemonBootCapabilitySnapshotID(cfg RuntimeConfig, generatedAt time.Time) string {
	cfg.ApplyDefaults()
	parts := []string{
		daemonCapabilitySnapshotSchema,
		"boot",
		strings.TrimSpace(cfg.WorkspaceID),
		strings.TrimSpace(cfg.AgentID),
		strings.TrimSpace(cfg.OwnerUserID),
		strings.TrimSpace(string(cfg.Mode)),
		strings.TrimSpace(cfg.ProtocolVersion),
		generatedAt.UTC().Format(time.RFC3339Nano),
	}
	return "cap_" + shortHash(strings.Join(parts, "|"))
}

func daemonRunCapabilitySnapshotID(cfg RuntimeConfig, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string) string {
	cfg.ApplyDefaults()
	parts := []string{
		daemonCapabilitySnapshotSchema,
		"run",
		strings.TrimSpace(cfg.WorkspaceID),
		strings.TrimSpace(cfg.AgentID),
		strings.TrimSpace(task.TaskID),
		strings.TrimSpace(session.SessionID),
		strings.TrimSpace(runID),
		strings.TrimSpace(cfg.ProtocolVersion),
		strings.TrimSpace(cfg.Model),
		strings.TrimSpace(cfg.LLMBackend),
		strings.Join(uniqueTrimmedCSVStrings(cfg.Capabilities), ","),
	}
	return "cap_" + shortHash(strings.Join(parts, "|"))
}

func capabilitySnapshotEvidenceRef(snapshotID string) string {
	if strings.TrimSpace(snapshotID) == "" {
		return ""
	}
	return "capability_snapshot:" + strings.TrimSpace(snapshotID)
}

func prependCapabilitySnapshotEvidence(snapshotID string, evidence []string) []string {
	ref := capabilitySnapshotEvidenceRef(snapshotID)
	out := make([]string, 0, len(evidence)+1)
	if ref != "" {
		out = append(out, ref)
	}
	for _, item := range evidence {
		if trimmed := strings.TrimSpace(item); trimmed != "" && trimmed != ref {
			out = append(out, trimmed)
		}
	}
	return out
}

func daemonPromptCapabilityEvidence(snapshot DaemonCapabilitySnapshot, snapshotPath string) map[string]any {
	projection := renderDaemonCapabilityPromptProjection(snapshot)
	return map[string]any{
		"contract":                 daemonPromptCapabilityEvidenceContract,
		"prompt_compiler_status":   daemonPromptCompilerStatusConverged,
		"c2_1_convergence":         daemonPromptCompilerConvergenceAccepted,
		"deployment_evidence":      daemonPromptCompilerDeploymentEvidenceAccepted,
		"capability_snapshot_id":   strings.TrimSpace(snapshot.SnapshotID),
		"capability_snapshot_ref":  capabilitySnapshotEvidenceRef(snapshot.SnapshotID),
		"capability_snapshot_path": strings.TrimSpace(snapshotPath),
		"projection_source":        daemonPromptCapabilityProjectionSource,
		"projection_contract":      daemonPromptCapabilityProjectionContract,
		"projection_digest":        daemonCapabilityProjectionDigest(projection),
		"snapshot_schema":          strings.TrimSpace(snapshot.Schema),
		"snapshot_kind":            strings.TrimSpace(snapshot.SnapshotKind),
		"snapshot_status":          strings.TrimSpace(snapshot.Status.Overall),
		"prompt_contract":          firstNonEmpty(strings.TrimSpace(snapshot.PromptContract.ContractID), daemonPromptCapabilityPromptContract),
	}
}

func capabilitySnapshotStatus(cfg RuntimeConfig, kind string, binding CapabilitySnapshotBinding) CapabilitySnapshotStatus {
	cfg.ApplyDefaults()
	reasons := []string{}
	if cfg.Mode != RuntimeModeDaemon {
		reasons = append(reasons, "runtime.mode_not_daemon")
	}
	if strings.TrimSpace(cfg.AgentID) == "" {
		reasons = append(reasons, "identity.missing_agent_id")
	}
	if strings.TrimSpace(cfg.WorkspaceID) == "" {
		reasons = append(reasons, "identity.missing_workspace_id")
	}
	if strings.TrimSpace(kind) == "run" {
		if strings.TrimSpace(binding.TaskID) == "" || strings.TrimSpace(binding.SessionID) == "" || strings.TrimSpace(binding.RunID) == "" {
			reasons = append(reasons, "authority.missing_claim_term")
		}
	}
	if len(reasons) > 0 {
		return CapabilitySnapshotStatus{Overall: "blocked", DisabledReasonCodes: uniqueTrimmedCSVStrings(reasons)}
	}
	return CapabilitySnapshotStatus{
		Overall: "enabled",
		DegradedReasonCodes: []string{
			"authority.missing_workspace_term",
			"policy.not_checked",
			"budget.unavailable",
		},
	}
}

func capabilitySurfaces(cfg RuntimeConfig, agent *Agent, toolNames []string) map[string]CapabilitySurface {
	cfg.ApplyDefaults()
	identity := CapabilitySurfaceIdentity{
		WorkspaceID:   strings.TrimSpace(cfg.WorkspaceID),
		AgentID:       strings.TrimSpace(cfg.AgentID),
		PrincipalType: "agent",
		PrincipalID:   strings.TrimSpace(cfg.AgentID),
		OwnerUserID:   strings.TrimSpace(cfg.OwnerUserID),
	}
	surfaces := map[string]CapabilitySurface{}
	surfaces["local_tools"] = capabilitySurface(identity, "local_tools", "local_tool", "enabled", "safe_local", true, true, runtimeAllowsLocalMutation(cfg), []CapabilityDisabledReason{}, map[string]any{
		"enabled_tool_names":  append([]string(nil), toolNames...),
		"disabled_tool_names": capabilityDisabledToolNames(cfg, toolNames),
	})
	if surface, ok := capabilityToolBundleSurface(identity, agent); ok {
		surfaces["tool_bundles"] = surface
	}
	workspaceToolStatus := "inspection_only"
	workspaceToolVisible := false
	workspaceToolReasons := []CapabilityDisabledReason{
		capabilityReason("policy.not_checked", "warning", "Workspace dynamic tools are not refreshed into daemon prompt-visible tools until policy epoch is represented.", "program_a_default", true),
	}
	if cfg.DaemonWorkspaceTools {
		workspaceToolStatus = "degraded"
		workspaceToolVisible = true
		workspaceToolReasons = []CapabilityDisabledReason{
			capabilityReason("deployment.opt_in_workspace_tools", "warning", "Daemon workspace tool discovery is enabled by explicit runtime opt-in and still depends on per-call policy and operation-ledger evidence.", "runtime_config", true),
		}
	}
	surfaces["workspace_tools"] = capabilitySurface(identity, "workspace_tools", "workspace_tool", workspaceToolStatus, "networked", true, workspaceToolVisible, false, workspaceToolReasons, nil)
	workspaceDocVisible := hasCapabilityTool(toolNames, "workspace_doc_put")
	workspaceDocMaterialize := strings.TrimSpace(cfg.WorkspaceID) != "" && strings.TrimSpace(cfg.AgentID) != ""
	workspaceDocStatus := "enabled"
	var workspaceDocReasons []CapabilityDisabledReason
	if !workspaceDocMaterialize {
		workspaceDocStatus = "disabled"
		workspaceDocReasons = []CapabilityDisabledReason{
			capabilityReason("workspace_doc.identity_missing", "blocker", "Workspace document materialization requires workspace and agent identity.", "runtime_config", true),
		}
	} else if !workspaceDocVisible {
		workspaceDocStatus = "degraded"
		workspaceDocReasons = []CapabilityDisabledReason{
			capabilityReason("workspace_doc.put_tool_not_visible", "warning", "Mid-cycle workspace_doc_put is unavailable; final structured materialize remains the daemon-safe doc write path.", "runtime_config", true),
		}
	}
	surfaces["workspace_docs"] = capabilitySurface(identity, "workspace_docs", "workspace_doc", workspaceDocStatus, "networked", true, workspaceDocVisible, workspaceDocMaterialize, workspaceDocReasons, map[string]any{
		"tool_name":             "workspace_doc_put",
		"tool_visible":          workspaceDocVisible,
		"materialization_path":  "structured_task_result.materialize",
		"local_file_mutation":   false,
		"canonical_write_plane": "rhizome_rpc",
	})
	surfaces["mcp"] = capabilitySurface(identity, "mcp", "mcp", "disabled", "networked", true, false, false, []CapabilityDisabledReason{
		capabilityReason("mcp.stdio.host_process_high_risk", "warning", "MCP stdio stays disabled until child process death, stderr, timeout, and ownership evidence are hardened.", "program_a_default", true),
		capabilityReason("mcp.http.unhardened_or_unproven", "warning", "MCP HTTP stays disabled until discovery, owner visibility, timeout, and policy envelope evidence are present.", "program_a_default", true),
	}, nil)
	surfaces["bridges"] = capabilitySurface(identity, "bridges", "bridge", "inspection_only", "high_risk", true, false, false, []CapabilityDisabledReason{
		capabilityReason("bridge.adapters_unavailable", "warning", "Bridge adapters are unavailable in Program A until represented by hardened capability snapshots.", "program_a_default", true),
	}, nil)
	surfaces["executor"] = capabilitySurface(identity, "executor", "executor", "disabled", "code_exec", true, false, false, []CapabilityDisabledReason{
		capabilityReason("executor.operation_ledger_required", "blocker", "Subprocess executor is disabled until operation ledger evidence is mandatory.", "program_a_default", true),
	}, nil)
	shellStatus := "disabled"
	shellToolVisible := false
	shellReasons := []CapabilityDisabledReason{
		capabilityReason("local.shell.disabled_by_policy", "warning", "Local shell is not available in the active runtime tool registry.", "runtime_config", true),
	}
	if hasCapabilityTool(toolNames, "shell") {
		shellStatus = "enabled"
		shellToolVisible = true
		shellReasons = nil
	}
	surfaces["shell"] = capabilitySurface(identity, "shell", "local_tool", shellStatus, "code_exec", true, shellToolVisible, false, shellReasons, map[string]any{
		"os":      runtimeOS(),
		"workdir": strings.TrimSpace(cfg.Workdir),
	})
	repoStatus := "inspection_only"
	repoReasons := []CapabilityDisabledReason{
		capabilityReason("repo.no_lease", "warning", "No first-class repo lease/write-scope is bound to the daemon snapshot yet.", "program_a_default", true),
	}
	writeVisible := hasCapabilityTool(toolNames, "write_file")
	if writeVisible && runtimeAllowsLocalMutation(cfg) {
		repoStatus = "degraded"
	}
	surfaces["repo"] = capabilitySurface(identity, "repo", "repo", repoStatus, "code_exec", true, writeVisible, writeVisible && runtimeAllowsLocalMutation(cfg), repoReasons, map[string]any{
		"workdir": strings.TrimSpace(cfg.Workdir),
	})
	surfaces["memory"] = capabilitySurface(identity, "memory", "memory", "disabled", "networked", true, false, false, []CapabilityDisabledReason{
		capabilityReason("memory.local.disabled_in_daemon", "info", "Local memory context is disabled in daemon mode.", "runtime_config", true),
		capabilityReason("program_a.no_direct_promotion", "warning", "Program A disables canonical memory promotion by default.", "program_a_default", true),
		capabilityReason("memory.workspace_write_disabled", "warning", "Daemon result memory writes are skipped as non-canonical observations.", "program_a_default", true),
	}, nil)
	surfaces["provider"] = capabilitySurface(identity, "provider", "provider", "enabled", "networked", true, true, false, nil, map[string]any{
		"backend": strings.TrimSpace(cfg.LLMBackend),
		"model":   strings.TrimSpace(cfg.Model),
	})
	surfaces["budget"] = capabilitySurface(identity, "budget", "budget", "degraded", "safe_local", true, false, false, []CapabilityDisabledReason{
		capabilityReason("budget.unavailable", "warning", "Full cost ledger is unavailable; only local cycle and prompt ceilings are snapshot-visible.", "runtime_config", true),
	}, map[string]any{
		"max_tool_iterations":         cfg.MaxToolLoopIterations,
		"max_provider_retry_attempts": cfg.MaxProviderRetryAttempts,
		"max_smoke_cycles_per_agent":  cfg.MaxSmokeCyclesPerAgent,
		"max_smoke_cycles_per_task":   cfg.MaxSmokeCyclesPerTask,
	})
	surfaces["ui"] = capabilitySurface(identity, "ui", "ui", "inspection_only", "safe_local", true, false, false, []CapabilityDisabledReason{
		capabilityReason("program_a.ui_no_authority_bearing_actions", "info", "TUI and web dashboard are inspection-only for Program A authority-bearing actions.", "program_a_default", true),
	}, nil)
	surfaces["network"] = capabilitySurface(identity, "network", "network", "degraded", "networked", true, true, false, []CapabilityDisabledReason{
		capabilityReason("policy.not_checked", "warning", "Network surfaces depend on provider/server policy checks at call time.", "runtime_config", true),
	}, map[string]any{
		"provider":    true,
		"mcp_http":    false,
		"bridge_http": false,
	})
	return surfaces
}

func capabilityToolBundleSurface(identity CapabilitySurfaceIdentity, agent *Agent) (CapabilitySurface, bool) {
	if agent == nil || agent.registry == nil {
		return CapabilitySurface{}, false
	}
	installed := capabilityInstalledToolBundleMetadata(agent.registry)
	report := agent.toolBundleDiscovery
	suites := toolBundleSuiteReadinessForAgent(agent)
	hasReport := report.Config.Present || len(report.ConfigDiagnostics) > 0 || len(report.Installed) > 0 || len(report.Dependencies) > 0 || len(report.Healthchecks) > 0 || len(report.Skipped) > 0 || len(report.Errors) > 0 || len(report.Collisions) > 0
	if len(installed) == 0 && !hasReport && len(suites) == 0 {
		return CapabilitySurface{}, false
	}
	status := "enabled"
	reasons := []CapabilityDisabledReason{}
	if len(report.Errors) > 0 || len(report.Collisions) > 0 {
		status = "degraded"
		reasons = append(reasons, capabilityReason("tool_bundle.discovery_diagnostics", "warning", "One or more local tool bundles failed discovery or collided with an existing tool.", "tool_bundle_discovery", true))
	} else if len(report.ConfigDiagnostics) > 0 {
		status = "degraded"
		reasons = append(reasons, capabilityReason("tool_bundle.config_diagnostics", "warning", "The local tool bundle registry config has warnings or errors.", "tool_bundle_discovery", true))
	} else if toolBundleSuitesHaveBlockingRequired(suites) {
		status = "degraded"
		reasons = append(reasons, capabilityReason("tool_bundle.required_suite_unavailable", "warning", "One or more anatomy heartbeat tool suites has no executable bundle or native provider.", "tool_bundle_discovery", true))
	} else if toolBundleDependencyDiagnosticsHaveWarnings(report.Dependencies) {
		status = "degraded"
		reasons = append(reasons, capabilityReason("tool_bundle.dependency_diagnostics", "warning", "One or more local tool bundle dependencies is missing or unchecked.", "tool_bundle_discovery", true))
	} else if toolBundleSuitesHaveRequiredWarnings(suites) {
		status = "degraded"
		reasons = append(reasons, capabilityReason("tool_bundle.required_suite_warnings", "warning", "One or more required heartbeat tool suites is available only through bundles with warnings.", "tool_bundle_discovery", true))
	} else if len(installed) == 0 && (len(report.Skipped) > 0 || hasReport) {
		status = "degraded"
		reasons = append(reasons, capabilityReason("tool_bundle.no_installed_bundles", "info", "Tool bundle discovery ran but no executable bundles were installed.", "tool_bundle_discovery", true))
	} else if len(report.Skipped) > 0 {
		status = "degraded"
		reasons = append(reasons, capabilityReason("tool_bundle.skipped_entries", "info", "Some local tool bundle directories were skipped.", "tool_bundle_discovery", true))
	}
	metadata := map[string]any{
		"installed_bundles": installed,
		"discovery": map[string]any{
			"workdir":                    strings.TrimSpace(report.Workdir),
			"roots":                      capabilityToolBundleRootsMetadata(report.Roots),
			"config":                     capabilityToolBundleRegistryConfigMetadata(report.Config),
			"candidate_count":            len(report.Candidates),
			"candidates":                 capabilityToolBundleReadinessMetadata(report.Candidates),
			"suite_count":                len(suites),
			"suites":                     capabilityToolBundleSuiteReadinessMetadata(suites),
			"installed_count":            len(installed),
			"registered_installed_count": len(installed),
			"discovery_installed_count":  len(report.Installed),
			"dependency_count":           len(report.Dependencies),
			"healthcheck_count":          len(report.Healthchecks),
			"config_count":               len(report.ConfigDiagnostics),
			"skipped_count":              len(report.Skipped),
			"error_count":                len(report.Errors),
			"collision_count":            len(report.Collisions),
			"dependencies":               capabilityToolBundleDiagnosticsMetadata(report.Dependencies),
			"healthchecks":               capabilityToolBundleDiagnosticsMetadata(report.Healthchecks),
			"config_diagnostics":         capabilityToolBundleDiagnosticsMetadata(report.ConfigDiagnostics),
			"skipped":                    capabilityToolBundleDiagnosticsMetadata(report.Skipped),
			"errors":                     capabilityToolBundleDiagnosticsMetadata(report.Errors),
			"collisions":                 capabilityToolBundleDiagnosticsMetadata(report.Collisions),
			"has_dependency_diagnostics": toolBundleDependencyDiagnosticsHaveWarnings(report.Dependencies),
			"has_config_diagnostics":     len(report.ConfigDiagnostics) > 0,
			"has_diagnostics":            toolBundleDiscoveryReportHasDiagnostics(report) || toolBundleDependencyDiagnosticsHaveWarnings(report.Dependencies),
			"copy_in_contract":           ".runtime-config/tool-bundles/<bundle>/tool.json or tools/<bundle>/tool.json",
		},
	}
	return capabilitySurface(identity, "tool_bundles", "local_tool_bundle", status, "safe_local", true, len(installed) > 0, false, reasons, metadata), true
}

func capabilityInstalledToolBundleMetadata(registry *ToolRegistry) []map[string]any {
	if registry == nil {
		return nil
	}
	items := []map[string]any{}
	names := make([]string, 0, len(registry.tools))
	byName := map[string]*InstalledToolBundleTool{}
	for name, tool := range registry.tools {
		bundle, ok := tool.(*InstalledToolBundleTool)
		if !ok || bundle == nil {
			continue
		}
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			trimmed = strings.TrimSpace(bundle.Name())
		}
		if trimmed == "" {
			continue
		}
		names = append(names, trimmed)
		byName[trimmed] = bundle
	}
	sort.Strings(names)
	for _, name := range names {
		bundle := byName[name]
		item := map[string]any{
			"name":               strings.TrimSpace(bundle.Name()),
			"description":        strings.TrimSpace(bundle.Description()),
			"version":            strings.TrimSpace(bundle.manifest.Version),
			"capability_suites":  uniqueTrimmedCSVStrings(bundle.manifest.CapabilitySuites),
			"artifact_contracts": capabilityToolBundleArtifactContractsMetadata(bundle.manifest.ArtifactContracts),
			"dependencies":       installedToolBundlePromptDependencies(bundle.manifest.Dependencies),
			"timeout_seconds":    int(installedToolBundleTimeout(bundle.manifest.TimeoutSeconds) / time.Second),
			"source_dir":         strings.TrimSpace(bundle.dir),
		}
		items = append(items, item)
	}
	return items
}

func capabilityToolBundleReadinessMetadata(items []ToolBundleReadinessItem) []map[string]any {
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, map[string]any{
			"name":         strings.TrimSpace(item.Name),
			"status":       strings.TrimSpace(item.Status),
			"code":         strings.TrimSpace(item.Code),
			"root_kind":    strings.TrimSpace(item.RootKind),
			"dir":          strings.TrimSpace(item.Dir),
			"existing_dir": strings.TrimSpace(item.ExistingDir),
			"suites":       uniqueTrimmedCSVStrings(item.CapabilitySuites),
			"message":      strings.TrimSpace(item.Message),
			"registered":   item.Registered,
			"configured":   item.Configured,
		})
	}
	return out
}

func capabilityToolBundleSuiteReadinessMetadata(items []ToolBundleSuiteReadinessItem) []map[string]any {
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, map[string]any{
			"suite":             strings.TrimSpace(item.Suite),
			"status":            strings.TrimSpace(item.Status),
			"required":          item.Required,
			"heartbeats":        uniqueTrimmedCSVStrings(item.Heartbeats),
			"ready_bundles":     uniqueTrimmedCSVStrings(item.ReadyBundles),
			"warning_bundles":   uniqueTrimmedCSVStrings(item.WarningBundles),
			"blocked_bundles":   uniqueTrimmedCSVStrings(item.BlockedBundles),
			"candidate_bundles": uniqueTrimmedCSVStrings(item.CandidateBundles),
			"native_providers":  uniqueTrimmedCSVStrings(item.NativeProviders),
			"suggested_bundles": uniqueTrimmedCSVStrings(item.SuggestedBundles),
			"suggested_actions": uniqueTrimmedCSVStrings(item.SuggestedActions),
			"message":           strings.TrimSpace(item.Message),
		})
	}
	return out
}

func capabilityToolBundleRegistryConfigMetadata(status ToolBundleRegistryConfigStatus) map[string]any {
	return map[string]any{
		"path":     strings.TrimSpace(status.Path),
		"present":  status.Present,
		"status":   strings.TrimSpace(status.Status),
		"mode":     strings.TrimSpace(status.Mode),
		"enabled":  uniqueTrimmedCSVStrings(status.Enabled),
		"disabled": uniqueTrimmedCSVStrings(status.Disabled),
		"message":  strings.TrimSpace(status.Message),
	}
}

func capabilityToolBundleArtifactContractsMetadata(contracts []InstalledToolBundleArtifactContract) []map[string]any {
	out := make([]map[string]any, 0, len(contracts))
	for _, contract := range contracts {
		item := map[string]any{
			"name":        strings.TrimSpace(contract.Name),
			"type":        strings.TrimSpace(contract.Type),
			"path":        strings.TrimSpace(contract.Path),
			"description": strings.TrimSpace(contract.Description),
			"required":    contract.Required,
		}
		out = append(out, item)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return fmt.Sprint(out[i]["name"], out[i]["path"]) < fmt.Sprint(out[j]["name"], out[j]["path"])
	})
	return out
}

func capabilityToolBundleRootsMetadata(roots []ToolBundleDiscoveryRoot) []map[string]string {
	out := make([]map[string]string, 0, len(roots))
	for _, root := range roots {
		out = append(out, map[string]string{
			"kind": strings.TrimSpace(root.Kind),
			"path": strings.TrimSpace(root.Path),
		})
	}
	return out
}

func capabilityToolBundleDiagnosticsMetadata(diagnostics []ToolBundleDiscoveryDiagnostic) []map[string]string {
	out := make([]map[string]string, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		out = append(out, map[string]string{
			"code":         strings.TrimSpace(diagnostic.Code),
			"name":         strings.TrimSpace(diagnostic.Name),
			"root_kind":    strings.TrimSpace(diagnostic.RootKind),
			"dir":          strings.TrimSpace(diagnostic.Dir),
			"existing_dir": strings.TrimSpace(diagnostic.ExistingDir),
			"suites":       strings.Join(uniqueTrimmedCSVStrings(diagnostic.CapabilitySuites), ","),
			"message":      strings.TrimSpace(diagnostic.Message),
		})
	}
	return out
}

func capabilitySurface(identity CapabilitySurfaceIdentity, id, kind, status, tier string, promptVisible, toolVisible, mutationAllowed bool, reasons []CapabilityDisabledReason, metadata map[string]any) CapabilitySurface {
	policyVerdict := "NOT_CHECKED"
	if status == "disabled" {
		policyVerdict = "DENY"
	}
	return CapabilitySurface{
		SurfaceID:                id,
		SurfaceKind:              kind,
		Status:                   status,
		AutonomyState:            capabilitySurfaceAutonomyState(status, toolVisible, reasons),
		CapabilityTier:           tier,
		PromptVisible:            promptVisible,
		ToolVisible:              toolVisible,
		MutationAllowed:          mutationAllowed,
		RequiresPolicyAllow:      true,
		RequiresOperatorApproval: status == "inspection_only",
		DisabledReasons:          reasons,
		IdentityBinding:          identity,
		Policy: CapabilitySurfacePolicy{
			RequestedCapability: "tool.call",
			PolicyVerdict:       policyVerdict,
		},
		Limits: CapabilitySurfaceLimits{
			TimeoutSec:       60,
			MaxOutputBytes:   65536,
			MaxCallsPerCycle: 1,
			RetryCeiling:     0,
		},
		Evidence: CapabilitySurfaceEvidence{
			ProbeStatus: "not_run",
			SourceRef:   "agent.runtime",
		},
		Metadata: metadata,
	}
}

func capabilitySurfaceAutonomyState(status string, toolVisible bool, reasons []CapabilityDisabledReason) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "enabled":
		return "enabled"
	case "degraded":
		if toolVisible {
			return "fallback_allowed"
		}
		return "degraded"
	case "inspection_only":
		return "advisory"
	case "disabled":
		return "hard_disabled"
	default:
		for _, reason := range reasons {
			if strings.EqualFold(strings.TrimSpace(reason.Severity), "blocker") {
				return "hard_disabled"
			}
		}
		return "advisory"
	}
}

func capabilityReason(code, severity, message, source string, recoverable bool) CapabilityDisabledReason {
	return CapabilityDisabledReason{
		Code:        code,
		Severity:    severity,
		Message:     message,
		Source:      source,
		Recoverable: recoverable,
	}
}

func capabilitySnapshotToolNames(agent *Agent) []string {
	if agent == nil || agent.registry == nil {
		return nil
	}
	names := make([]string, 0, len(agent.registry.tools))
	for name := range agent.registry.tools {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			names = append(names, trimmed)
		}
	}
	sort.Strings(names)
	return names
}

func capabilityDisabledToolNames(cfg RuntimeConfig, enabled []string) []CapabilityDisabledToolName {
	cfg.ApplyDefaults()
	disabled := []CapabilityDisabledToolName{}
	seen := map[string]struct{}{}
	add := func(name, reason string) {
		name = strings.TrimSpace(name)
		reason = strings.TrimSpace(reason)
		if name == "" || reason == "" {
			return
		}
		key := name + "\x00" + reason
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		disabled = append(disabled, CapabilityDisabledToolName{Name: name, ReasonCode: reason})
	}
	addIfMissing := func(name, reason string) {
		if !hasCapabilityTool(enabled, name) {
			add(name, reason)
		}
	}
	if cfg.Mode == RuntimeModeDaemon {
		if !runtimeAllowsLocalShell(cfg) {
			add("shell", "program_b.raw_shell_disabled")
		}
		if !runtimeAllowsLocalMutation(cfg) {
			add("write_file", "program_b.raw_repo_mutation_disabled")
		}
		add("memory_write", "program_b.local_memory_write_disabled")
		add("daily_note", "program_b.local_memory_write_disabled")
		add("daily_note_append", "program_b.local_memory_write_disabled")
		add("mcp__*", "program_b.unknown_mcp_disabled")
		add("executor.run_node", "program_b.raw_executor_disabled")
		add("dev.implement", "program_b.bridge_mutation_disabled")
	} else {
		addIfMissing("shell", "local.shell.disabled_by_policy")
		addIfMissing("write_file", "local.fs.write_disabled")
		addIfMissing("memory_write", "memory.local.disabled_in_daemon")
		addIfMissing("daily_note", "memory.local.disabled_in_daemon")
		addIfMissing("daily_note_append", "memory.local.disabled_in_daemon")
	}
	add("memory_promotion_write", "program_a.no_direct_promotion")
	add("mcp.*", "mcp.http.unhardened_or_unproven")
	add("bridge.*", "bridge.adapters_unavailable")
	add("executor.subprocess", "executor.operation_ledger_required")
	for _, item := range runtimeDisabledToolNameItems(cfg.DisabledToolNames) {
		add(item.Name, item.ReasonCode)
	}
	sort.Slice(disabled, func(i, j int) bool {
		if disabled[i].Name == disabled[j].Name {
			return disabled[i].ReasonCode < disabled[j].ReasonCode
		}
		return disabled[i].Name < disabled[j].Name
	})
	return disabled
}

func capabilityEnabledToolNames(registered []string, disabled []CapabilityDisabledToolName) []string {
	enabled := uniqueTrimmedCSVStrings(registered)
	filtered := make([]string, 0, len(enabled))
	for _, name := range enabled {
		if _, ok := capabilityDisabledToolNameMatch(name, disabled); ok {
			continue
		}
		filtered = append(filtered, name)
	}
	sort.Strings(filtered)
	return filtered
}

func capabilityDisabledToolNameMatch(name string, disabled []CapabilityDisabledToolName) (CapabilityDisabledToolName, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return CapabilityDisabledToolName{}, false
	}
	for _, item := range disabled {
		disabledName := strings.TrimSpace(item.Name)
		if disabledName == name {
			return item, true
		}
		if strings.HasSuffix(disabledName, ".*") {
			prefix := strings.TrimSuffix(disabledName, ".*")
			if strings.HasPrefix(name, prefix+".") || strings.HasPrefix(name, prefix+"_") {
				return item, true
			}
		}
		if strings.HasSuffix(disabledName, "*") {
			prefix := strings.TrimSuffix(disabledName, "*")
			if prefix != "" && strings.HasPrefix(name, prefix) {
				return item, true
			}
		}
	}
	return CapabilityDisabledToolName{}, false
}

func capabilityDisabledReasonForTool(cfg RuntimeConfig, toolName string) (string, bool) {
	cfg.ApplyDefaults()
	if cfg.Mode != RuntimeModeDaemon {
		return "", false
	}
	if item, ok := capabilityDisabledToolNameMatch(toolName, capabilityDisabledToolNames(cfg, nil)); ok {
		return strings.TrimSpace(item.ReasonCode), true
	}
	return "", false
}

func hasCapabilityTool(enabled []string, name string) bool {
	name = strings.TrimSpace(name)
	for _, item := range enabled {
		if strings.TrimSpace(item) == name {
			return true
		}
	}
	return false
}

func capabilityPromptMustInclude() []string {
	return []string{
		"Only use enabled tools listed in this capability snapshot.",
		"Unavailable surfaces are listed with disabled reasons.",
		"Do not claim MCP, executor, browser, memory promotion, or bridge availability unless enabled here.",
		"Workspace document materialization through workspace_doc_put or structured result materialize is always daemon-safe; local shell and file writes are allowed only when listed as enabled in this snapshot.",
	}
}

func capabilitySnapshotNotes(cfg RuntimeConfig) []string {
	notes := []string{
		"Program A snapshot is conservative: MCP, bridge, executor, and memory promotion remain disabled or inspection-only until later hardening tasks enable them.",
		"Program B patch-only baseline hides raw repo mutation surfaces from the daemon capability contract until repo authority wrappers exist.",
		"Workspace document materialization remains enabled through Rhizome RPC even when raw local file mutation is disabled.",
	}
	if cfg.Mode == RuntimeModeDaemon {
		notes = append(notes, "Daemon mode skips canonical memory materialization and local memory context; workspace docs are the canonical artifact path.")
	}
	return notes
}

func capabilitySnapshotDeclaredCapabilities(cfg RuntimeConfig) []string {
	cfg.ApplyDefaults()
	capabilities := uniqueTrimmedCSVStrings(cfg.Capabilities)
	if cfg.Mode != RuntimeModeDaemon {
		return capabilities
	}
	filtered := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		switch strings.TrimSpace(capability) {
		case "local.shell":
			if !runtimeAllowsLocalShell(cfg) {
				continue
			}
		case "local.fs.write":
			if !runtimeAllowsLocalMutation(cfg) {
				continue
			}
		default:
		}
		filtered = append(filtered, capability)
	}
	sort.Strings(filtered)
	return filtered
}

func capabilityIdentitySource(cfg RuntimeConfig) string {
	if strings.TrimSpace(cfg.RhizomeToken) != "" {
		return "token"
	}
	return "profile"
}

func capabilityIdentityBindingHash(cfg RuntimeConfig, bootSnapshotID string) string {
	cfg.ApplyDefaults()
	parts := []string{
		strings.TrimSpace(cfg.WorkspaceID),
		strings.TrimSpace(cfg.AgentID),
		strings.TrimSpace(cfg.OwnerUserID),
		strings.TrimSpace(string(cfg.Mode)),
		strings.TrimSpace(bootSnapshotID),
	}
	return "sha256:" + sha256Hex(strings.Join(parts, "|"))
}

func capabilityConfigFingerprintParts(cfg RuntimeConfig) []string {
	cfg.ApplyDefaults()
	return []string{
		strings.TrimSpace(string(cfg.Mode)),
		strings.TrimSpace(cfg.ProtocolVersion),
		strings.TrimSpace(cfg.WorkspaceID),
		strings.TrimSpace(cfg.AgentID),
		strings.TrimSpace(cfg.OwnerUserID),
		strings.TrimSpace(cfg.Model),
		strings.TrimSpace(cfg.LLMBackend),
		fmt.Sprintf("prompt_doc=%d", cfg.MaxPromptDocChars),
		fmt.Sprintf("prompt_spec=%d", cfg.MaxPromptSpecChars),
		fmt.Sprintf("tool_loop=%d", cfg.MaxToolLoopIterations),
	}
}

func capabilitySnapshotGenerator(cfg RuntimeConfig) CapabilitySnapshotGenerator {
	cfg.ApplyDefaults()
	capabilitySnapshotBinaryFingerprint.once.Do(func() {
		if path, err := os.Executable(); err == nil {
			capabilitySnapshotBinaryFingerprint.path = path
			if sha, err := fileSHA256(path); err == nil {
				capabilitySnapshotBinaryFingerprint.sha = sha
			}
		}
	})
	return CapabilitySnapshotGenerator{
		Component:       "agent",
		RuntimeMode:     string(cfg.Mode),
		BinaryPath:      capabilitySnapshotBinaryFingerprint.path,
		BinarySHA256:    capabilitySnapshotBinaryFingerprint.sha,
		ProtocolVersion: strings.TrimSpace(cfg.ProtocolVersion),
	}
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func capabilityWorkPacketID(work *AgentWorkPacket) string {
	if work == nil {
		return ""
	}
	parts := []string{
		strings.TrimSpace(work.WorkType),
		strings.TrimSpace(work.ClaimAction),
		strings.TrimSpace(work.SessionAction),
		strings.TrimSpace(work.PreferredTransition),
		strings.TrimSpace(work.WhyNow),
	}
	return "wp_" + shortHash(strings.Join(parts, "|"))
}

func runtimeOS() string {
	return goruntime.GOOS
}
