package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/mcp"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"github.com/Rhizome-Project/rhizome-runtime/internal/tools"
)

// ── Tool Deploy/Call/Undeploy ───────────────────────────────────────

type toolDeployParams struct {
	ToolID      string `json:"tool_id"`
	WorkspaceID string `json:"workspace_id"`
	Runtime     string `json:"runtime"`
	SourceCode  string `json:"source_code"`
	EntryPoint  string `json:"entry_point"`
	DeployedBy  string `json:"deployed_by"`
}

func (h *Handler) toolDeploy(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p toolDeployParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if sqlite.IsRemovedWorkspaceToolID(p.ToolID) {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: fmt.Sprintf("workspace tool %q has been removed from Rhizome", p.ToolID)}
	}

	principal, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}

	previousDeployment, previousDeploymentErr := h.toolLifecyclePreviousDeployment(p.WorkspaceID, p.ToolID)
	operationLedger := h.newToolDeployOperationLedger(ctx, p, principal, previousDeployment, previousDeploymentErr)
	ledgerCtx := context.WithoutCancel(ctx)
	ledgerDegraded := false
	if rpcErr := h.recordToolLifecycleOperationLedger(ledgerCtx, operationLedger, "ACTIVE", "RUNNING", false, nil); rpcErr != nil {
		ledgerDegraded = true
		log.Printf("[tool.deploy] active operation ledger degraded workspace=%s tool=%s operation=%s: %s", p.WorkspaceID, p.ToolID, operationLedger.operationID, rpcErr.Message)
	}

	scriptPath, err := h.toolExec.Deploy(tools.DeployInput{
		ToolID:      p.ToolID,
		WorkspaceID: p.WorkspaceID,
		Runtime:     p.Runtime,
		SourceCode:  p.SourceCode,
		EntryPoint:  p.EntryPoint,
		DeployedBy:  p.DeployedBy,
	})
	if err != nil {
		result := toolLifecycleErrorResultMap("tool.deploy", p.ToolID, operationLedger.operationID, err)
		if rpcErr := h.recordToolLifecycleOperationLedger(ledgerCtx, operationLedger, "FAILED", "FAILED", true, result); rpcErr != nil {
			log.Printf("[tool.deploy] terminal operation ledger degraded workspace=%s tool=%s operation=%s: %s", p.WorkspaceID, p.ToolID, operationLedger.operationID, rpcErr.Message)
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	result := map[string]any{
		"operation_id":        operationLedger.operationID,
		"tool_id":             p.ToolID,
		"script_path":         scriptPath,
		"runtime":             normalizedToolDeployRuntime(p.Runtime),
		"entry_point":         normalizedToolDeployEntryPoint(p.Runtime, p.EntryPoint),
		"previous_deployment": previousDeployment,
		"status":              "DEPLOYED",
	}
	if previousDeploymentErr != "" {
		result["previous_deployment_check_error"] = previousDeploymentErr
	}
	if rpcErr := h.recordToolLifecycleOperationLedger(ledgerCtx, operationLedger, "COMPLETED", "COMPLETED", true, result); rpcErr != nil {
		ledgerDegraded = true
		log.Printf("[tool.deploy] terminal operation ledger degraded workspace=%s tool=%s operation=%s: %s", p.WorkspaceID, p.ToolID, operationLedger.operationID, rpcErr.Message)
	}
	if ledgerDegraded {
		result["operation_ledger_degraded"] = true
	}
	return result, nil
}

func toolCallOperationRequestHash(request map[string]any) string {
	raw, err := json.Marshal(request)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func toolCallOperationName(toolID string, toolRecord *sqlite.WorkspaceToolRecord) string {
	toolID = strings.TrimSpace(toolID)
	if toolRecord != nil {
		manifest := parseWorkspaceToolManifest(toolRecord.ManifestJSON)
		if manifest.Route != nil && strings.EqualFold(strings.TrimSpace(manifest.Route.Kind), "mcp") {
			serverID := strings.TrimSpace(manifest.Route.ServerID)
			toolName := strings.TrimSpace(manifest.Route.ToolName)
			if serverID != "" || toolName != "" {
				if serverID == "" {
					serverID = "mcp"
				}
				return serverID + "/" + firstNonEmpty(toolName, toolID)
			}
		}
	}
	return firstNonEmpty(toolID, "tool.call")
}

func toolCallOperationBinding(workspaceID, principalType, principalID, actorType, actorID string, authority sqlite.WorkspaceAuthorityRecord, operationID, taskID, sessionID, parentRunID string) map[string]any {
	actorType = strings.TrimSpace(firstNonEmpty(actorType, principalType))
	actorID = strings.TrimSpace(firstNonEmpty(actorID, principalID))
	binding := map[string]any{
		"workspace_id":            strings.TrimSpace(workspaceID),
		"principal_type":          actorType,
		"principal_id":            actorID,
		"agent_id":                "",
		"owner_user_id":           "",
		"task_id":                 strings.TrimSpace(taskID),
		"session_id":              strings.TrimSpace(sessionID),
		"run_id":                  strings.TrimSpace(operationID),
		"parent_run_id":           strings.TrimSpace(parentRunID),
		"claim_agent_id":          "",
		"claim_status_at_start":   "",
		"session_status_at_start": "",
		"authority_scope":         strings.TrimSpace(authority.Scope),
		"authority_term":          authority.Term,
		"claim_term":              "",
		"session_term":            "",
	}
	if strings.EqualFold(actorType, "agent") {
		binding["agent_id"] = actorID
	}
	return binding
}

func attachToolCallLiveBindingProof(binding map[string]any, proof sqlite.ExecutionRunBindingProof) map[string]any {
	if binding == nil {
		binding = map[string]any{}
	}
	if strings.TrimSpace(proof.BindingReceiptContract) == "" {
		return binding
	}
	binding["agent_id"] = strings.TrimSpace(firstNonEmpty(proof.AgentID, fmt.Sprint(binding["agent_id"])))
	binding["task_id"] = strings.TrimSpace(firstNonEmpty(proof.TaskID, fmt.Sprint(binding["task_id"])))
	binding["session_id"] = strings.TrimSpace(firstNonEmpty(proof.SessionID, fmt.Sprint(binding["session_id"])))
	binding["parent_run_id"] = strings.TrimSpace(firstNonEmpty(proof.ParentRunID, fmt.Sprint(binding["parent_run_id"])))
	binding["claim_agent_id"] = strings.TrimSpace(proof.ClaimAgentID)
	binding["claim_status_at_start"] = strings.TrimSpace(proof.ClaimStatusAtStart)
	binding["task_status_at_start"] = strings.TrimSpace(proof.TaskStatusAtStart)
	binding["session_status_at_start"] = strings.TrimSpace(proof.SessionStatusAtStart)
	binding["parent_run_status_at_start"] = strings.TrimSpace(proof.ParentRunStatusAtStart)
	binding["binding_receipt_contract"] = strings.TrimSpace(proof.BindingReceiptContract)
	binding["receiptless_session_policy"] = strings.TrimSpace(proof.ReceiptlessSessionPolicy)
	return binding
}

func toolCallOperationCapabilitySnapshot(requestedCapability string, toolRecord *sqlite.WorkspaceToolRecord, policyVerdict string) map[string]any {
	snapshot := map[string]any{
		"snapshot_id":                    "tool-call-ledger-snapshot",
		"snapshot_schema":                "operation_ledger.v1",
		"surface_id":                     "tool.call",
		"surface_status_at_start":        "enabled",
		"requested_capability":           strings.TrimSpace(firstNonEmpty(requestedCapability, "tool.call")),
		"policy_epoch":                   "",
		"policy_verdict_at_start":        strings.TrimSpace(firstNonEmpty(policyVerdict, "ALLOW")),
		"disabled_reason_codes_at_start": []string{},
	}
	if toolRecord != nil {
		manifest := parseWorkspaceToolManifest(toolRecord.ManifestJSON)
		if manifest.Route != nil && strings.EqualFold(strings.TrimSpace(manifest.Route.Kind), "mcp") {
			snapshot["surface_id"] = "mcp:" + strings.TrimSpace(manifest.Route.ServerID) + "/" + strings.TrimSpace(manifest.Route.ToolName)
		}
	}
	return snapshot
}

func toolCallOperationRequestDetails(toolID string, toolRecord *sqlite.WorkspaceToolRecord, requestedCapability string, timeoutSec int, routed bool, mcpTransport, taskID, sessionID, parentRunID string) map[string]any {
	details := map[string]any{
		"tool_id":              strings.TrimSpace(toolID),
		"requested_capability": strings.TrimSpace(firstNonEmpty(requestedCapability, "tool.call")),
		"timeout_sec":          timeoutSec,
		"execution_kind":       "tool_call",
		"mutation_kind":        "none",
	}
	if strings.TrimSpace(taskID) != "" {
		details["task_id"] = strings.TrimSpace(taskID)
	}
	if strings.TrimSpace(sessionID) != "" {
		details["session_id"] = strings.TrimSpace(sessionID)
	}
	if strings.TrimSpace(parentRunID) != "" {
		details["parent_run_id"] = strings.TrimSpace(parentRunID)
	}
	if routed {
		details["router_kind"] = "mcp"
	}
	if toolRecord != nil {
		manifest := parseWorkspaceToolManifest(toolRecord.ManifestJSON)
		if manifest.Route != nil && strings.EqualFold(strings.TrimSpace(manifest.Route.Kind), "mcp") {
			details["mcp_server_id"] = strings.TrimSpace(manifest.Route.ServerID)
			details["mcp_tool_name"] = strings.TrimSpace(manifest.Route.ToolName)
			details["mcp_transport"] = strings.TrimSpace(firstNonEmpty(mcpTransport, "unknown"))
		}
	}
	return details
}

func toolCallOperationFence(authority sqlite.WorkspaceAuthorityRecord, operationID string) map[string]any {
	return map[string]any{
		"expected_status_before_terminal": "running",
		"expected_authority_term":         authority.Term,
		"expected_run_id":                 strings.TrimSpace(operationID),
		"completion_token":                strings.TrimSpace(operationID),
		"canonical_mutation_allowed":      false,
		"canonical_mutation_reason":       "tool.call records execution outcome; canonical state mutations require a separate authority-bound surface",
	}
}

func toolCallOperationLedgerPayload(operationID, operationKey, operationName, status string, terminal bool, createdAt, updatedAt, deadlineAt string, timeoutSec int, binding, capabilitySnapshot, requestDetails, fence map[string]any, result map[string]any) map[string]any {
	payload := map[string]any{
		"schema":              "operation_ledger.v1",
		"operation_id":        strings.TrimSpace(operationID),
		"operation_key":       strings.TrimSpace(operationKey),
		"operation_kind":      "tool_call",
		"operation_name":      strings.TrimSpace(operationName),
		"status":              strings.TrimSpace(status),
		"terminal":            terminal,
		"created_at":          strings.TrimSpace(createdAt),
		"started_at":          strings.TrimSpace(createdAt),
		"updated_at":          strings.TrimSpace(updatedAt),
		"deadline_at":         strings.TrimSpace(deadlineAt),
		"timeout_sec":         timeoutSec,
		"attempt":             1,
		"binding":             binding,
		"capability_snapshot": capabilitySnapshot,
		"request": map[string]any{
			"request_hash":      operationKey,
			"idempotency_scope": "workspace",
			"redaction_policy":  "secrets-redacted-v1",
			"summary":           strings.TrimSpace(operationName),
			"details":           requestDetails,
		},
		"fence": fence,
		"causality": map[string]any{
			"source":      "server",
			"parent_refs": []string{},
		},
	}
	if terminal {
		payload["terminal_at"] = strings.TrimSpace(updatedAt)
	}
	if result != nil {
		payload["result"] = result
	}
	return payload
}

func toolCallOperationResultSummary(result map[string]any) string {
	if result == nil {
		return ""
	}
	if timedOut, _ := result["timed_out"].(bool); timedOut {
		return "tool call timed out"
	}
	if summary, _ := result["summary"].(string); strings.TrimSpace(summary) != "" {
		return strings.TrimSpace(summary)
	}
	if stderr, _ := result["stderr"].(string); strings.TrimSpace(stderr) != "" {
		switch exitCode := result["exit_code"].(type) {
		case int:
			if exitCode != 0 {
				return strings.TrimSpace(stderr)
			}
		case int64:
			if exitCode != 0 {
				return strings.TrimSpace(stderr)
			}
		case float64:
			if int(exitCode) != 0 {
				return strings.TrimSpace(stderr)
			}
		}
	}
	if stdout, _ := result["stdout"].(string); strings.TrimSpace(stdout) != "" {
		return strings.TrimSpace(stdout)
	}
	return ""
}

func operationLedgerID(prefix, operationKey string, now time.Time) string {
	key := strings.TrimPrefix(strings.TrimSpace(operationKey), "sha256:")
	if len(key) > 16 {
		key = key[:16]
	}
	if key == "" {
		key = "unhashed"
	}
	stamp := strings.ReplaceAll(now.UTC().Format("20060102T150405.000000000Z"), ".", "")
	prefix = strings.Trim(strings.TrimSpace(prefix), "_")
	if prefix == "" {
		prefix = "operation"
	}
	return prefix + "_" + key + "_" + stamp
}

func toolCallOperationID(operationKey string, now time.Time) string {
	return operationLedgerID("toolcall", operationKey, now)
}

type toolLifecycleOperationLedgerContext struct {
	operationID        string
	operationKey       string
	operationKind      string
	operationName      string
	createdAt          time.Time
	workspaceID        string
	agentID            string
	binding            map[string]any
	capabilitySnapshot map[string]any
	requestDetails     map[string]any
	fence              map[string]any
}

func normalizedToolDeployRuntime(runtime string) string {
	runtime = strings.TrimSpace(strings.ToLower(runtime))
	if runtime == "" {
		return "python"
	}
	return runtime
}

func normalizedToolDeployEntryPoint(runtime, entryPoint string) string {
	entryPoint = strings.TrimSpace(entryPoint)
	if entryPoint != "" {
		return entryPoint
	}
	switch normalizedToolDeployRuntime(runtime) {
	case "python":
		return "main.py"
	case "bash":
		return "main.sh"
	case "node":
		return "main.js"
	default:
		return "main"
	}
}

func toolSourceSHA256(source string) string {
	sum := sha256.Sum256([]byte(source))
	return hex.EncodeToString(sum[:])
}

func toolLifecycleOperationBinding(workspaceID string, principal AuthPrincipal, operationID string) map[string]any {
	principalType := strings.TrimSpace(principal.PrincipalType)
	principalID := strings.TrimSpace(principal.PrincipalID)
	binding := map[string]any{
		"workspace_id":            strings.TrimSpace(workspaceID),
		"principal_type":          principalType,
		"principal_id":            principalID,
		"agent_id":                "",
		"owner_user_id":           "",
		"task_id":                 "",
		"session_id":              "",
		"run_id":                  strings.TrimSpace(operationID),
		"claim_agent_id":          "",
		"claim_status_at_start":   "",
		"session_status_at_start": "",
		"authority_scope":         "",
		"authority_term":          0,
		"claim_term":              "",
		"session_term":            "",
		"authority_binding":       "none",
	}
	if strings.EqualFold(principalType, "agent") {
		binding["agent_id"] = principalID
	}
	return binding
}

func toolLifecycleCapabilitySnapshot(surface string) map[string]any {
	return map[string]any{
		"snapshot_id":                    strings.TrimSpace(surface) + "-ledger-snapshot",
		"snapshot_schema":                "operation_ledger.v1",
		"surface_id":                     strings.TrimSpace(surface),
		"surface_status_at_start":        "enabled",
		"requested_capability":           strings.TrimSpace(surface),
		"policy_epoch":                   "",
		"policy_verdict_at_start":        "ALLOW",
		"disabled_reason_codes_at_start": []string{},
		"authority_model":                "workspace_principal_only",
	}
}

func toolLifecycleFence(operationID string) map[string]any {
	return map[string]any{
		"expected_status_before_terminal": "running",
		"expected_run_id":                 strings.TrimSpace(operationID),
		"completion_token":                strings.TrimSpace(operationID),
		"canonical_mutation_allowed":      false,
		"canonical_mutation_reason":       "tool lifecycle mutates local tool files under workspace-principal preflight without a workspace authority term",
		"filesystem_mutation_expected":    true,
		"mutation_recording_mode":         "observability_only",
	}
}

func (h *Handler) toolLifecyclePreviousDeployment(workspaceID, toolID string) (bool, string) {
	deployed, err := h.toolExec.IsDeployed(workspaceID, toolID)
	if err != nil {
		return false, err.Error()
	}
	return deployed, ""
}

func (h *Handler) registeredOperationAgentID(ctx context.Context, workspaceID string, principal AuthPrincipal) string {
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		return ""
	}
	agentID := strings.TrimSpace(principal.PrincipalID)
	if agentID == "" {
		return ""
	}
	if _, err := h.store.GetAgent(ctx, workspaceID, agentID); err != nil {
		return ""
	}
	return agentID
}

func (h *Handler) newToolDeployOperationLedger(ctx context.Context, p toolDeployParams, principal AuthPrincipal, previousDeployment bool, previousDeploymentErr string) toolLifecycleOperationLedgerContext {
	started := time.Now().UTC()
	sourceSize := len([]byte(p.SourceCode))
	sourceHash := toolSourceSHA256(p.SourceCode)
	details := map[string]any{
		"tool_id":               strings.TrimSpace(p.ToolID),
		"workspace_id":          strings.TrimSpace(p.WorkspaceID),
		"runtime":               normalizedToolDeployRuntime(p.Runtime),
		"entry_point":           normalizedToolDeployEntryPoint(p.Runtime, p.EntryPoint),
		"requested_entry_point": strings.TrimSpace(p.EntryPoint),
		"deployed_by":           strings.TrimSpace(p.DeployedBy),
		"source_size_bytes":     sourceSize,
		"source_sha256":         sourceHash,
		"execution_kind":        "tool_deploy",
		"mutation_kind":         "tool_filesystem_deploy",
		"source_redaction":      "source_code_omitted",
		"authority_model_used":  "workspace_principal_only",
		"previous_deployment":   previousDeployment,
	}
	if previousDeploymentErr != "" {
		details["previous_deployment_check_error"] = previousDeploymentErr
	}
	request := map[string]any{
		"workspace_id":        strings.TrimSpace(p.WorkspaceID),
		"tool_id":             strings.TrimSpace(p.ToolID),
		"runtime":             details["runtime"],
		"entry_point":         details["entry_point"],
		"deployed_by":         details["deployed_by"],
		"source_size_bytes":   sourceSize,
		"source_sha256":       sourceHash,
		"previous_deployment": previousDeployment,
	}
	operationKey := "sha256:" + toolCallOperationRequestHash(request)
	operationID := operationLedgerID("tooldeploy", operationKey, started)
	return toolLifecycleOperationLedgerContext{
		operationID:        operationID,
		operationKey:       operationKey,
		operationKind:      "tool_deploy",
		operationName:      "tool.deploy:" + strings.TrimSpace(p.ToolID),
		createdAt:          started,
		workspaceID:        strings.TrimSpace(p.WorkspaceID),
		agentID:            h.registeredOperationAgentID(ctx, p.WorkspaceID, principal),
		binding:            toolLifecycleOperationBinding(p.WorkspaceID, principal, operationID),
		capabilitySnapshot: toolLifecycleCapabilitySnapshot("tool.deploy"),
		requestDetails:     details,
		fence:              toolLifecycleFence(operationID),
	}
}

func (h *Handler) newToolUndeployOperationLedger(ctx context.Context, p toolUndeployParams, principal AuthPrincipal, previousDeployment bool, previousDeploymentErr string) toolLifecycleOperationLedgerContext {
	started := time.Now().UTC()
	request := map[string]any{
		"workspace_id":        strings.TrimSpace(p.WorkspaceID),
		"tool_id":             strings.TrimSpace(p.ToolID),
		"previous_deployment": previousDeployment,
	}
	operationKey := "sha256:" + toolCallOperationRequestHash(request)
	operationID := operationLedgerID("toolundeploy", operationKey, started)
	details := map[string]any{
		"tool_id":              strings.TrimSpace(p.ToolID),
		"workspace_id":         strings.TrimSpace(p.WorkspaceID),
		"execution_kind":       "tool_undeploy",
		"mutation_kind":        "tool_filesystem_undeploy",
		"authority_model_used": "workspace_principal_only",
		"previous_deployment":  previousDeployment,
	}
	if previousDeploymentErr != "" {
		details["previous_deployment_check_error"] = previousDeploymentErr
	}
	return toolLifecycleOperationLedgerContext{
		operationID:        operationID,
		operationKey:       operationKey,
		operationKind:      "tool_undeploy",
		operationName:      "tool.undeploy:" + strings.TrimSpace(p.ToolID),
		createdAt:          started,
		workspaceID:        strings.TrimSpace(p.WorkspaceID),
		agentID:            h.registeredOperationAgentID(ctx, p.WorkspaceID, principal),
		binding:            toolLifecycleOperationBinding(p.WorkspaceID, principal, operationID),
		capabilitySnapshot: toolLifecycleCapabilitySnapshot("tool.undeploy"),
		requestDetails:     details,
		fence:              toolLifecycleFence(operationID),
	}
}

func toolLifecycleOperationLedgerPayload(op toolLifecycleOperationLedgerContext, status string, terminal bool, updatedAt string, result map[string]any) map[string]any {
	payload := map[string]any{
		"schema":              "operation_ledger.v1",
		"operation_id":        strings.TrimSpace(op.operationID),
		"operation_key":       strings.TrimSpace(op.operationKey),
		"operation_kind":      strings.TrimSpace(op.operationKind),
		"operation_name":      strings.TrimSpace(op.operationName),
		"status":              strings.TrimSpace(status),
		"terminal":            terminal,
		"created_at":          op.createdAt.UTC().Format(time.RFC3339Nano),
		"started_at":          op.createdAt.UTC().Format(time.RFC3339Nano),
		"updated_at":          strings.TrimSpace(updatedAt),
		"attempt":             1,
		"binding":             op.binding,
		"capability_snapshot": op.capabilitySnapshot,
		"request": map[string]any{
			"request_hash":      op.operationKey,
			"idempotency_scope": "workspace",
			"redaction_policy":  "secrets-redacted-v1",
			"summary":           strings.TrimSpace(op.operationName),
			"details":           op.requestDetails,
		},
		"fence": op.fence,
		"causality": map[string]any{
			"source":      "server",
			"parent_refs": []string{},
		},
	}
	if terminal {
		payload["terminal_at"] = strings.TrimSpace(updatedAt)
	}
	if result != nil {
		payload["result"] = result
	}
	return payload
}

func toolLifecycleOperationResultSummary(result map[string]any) string {
	if result == nil {
		return ""
	}
	if summary, _ := result["summary"].(string); strings.TrimSpace(summary) != "" {
		return strings.TrimSpace(summary)
	}
	if status, _ := result["status"].(string); strings.TrimSpace(status) != "" {
		return strings.TrimSpace(status)
	}
	return ""
}

func toolLifecycleErrorResultMap(surface, toolID, operationID string, err error) map[string]any {
	summary := ""
	if err != nil {
		summary = err.Error()
	}
	return map[string]any{
		"surface_id":   strings.TrimSpace(surface),
		"operation_id": strings.TrimSpace(operationID),
		"tool_id":      strings.TrimSpace(toolID),
		"summary":      summary,
		"is_error":     true,
	}
}

func (h *Handler) recordToolLifecycleOperationLedger(ctx context.Context, op toolLifecycleOperationLedgerContext, runStatus, outcome string, terminal bool, result map[string]any) *RPCError {
	now := time.Now().UTC()
	payload := toolLifecycleOperationLedgerPayload(
		op,
		toolCallOperationLedgerStatus(runStatus),
		terminal,
		now.Format(time.RFC3339Nano),
		result,
	)
	surface := strings.ReplaceAll(strings.TrimPrefix(strings.TrimSpace(op.operationKind), "tool_"), "_", ".")
	if surface != "" {
		surface = "tool." + surface
	} else {
		surface = "tool.lifecycle"
	}
	summary := surface + " " + toolCallOperationLedgerStatus(runStatus) + ": " + op.operationName
	if resultSummary := toolLifecycleOperationResultSummary(result); resultSummary != "" {
		summary = resultSummary
	}
	_, _, rpcErr := h.recordOperationExecutionRun(ctx, sqlite.ExecutionRunInput{
		RunID:       op.operationID,
		WorkspaceID: op.workspaceID,
		AgentID:     op.agentID,
		Title:       surface + ": " + op.operationName,
		Summary:     summary,
		Status:      runStatus,
		Outcome:     outcome,
		Verification: map[string]any{
			"operation_ledger": payload,
		},
	}, surface)
	return rpcErr
}

func toolCallOperationLedgerStatus(runStatus string) string {
	switch strings.ToUpper(strings.TrimSpace(runStatus)) {
	case "ACTIVE":
		return "running"
	case "COMPLETED":
		return "completed"
	case "FAILED":
		return "failed"
	case "TIMED_OUT":
		return "timed_out"
	case "CANCELLED":
		return "cancelled"
	default:
		return strings.ToLower(strings.TrimSpace(runStatus))
	}
}

func toolCallResultMap(toolID string, result *tools.CallResult) map[string]any {
	if result == nil {
		return nil
	}
	return map[string]any{
		"tool_id":   strings.TrimSpace(toolID),
		"stdout":    result.Stdout,
		"stderr":    result.Stderr,
		"exit_code": result.ExitCode,
		"timed_out": result.TimedOut,
	}
}

func toolCallErrorResultMap(toolID string, err error) map[string]any {
	summary := ""
	if err != nil {
		summary = err.Error()
	}
	result := map[string]any{
		"tool_id":   strings.TrimSpace(toolID),
		"summary":   summary,
		"is_error":  true,
		"timed_out": false,
	}
	if errors.Is(err, context.DeadlineExceeded) {
		result["timed_out"] = true
		result["exit_code"] = -1
	}
	if errors.Is(err, context.Canceled) {
		result["canceled"] = true
	}
	if diagnostics, ok := mcp.StdioDiagnosticsFromError(err); ok {
		result["mcp_transport"] = "stdio"
		result["child_stdout_closed"] = diagnostics.StdoutClosed
		if stderr := strings.TrimSpace(diagnostics.Stderr); stderr != "" {
			result["stderr"] = stderr
			result["stderr_ref"] = "inline_bounded_stderr"
		} else {
			result["stderr_unavailable_reason"] = "no_stderr_captured"
		}
	}
	return result
}

func attachRoutedMCPTransportResult(result map[string]any, routed bool, transport string) map[string]any {
	if result == nil || !routed {
		return result
	}
	if existing, _ := result["mcp_transport"].(string); strings.TrimSpace(existing) != "" {
		return result
	}
	result["mcp_transport"] = strings.TrimSpace(firstNonEmpty(transport, "unknown"))
	return result
}

func attachToolCallOperationID(result map[string]any, operationID string) map[string]any {
	if result == nil {
		return result
	}
	result["operation_id"] = strings.TrimSpace(operationID)
	return result
}

func attachToolCallOperationIDToRPCError(rpcErr *RPCError, operationID string, result map[string]any) *RPCError {
	if rpcErr == nil {
		return nil
	}
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return rpcErr
	}
	clone := *rpcErr
	details := map[string]any{}
	switch existing := rpcErr.Details.(type) {
	case nil:
	case map[string]any:
		for key, value := range existing {
			details[key] = value
		}
	default:
		details["previous_details"] = existing
	}
	details["operation_id"] = operationID
	for _, key := range []string{"tool_id", "is_error", "timed_out", "canceled", "exit_code", "mcp_transport", "router_kind"} {
		if result == nil {
			continue
		}
		if value, ok := result[key]; ok {
			details[key] = value
		}
	}
	clone.Details = details
	return &clone
}

func toolCallTerminalStateFromResult(result map[string]any, callErr error) (status string, outcome string, timedOut bool, canceled bool) {
	if result != nil {
		if b, ok := result["timed_out"].(bool); ok && b {
			return "TIMED_OUT", "TIMED_OUT", true, false
		}
		if b, ok := result["canceled"].(bool); ok && b {
			return "CANCELLED", "CANCELLED", false, true
		}
		if b, ok := result["is_error"].(bool); ok && b {
			return "FAILED", "FAILED", false, false
		}
		switch exitCode := result["exit_code"].(type) {
		case int:
			if exitCode != 0 {
				return "FAILED", "FAILED", false, false
			}
		case int64:
			if exitCode != 0 {
				return "FAILED", "FAILED", false, false
			}
		case float64:
			if int(exitCode) != 0 {
				return "FAILED", "FAILED", false, false
			}
		}
	}
	switch {
	case errors.Is(callErr, context.DeadlineExceeded):
		return "TIMED_OUT", "TIMED_OUT", true, false
	case errors.Is(callErr, context.Canceled):
		return "CANCELLED", "CANCELLED", false, true
	case callErr != nil:
		return "FAILED", "FAILED", false, false
	case result != nil:
		return "COMPLETED", "COMPLETED", false, false
	default:
		return "FAILED", "FAILED", false, false
	}
}

type toolCallOperationLedgerContext struct {
	operationID        string
	operationKey       string
	operationName      string
	createdAt          time.Time
	deadlineAt         time.Time
	timeoutSec         int
	workspaceID        string
	agentID            string
	taskID             string
	sessionID          string
	parentRunID        string
	binding            map[string]any
	capabilitySnapshot map[string]any
	requestDetails     map[string]any
	fence              map[string]any
}

func (h *Handler) recordOperationExecutionRun(ctx context.Context, run sqlite.ExecutionRunInput, surface string) (sqlite.ExecutionRunRecord, sqlite.RuntimeEventRecord, *RPCError) {
	surface = strings.TrimSpace(firstNonEmpty(surface, "tool.call"))
	if run.PromptContextEnvelope == nil {
		run.PromptContextEnvelope = h.operationLedgerPromptContextEnvelope(ctx, run, surface)
	}
	record, event, err := h.store.UpsertExecutionRunWithEvent(ctx, run)
	if err != nil {
		if isControlPlaneValidationError(err) {
			return sqlite.ExecutionRunRecord{}, sqlite.RuntimeEventRecord{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return sqlite.ExecutionRunRecord{}, sqlite.RuntimeEventRecord{}, rpcErrorFromStoreAuthority(err, surface)
	}
	return record, event, nil
}

func (h *Handler) operationLedgerPromptContextEnvelope(ctx context.Context, run sqlite.ExecutionRunInput, surface string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	if principalID == "server_rpc" && strings.TrimSpace(run.AgentID) != "" {
		principalType = "agent"
		principalID = strings.TrimSpace(run.AgentID)
	}
	return sqlite.BuildExecutionPromptContextEnvelope(
		surface,
		"server_operation_ledger",
		run.WorkspaceID,
		principalType,
		principalID,
	)
}

func (h *Handler) recordToolCallExecutionRun(ctx context.Context, run sqlite.ExecutionRunInput) (sqlite.ExecutionRunRecord, sqlite.RuntimeEventRecord, *RPCError) {
	return h.recordOperationExecutionRun(ctx, run, "tool.call")
}

func (h *Handler) toolCallRuntimePayloadJSON(ctx context.Context, workspaceID, toolID, actorType, actorID, eventType, requestedCapability, operationID string, payload map[string]any) (string, *RPCError) {
	workspaceID = strings.TrimSpace(workspaceID)
	toolID = strings.TrimSpace(toolID)
	actorType = strings.TrimSpace(actorType)
	actorID = strings.TrimSpace(actorID)
	eventType = strings.TrimSpace(eventType)
	requestedCapability = strings.TrimSpace(firstNonEmpty(requestedCapability, "tool.call"))
	operationID = strings.TrimSpace(operationID)
	if payload == nil {
		payload = map[string]any{}
	} else {
		copied := make(map[string]any, len(payload)+10)
		for key, value := range payload {
			copied[key] = value
		}
		payload = copied
	}
	fields := map[string]string{
		"workspace_id":          workspaceID,
		"tool_id":               toolID,
		"event_type":            eventType,
		"entity_type":           "tool",
		"entity_id":             toolID,
		"actor_type":            actorType,
		"actor_id":              actorID,
		"requested_capability":  requestedCapability,
		"authority_event_scope": "tool.call",
	}
	if operationID != "" {
		fields["operation_id"] = operationID
	}
	for key, value := range fields {
		if strings.TrimSpace(value) != "" {
			payload[key] = value
		}
	}
	principalType, principalID := h.promptContextPrincipal(ctx)
	envelope := sqlite.BuildToolCallPromptContextEnvelope("tool.call", "server_rpc", workspaceID, principalType, principalID)
	payload, err := sqlite.AttachToolCallPromptContextEnvelope(payload, envelope, fields)
	if err != nil {
		return "", &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return string(mustJSON(payload)), nil
}

func attachToolCallExecutionBinding(payload map[string]any, taskID, sessionID, parentRunID string) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	if strings.TrimSpace(taskID) != "" {
		payload["task_id"] = strings.TrimSpace(taskID)
	}
	if strings.TrimSpace(sessionID) != "" {
		payload["session_id"] = strings.TrimSpace(sessionID)
	}
	if strings.TrimSpace(parentRunID) != "" {
		payload["task_run_id"] = strings.TrimSpace(parentRunID)
		payload["parent_run_id"] = strings.TrimSpace(parentRunID)
	}
	return payload
}

func (h *Handler) recordToolCallOperationLedger(ctx context.Context, op toolCallOperationLedgerContext, runStatus, outcome string, terminal bool, result map[string]any) *RPCError {
	now := time.Now().UTC()
	payload := toolCallOperationLedgerPayload(
		op.operationID,
		op.operationKey,
		op.operationName,
		toolCallOperationLedgerStatus(runStatus),
		terminal,
		op.createdAt.UTC().Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
		op.deadlineAt.UTC().Format(time.RFC3339Nano),
		op.timeoutSec,
		op.binding,
		op.capabilitySnapshot,
		op.requestDetails,
		op.fence,
		result,
	)
	summary := "tool.call " + toolCallOperationLedgerStatus(runStatus) + ": " + op.operationName
	if resultSummary := toolCallOperationResultSummary(result); resultSummary != "" {
		summary = resultSummary
	}
	_, _, rpcErr := h.recordToolCallExecutionRun(ctx, sqlite.ExecutionRunInput{
		RunID:       op.operationID,
		WorkspaceID: op.workspaceID,
		TaskID:      op.taskID,
		SessionID:   op.sessionID,
		AgentID:     op.agentID,
		Title:       "Tool call: " + op.operationName,
		Summary:     summary,
		Status:      runStatus,
		Outcome:     outcome,
		Verification: map[string]any{
			"operation_ledger": payload,
		},
	})
	return rpcErr
}

type toolCallParams struct {
	ToolID              string         `json:"tool_id"`
	WorkspaceID         string         `json:"workspace_id"`
	Arguments           map[string]any `json:"arguments"`
	TimeoutSec          int            `json:"timeout_sec"`
	ActorType           string         `json:"actor_type,omitempty"`
	ActorID             string         `json:"actor_id,omitempty"`
	RequestedCapability string         `json:"requested_capability,omitempty"`
	TaskID              string         `json:"task_id,omitempty"`
	SessionID           string         `json:"session_id,omitempty"`
	RunID               string         `json:"run_id,omitempty"`
}

func resolveRegisteredToolCallSubject(principal AuthPrincipal, actorType, actorID string) (string, string, *RPCError) {
	actorType = strings.TrimSpace(actorType)
	actorID = strings.TrimSpace(actorID)
	if (actorType == "") != (actorID == "") {
		return "", "", &RPCError{Code: errCodeInvalidParams, Message: "actor_type and actor_id must be provided together"}
	}
	if actorType == "" && actorID == "" {
		return principal.PrincipalType, principal.PrincipalID, nil
	}
	if actorType != principal.PrincipalType || actorID != principal.PrincipalID {
		return "", "", &RPCError{
			Code:    errCodePermissionDenied,
			Message: "actor context does not match authenticated principal",
			Details: map[string]any{
				"principal_type": principal.PrincipalType,
				"principal_id":   principal.PrincipalID,
				"actor_type":     actorType,
				"actor_id":       actorID,
			},
		}
	}
	return actorType, actorID, nil
}

func normalizeRegisteredToolCallCapability(requested string) (string, *RPCError) {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return "tool.call", nil
	}
	if requested != "tool.call" {
		return "", &RPCError{
			Code:    errCodeInvalidParams,
			Message: "registered tool execution only supports requested_capability=tool.call",
		}
	}
	return requested, nil
}

func (h *Handler) preflightMCPAliasUse(ctx context.Context, workspaceID string, record sqlite.WorkspaceToolRecord, principal AuthPrincipal) *RPCError {
	manifest := parseWorkspaceToolManifest(record.ManifestJSON)
	if manifest.Route == nil || !strings.EqualFold(strings.TrimSpace(manifest.Route.Kind), "mcp") {
		return nil
	}
	server, err := h.mcpStore.GetServer(ctx, workspaceID, manifest.Route.ServerID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &RPCError{Code: errCodeInvalidParams, Message: "server not found"}
		}
		return &RPCError{Code: errCodeInternal, Message: "server not found: " + err.Error()}
	}
	if !mcpServerVisibleToPrincipal(server, principal) {
		return &RPCError{
			Code:    errCodePermissionDenied,
			Message: "mcp server is owned by another principal",
		}
	}
	if mcpWorkspaceToolClassificationStale(record, server) {
		return &RPCError{
			Code:    errCodeInvalidParams,
			Message: "mcp tool alias metadata is stale; run mcp.tool.discover again",
		}
	}
	return nil
}

func (h *Handler) toolCall(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p toolCallParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if sqlite.IsRemovedWorkspaceToolID(p.ToolID) {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: fmt.Sprintf("workspace tool %q has been removed from Rhizome", p.ToolID)}
	}

	principal, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}

	publishToolRuntimeEvent := func(event sqlite.RuntimeEventRecord) {
		h.publishRuntimeEventRecord(event)
	}
	record, err := h.store.GetWorkspaceTool(ctx, p.WorkspaceID, p.ToolID)
	var toolRecord *sqlite.WorkspaceToolRecord
	if err == nil {
		toolRecord = &record
	} else if !errors.Is(err, sqlite.ErrToolNotFound) {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	actorType, actorID, rpcErr := resolveRegisteredToolCallSubject(principal, p.ActorType, p.ActorID)
	if rpcErr != nil {
		return nil, rpcErr
	}
	requestedCapability, rpcErr := normalizeRegisteredToolCallCapability(p.RequestedCapability)
	if rpcErr != nil {
		return nil, rpcErr
	}
	taskID := strings.TrimSpace(p.TaskID)
	sessionID := strings.TrimSpace(p.SessionID)
	parentRunID := strings.TrimSpace(p.RunID)
	liveBindingProof := sqlite.ExecutionRunBindingProof{}
	if taskID != "" || sessionID != "" || parentRunID != "" {
		if !strings.EqualFold(actorType, "agent") {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: "tool call execution binding requires an agent actor"}
		}
		proof, err := h.store.VerifyLiveExecutionRunBinding(ctx, sqlite.ExecutionRunLiveBindingInput{
			WorkspaceID: p.WorkspaceID,
			AgentID:     actorID,
			TaskID:      taskID,
			SessionID:   sessionID,
			ParentRunID: parentRunID,
		})
		if err != nil {
			if errors.Is(err, sqlite.ErrExecutionRunBindingInvalid) {
				return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
			}
			return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
		}
		liveBindingProof = proof
	}
	operationAgentID := ""
	if strings.EqualFold(actorType, "agent") {
		if _, err := h.store.GetAgent(ctx, p.WorkspaceID, actorID); err == nil {
			operationAgentID = actorID
		}
	}
	if toolRecord != nil {
		if rpcErr := h.preflightMCPAliasUse(ctx, p.WorkspaceID, *toolRecord, principal); rpcErr != nil {
			return nil, rpcErr
		}
	}
	authority, rpcErr := h.loadWorkspaceOperatorWriteAuthority(ctx, p.WorkspaceID, "tool.call")
	if rpcErr != nil {
		return nil, rpcErr
	}
	highRiskHandled := false
	if toolRecord != nil {
		if handled, rpcErr := h.enforceHighRiskToolCallGate(ctx, authority, *toolRecord, p.WorkspaceID, actorType, actorID, requestedCapability, publishToolRuntimeEvent); rpcErr != nil {
			return nil, rpcErr
		} else if handled {
			highRiskHandled = true
		}
	}
	if !highRiskHandled {
		check, err := h.store.CheckCapabilityPolicy(ctx, sqlite.CapabilityCheckInput{
			WorkspaceID: p.WorkspaceID,
			SubjectType: actorType,
			SubjectID:   actorID,
			Capability:  requestedCapability,
			ToolID:      p.ToolID,
		})
		if err != nil {
			if isControlPlaneValidationError(err) {
				return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
			}
			return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
		}
		switch check.Verdict {
		case "DENY":
			payloadJSON, rpcErr := h.toolCallRuntimePayloadJSON(ctx, p.WorkspaceID, p.ToolID, actorType, actorID, "tool.call.denied", requestedCapability, "", attachToolCallExecutionBinding(map[string]any{
				"policy_verdict": check.Verdict,
				"policy_check":   check,
			}, p.TaskID, p.SessionID, p.RunID))
			if rpcErr != nil {
				return nil, rpcErr
			}
			if event, rpcErr := h.recordAuthorityBackedRuntimeEvent(ctx, authority, "tool.call", sqlite.RuntimeEventInput{
				WorkspaceID: p.WorkspaceID,
				EventType:   "tool.call.denied",
				EntityType:  "tool",
				EntityID:    p.ToolID,
				ActorType:   actorType,
				ActorID:     actorID,
				AgentID:     operationAgentID,
				SessionID:   sessionID,
				TaskID:      taskID,
				PayloadJSON: payloadJSON,
			}); rpcErr != nil {
				return nil, rpcErr
			} else {
				publishToolRuntimeEvent(event)
			}
			return nil, &RPCError{Code: errCodePermissionDenied, Message: "tool call denied by capability policy", Details: map[string]any{"check": check}}
		case "REQUIRE_APPROVAL":
			payloadJSON, rpcErr := h.toolCallRuntimePayloadJSON(ctx, p.WorkspaceID, p.ToolID, actorType, actorID, "tool.call.approval_required", requestedCapability, "", attachToolCallExecutionBinding(map[string]any{
				"policy_verdict": check.Verdict,
				"policy_check":   check,
			}, p.TaskID, p.SessionID, p.RunID))
			if rpcErr != nil {
				return nil, rpcErr
			}
			if event, rpcErr := h.recordAuthorityBackedRuntimeEvent(ctx, authority, "tool.call", sqlite.RuntimeEventInput{
				WorkspaceID: p.WorkspaceID,
				EventType:   "tool.call.approval_required",
				EntityType:  "tool",
				EntityID:    p.ToolID,
				ActorType:   actorType,
				ActorID:     actorID,
				AgentID:     operationAgentID,
				SessionID:   sessionID,
				TaskID:      taskID,
				PayloadJSON: payloadJSON,
			}); rpcErr != nil {
				return nil, rpcErr
			} else {
				publishToolRuntimeEvent(event)
			}
			return nil, &RPCError{Code: errCodePermissionDenied, Message: "tool call requires approval", Details: map[string]any{"check": check}}
		}
	}

	routedCandidate := false
	routedMCPTransport := ""
	if toolRecord != nil {
		manifest := parseWorkspaceToolManifest(toolRecord.ManifestJSON)
		routedCandidate = manifest.Route != nil && strings.EqualFold(strings.TrimSpace(manifest.Route.Kind), "mcp")
		if routedCandidate {
			if server, err := h.mcpStore.GetServer(ctx, p.WorkspaceID, manifest.Route.ServerID); err == nil {
				routedMCPTransport = strings.TrimSpace(server.Transport)
			}
		}
	}
	operationStartedAt := time.Now().UTC()
	operationRequest := map[string]any{
		"tool_id":              p.ToolID,
		"workspace_id":         p.WorkspaceID,
		"arguments":            p.Arguments,
		"timeout_sec":          p.TimeoutSec,
		"actor_type":           actorType,
		"actor_id":             actorID,
		"requested_capability": requestedCapability,
		"routed":               routedCandidate,
	}
	if taskID != "" {
		operationRequest["task_id"] = taskID
	}
	if sessionID != "" {
		operationRequest["session_id"] = sessionID
	}
	if parentRunID != "" {
		operationRequest["parent_run_id"] = parentRunID
	}
	operationKey := "sha256:" + toolCallOperationRequestHash(operationRequest)
	operationID := toolCallOperationID(operationKey, operationStartedAt)
	operationName := toolCallOperationName(p.ToolID, toolRecord)
	operationLedger := toolCallOperationLedgerContext{
		operationID:        operationID,
		operationKey:       operationKey,
		operationName:      operationName,
		createdAt:          operationStartedAt,
		deadlineAt:         operationStartedAt.Add(tools.EffectiveCallTimeout(ctx, p.TimeoutSec)),
		timeoutSec:         p.TimeoutSec,
		workspaceID:        p.WorkspaceID,
		agentID:            operationAgentID,
		taskID:             taskID,
		sessionID:          sessionID,
		parentRunID:        parentRunID,
		binding:            attachToolCallLiveBindingProof(toolCallOperationBinding(p.WorkspaceID, principal.PrincipalType, principal.PrincipalID, actorType, actorID, authority, operationID, taskID, sessionID, parentRunID), liveBindingProof),
		capabilitySnapshot: toolCallOperationCapabilitySnapshot(requestedCapability, toolRecord, "ALLOW"),
		requestDetails:     toolCallOperationRequestDetails(p.ToolID, toolRecord, requestedCapability, p.TimeoutSec, routedCandidate, routedMCPTransport, taskID, sessionID, parentRunID),
		fence:              toolCallOperationFence(authority, operationID),
	}
	ledgerCtx := context.WithoutCancel(ctx)
	if rpcErr := h.recordToolCallOperationLedger(ledgerCtx, operationLedger, "ACTIVE", "RUNNING", false, nil); rpcErr != nil {
		return nil, rpcErr
	}

	routedCtx := ctx
	routedCancel := func() {}
	if routedCandidate {
		routedCtx, routedCancel = context.WithTimeout(ctx, tools.EffectiveCallTimeout(ctx, p.TimeoutSec))
	}
	defer routedCancel()

	if routed, ok, err := h.routedToolCall(routedCtx, p.WorkspaceID, p.ToolID, p.Arguments); err != nil {
		result := toolCallErrorResultMap(p.ToolID, err)
		attachRoutedMCPTransportResult(result, routedCandidate, routedMCPTransport)
		attachToolCallOperationID(result, operationLedger.operationID)
		status, outcome, _, _ := toolCallTerminalStateFromResult(result, err)
		if rpcErr := h.recordToolCallOperationLedger(ledgerCtx, operationLedger, status, outcome, true, result); rpcErr != nil {
			return nil, attachToolCallOperationIDToRPCError(rpcErr, operationLedger.operationID, result)
		}
		return nil, attachToolCallOperationIDToRPCError(&RPCError{Code: errCodeInternal, Message: err.Error()}, operationLedger.operationID, result)
	} else if ok {
		attachRoutedMCPTransportResult(routed, routedCandidate, routedMCPTransport)
		attachToolCallOperationID(routed, operationLedger.operationID)
		status, outcome, _, _ := toolCallTerminalStateFromResult(routed, nil)
		if rpcErr := h.recordToolCallOperationLedger(ledgerCtx, operationLedger, status, outcome, true, routed); rpcErr != nil {
			return nil, rpcErr
		}
		payloadJSON, rpcErr := h.toolCallRuntimePayloadJSON(ctx, p.WorkspaceID, p.ToolID, actorType, actorID, "tool.call.executed", requestedCapability, operationLedger.operationID, attachToolCallExecutionBinding(map[string]any{
			"exit_code":     routed["exit_code"],
			"timed_out":     routed["timed_out"],
			"router_kind":   routed["router_kind"],
			"is_error":      routed["is_error"],
			"mcp_transport": routed["mcp_transport"],
		}, taskID, sessionID, parentRunID))
		if rpcErr != nil {
			return nil, rpcErr
		}
		if event, rpcErr := h.recordAuthorityBackedRuntimeEvent(ctx, authority, "tool.call", sqlite.RuntimeEventInput{
			WorkspaceID: p.WorkspaceID,
			EventType:   "tool.call.executed",
			EntityType:  "tool",
			EntityID:    p.ToolID,
			ActorType:   actorType,
			ActorID:     actorID,
			AgentID:     operationAgentID,
			SessionID:   sessionID,
			TaskID:      taskID,
			PayloadJSON: payloadJSON,
		}); rpcErr != nil {
			log.Printf("[tool.call] executed runtime event failed after terminal ledger workspace=%s tool=%s operation=%s: %s", p.WorkspaceID, p.ToolID, operationLedger.operationID, rpcErr.Message)
			return nil, attachToolCallOperationIDToRPCError(&RPCError{Code: errCodeInternal, Message: "tool call completed but runtime event journal append failed: " + rpcErr.Message}, operationLedger.operationID, routed)
		} else {
			publishToolRuntimeEvent(event)
		}
		return routed, nil
	}

	result, err := h.toolExec.Call(ctx, tools.CallInput{
		ToolID:      p.ToolID,
		WorkspaceID: p.WorkspaceID,
		Arguments:   p.Arguments,
		TimeoutSec:  p.TimeoutSec,
	})
	if err != nil {
		resultPayload := toolCallErrorResultMap(p.ToolID, err)
		attachToolCallOperationID(resultPayload, operationLedger.operationID)
		status, outcome, _, _ := toolCallTerminalStateFromResult(resultPayload, err)
		if rpcErr := h.recordToolCallOperationLedger(ledgerCtx, operationLedger, status, outcome, true, resultPayload); rpcErr != nil {
			return nil, attachToolCallOperationIDToRPCError(rpcErr, operationLedger.operationID, resultPayload)
		}
		return nil, attachToolCallOperationIDToRPCError(&RPCError{Code: errCodeInternal, Message: err.Error()}, operationLedger.operationID, resultPayload)
	}
	resultPayload := toolCallResultMap(p.ToolID, result)
	attachToolCallOperationID(resultPayload, operationLedger.operationID)
	status, outcome, _, _ := toolCallTerminalStateFromResult(resultPayload, nil)
	if rpcErr := h.recordToolCallOperationLedger(ledgerCtx, operationLedger, status, outcome, true, resultPayload); rpcErr != nil {
		return nil, rpcErr
	}
	payloadJSON, rpcErr := h.toolCallRuntimePayloadJSON(ctx, p.WorkspaceID, p.ToolID, actorType, actorID, "tool.call.executed", requestedCapability, operationLedger.operationID, attachToolCallExecutionBinding(map[string]any{
		"exit_code": result.ExitCode,
		"timed_out": result.TimedOut,
	}, taskID, sessionID, parentRunID))
	if rpcErr != nil {
		return nil, rpcErr
	}
	if event, rpcErr := h.recordAuthorityBackedRuntimeEvent(ctx, authority, "tool.call", sqlite.RuntimeEventInput{
		WorkspaceID: p.WorkspaceID,
		EventType:   "tool.call.executed",
		EntityType:  "tool",
		EntityID:    p.ToolID,
		ActorType:   actorType,
		ActorID:     actorID,
		AgentID:     operationAgentID,
		SessionID:   sessionID,
		TaskID:      taskID,
		PayloadJSON: payloadJSON,
	}); rpcErr != nil {
		log.Printf("[tool.call] executed runtime event failed after terminal ledger workspace=%s tool=%s operation=%s: %s", p.WorkspaceID, p.ToolID, operationLedger.operationID, rpcErr.Message)
		return nil, attachToolCallOperationIDToRPCError(&RPCError{Code: errCodeInternal, Message: "tool call completed but runtime event journal append failed: " + rpcErr.Message}, operationLedger.operationID, resultPayload)
	} else {
		publishToolRuntimeEvent(event)
	}
	return resultPayload, nil
}

type toolUndeployParams struct {
	ToolID      string `json:"tool_id"`
	WorkspaceID string `json:"workspace_id"`
}

func (h *Handler) toolUndeploy(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p toolUndeployParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	principal, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}

	previousDeployment, previousDeploymentErr := h.toolLifecyclePreviousDeployment(p.WorkspaceID, p.ToolID)
	operationLedger := h.newToolUndeployOperationLedger(ctx, p, principal, previousDeployment, previousDeploymentErr)
	ledgerCtx := context.WithoutCancel(ctx)
	ledgerDegraded := false
	if rpcErr := h.recordToolLifecycleOperationLedger(ledgerCtx, operationLedger, "ACTIVE", "RUNNING", false, nil); rpcErr != nil {
		ledgerDegraded = true
		log.Printf("[tool.undeploy] active operation ledger degraded workspace=%s tool=%s operation=%s: %s", p.WorkspaceID, p.ToolID, operationLedger.operationID, rpcErr.Message)
	}

	if err := h.toolExec.Undeploy(p.WorkspaceID, p.ToolID); err != nil {
		result := toolLifecycleErrorResultMap("tool.undeploy", p.ToolID, operationLedger.operationID, err)
		if rpcErr := h.recordToolLifecycleOperationLedger(ledgerCtx, operationLedger, "FAILED", "FAILED", true, result); rpcErr != nil {
			log.Printf("[tool.undeploy] terminal operation ledger degraded workspace=%s tool=%s operation=%s: %s", p.WorkspaceID, p.ToolID, operationLedger.operationID, rpcErr.Message)
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	result := map[string]any{
		"operation_id":        operationLedger.operationID,
		"tool_id":             p.ToolID,
		"previous_deployment": previousDeployment,
		"removed":             previousDeployment,
		"status":              "UNDEPLOYED",
	}
	if previousDeploymentErr != "" {
		result["previous_deployment_check_error"] = previousDeploymentErr
	}
	if rpcErr := h.recordToolLifecycleOperationLedger(ledgerCtx, operationLedger, "COMPLETED", "COMPLETED", true, result); rpcErr != nil {
		ledgerDegraded = true
		log.Printf("[tool.undeploy] terminal operation ledger degraded workspace=%s tool=%s operation=%s: %s", p.WorkspaceID, p.ToolID, operationLedger.operationID, rpcErr.Message)
	}
	if ledgerDegraded {
		result["operation_ledger_degraded"] = true
	}
	return result, nil
}

// ── SSE Event Stream ────────────────────────────────────────────────

// EventBus is a simple pub/sub for workspace events.
type EventBus struct {
	shards      sync.Map // workspace_id -> *workspaceSubs
	globalMu    sync.RWMutex
	globalSubs  map[chan EventMessage]struct{}
	middlewares []func(EventMessage)
}

type workspaceSubs struct {
	mu   sync.RWMutex
	subs map[chan EventMessage]struct{}
}

// EventMessage is a single event.
type EventMessage struct {
	Type               string `json:"type"`
	CanonicalEventType string `json:"canonical_event_type,omitempty"`
	WorkspaceID        string `json:"workspace_id"`
	AgentID            string `json:"agent_id,omitempty"`
	EventID            string `json:"event_id,omitempty"`
	IngestSeq          int64  `json:"ingest_seq,omitempty"`
	DedupKey           string `json:"dedup_key,omitempty"`
	EntityType         string `json:"entity_type,omitempty"`
	EntityID           string `json:"entity_id,omitempty"`
	RootCauseID        string `json:"root_cause_id,omitempty"`
	ProvenanceGroupID  string `json:"provenance_group_id,omitempty"`
	ParentRefsJSON     string `json:"parent_refs_json,omitempty"`
	Summary            string `json:"summary"`
	Timestamp          string `json:"timestamp"`
	PayloadJSON        string `json:"payload_json,omitempty"`
}

// NewEventBus creates a new EventBus.
func NewEventBus() *EventBus {
	return &EventBus{
		globalSubs: make(map[chan EventMessage]struct{}),
	}
}

// RegisterMiddleware adds a function to observe events before publication.
func (b *EventBus) RegisterMiddleware(mw func(EventMessage)) {
	b.globalMu.Lock()
	defer b.globalMu.Unlock()
	b.middlewares = append(b.middlewares, mw)
}

func (b *EventBus) getShard(workspaceID string) *workspaceSubs {
	v, ok := b.shards.Load(workspaceID)
	if ok {
		return v.(*workspaceSubs)
	}
	newShard := &workspaceSubs{
		subs: make(map[chan EventMessage]struct{}),
	}
	actual, _ := b.shards.LoadOrStore(workspaceID, newShard)
	return actual.(*workspaceSubs)
}

// Subscribe returns a channel that receives events for the given workspace, or nil if the limit is reached.
func (b *EventBus) Subscribe(workspaceID string) chan EventMessage {
	shard := b.getShard(workspaceID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if len(shard.subs) >= 50 {
		return nil
	}

	ch := make(chan EventMessage, 32)
	shard.subs[ch] = struct{}{}
	return ch
}

// Unsubscribe removes a channel from the workspace.
func (b *EventBus) Unsubscribe(workspaceID string, ch chan EventMessage) {
	shard := b.getShard(workspaceID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, ok := shard.subs[ch]; ok {
		delete(shard.subs, ch)
		close(ch)
	}
}

// SubscribeGlobal returns a channel that receives events for all workspaces.
func (b *EventBus) SubscribeGlobal() chan EventMessage {
	b.globalMu.Lock()
	defer b.globalMu.Unlock()

	ch := make(chan EventMessage, 128)
	b.globalSubs[ch] = struct{}{}
	return ch
}

// UnsubscribeGlobal removes a global channel.
func (b *EventBus) UnsubscribeGlobal(ch chan EventMessage) {
	b.globalMu.Lock()
	defer b.globalMu.Unlock()

	if _, ok := b.globalSubs[ch]; ok {
		delete(b.globalSubs, ch)
		close(ch)
	}
}

// Publish sends an event to all subscribers of the workspace.
func (b *EventBus) Publish(msg EventMessage) {
	b.globalMu.RLock()
	mws := b.middlewares
	b.globalMu.RUnlock()
	for _, mw := range mws {
		mw(msg)
	}

	// Publish to global subscribers
	b.globalMu.RLock()
	for ch := range b.globalSubs {
		select {
		case ch <- msg:
		default:
			// drop if subscriber is too slow
		}
	}
	b.globalMu.RUnlock()

	v, ok := b.shards.Load(msg.WorkspaceID)
	if !ok {
		return
	}
	shard := v.(*workspaceSubs)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	for ch := range shard.subs {
		select {
		case ch <- msg:
		default:
			// drop if subscriber is too slow
		}
	}
}

// ── Event Emit (called from handlers) ───────────────────────────────

type eventEmitParams struct {
	Type        string `json:"type"`
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	Summary     string `json:"summary"`
	PayloadJSON string `json:"payload_json"`
}

func isEphemeralEventType(eventType string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(eventType)), "ephemeral.")
}

func (h *Handler) eventEmit(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p eventEmitParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	workspaceID := strings.TrimSpace(p.WorkspaceID)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	eventType := strings.TrimSpace(p.Type)
	if rpcErr := requireTrimmedParam(eventType, "type"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(p.Summary, "summary"); rpcErr != nil {
		return nil, rpcErr
	}
	if !isEphemeralEventType(eventType) {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "event.emit only supports ephemeral.* event types; canonical runtime events must use dedicated handlers"}
	}
	principal, rpcErr := requireWorkspacePrincipal(ctx, workspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}
	payloadJSON := strings.TrimSpace(p.PayloadJSON)
	if payloadJSON != "" && !json.Valid([]byte(payloadJSON)) {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "payload_json must be valid JSON"}
	}
	agentID := strings.TrimSpace(p.AgentID)
	if principal.PrincipalType == "agent" {
		if agentID == "" {
			agentID = principal.PrincipalID
		} else if agentID != principal.PrincipalID {
			return nil, &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match agent_id"}
		}
	}
	msg := EventMessage{
		Type:        eventType,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Summary:     strings.TrimSpace(p.Summary),
		Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
		PayloadJSON: payloadJSON,
	}

	h.eventBus.Publish(msg)

	return map[string]any{
		"type":         eventType,
		"workspace_id": workspaceID,
		"status":       "EMITTED",
	}, nil
}

// ServeEventsHTTP returns an http.HandlerFunc for the SSE /events endpoint.
func (h *Handler) ServeEventsHTTP() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		workspaceID := r.URL.Query().Get("workspace_id")
		if workspaceID == "" {
			http.Error(w, "workspace_id required", http.StatusBadRequest)
			return
		}

		// P1A-004: enforce workspace principal scoping
		principal, ok := authPrincipalFromContext(r.Context())
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		// Legacy principals (empty WorkspaceID) are allowed for backward compatibility
		if principal.WorkspaceID != "" && principal.WorkspaceID != workspaceID {
			http.Error(w, "workspace isolation violation", http.StatusForbidden)
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		// P1A-005: auth-aware origin echo instead of wildcard
		if origin := strings.TrimSpace(r.Header.Get("Origin")); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
		}

		ch := h.eventBus.Subscribe(workspaceID)
		if ch == nil {
			http.Error(w, "too many active subscriptions for this workspace", http.StatusTooManyRequests)
			return
		}
		defer h.eventBus.Unsubscribe(workspaceID, ch)

		ctx := r.Context()

		// Send initial heartbeat
		fmt.Fprintf(w, ": connected to workspace %s\n\n", workspaceID)
		flusher.Flush()

		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-ch:
				fmt.Fprint(w, FormatSSE(msg))
				flusher.Flush()
			}
		}
	}
}

// FormatSSE formats an EventMessage as an SSE event.
func FormatSSE(msg EventMessage) string {
	data, _ := json.Marshal(msg)
	return fmt.Sprintf("event: %s\ndata: %s\n\n", msg.Type, string(data))
}
