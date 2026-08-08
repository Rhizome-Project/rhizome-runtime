package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/mcp"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	toolruntime "github.com/Rhizome-Project/rhizome-runtime/internal/tools"
)

var errMCPDiscoverStaleServerSnapshot = errors.New("mcp server changed during discovery; run mcp.tool.discover again")

const defaultMCPDiscoverTimeoutSec = 60

// ── MCP Server Operations ───────────────────────────────────────────

type mcpServerRegisterParams struct {
	ServerID     string `json:"server_id"`
	WorkspaceID  string `json:"workspace_id"`
	DisplayName  string `json:"display_name"`
	Transport    string `json:"transport"`
	URL          string `json:"url"`
	Command      string `json:"command"`
	ArgsJSON     string `json:"args_json"`
	EnvJSON      string `json:"env_json"`
	HeadersJSON  string `json:"headers_json"`
	RegisteredBy string `json:"registered_by"`
}

func (h *Handler) mcpServerRegister(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p mcpServerRegisterParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	principal, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}
	authority, rpcErr := h.loadWorkspaceOperatorWriteAuthority(ctx, p.WorkspaceID, "mcp.server.register")
	if rpcErr != nil {
		return nil, rpcErr
	}
	registeredBy := mcpRegisteredByForPrincipal(principal.PrincipalType, principal.PrincipalID)
	if registeredBy == "" {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "authenticated principal is missing an id"}
	}
	if declared := strings.TrimSpace(p.RegisteredBy); declared != "" &&
		!mcpRegisteredByMatchesPrincipal(declared, principal.PrincipalType, principal.PrincipalID) {
		return nil, &RPCError{
			Code:    errCodeInvalidParams,
			Message: "registered_by must match the authenticated principal",
		}
	}
	if strings.EqualFold(strings.TrimSpace(p.Transport), "stdio") && !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "system") {
		return nil, &RPCError{
			Code:    errCodePermissionDenied,
			Message: "stdio MCP registration requires an internal system path",
		}
	}
	existingServer := false
	if existing, err := h.mcpStore.GetServerAnyStatus(ctx, p.WorkspaceID, p.ServerID); err == nil {
		existingServer = true
		if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "system") &&
			!mcpRegisteredByMatchesPrincipal(existing.RegisteredBy, principal.PrincipalType, principal.PrincipalID) {
			return nil, &RPCError{
				Code:    errCodePermissionDenied,
				Message: "mcp server is owned by another principal",
			}
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, &RPCError{Code: errCodeInternal, Message: "load existing mcp server failed: " + err.Error()}
	}

	servers, err := h.mcpStore.ListServers(ctx, p.WorkspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if len(servers) >= 40 {
		return nil, &RPCError{Code: errCodeInternal, Message: "workspace MCP server limit reached (max 40)"}
	}

	started := time.Now().UTC()
	operation := h.newMCPServerLifecycleOperationLedgerContext(
		ctx,
		"mcp.server.register",
		"mcp_server_register",
		"mcpregister",
		p.WorkspaceID,
		p.ServerID,
		principal,
		authority,
		mcpServerRegisterRequestDetails(p, registeredBy, existingServer, len(servers)),
		started,
	)
	ledgerCtx := context.WithoutCancel(ctx)
	if startErr := h.recordMCPServerLifecycleOperationLedger(ledgerCtx, operation, "ACTIVE", "RUNNING", false, nil); startErr != nil {
		return nil, startErr
	}
	if err := h.registerMCPServerAndResetDiscoverState(ctx, authority, mcp.RegisterInput{
		ServerID:     p.ServerID,
		WorkspaceID:  p.WorkspaceID,
		DisplayName:  p.DisplayName,
		Transport:    p.Transport,
		URL:          p.URL,
		Command:      p.Command,
		ArgsJSON:     p.ArgsJSON,
		EnvJSON:      p.EnvJSON,
		HeadersJSON:  p.HeadersJSON,
		RegisteredBy: registeredBy,
	}, registeredBy, operation.operationID); err != nil {
		result := mcpServerLifecycleErrorResultMap("mcp.server.register", p.ServerID, err)
		if rpcErr := authorityRejectRPCError(err, "mcp.server.register"); rpcErr != nil {
			_ = h.recordMCPServerLifecycleOperationLedger(ledgerCtx, operation, "FAILED", "FAILED", true, result)
			return nil, rpcErr
		}
		_ = h.recordMCPServerLifecycleOperationLedger(ledgerCtx, operation, "FAILED", "FAILED", true, result)
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	result := mcpServerRegisterSuccessResultMap(p, registeredBy, existingServer, operation.operationID)
	if terminalErr := h.recordMCPServerLifecycleOperationLedger(ledgerCtx, operation, "COMPLETED", "COMPLETED", true, result); terminalErr != nil {
		result["operation_ledger_degraded"] = true
		result["operation_ledger_error"] = terminalErr.Message
	}
	return result, nil
}

type mcpServerListParams struct {
	WorkspaceID string `json:"workspace_id"`
}

func (h *Handler) mcpServerList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p mcpServerListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	principal, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}

	servers, err := h.mcpStore.ListServers(ctx, p.WorkspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	servers = filterVisibleMCPServers(servers, principal)
	// P1B-003: Redact at read
	for i := range servers {
		if servers[i].EnvJSON != "" && servers[i].EnvJSON != "{}" {
			servers[i].EnvJSON = "\"[REDACTED]\""
		}
		if servers[i].HeadersJSON != "" && servers[i].HeadersJSON != "{}" {
			servers[i].HeadersJSON = "\"[REDACTED]\""
		}
	}
	return map[string]any{
		"servers": servers,
		"count":   len(servers),
	}, nil
}

type mcpServerRemoveParams struct {
	WorkspaceID string `json:"workspace_id"`
	ServerID    string `json:"server_id"`
}

func (h *Handler) mcpServerRemove(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p mcpServerRemoveParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if p.WorkspaceID == "" {
		// Fallback for dashboards that might not send workspace_id yet, but we require authentication context
		principal, ok := authPrincipalFromContext(ctx)
		if !ok {
			return nil, &RPCError{Code: errCodePermissionDenied, Message: "unauthorized"}
		}
		p.WorkspaceID = principal.WorkspaceID
	}
	principal, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}
	authority, rpcErr := h.loadWorkspaceOperatorWriteAuthority(ctx, p.WorkspaceID, "mcp.server.remove")
	if rpcErr != nil {
		return nil, rpcErr
	}
	registeredBy := mcpRegisteredByForPrincipal(principal.PrincipalType, principal.PrincipalID)
	if registeredBy == "" {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "authenticated principal is missing an id"}
	}
	server, err := h.mcpStore.GetServer(ctx, p.WorkspaceID, p.ServerID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: "server not found"}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "system") &&
		!mcpRegisteredByMatchesPrincipal(server.RegisteredBy, principal.PrincipalType, principal.PrincipalID) {
		return nil, &RPCError{
			Code:    errCodePermissionDenied,
			Message: "mcp server is owned by another principal",
		}
	}
	started := time.Now().UTC()
	operation := h.newMCPServerLifecycleOperationLedgerContext(
		ctx,
		"mcp.server.remove",
		"mcp_server_remove",
		"mcpremove",
		p.WorkspaceID,
		p.ServerID,
		principal,
		authority,
		mcpServerRemoveRequestDetails(server, registeredBy),
		started,
	)
	ledgerCtx := context.WithoutCancel(ctx)
	if startErr := h.recordMCPServerLifecycleOperationLedger(ledgerCtx, operation, "ACTIVE", "RUNNING", false, nil); startErr != nil {
		return nil, startErr
	}
	if err := h.removeMCPServerAndResetDiscoverState(ctx, authority, p.WorkspaceID, p.ServerID, registeredBy, operation.operationID); err != nil {
		result := mcpServerLifecycleErrorResultMap("mcp.server.remove", p.ServerID, err)
		if rpcErr := authorityRejectRPCError(err, "mcp.server.remove"); rpcErr != nil {
			_ = h.recordMCPServerLifecycleOperationLedger(ledgerCtx, operation, "FAILED", "FAILED", true, result)
			return nil, rpcErr
		}
		_ = h.recordMCPServerLifecycleOperationLedger(ledgerCtx, operation, "FAILED", "FAILED", true, result)
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	result := mcpServerRemoveSuccessResultMap(server, registeredBy, operation.operationID)
	if terminalErr := h.recordMCPServerLifecycleOperationLedger(ledgerCtx, operation, "COMPLETED", "COMPLETED", true, result); terminalErr != nil {
		result["operation_ledger_degraded"] = true
		result["operation_ledger_error"] = terminalErr.Message
	}
	return result, nil
}

// ── MCP Tool Operations ─────────────────────────────────────────────

type mcpServerLifecycleOperationLedgerContext struct {
	operationID        string
	operationKey       string
	operationKind      string
	operationName      string
	surface            string
	workspaceID        string
	agentID            string
	createdAt          time.Time
	binding            map[string]any
	capabilitySnapshot map[string]any
	requestDetails     map[string]any
	fence              map[string]any
}

func (h *Handler) newMCPServerLifecycleOperationLedgerContext(ctx context.Context, surface, operationKind, prefix, workspaceID, serverID string, principal AuthPrincipal, authority sqlite.WorkspaceAuthorityRecord, requestDetails map[string]any, started time.Time) mcpServerLifecycleOperationLedgerContext {
	workspaceID = strings.TrimSpace(workspaceID)
	serverID = strings.TrimSpace(serverID)
	surface = strings.TrimSpace(surface)
	operationKind = strings.TrimSpace(operationKind)
	request := map[string]any{
		"operation_kind": operationKind,
		"server_id":      serverID,
		"surface":        surface,
		"workspace_id":   workspaceID,
	}
	for k, v := range requestDetails {
		request[k] = v
	}
	operationKey := "sha256:" + toolCallOperationRequestHash(request)
	operationID := operationLedgerID(prefix, operationKey, started)
	return mcpServerLifecycleOperationLedgerContext{
		operationID:   operationID,
		operationKey:  operationKey,
		operationKind: operationKind,
		operationName: surface + ":" + serverID,
		surface:       surface,
		workspaceID:   workspaceID,
		agentID:       h.registeredOperationAgentID(ctx, workspaceID, principal),
		createdAt:     started,
		binding: toolCallOperationBinding(
			workspaceID,
			principal.PrincipalType,
			principal.PrincipalID,
			principal.PrincipalType,
			principal.PrincipalID,
			authority,
			operationID,
			"",
			"",
			"",
		),
		capabilitySnapshot: map[string]any{
			"schema":                "capability_snapshot.v1",
			"surface_id":            surface,
			"requested_capability":  surface,
			"status":                "enabled",
			"policy_verdict":        "allow",
			"requires_authority":    true,
			"authority_model":       "workspace_operator_write_authority",
			"operation_ledger_kind": operationKind,
		},
		requestDetails: requestDetails,
		fence:          toolCallOperationFence(authority, operationID),
	}
}

func mcpServerRegisterRequestDetails(p mcpServerRegisterParams, registeredBy string, existingServer bool, serverCountBefore int) map[string]any {
	transport := strings.TrimSpace(p.Transport)
	if transport == "" {
		transport = "streamable-http"
	}
	return map[string]any{
		"args_present":         strings.TrimSpace(p.ArgsJSON) != "",
		"command_present":      strings.TrimSpace(p.Command) != "",
		"display_name_present": strings.TrimSpace(p.DisplayName) != "",
		"env_present":          strings.TrimSpace(p.EnvJSON) != "",
		"existing_server":      existingServer,
		"headers_present":      strings.TrimSpace(p.HeadersJSON) != "",
		"mcp_transport":        transport,
		"registered_by":        strings.TrimSpace(registeredBy),
		"server_count_before":  serverCountBefore,
		"url_present":          strings.TrimSpace(p.URL) != "",
	}
}

func mcpServerRemoveRequestDetails(existing mcp.ServerRecord, registeredBy string) map[string]any {
	transport := strings.TrimSpace(existing.Transport)
	if transport == "" {
		transport = "streamable-http"
	}
	return map[string]any{
		"command_present":      strings.TrimSpace(existing.Command) != "",
		"display_name_present": strings.TrimSpace(existing.DisplayName) != "",
		"mcp_transport":        transport,
		"registered_by":        strings.TrimSpace(registeredBy),
		"server_status_before": strings.TrimSpace(existing.Status),
		"url_present":          strings.TrimSpace(existing.URL) != "",
	}
}

func mcpServerLifecycleErrorResultMap(surface, serverID string, err error) map[string]any {
	result := map[string]any{
		"is_error":  true,
		"server_id": strings.TrimSpace(serverID),
		"status":    "FAILED",
		"surface":   strings.TrimSpace(surface),
		"summary":   "MCP server lifecycle operation failed",
		"timed_out": false,
	}
	if err != nil {
		result["summary"] = err.Error()
		if errors.Is(err, context.DeadlineExceeded) {
			result["timed_out"] = true
		}
		if errors.Is(err, context.Canceled) {
			result["canceled"] = true
		}
	}
	return result
}

func mcpServerRegisterSuccessResultMap(p mcpServerRegisterParams, registeredBy string, existingServer bool, operationID string) map[string]any {
	transport := strings.TrimSpace(p.Transport)
	if transport == "" {
		transport = "streamable-http"
	}
	return map[string]any{
		"discover_state_reset":     true,
		"existing_server_replaced": existingServer,
		"is_error":                 false,
		"mcp_transport":            transport,
		"operation_id":             operationID,
		"registered_by":            strings.TrimSpace(registeredBy),
		"server_id":                strings.TrimSpace(p.ServerID),
		"status":                   "REGISTERED",
		"timed_out":                false,
	}
}

func mcpServerRemoveSuccessResultMap(existing mcp.ServerRecord, registeredBy string, operationID string) map[string]any {
	transport := strings.TrimSpace(existing.Transport)
	if transport == "" {
		transport = "streamable-http"
	}
	return map[string]any{
		"discover_state_reset": true,
		"is_error":             false,
		"mcp_transport":        transport,
		"operation_id":         operationID,
		"registered_by":        strings.TrimSpace(registeredBy),
		"server_id":            strings.TrimSpace(existing.ServerID),
		"status":               "REMOVED",
		"timed_out":            false,
	}
}

func (h *Handler) recordMCPServerLifecycleOperationLedger(ctx context.Context, op mcpServerLifecycleOperationLedgerContext, runStatus, outcome string, terminal bool, result map[string]any) *RPCError {
	now := time.Now().UTC()
	payload := map[string]any{
		"schema":              "operation_ledger.v1",
		"operation_id":        strings.TrimSpace(op.operationID),
		"operation_key":       strings.TrimSpace(op.operationKey),
		"operation_kind":      strings.TrimSpace(op.operationKind),
		"operation_name":      strings.TrimSpace(op.operationName),
		"status":              toolCallOperationLedgerStatus(runStatus),
		"terminal":            terminal,
		"created_at":          op.createdAt.UTC().Format(time.RFC3339Nano),
		"started_at":          op.createdAt.UTC().Format(time.RFC3339Nano),
		"updated_at":          now.Format(time.RFC3339Nano),
		"attempt":             1,
		"binding":             op.binding,
		"capability_snapshot": op.capabilitySnapshot,
		"request": map[string]any{
			"request_hash":      op.operationKey,
			"idempotency_scope": "workspace",
			"redaction_policy":  "mcp-lifecycle-secrets-redacted-v1",
			"summary":           strings.TrimSpace(op.operationName),
			"details":           op.requestDetails,
		},
		"fence": op.fence,
		"causality": map[string]any{
			"source":      "server",
			"surface":     strings.TrimSpace(op.surface),
			"parent_refs": []string{},
		},
	}
	if terminal {
		payload["terminal_at"] = now.Format(time.RFC3339Nano)
	}
	if result != nil {
		payload["result"] = result
	}
	summary := op.operationKind + " " + toolCallOperationLedgerStatus(runStatus) + ": " + op.operationName
	if resultSummary := toolCallOperationResultSummary(result); resultSummary != "" {
		summary = resultSummary
	}
	_, _, rpcErr := h.recordOperationExecutionRun(ctx, sqlite.ExecutionRunInput{
		RunID:       op.operationID,
		WorkspaceID: op.workspaceID,
		AgentID:     op.agentID,
		Title:       "MCP server lifecycle: " + op.operationName,
		Summary:     summary,
		Status:      runStatus,
		Outcome:     outcome,
		Verification: map[string]any{
			"operation_ledger": payload,
		},
	}, op.surface)
	return rpcErr
}

type mcpToolDiscoverParams struct {
	WorkspaceID string `json:"workspace_id"`
	ServerID    string `json:"server_id"`
}

type mcpDiscoverOperationLedgerContext struct {
	operationID        string
	operationKey       string
	operationName      string
	createdAt          time.Time
	deadlineAt         time.Time
	timeoutMS          int
	workspaceID        string
	binding            map[string]any
	capabilitySnapshot map[string]any
	requestDetails     map[string]any
	fence              map[string]any
}

func newMCPDiscoverOperationLedgerContext(server mcp.ServerRecord, principal AuthPrincipal, authority sqlite.WorkspaceAuthorityRecord, timeout time.Duration, started time.Time) mcpDiscoverOperationLedgerContext {
	if timeout < 0 {
		timeout = 0
	}
	timeoutMS := int(timeout / time.Millisecond)
	request := map[string]any{
		"workspace_id":  strings.TrimSpace(server.WorkspaceID),
		"server_id":     strings.TrimSpace(server.ServerID),
		"mcp_transport": strings.TrimSpace(server.Transport),
		"timeout_ms":    timeoutMS,
	}
	operationKey := "sha256:" + toolCallOperationRequestHash(request)
	operationID := operationLedgerID("mcpdiscover", operationKey, started)
	operationName := "mcp.tool.discover:" + strings.TrimSpace(server.ServerID)
	return mcpDiscoverOperationLedgerContext{
		operationID:   operationID,
		operationKey:  operationKey,
		operationName: operationName,
		createdAt:     started,
		deadlineAt:    started.Add(timeout),
		timeoutMS:     timeoutMS,
		workspaceID:   strings.TrimSpace(server.WorkspaceID),
		binding: toolCallOperationBinding(
			server.WorkspaceID,
			principal.PrincipalType,
			principal.PrincipalID,
			principal.PrincipalType,
			principal.PrincipalID,
			authority,
			operationID,
			"",
			"",
			"",
		),
		capabilitySnapshot: map[string]any{
			"snapshot_id":                    "mcp-discover-ledger-snapshot",
			"snapshot_schema":                "operation_ledger.v1",
			"surface_id":                     "mcp:" + strings.TrimSpace(server.ServerID) + "/discover",
			"surface_status_at_start":        "enabled",
			"requested_capability":           "mcp.tool.discover",
			"policy_epoch":                   "",
			"policy_verdict_at_start":        "ALLOW",
			"disabled_reason_codes_at_start": []string{},
		},
		requestDetails: map[string]any{
			"workspace_id":    strings.TrimSpace(server.WorkspaceID),
			"server_id":       strings.TrimSpace(server.ServerID),
			"mcp_transport":   strings.TrimSpace(firstNonEmpty(server.Transport, "unknown")),
			"timeout_ms":      timeoutMS,
			"url_present":     strings.TrimSpace(server.URL) != "",
			"command_present": strings.TrimSpace(server.Command) != "",
		},
		fence: toolCallOperationFence(authority, operationID),
	}
}

func (h *Handler) recordMCPDiscoverOperationLedger(ctx context.Context, op mcpDiscoverOperationLedgerContext, runStatus, outcome string, terminal bool, result map[string]any) *RPCError {
	now := time.Now().UTC()
	payload := map[string]any{
		"schema":              "operation_ledger.v1",
		"operation_id":        strings.TrimSpace(op.operationID),
		"operation_key":       strings.TrimSpace(op.operationKey),
		"operation_kind":      "mcp_discover",
		"operation_name":      strings.TrimSpace(op.operationName),
		"status":              toolCallOperationLedgerStatus(runStatus),
		"terminal":            terminal,
		"created_at":          op.createdAt.UTC().Format(time.RFC3339Nano),
		"started_at":          op.createdAt.UTC().Format(time.RFC3339Nano),
		"updated_at":          now.Format(time.RFC3339Nano),
		"deadline_at":         op.deadlineAt.UTC().Format(time.RFC3339Nano),
		"timeout_ms":          op.timeoutMS,
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
			"surface":     "mcp.tool.discover",
			"parent_refs": []string{},
		},
	}
	if terminal {
		payload["terminal_at"] = now.Format(time.RFC3339Nano)
	}
	if result != nil {
		payload["result"] = result
	}
	summary := "mcp.tool.discover " + toolCallOperationLedgerStatus(runStatus) + ": " + op.operationName
	if resultSummary := toolCallOperationResultSummary(result); resultSummary != "" {
		summary = resultSummary
	}
	_, _, rpcErr := h.recordOperationExecutionRun(ctx, sqlite.ExecutionRunInput{
		RunID:       op.operationID,
		WorkspaceID: op.workspaceID,
		Title:       "MCP discover: " + op.operationName,
		Summary:     summary,
		Status:      runStatus,
		Outcome:     outcome,
		Verification: map[string]any{
			"operation_ledger": payload,
		},
	}, "mcp.tool.discover")
	return rpcErr
}

func mcpDiscoverErrorResultMap(server mcp.ServerRecord, phase string, err error) map[string]any {
	summary := ""
	if err != nil {
		summary = err.Error()
	}
	result := map[string]any{
		"server_id":     strings.TrimSpace(server.ServerID),
		"mcp_transport": strings.TrimSpace(firstNonEmpty(server.Transport, "unknown")),
		"phase":         strings.TrimSpace(phase),
		"summary":       summary,
		"is_error":      true,
		"timed_out":     false,
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

func mcpDiscoverSuccessResultMap(server mcp.ServerRecord, initResult *mcp.InitializeResult, discoveredTools []mcp.Tool) map[string]any {
	toolNames := make([]string, len(discoveredTools))
	for i, t := range discoveredTools {
		toolNames[i] = t.Name
	}
	result := map[string]any{
		"server_id":        strings.TrimSpace(server.ServerID),
		"mcp_transport":    strings.TrimSpace(firstNonEmpty(server.Transport, "unknown")),
		"phase":            "publish",
		"tools_discovered": len(discoveredTools),
		"tool_names":       toolNames,
		"timed_out":        false,
	}
	if initResult != nil {
		result["server_name"] = initResult.ServerInfo.Name
		result["server_version"] = initResult.ServerInfo.Version
		result["protocol_version"] = initResult.ProtocolVersion
	}
	return result
}

func (h *Handler) mcpToolDiscover(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p mcpToolDiscoverParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if p.WorkspaceID == "" {
		principal, ok := authPrincipalFromContext(ctx)
		if !ok {
			return nil, &RPCError{Code: errCodePermissionDenied, Message: "unauthorized"}
		}
		p.WorkspaceID = principal.WorkspaceID
	}
	principal, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}
	authority, rpcErr := h.loadWorkspaceOperatorWriteAuthority(ctx, p.WorkspaceID, "mcp.tool.discover")
	if rpcErr != nil {
		return nil, rpcErr
	}

	server, err := h.mcpStore.GetServer(ctx, p.WorkspaceID, p.ServerID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: "server not found"}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: "server not found: " + err.Error()}
	}
	if strings.EqualFold(strings.TrimSpace(server.Transport), "stdio") &&
		!strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "system") {
		return nil, &RPCError{
			Code:    errCodePermissionDenied,
			Message: "stdio MCP discovery requires an internal system path",
		}
	}
	if !mcpServerVisibleToPrincipal(server, principal) ||
		(!strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "system") &&
			!mcpRegisteredByMatchesPrincipal(server.RegisteredBy, principal.PrincipalType, principal.PrincipalID)) {
		return nil, &RPCError{
			Code:    errCodePermissionDenied,
			Message: "mcp server is owned by another principal",
		}
	}

	discoverTimeout := toolruntime.EffectiveCallTimeout(ctx, defaultMCPDiscoverTimeoutSec)
	operationStartedAt := time.Now().UTC()
	operationLedger := newMCPDiscoverOperationLedgerContext(server, principal, authority, discoverTimeout, operationStartedAt)
	ledgerCtx := context.WithoutCancel(ctx)
	if rpcErr := h.recordMCPDiscoverOperationLedger(ledgerCtx, operationLedger, "ACTIVE", "RUNNING", false, nil); rpcErr != nil {
		return nil, rpcErr
	}
	discoverCtx, discoverCancel := context.WithTimeout(ctx, discoverTimeout)
	defer discoverCancel()
	failDiscover := func(phase, message string, err error, code int) (any, *RPCError) {
		result := mcpDiscoverErrorResultMap(server, phase, err)
		attachToolCallOperationID(result, operationLedger.operationID)
		status, outcome, _, _ := toolCallTerminalStateFromResult(result, err)
		if rpcErr := h.recordMCPDiscoverOperationLedger(ledgerCtx, operationLedger, status, outcome, true, result); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, &RPCError{Code: code, Message: message + err.Error()}
	}

	var initResult *mcp.InitializeResult
	var discoveredTools []mcp.Tool

	switch server.Transport {
	case "streamable-http":
		headers := h.mcpStore.GetServerHeaders(server)
		initResult, err = h.mcpClient.Initialize(discoverCtx, server.URL, headers)
		if err != nil {
			return failDiscover("initialize", "initialize failed: ", err, errCodeInternal)
		}
		discoveredTools, err = h.mcpClient.ListTools(discoverCtx, server.URL, headers)
		if err != nil {
			return failDiscover("list_tools", "list tools failed: ", err, errCodeInternal)
		}

	case "stdio":
		command, args, env := h.mcpStore.GetServerStdioConfig(server)
		sc := mcp.NewStdioClient()
		if err := sc.Start(command, args, env); err != nil {
			return failDiscover("start_process", "start process failed: ", err, errCodeInternal)
		}
		defer sc.Stop()

		initResult, err = sc.Initialize(discoverCtx)
		if err != nil {
			return failDiscover("initialize", "initialize failed: ", err, errCodeInternal)
		}
		discoveredTools, err = sc.ListTools(discoverCtx)
		if err != nil {
			return failDiscover("list_tools", "list tools failed: ", err, errCodeInternal)
		}

	default:
		return failDiscover("preflight", "unsupported transport: ", errors.New(server.Transport), errCodeInvalidParams)
	}

	if err := h.publishDiscoveredMCPWorkspaceState(ctx, authority, server, discoveredTools, operationLedger.operationID); err != nil {
		if errors.Is(err, errMCPDiscoverStaleServerSnapshot) {
			return failDiscover("publish", "", err, errCodeInvalidParams)
		}
		if rpcErr := authorityRejectRPCError(err, "mcp.tool.discover"); rpcErr != nil {
			result := mcpDiscoverErrorResultMap(server, "publish", err)
			attachToolCallOperationID(result, operationLedger.operationID)
			if ledgerErr := h.recordMCPDiscoverOperationLedger(ledgerCtx, operationLedger, "FAILED", "FAILED", true, result); ledgerErr != nil {
				return nil, ledgerErr
			}
			return nil, rpcErr
		}
		return failDiscover("publish", "publish discovered MCP state failed: ", err, errCodeInternal)
	}

	result := mcpDiscoverSuccessResultMap(server, initResult, discoveredTools)
	attachToolCallOperationID(result, operationLedger.operationID)
	if rpcErr := h.recordMCPDiscoverOperationLedger(ledgerCtx, operationLedger, "COMPLETED", "COMPLETED", true, result); rpcErr != nil {
		return nil, rpcErr
	}

	return map[string]any{
		"server_id":        p.ServerID,
		"operation_id":     operationLedger.operationID,
		"mcp_transport":    result["mcp_transport"],
		"server_name":      initResult.ServerInfo.Name,
		"server_version":   initResult.ServerInfo.Version,
		"protocol_version": initResult.ProtocolVersion,
		"tools_discovered": len(discoveredTools),
		"tool_names":       result["tool_names"],
	}, nil
}

type mcpToolCallParams struct {
	WorkspaceID string         `json:"workspace_id"`
	ServerID    string         `json:"server_id"`
	ToolName    string         `json:"tool_name"`
	Arguments   map[string]any `json:"arguments"`
}

func (h *Handler) mcpToolCall(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p mcpToolCallParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if p.WorkspaceID == "" {
		principal, ok := authPrincipalFromContext(ctx)
		if !ok {
			return nil, &RPCError{Code: errCodePermissionDenied, Message: "unauthorized"}
		}
		p.WorkspaceID = principal.WorkspaceID
	}
	principal, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if _, rpcErr := h.loadWorkspaceOperatorWriteAuthority(ctx, p.WorkspaceID, "mcp.tool.call"); rpcErr != nil {
		return nil, rpcErr
	}
	toolID := mcpWorkspaceToolID(p.ServerID, p.ToolName)
	record, err := h.store.GetWorkspaceTool(ctx, p.WorkspaceID, toolID)
	if err != nil {
		if errors.Is(err, sqlite.ErrToolNotFound) {
			return nil, &RPCError{
				Code:    errCodeInvalidParams,
				Message: "mcp tool is not discovered in the workspace; run mcp.tool.discover first",
			}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: "load workspace tool alias failed: " + err.Error()}
	}
	server, err := h.mcpStore.GetServer(ctx, p.WorkspaceID, p.ServerID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: "server not found"}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: "server not found: " + err.Error()}
	}
	if !mcpServerVisibleToPrincipal(server, principal) {
		return nil, &RPCError{
			Code:    errCodePermissionDenied,
			Message: "mcp server is owned by another principal",
		}
	}
	if mcpWorkspaceToolClassificationStale(record, server) {
		return nil, &RPCError{
			Code:    errCodeInvalidParams,
			Message: "mcp tool alias metadata is stale; run mcp.tool.discover again",
		}
	}

	toolRaw, err := json.Marshal(toolCallParams{
		ToolID:              toolID,
		WorkspaceID:         p.WorkspaceID,
		Arguments:           p.Arguments,
		RequestedCapability: "tool.call",
	})
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: "marshal delegated tool call: " + err.Error()}
	}
	respAny, rpcErr := h.toolCall(ctx, toolRaw)
	if rpcErr != nil {
		return nil, rpcErr
	}
	resp, ok := respAny.(map[string]any)
	if !ok {
		return nil, &RPCError{Code: errCodeInternal, Message: "unexpected delegated tool response"}
	}
	return map[string]any{
		"server_id":     p.ServerID,
		"tool_name":     p.ToolName,
		"operation_id":  resp["operation_id"],
		"mcp_transport": resp["mcp_transport"],
		"router_kind":   resp["router_kind"],
		"is_error":      resp["is_error"],
		"timed_out":     resp["timed_out"],
		"exit_code":     resp["exit_code"],
		"stdout":        resp["stdout"],
		"stderr":        resp["stderr"],
		"content":       resp["content"],
	}, nil
}

type mcpToolListParams struct {
	WorkspaceID string `json:"workspace_id"`
}

func (h *Handler) mcpToolList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p mcpToolListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	principal, rpcErr := requireWorkspacePrincipal(ctx, p.WorkspaceID)
	if rpcErr != nil {
		return nil, rpcErr
	}

	tools, err := h.mcpStore.ListServerTools(ctx, p.WorkspaceID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	visibleTools := make([]mcp.ServerToolRecord, 0, len(tools))
	for _, tool := range tools {
		server, err := h.mcpStore.GetServer(ctx, p.WorkspaceID, tool.ServerID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, &RPCError{Code: errCodeInternal, Message: "load mcp server for tool list failed: " + err.Error()}
		}
		if !mcpServerVisibleToPrincipal(server, principal) {
			continue
		}
		visibleTools = append(visibleTools, tool)
	}
	return map[string]any{
		"tools": visibleTools,
		"count": len(visibleTools),
	}, nil
}

func (h *Handler) registerDiscoveredMCPWorkspaceTools(ctx context.Context, server mcp.ServerRecord, tools []mcp.Tool, operationID string) error {
	authority, err := h.store.GetWorkspaceAuthority(ctx, server.WorkspaceID, "workspace")
	if err != nil {
		return err
	}
	return h.reconcileDiscoveredMCPWorkspaceAliases(ctx, authority, server, tools, operationID)
}

func (h *Handler) purgeMCPServerDiscoverState(ctx context.Context, workspaceID, serverID, actorID string) error {
	toolIDs := make(map[string]struct{})
	tools, err := h.mcpStore.ListServerTools(ctx, workspaceID)
	if err != nil {
		return err
	}
	for _, tool := range tools {
		if !strings.EqualFold(strings.TrimSpace(tool.ServerID), strings.TrimSpace(serverID)) {
			continue
		}
		toolIDs[mcpWorkspaceToolID(serverID, tool.ToolName)] = struct{}{}
	}
	workspaceTools, err := h.store.ListWorkspaceTools(ctx, sqlite.WorkspaceToolFilter{WorkspaceID: workspaceID})
	if err != nil {
		return err
	}
	for _, record := range workspaceTools {
		manifest := parseWorkspaceToolManifest(record.ManifestJSON)
		if manifest.Route == nil ||
			!strings.EqualFold(strings.TrimSpace(manifest.Route.Kind), "mcp") ||
			!strings.EqualFold(strings.TrimSpace(manifest.Route.ServerID), strings.TrimSpace(serverID)) {
			continue
		}
		toolID := strings.TrimSpace(record.ToolID)
		if toolID == "" {
			toolID = mcpWorkspaceToolID(serverID, manifest.Route.ToolName)
		}
		toolIDs[toolID] = struct{}{}
	}
	for toolID := range toolIDs {
		if _, err := h.store.RemoveWorkspaceTool(ctx, sqlite.WorkspaceToolRemoveInput{
			WorkspaceID: workspaceID,
			ToolID:      toolID,
			RemovedBy:   actorID,
		}); err != nil {
			return err
		}
	}
	return h.mcpStore.SaveDiscoveredTools(ctx, workspaceID, serverID, nil)
}

func mcpRegisteredByForPrincipal(principalType, principalID string) string {
	principalID = strings.TrimSpace(principalID)
	if principalID == "" {
		return ""
	}
	principalType = strings.ToLower(strings.TrimSpace(principalType))
	if principalType == "" || principalType == "human" {
		return principalID
	}
	return principalType + ":" + principalID
}

func mcpRegisteredByMatchesPrincipal(owner, principalType, principalID string) bool {
	owner = strings.TrimSpace(owner)
	principalID = strings.TrimSpace(principalID)
	if owner == "" || principalID == "" {
		return false
	}
	if ownerType, ownerID, typed := splitMCPRegisteredBy(owner); typed {
		return strings.EqualFold(ownerType, strings.TrimSpace(principalType)) &&
			strings.EqualFold(ownerID, principalID)
	}
	if strings.EqualFold(owner, "system") {
		return strings.EqualFold(strings.TrimSpace(principalType), "system")
	}
	return strings.EqualFold(strings.TrimSpace(principalType), "human") &&
		strings.EqualFold(owner, principalID)
}

func mcpRegisteredByPrincipalID(owner string) string {
	if _, ownerID, typed := splitMCPRegisteredBy(owner); typed {
		return ownerID
	}
	return strings.TrimSpace(owner)
}

func mcpServerVisibleToPrincipal(server mcp.ServerRecord, principal AuthPrincipal) bool {
	principalType := strings.ToLower(strings.TrimSpace(principal.PrincipalType))
	switch principalType {
	case "human", "system":
		// Single-host policy: the human/operator and internal system paths stay
		// workspace-sovereign. Owner-aware restrictions are enforced only for
		// non-human principals until/if we introduce a stricter sharing model.
		return true
	default:
		if mcpRegisteredByIsSystem(server.RegisteredBy) {
			return true
		}
		return mcpRegisteredByMatchesPrincipal(server.RegisteredBy, principal.PrincipalType, principal.PrincipalID)
	}
}

func filterVisibleMCPServers(servers []mcp.ServerRecord, principal AuthPrincipal) []mcp.ServerRecord {
	if len(servers) == 0 {
		return nil
	}
	filtered := make([]mcp.ServerRecord, 0, len(servers))
	for _, server := range servers {
		if mcpServerVisibleToPrincipal(server, principal) {
			filtered = append(filtered, server)
		}
	}
	return filtered
}

func splitMCPRegisteredBy(owner string) (ownerType string, ownerID string, typed bool) {
	owner = strings.TrimSpace(owner)
	parts := strings.SplitN(owner, ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	ownerType = strings.TrimSpace(parts[0])
	ownerID = strings.TrimSpace(parts[1])
	if ownerType == "" || ownerID == "" {
		return "", "", false
	}
	return ownerType, ownerID, true
}

func mcpRegisteredByIsSystem(owner string) bool {
	ownerType, _, typed := splitMCPRegisteredBy(owner)
	if typed {
		return strings.EqualFold(ownerType, "system")
	}
	return strings.EqualFold(strings.TrimSpace(owner), "system")
}

func (h *Handler) publishDiscoveredMCPWorkspaceState(ctx context.Context, authority sqlite.WorkspaceAuthorityRecord, server mcp.ServerRecord, tools []mcp.Tool, operationID string) error {
	desired := desiredMCPWorkspaceTools(server, tools)
	fenceInput := workspaceAuthorityFenceInputForRecord(authority)
	_, err := h.store.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, checkedAuthority sqlite.WorkspaceAuthorityRecord) error {
		currentServer, err := h.mcpStore.GetServerTx(ctx, tx, server.WorkspaceID, server.ServerID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return errMCPDiscoverStaleServerSnapshot
			}
			return err
		}
		if !mcpServerSnapshotMatches(server, currentServer) {
			return errMCPDiscoverStaleServerSnapshot
		}
		if err := h.store.ReconcileMCPWorkspaceToolsTx(ctx, tx, checkedAuthority, server.WorkspaceID, server.ServerID, server.RegisteredBy, desired, "mcp.tool.discover", operationID); err != nil {
			return err
		}
		return h.mcpStore.SaveDiscoveredToolsTx(ctx, tx, server.WorkspaceID, server.ServerID, tools)
	})
	return err
}

func (h *Handler) reconcileDiscoveredMCPWorkspaceAliases(ctx context.Context, authority sqlite.WorkspaceAuthorityRecord, server mcp.ServerRecord, tools []mcp.Tool, operationID string) error {
	desired := desiredMCPWorkspaceTools(server, tools)
	fenceInput := workspaceAuthorityFenceInputForRecord(authority)
	_, err := h.store.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, checkedAuthority sqlite.WorkspaceAuthorityRecord) error {
		return h.store.ReconcileMCPWorkspaceToolsTx(ctx, tx, checkedAuthority, server.WorkspaceID, server.ServerID, server.RegisteredBy, desired, "mcp.tool.discover", operationID)
	})
	return err
}

func (h *Handler) registerMCPServerAndResetDiscoverState(ctx context.Context, authority sqlite.WorkspaceAuthorityRecord, input mcp.RegisterInput, actorID, operationID string) error {
	fenceInput := workspaceAuthorityFenceInputForRecord(authority)
	_, err := h.store.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, checkedAuthority sqlite.WorkspaceAuthorityRecord) error {
		if err := h.mcpStore.RegisterServerTx(ctx, tx, input); err != nil {
			return err
		}
		if err := h.store.ReconcileMCPWorkspaceToolsTx(ctx, tx, checkedAuthority, input.WorkspaceID, input.ServerID, actorID, nil, "mcp.server.register", operationID); err != nil {
			return err
		}
		return h.mcpStore.SaveDiscoveredToolsTx(ctx, tx, input.WorkspaceID, input.ServerID, nil)
	})
	return err
}

func (h *Handler) removeMCPServerAndResetDiscoverState(ctx context.Context, authority sqlite.WorkspaceAuthorityRecord, workspaceID, serverID, actorID, operationID string) error {
	fenceInput := workspaceAuthorityFenceInputForRecord(authority)
	_, err := h.store.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, checkedAuthority sqlite.WorkspaceAuthorityRecord) error {
		if err := h.store.ReconcileMCPWorkspaceToolsTx(ctx, tx, checkedAuthority, workspaceID, serverID, actorID, nil, "mcp.server.remove", operationID); err != nil {
			return err
		}
		if err := h.mcpStore.SaveDiscoveredToolsTx(ctx, tx, workspaceID, serverID, nil); err != nil {
			return err
		}
		return h.mcpStore.RemoveServerTx(ctx, tx, workspaceID, serverID)
	})
	return err
}

func workspaceAuthorityFenceInputForRecord(authority sqlite.WorkspaceAuthorityRecord) sqlite.WorkspaceAuthorityFenceInput {
	return sqlite.WorkspaceAuthorityFenceInput{
		WorkspaceID:                   strings.TrimSpace(authority.WorkspaceID),
		Scope:                         strings.TrimSpace(authority.Scope),
		ExpectedHolderAuthorityNodeID: strings.TrimSpace(authority.HolderAuthorityNodeID),
		ExpectedLeaseToken:            strings.TrimSpace(authority.LeaseToken),
		ExpectedTerm:                  authority.Term,
		ReferenceAt:                   time.Now().UTC().Format(time.RFC3339Nano),
	}
}

func desiredMCPWorkspaceTools(server mcp.ServerRecord, tools []mcp.Tool) []sqlite.WorkspaceToolInput {
	desired := make([]sqlite.WorkspaceToolInput, 0, len(tools))
	for _, tool := range tools {
		desired = append(desired, sqlite.WorkspaceToolInput{
			WorkspaceID:  server.WorkspaceID,
			ToolID:       mcpWorkspaceToolID(server.ServerID, tool.Name),
			DisplayName:  firstNonEmpty(tool.Name, server.DisplayName),
			Description:  firstNonEmpty(tool.Description, "MCP tool "+server.ServerID+"/"+tool.Name),
			OwnerUserID:  mcpRegisteredByPrincipalID(server.RegisteredBy),
			Kind:         model.ToolKindIntegration,
			Status:       model.ToolStatusActive,
			AccessLevel:  model.ToolAccessWorkspace,
			Endpoint:     firstNonEmpty(server.URL, "mcp:"+server.ServerID),
			Capabilities: mcpWorkspaceToolCapabilities(server),
			ManifestJSON: mcpWorkspaceToolManifest(server, tool),
		})
	}
	return desired
}

func mcpServerSnapshotMatches(expected, current mcp.ServerRecord) bool {
	return strings.EqualFold(strings.TrimSpace(expected.ServerID), strings.TrimSpace(current.ServerID)) &&
		strings.EqualFold(strings.TrimSpace(expected.WorkspaceID), strings.TrimSpace(current.WorkspaceID)) &&
		strings.EqualFold(strings.TrimSpace(expected.Transport), strings.TrimSpace(current.Transport)) &&
		strings.TrimSpace(expected.URL) == strings.TrimSpace(current.URL) &&
		strings.TrimSpace(expected.Command) == strings.TrimSpace(current.Command) &&
		strings.TrimSpace(expected.ArgsJSON) == strings.TrimSpace(current.ArgsJSON) &&
		strings.TrimSpace(expected.EnvJSON) == strings.TrimSpace(current.EnvJSON) &&
		strings.TrimSpace(expected.HeadersJSON) == strings.TrimSpace(current.HeadersJSON) &&
		strings.TrimSpace(expected.RegisteredBy) == strings.TrimSpace(current.RegisteredBy) &&
		strings.TrimSpace(expected.UpdatedAt) == strings.TrimSpace(current.UpdatedAt)
}
