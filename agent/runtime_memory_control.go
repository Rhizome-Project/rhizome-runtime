package main

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

type runtimeMemoryPacketRequest struct {
	MaxChars int `json:"max_chars,omitempty"`
}

func (r *Runtime) runtimeStatusPayload(now time.Time) map[string]any {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	control := r.runtimeControlSnapshot(now)
	status := map[string]any{
		"status":     "ok",
		"agent_id":   r.cfg.AgentID,
		"summary":    r.currentSummary(),
		"task_id":    "",
		"session_id": "",
		"paused":     control.Paused,
		"attachable": control.Attachable,
		"control":    control,
	}
	if task := r.currentTaskCopy(); task != nil {
		status["active_task"] = task
	}
	if session := r.currentSessionCopy(); session != nil {
		status["active_session"] = session
	}
	if session := r.currentSessionCopy(); session != nil {
		status["session_id"] = session.SessionID
		status["task_id"] = session.TaskID
	}
	if inbox := r.messageInbox(); inbox != nil {
		stats := inbox.Stats()
		status["message_inbox"] = map[string]any{
			"total":               stats.Total,
			"unread":              stats.Unread,
			"pending":             stats.Pending,
			"unacked":             stats.Unacked,
			"missed_since_start":  stats.MissedSinceStart,
			"carryover_pending":   stats.CarryoverPending,
			"runtime_started_at":  stats.RuntimeStartedAt,
			"previous_started_at": stats.PreviousStartedAt,
			"last_synced_cursor":  stats.LastSyncedCursor,
			"last_synced_at":      stats.LastSyncedAt,
		}
	}
	if createdAt, newsID := r.newsCursor(); createdAt != "" || newsID != "" {
		status["news_feed"] = map[string]any{
			"cursor_created_at": createdAt,
			"cursor_news_id":    newsID,
		}
	}
	if trigger := r.currentPendingWorkTrigger(); trigger.Trigger != "" {
		status["work_trigger"] = map[string]any{
			"trigger":    trigger.Trigger,
			"task_id":    trigger.TaskID,
			"session_id": trigger.SessionID,
		}
	}
	status["bootstrap_work_fallback"] = r.bootstrapWorkFallbackStatus()
	if packet := r.currentWorkPacketCopy(); packet != nil {
		status["work_packet"] = packet
		status["work_type"] = packet.WorkType
		status["work_coordination_state"] = packet.CoordinationState
		status["work_preferred_transition"] = packet.PreferredTransition
		status["work_gate_reason"] = packet.WhyNow
		if packet.Gate != nil {
			status["work_gate_state"] = packet.Gate.GateState
			status["work_gate_type"] = packet.Gate.GateType
			status["work_gate_needed_from"] = packet.Gate.NeededFrom
			status["work_gate_summary"] = packet.Gate.Summary
		}
	}
	status["ambient_autonomy"] = r.ambientAutonomyStatusSnapshot(now)
	status["internal_heartbeats"] = r.internalHeartbeatStatusSnapshot(now)
	status["tool_bundles"] = toolBundleStatusSnapshot(r.agent)
	if focus := r.currentFocusCopy(); focus != nil {
		status["focus"] = focus
	}
	status["watchdog"] = r.watchdogSnapshot(now)
	status["loop_health"] = r.runtimeLoopHealthSnapshot(now)
	status["memory"] = r.memoryStatsSnapshot()
	status["memory_store"] = r.memoryStoreStatsSnapshot()
	status["memory_control"] = r.memoryControlSnapshot()
	return status
}

func runtimeStatusResponseText(status map[string]any, raw []byte) string {
	if len(raw) <= maxAgentRespondResponseBytes {
		return string(raw)
	}
	compact := map[string]any{
		"status":                     runtimeStatusString(status, "status"),
		"agent_id":                   runtimeStatusString(status, "agent_id"),
		"method":                     runtimeStatusString(status, "method"),
		"summary":                    truncateUTF8Bytes(runtimeStatusString(status, "summary"), 4096),
		"task_id":                    runtimeStatusString(status, "task_id"),
		"session_id":                 runtimeStatusString(status, "session_id"),
		"paused":                     runtimeStatusBool(status, "paused"),
		"attachable":                 runtimeStatusBool(status, "attachable"),
		"work_type":                  runtimeStatusString(status, "work_type"),
		"work_coordination_state":    runtimeStatusString(status, "work_coordination_state"),
		"work_preferred_transition":  runtimeStatusString(status, "work_preferred_transition"),
		"work_gate_state":            runtimeStatusString(status, "work_gate_state"),
		"work_gate_type":             runtimeStatusString(status, "work_gate_type"),
		"work_gate_needed_from":      runtimeStatusString(status, "work_gate_needed_from"),
		"work_gate_summary":          truncateUTF8Bytes(runtimeStatusString(status, "work_gate_summary"), 2048),
		"tool_bundles":               compactToolBundleStatusForRuntimeStatus(status["tool_bundles"]),
		"response_truncated":         true,
		"original_response_bytes":    len(raw),
		"transport_limit_bytes_hint": maxAgentRespondResponseBytes,
		"omitted_fields": []string{
			"active_task",
			"active_session",
			"ambient_autonomy",
			"bootstrap_work_fallback",
			"control",
			"focus",
			"internal_heartbeats",
			"loop_health",
			"memory",
			"memory_control",
			"memory_store",
			"message_inbox",
			"news_feed",
			"watchdog",
			"work_packet",
		},
	}
	if rawCompact, err := json.Marshal(compact); err == nil {
		return clampAgentRespondResponse(string(rawCompact))
	}
	return clampAgentRespondResponse(string(raw))
}

func runtimeStatusString(status map[string]any, key string) string {
	if len(status) == 0 {
		return ""
	}
	value, _ := status[key].(string)
	return strings.TrimSpace(value)
}

func runtimeStatusBool(status map[string]any, key string) bool {
	if len(status) == 0 {
		return false
	}
	value, _ := status[key].(bool)
	return value
}

func compactToolBundleStatusForRuntimeStatus(value any) any {
	var snapshot ToolBundleStatusSnapshot
	switch typed := value.(type) {
	case ToolBundleStatusSnapshot:
		snapshot = typed
	case *ToolBundleStatusSnapshot:
		if typed == nil {
			return nil
		}
		snapshot = *typed
	default:
		return nil
	}
	return map[string]any{
		"schema":             snapshot.Schema,
		"status":             snapshot.Status,
		"prompt_visible":     snapshot.PromptVisible,
		"tool_visible":       snapshot.ToolVisible,
		"copy_in_contract":   snapshot.CopyInContract,
		"candidate_count":    snapshot.CandidateCount,
		"suite_count":        snapshot.SuiteCount,
		"installed_count":    snapshot.InstalledCount,
		"dependency_count":   snapshot.DependencyCount,
		"healthcheck_count":  snapshot.HealthcheckCount,
		"config_count":       snapshot.ConfigCount,
		"skipped_count":      snapshot.SkippedCount,
		"error_count":        snapshot.ErrorCount,
		"collision_count":    snapshot.CollisionCount,
		"root_count":         len(snapshot.Roots),
		"config":             compactToolBundleRegistryConfigForRuntimeStatus(snapshot.Config),
		"candidates":         compactToolBundleReadinessForRuntimeStatus(snapshot.Candidates, 12),
		"suites":             compactToolBundleSuitesForRuntimeStatus(snapshot.Suites, 16),
		"installed":          compactToolBundleStatusItemsForRuntimeStatus(snapshot.Installed, 12),
		"dependencies":       compactToolBundleDiagnosticsForRuntimeStatus(snapshot.Dependencies, 6),
		"healthchecks":       compactToolBundleDiagnosticsForRuntimeStatus(snapshot.Healthchecks, 6),
		"config_diagnostics": compactToolBundleDiagnosticsForRuntimeStatus(snapshot.ConfigDiagnostics, 6),
		"skipped":            compactToolBundleDiagnosticsForRuntimeStatus(snapshot.Skipped, 6),
		"errors":             compactToolBundleDiagnosticsForRuntimeStatus(snapshot.Errors, 6),
		"collisions":         compactToolBundleDiagnosticsForRuntimeStatus(snapshot.Collisions, 6),
	}
}

func compactToolBundleReadinessForRuntimeStatus(items []ToolBundleReadinessItem, limit int) []map[string]any {
	if limit <= 0 || len(items) == 0 {
		return nil
	}
	if len(items) < limit {
		limit = len(items)
	}
	out := make([]map[string]any, 0, limit)
	for _, item := range items[:limit] {
		out = append(out, map[string]any{
			"name":         truncateUTF8Bytes(strings.TrimSpace(item.Name), 120),
			"status":       truncateUTF8Bytes(strings.TrimSpace(item.Status), 40),
			"code":         truncateUTF8Bytes(strings.TrimSpace(item.Code), 80),
			"root_kind":    truncateUTF8Bytes(strings.TrimSpace(item.RootKind), 40),
			"registered":   item.Registered,
			"configured":   item.Configured,
			"message":      truncateUTF8Bytes(strings.TrimSpace(item.Message), 240),
			"existing_dir": truncateUTF8Bytes(strings.TrimSpace(item.ExistingDir), 240),
			"suites":       compactStringListForRuntimeStatus(item.CapabilitySuites, 6, 80),
		})
	}
	return out
}

func compactToolBundleSuitesForRuntimeStatus(items []ToolBundleSuiteReadinessItem, limit int) []map[string]any {
	if limit <= 0 || len(items) == 0 {
		return nil
	}
	if len(items) < limit {
		limit = len(items)
	}
	out := make([]map[string]any, 0, limit)
	for _, item := range items[:limit] {
		out = append(out, map[string]any{
			"suite":             truncateUTF8Bytes(strings.TrimSpace(item.Suite), 120),
			"status":            truncateUTF8Bytes(strings.TrimSpace(item.Status), 40),
			"required":          item.Required,
			"heartbeats":        compactStringListForRuntimeStatus(item.Heartbeats, 8, 120),
			"ready_bundles":     compactStringListForRuntimeStatus(item.ReadyBundles, 6, 120),
			"warning_bundles":   compactStringListForRuntimeStatus(item.WarningBundles, 6, 120),
			"blocked_bundles":   compactStringListForRuntimeStatus(item.BlockedBundles, 6, 120),
			"candidate_bundles": compactStringListForRuntimeStatus(item.CandidateBundles, 8, 120),
			"native_providers":  compactStringListForRuntimeStatus(item.NativeProviders, 8, 120),
			"suggested_bundles": compactStringListForRuntimeStatus(item.SuggestedBundles, 6, 120),
			"suggested_actions": compactStringListForRuntimeStatus(item.SuggestedActions, 4, 240),
			"message":           truncateUTF8Bytes(strings.TrimSpace(item.Message), 240),
		})
	}
	return out
}

func compactToolBundleRegistryConfigForRuntimeStatus(status ToolBundleRegistryConfigStatus) map[string]any {
	if !status.Present && len(status.Enabled) == 0 && len(status.Disabled) == 0 && strings.TrimSpace(status.Status) == "" {
		return nil
	}
	return map[string]any{
		"present":  status.Present,
		"status":   truncateUTF8Bytes(strings.TrimSpace(status.Status), 80),
		"mode":     truncateUTF8Bytes(strings.TrimSpace(status.Mode), 40),
		"enabled":  compactStringListForRuntimeStatus(status.Enabled, 12, 80),
		"disabled": compactStringListForRuntimeStatus(status.Disabled, 12, 80),
		"message":  truncateUTF8Bytes(strings.TrimSpace(status.Message), 240),
	}
}

func compactToolBundleStatusItemsForRuntimeStatus(items []ToolBundleStatusItem, limit int) []map[string]any {
	if limit <= 0 || len(items) == 0 {
		return nil
	}
	if len(items) < limit {
		limit = len(items)
	}
	out := make([]map[string]any, 0, limit)
	for _, item := range items[:limit] {
		out = append(out, map[string]any{
			"name":               truncateUTF8Bytes(strings.TrimSpace(item.Name), 120),
			"version":            truncateUTF8Bytes(strings.TrimSpace(item.Version), 64),
			"capability_suites":  compactStringListForRuntimeStatus(item.CapabilitySuites, 6, 80),
			"artifact_contracts": compactStringListForRuntimeStatus(item.ArtifactContracts, 6, 80),
			"dependencies":       compactStringListForRuntimeStatus(item.Dependencies, 6, 80),
			"timeout_seconds":    item.TimeoutSeconds,
		})
	}
	return out
}

func compactToolBundleDiagnosticsForRuntimeStatus(diagnostics []ToolBundleDiscoveryDiagnostic, limit int) []map[string]any {
	if limit <= 0 || len(diagnostics) == 0 {
		return nil
	}
	if len(diagnostics) < limit {
		limit = len(diagnostics)
	}
	out := make([]map[string]any, 0, limit)
	for _, diagnostic := range diagnostics[:limit] {
		out = append(out, map[string]any{
			"code":      truncateUTF8Bytes(strings.TrimSpace(diagnostic.Code), 80),
			"name":      truncateUTF8Bytes(strings.TrimSpace(diagnostic.Name), 120),
			"root_kind": truncateUTF8Bytes(strings.TrimSpace(diagnostic.RootKind), 40),
			"message":   truncateUTF8Bytes(strings.TrimSpace(diagnostic.Message), 240),
		})
	}
	return out
}

func compactStringListForRuntimeStatus(values []string, limit, maxBytes int) []string {
	if limit <= 0 || len(values) == 0 {
		return nil
	}
	if len(values) < limit {
		limit = len(values)
	}
	out := make([]string, 0, limit)
	for _, value := range values[:limit] {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, truncateUTF8Bytes(trimmed, maxBytes))
		}
	}
	return out
}

func (r *Runtime) bootstrapWorkFallbackStatus() map[string]any {
	mode := RuntimeModeDaemon
	if r != nil && r.cfg.Mode != "" {
		mode = r.cfg.Mode
	}
	disabled := true
	if r != nil {
		disabled = r.bootstrapWorkFallbackDisabled()
	}
	status := map[string]any{
		"mode":             string(mode),
		"selector":         "bootstrap_snapshot",
		"can_consume_work": !disabled,
	}
	if disabled {
		status["posture"] = "disabled"
		status["gate_state"] = "closed"
		status["scope"] = "daemon"
		status["summary"] = "bootstrap compatibility fallback is disabled in daemon mode after agent.work.next fallback-class failure"
		return status
	}
	status["posture"] = "compatibility_enabled"
	status["gate_state"] = "open"
	status["scope"] = "non_daemon"
	status["summary"] = "bootstrap compatibility fallback can consume work outside daemon mode"
	return status
}

func decodeRuntimeRequestPayload(request AgentRequestRecord, target any) error {
	if target == nil {
		return nil
	}
	raw := strings.TrimSpace(request.Payload)
	if raw == "" {
		return nil
	}
	return json.Unmarshal([]byte(raw), target)
}

func (r *Runtime) handleRuntimeMemoryRequest(ctx context.Context, request AgentRequestRecord, status map[string]any) bool {
	method := strings.ToLower(strings.TrimSpace(request.Method))
	switch method {
	case "runtime.memory.status":
		status["summary"] = "memory status snapshot"
		return true
	case "runtime.memory.packet":
		var payload runtimeMemoryPacketRequest
		if err := decodeRuntimeRequestPayload(request, &payload); err != nil {
			status["status"] = "error"
			status["details"] = err.Error()
			return true
		}
		status["summary"] = "memory packet preview"
		status["memory_body"] = r.currentMemoryPacket(payload.MaxChars)
		status["memory_control"] = r.memoryControlSnapshot()
		return true
	case "runtime.memory.rebuild":
		service := r.memoryService()
		if service == nil {
			status["status"] = "noop"
			status["details"] = "memory service is not active"
			return true
		}
		if err := service.forceRebuild(); err != nil {
			status["status"] = "error"
			status["details"] = err.Error()
			return true
		}
		status["summary"] = "memory rebuilt from local store"
		status["memory_control"] = r.memoryControlSnapshot()
		return true
	case "runtime.memory.invalidate":
		service := r.memoryService()
		if service == nil {
			status["status"] = "noop"
			status["details"] = "memory service is not active"
			return true
		}
		var payload LocalMemoryInvalidationInput
		if err := decodeRuntimeRequestPayload(request, &payload); err != nil {
			status["status"] = "error"
			status["details"] = err.Error()
			return true
		}
		if err := service.invalidate(payload); err != nil {
			status["status"] = "error"
			status["details"] = err.Error()
			return true
		}
		status["summary"] = "memory invalidated and rebuilt"
		status["memory_control"] = r.memoryControlSnapshot()
		status["memory_body"] = r.currentMemoryPacket(1600)
		return true
	default:
		return false
	}
}
