package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

type taskLoopObserver struct {
	ctx     context.Context
	trace   *TaskRunTrace
	runtime *Runtime
}

type ToolPolicySignal struct {
	ToolName    string
	Verdict     string
	Capability  string
	Description string
}

type taskCycleRequiredToolGuardContextKey struct{}

const (
	structuredOutputFailureCodeInvalid = "invalid_output"
	structuredOutputRepairAction       = "repair_structured_output"
	structuredOutputRepairContract     = "structured_output_repair.v1"
	structuredOutputRepairMode         = "daemon_repair_only"
)

type structuredOutputRepairInfo struct {
	Attempted    bool
	Attempts     int
	ParseError   string
	InitialRaw   string
	RepairOutput string
	RepairError  string
}

func taskCycleRequiredToolGuardApplied(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	applied, _ := ctx.Value(taskCycleRequiredToolGuardContextKey{}).(bool)
	return applied
}

func (r *Runtime) activeTaskBoundRequiredToolSubstituteToolResult(toolName string) (ToolResult, bool) {
	toolName = strings.TrimSpace(toolName)
	if r == nil || !strings.EqualFold(toolName, "browser_visual_probe") {
		return ToolResult{}, false
	}
	activeTask := r.currentTaskCopy()
	if activeTask == nil || !isTaskCandidate(*activeTask, r.cfg.AgentID) {
		return ToolResult{}, false
	}
	requiredTool := taskBoundRequiredToolForTaskCycle(*activeTask)
	if !strings.EqualFold(requiredTool, "browser_session") {
		return ToolResult{}, false
	}
	if !taskRequirementsHasForbiddenSubstitute(*activeTask, "browser_visual_probe_only") &&
		!taskRequirementsHasForbiddenSubstitute(*activeTask, "browser_visual_probe") {
		return ToolResult{}, false
	}
	r.recordPolicySignal(ToolPolicySignal{
		ToolName:    toolName,
		Verdict:     "YIELD",
		Capability:  toolCapabilityForName(toolName),
		Description: fmt.Sprintf("active task %s requires browser_session; blocking browser_visual_probe outside task-cycle required-tool guard", strings.TrimSpace(activeTask.TaskID)),
	})
	return taskBoundRequiredToolSubstituteYieldToolResult(toolName, requiredTool, "browser_visual_probe is probe-only evidence and cannot satisfy the active task-bound browser_session requirement"), true
}

func (o *taskLoopObserver) OnLLMResponse(_ int, response *LLMResponse) {
	if o == nil || o.trace == nil || response == nil {
		return
	}
	o.trace.AssistantTurns++
	inputTokens, outputTokens := taskRunTraceUsageSplit(response.Usage)
	o.trace.TotalInputTokens += inputTokens
	o.trace.TotalOutputTokens += outputTokens
	if o.runtime != nil && o.runtime.cfg.RealLLMPilot && len(response.ToolCalls) > 0 {
		if err := o.runtime.keepaliveTaskLoopSession(o.context(), "llm_response", "assistant requested tools", false); err != nil {
			log.Printf("[runtime] task loop keepalive after llm response failed: %v", err)
		}
	}
}

// OnPromptSectionBytes records the per-iteration prompt composition byte
// breakdown into the task run trace. taskLoopObserver
// opts into the optional promptSectionObserver hook so the task/session trace
// exposes section bytes for every backend (codex, qwen, openai) without
// changing the ToolLoopObserver interface.
func (o *taskLoopObserver) OnPromptSectionBytes(_ int, sections PromptSectionBytes) {
	if o == nil || o.trace == nil {
		return
	}
	o.trace.PromptSectionBytes = append(o.trace.PromptSectionBytes, sections)
}

func taskRunTraceUsageSplit(usage TokenUsage) (int, int) {
	inputTokens := usage.PromptTokens
	outputTokens := usage.CompletionTokens
	if inputTokens < 0 {
		inputTokens = 0
	}
	if outputTokens < 0 {
		outputTokens = 0
	}
	totalTokens := usage.TotalTokens
	if totalTokens < 0 {
		totalTokens = 0
	}
	if inputTokens == 0 && outputTokens == 0 && totalTokens > 0 {
		return totalTokens, 0
	}
	if totalTokens > inputTokens+outputTokens {
		if inputTokens == 0 {
			inputTokens = totalTokens - outputTokens
		} else if outputTokens == 0 {
			outputTokens = totalTokens - inputTokens
		}
	}
	return inputTokens, outputTokens
}

func sessionEventWithTaskRunTrace(input SessionEventInput, trace *TaskRunTrace) SessionEventInput {
	if trace == nil {
		return input
	}
	input.Iterations = maxTaskRunTraceMetric(input.Iterations, trace.AssistantTurns)
	input.TotalInputTokens = maxTaskRunTraceMetric(input.TotalInputTokens, trace.TotalInputTokens)
	input.TotalOutputTokens = maxTaskRunTraceMetric(input.TotalOutputTokens, trace.TotalOutputTokens)
	input.ToolCalls = maxTaskRunTraceMetric(input.ToolCalls, len(trace.ToolCalls))
	return input
}

func maxTaskRunTraceMetric(current, incoming int) int {
	if current < 0 {
		current = 0
	}
	if incoming > current {
		return incoming
	}
	return current
}

func (o *taskLoopObserver) OnToolResult(_ int, call ToolCall, result ToolResult) {
	if o == nil || o.trace == nil {
		return
	}
	toolName := strings.TrimSpace(call.Function.Name)
	if toolName == "" {
		return
	}
	o.trace.ToolCalls = append(o.trace.ToolCalls, toolName)
	o.trace.ToolReceipts = append(o.trace.ToolReceipts, TaskRunToolReceipt{
		ToolName: toolName,
		IsError:  result.IsError,
		Output:   result.Output,
	})
	if result.IsError {
		o.trace.FailedToolCalls = append(o.trace.FailedToolCalls, toolName)
	} else {
		o.trace.SuccessfulToolCalls = append(o.trace.SuccessfulToolCalls, taskRunTraceSuccessfulToolName(call, result))
	}
}

func (o *taskLoopObserver) context() context.Context {
	if o == nil || o.ctx == nil {
		return context.Background()
	}
	return o.ctx
}

func taskRunTraceSuccessfulToolName(call ToolCall, results ...ToolResult) string {
	toolName := strings.TrimSpace(call.Function.Name)
	if strings.EqualFold(toolName, "tool_bundle_registry") {
		var args struct {
			Action string `json:"action"`
		}
		action := "list"
		if err := json.Unmarshal([]byte(call.Function.Arguments), &args); err == nil {
			if trimmed := strings.ToLower(strings.TrimSpace(args.Action)); trimmed != "" {
				action = trimmed
			}
		}
		if len(results) > 0 {
			if toolBundleRegistryResultMutated(results[0]) {
				return "tool_bundle_registry:mutated"
			}
			switch action {
			case "list", "status", "refresh":
				return "tool_bundle_registry:" + action
			default:
				return toolName
			}
		}
		switch action {
		case "list", "status", "refresh":
			return "tool_bundle_registry:" + action
		case "register", "enable", "disable", "scaffold", "install", "download", "install_url", "migrate", "migrate_manifest":
			return "tool_bundle_registry:mutated"
		default:
			return toolName
		}
	}
	if !strings.EqualFold(toolName, "write_file") {
		return toolName
	}
	var args struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal([]byte(call.Function.Arguments), &args); err != nil {
		return toolName
	}
	if writeFilePathIsEphemeralProgress(args.Path) {
		return "write_file:ephemeral"
	}
	return toolName
}

func toolBundleRegistryResultMutated(result ToolResult) bool {
	if result.IsError {
		return false
	}
	output := strings.TrimSpace(result.Output)
	if output == "" {
		return false
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(output), &payload); err != nil {
		return false
	}
	return boolMapValue(payload, "mutated")
}

func writeFilePathIsEphemeralProgress(pathValue string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(pathValue), "\\", "/"))
	normalized = strings.TrimPrefix(normalized, "./")
	if normalized == "" {
		return false
	}
	if writeFilePathLooksEphemeralRoot(normalized) {
		return true
	}
	switch {
	case strings.HasSuffix(normalized, ".png"),
		strings.HasSuffix(normalized, ".jpg"),
		strings.HasSuffix(normalized, ".jpeg"),
		strings.HasSuffix(normalized, ".webp"),
		strings.HasSuffix(normalized, ".gif"),
		strings.HasSuffix(normalized, ".log"),
		strings.HasSuffix(normalized, ".tsbuildinfo"):
		return writeFilePathLooksEphemeralOutput(normalized)
	default:
		return false
	}
}

func writeFilePathLooksEphemeralRoot(normalized string) bool {
	return strings.HasPrefix(normalized, ".tmp/") ||
		strings.Contains(normalized, "/.tmp/") ||
		strings.HasPrefix(normalized, ".runtime-config/") ||
		strings.Contains(normalized, "/.runtime-config/") ||
		strings.HasPrefix(normalized, "node_modules/") ||
		strings.Contains(normalized, "/node_modules/") ||
		strings.HasPrefix(normalized, "dist/") ||
		strings.Contains(normalized, "/dist/")
}

func writeFilePathLooksEphemeralOutput(normalized string) bool {
	return writeFilePathLooksEphemeralRoot(normalized) ||
		strings.HasPrefix(normalized, "tmp/") ||
		strings.Contains(normalized, "/tmp/") ||
		strings.HasPrefix(normalized, "temp/") ||
		strings.Contains(normalized, "/temp/") ||
		strings.HasPrefix(normalized, "cache/") ||
		strings.Contains(normalized, "/cache/") ||
		strings.HasPrefix(normalized, ".cache/") ||
		strings.Contains(normalized, "/.cache/") ||
		strings.HasPrefix(normalized, "coverage/") ||
		strings.Contains(normalized, "/coverage/") ||
		strings.HasPrefix(normalized, "logs/") ||
		strings.Contains(normalized, "/logs/")
}

func (r *Runtime) resolveStructuredTaskResult(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID, raw string) (StructuredTaskResult, *structuredOutputRepairInfo, error) {
	repairInfo := &structuredOutputRepairInfo{
		InitialRaw: strings.TrimSpace(raw),
	}

	result, err := parseStructuredTaskResultStrict(raw)
	if err == nil {
		if runtimeTrustFirst(r.cfg) && !taskResultReflectionComplete(result) {
			return r.repairTrustFirstReflection(ctx, task, session, runID, raw, result, repairInfo)
		}
		return result, repairInfo, nil
	}

	repairInfo.Attempted = true
	repairInfo.Attempts = 1
	repairInfo.ParseError = err.Error()

	repairedRaw, repairErr := r.repairStructuredTaskResult(ctx, task, session, runID, raw, err)
	if repairErr != nil {
		repairInfo.RepairError = repairErr.Error()
		return StructuredTaskResult{}, repairInfo, fmt.Errorf("%s: %w", structuredOutputFailureCodeInvalid, repairErr)
	}
	repairInfo.RepairOutput = strings.TrimSpace(repairedRaw)

	repairedResult, repairParseErr := parseStructuredTaskResultStrict(repairedRaw)
	if repairParseErr != nil {
		repairInfo.RepairError = repairParseErr.Error()
		return StructuredTaskResult{}, repairInfo, fmt.Errorf("%s: %w", structuredOutputFailureCodeInvalid, repairParseErr)
	}
	if runtimeTrustFirst(r.cfg) && !taskResultReflectionComplete(repairedResult) {
		repairInfo.RepairError = "trust_first structured result is missing non-empty reflection after repair"
		return trustFirstReflectionMissingResult(repairedResult, repairInfo.RepairError), repairInfo, nil
	}
	return repairedResult, repairInfo, nil
}

func (r *Runtime) repairTrustFirstReflection(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID, raw string, result StructuredTaskResult, repairInfo *structuredOutputRepairInfo) (StructuredTaskResult, *structuredOutputRepairInfo, error) {
	if repairInfo == nil {
		repairInfo = &structuredOutputRepairInfo{InitialRaw: strings.TrimSpace(raw)}
	}
	repairInfo.Attempted = true
	repairInfo.Attempts = 1
	repairInfo.ParseError = "trust_first structured result requested non-empty reflection.current_intent, fresh_evidence, blocker_freshness, and next_useful_move"

	repairedRaw, repairErr := r.repairStructuredTaskResult(ctx, task, session, runID, raw, errors.New(repairInfo.ParseError))
	if repairErr != nil {
		repairInfo.RepairError = repairErr.Error()
		return trustFirstReflectionMissingResult(result, repairInfo.RepairError), repairInfo, nil
	}
	repairInfo.RepairOutput = strings.TrimSpace(repairedRaw)

	repairedResult, repairParseErr := parseStructuredTaskResultStrict(repairedRaw)
	if repairParseErr != nil {
		repairInfo.RepairError = repairParseErr.Error()
		return trustFirstReflectionMissingResult(result, repairInfo.RepairError), repairInfo, nil
	}
	if !taskResultReflectionComplete(repairedResult) {
		repairInfo.RepairError = "trust_first structured result is missing non-empty reflection after repair"
		return trustFirstReflectionMissingResult(repairedResult, repairInfo.RepairError), repairInfo, nil
	}
	return repairedResult, repairInfo, nil
}

func taskResultReflectionComplete(result StructuredTaskResult) bool {
	if result.Reflection == nil {
		return false
	}
	return strings.TrimSpace(result.Reflection.CurrentIntent) != "" &&
		strings.TrimSpace(result.Reflection.FreshEvidence) != "" &&
		strings.TrimSpace(result.Reflection.BlockerFreshness) != "" &&
		strings.TrimSpace(result.Reflection.NextUsefulMove) != ""
}

func trustFirstReflectionMissingResult(result StructuredTaskResult, cause string) StructuredTaskResult {
	if strings.TrimSpace(result.Summary) == "" {
		result.Summary = "Trust-first reflection missing from structured result"
	}
	detail := "trust_first reflection advisory missing"
	if strings.TrimSpace(cause) != "" {
		detail += ": " + strings.TrimSpace(cause)
	}
	if strings.TrimSpace(result.Details) == "" {
		result.Details = detail
	} else if !strings.Contains(result.Details, "trust_first reflection advisory missing") {
		result.Details = strings.TrimSpace(result.Details) + "\n" + detail
	}
	if strings.TrimSpace(result.NextAction) == "" {
		result.NextAction = "Continue useful work and include public reflection when the next structured result is emitted."
	}
	return result
}

func (r *Runtime) repairStructuredTaskResult(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID, raw string, parseErr error) (string, error) {
	if r == nil || r.agent == nil || r.agent.LLM == nil {
		return "", fmt.Errorf("repair backend unavailable")
	}
	if err := r.validateStructuredOutputRepairAuthority(task, session, runID); err != nil {
		return "", err
	}

	messages := []Message{
		{
			Role:    "system",
			Content: r.buildStructuredOutputRepairSystemPrompt(task, session, runID),
		},
		{
			Role:    "user",
			Content: buildStructuredOutputRepairPrompt(r.cfg, task, session, runID, raw, parseErr),
		},
	}

	response, err := r.agent.LLM.Chat(ctx, messages, nil)
	if err != nil {
		return "", err
	}
	if response == nil {
		return "", fmt.Errorf("repair backend returned nil response")
	}
	if len(response.ToolCalls) > 0 {
		return "", fmt.Errorf("repair backend returned %d tool calls", len(response.ToolCalls))
	}

	return strings.TrimSpace(response.Content), nil
}

func (r *Runtime) validateStructuredOutputRepairAuthority(task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string) error {
	if r == nil {
		return fmt.Errorf("structured output repair authority unavailable")
	}
	if r.cfg.Mode != RuntimeModeDaemon {
		return fmt.Errorf("structured output repair requires daemon runtime mode")
	}

	taskID := strings.TrimSpace(task.TaskID)
	sessionID := strings.TrimSpace(session.SessionID)
	runID = strings.TrimSpace(runID)
	if taskID == "" || sessionID == "" || runID == "" {
		return fmt.Errorf("structured output repair requires non-empty task_id, session_id, and run_id")
	}
	if sessionTaskID := strings.TrimSpace(session.TaskID); sessionTaskID != "" && sessionTaskID != taskID {
		return fmt.Errorf("structured output repair session task mismatch: session task_id=%s repair task_id=%s", sessionTaskID, taskID)
	}

	expectedSnapshotID := daemonRunCapabilitySnapshotID(r.cfg, task, session, runID)
	r.mu.Lock()
	activeTaskID := ""
	if r.activeTask != nil {
		activeTaskID = strings.TrimSpace(r.activeTask.TaskID)
	}
	activeSessionID := ""
	activeSessionTaskID := ""
	if r.activeSession != nil {
		activeSessionID = strings.TrimSpace(r.activeSession.SessionID)
		activeSessionTaskID = strings.TrimSpace(r.activeSession.TaskID)
	}
	activeRunID := strings.TrimSpace(r.activeRunID)
	scratchTaskID := strings.TrimSpace(r.scratch.ActiveTaskID)
	scratchSessionID := strings.TrimSpace(r.scratch.ActiveSessionID)
	scratchRunID := strings.TrimSpace(r.scratch.ActiveRunID)
	activeSnapshotID := strings.TrimSpace(r.activeCapabilitySnapshotID)
	scratchSnapshotID := strings.TrimSpace(r.scratch.ActiveCapabilitySnapshotID)
	r.mu.Unlock()

	if activeTaskID != "" && activeTaskID != taskID {
		return fmt.Errorf("structured output repair active task mismatch: active=%s repair=%s", activeTaskID, taskID)
	}
	if activeSessionID != "" && activeSessionID != sessionID {
		return fmt.Errorf("structured output repair active session mismatch: active=%s repair=%s", activeSessionID, sessionID)
	}
	if activeSessionTaskID != "" && activeSessionTaskID != taskID {
		return fmt.Errorf("structured output repair active session task mismatch: active=%s repair=%s", activeSessionTaskID, taskID)
	}
	if activeRunID != "" && activeRunID != runID {
		return fmt.Errorf("structured output repair active run mismatch: active=%s repair=%s", activeRunID, runID)
	}
	if scratchTaskID != "" && scratchTaskID != taskID {
		return fmt.Errorf("structured output repair scratch task mismatch: scratch=%s repair=%s", scratchTaskID, taskID)
	}
	if scratchSessionID != "" && scratchSessionID != sessionID {
		return fmt.Errorf("structured output repair scratch session mismatch: scratch=%s repair=%s", scratchSessionID, sessionID)
	}
	if scratchRunID != "" && scratchRunID != runID {
		return fmt.Errorf("structured output repair scratch run mismatch: scratch=%s repair=%s", scratchRunID, runID)
	}
	if activeSnapshotID != "" && activeSnapshotID != expectedSnapshotID {
		return fmt.Errorf("structured output repair active capability snapshot mismatch: active=%s repair=%s", activeSnapshotID, expectedSnapshotID)
	}
	if scratchSnapshotID != "" && scratchSnapshotID != expectedSnapshotID {
		return fmt.Errorf("structured output repair scratch capability snapshot mismatch: scratch=%s repair=%s", scratchSnapshotID, expectedSnapshotID)
	}
	if activeTaskID == "" || activeSessionID == "" || activeRunID == "" {
		return fmt.Errorf("structured output repair requires active task/session/run binding")
	}
	if scratchTaskID == "" || scratchSessionID == "" || scratchRunID == "" {
		return fmt.Errorf("structured output repair requires scratch task/session/run binding")
	}
	if activeSnapshotID == "" || scratchSnapshotID == "" {
		return fmt.Errorf("structured output repair requires active run capability snapshot binding")
	}
	return nil
}

func (r *Runtime) buildStructuredOutputRepairSystemPrompt(task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string) string {
	cfg := RuntimeConfig{}
	if r != nil {
		cfg = r.cfg
	}
	cfg.ApplyDefaults()
	snapshotID := daemonRunCapabilitySnapshotID(cfg, task, session, runID)

	var b strings.Builder
	b.WriteString("Repair the malformed structured task result.\n\n")
	b.WriteString("## Structured Output Repair Contract\n")
	b.WriteString(fmt.Sprintf("- repair_contract: %s\n", structuredOutputRepairContract))
	b.WriteString(fmt.Sprintf("- repair_mode: %s\n", structuredOutputRepairMode))
	b.WriteString(fmt.Sprintf("- repair_action: %s\n", structuredOutputRepairAction))
	b.WriteString("- tool_call_policy: forbidden\n")
	b.WriteString("- side_effect_policy: forbidden\n")
	b.WriteString("- allowed_output: valid_json_only\n")
	b.WriteString("- authority_scope: existing_task_cycle_result_repair_only\n")
	b.WriteString(fmt.Sprintf("- task_id: %s\n", strings.TrimSpace(task.TaskID)))
	b.WriteString(fmt.Sprintf("- session_id: %s\n", strings.TrimSpace(session.SessionID)))
	b.WriteString(fmt.Sprintf("- run_id: %s\n", strings.TrimSpace(runID)))
	b.WriteString(fmt.Sprintf("- capability_snapshot_id: %s\n", snapshotID))
	b.WriteString(fmt.Sprintf("- capability_snapshot_ref: %s\n", capabilitySnapshotEvidenceRef(snapshotID)))
	b.WriteString("\nHard rules:\n")
	b.WriteString("- Return only JSON that matches the structured task result contract.\n")
	b.WriteString("- Do not call tools, browse, inspect files, mutate state, or invent new work.\n")
	b.WriteString("- Preserve the original assistant intent where possible; only normalize it into the contract.\n")
	b.WriteString("- If the original output cannot support a valid result, return outcome=failed with a factual summary.\n")
	return strings.TrimSpace(b.String())
}

func buildStructuredOutputRepairPrompt(cfg RuntimeConfig, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID, raw string, parseErr error) string {
	var b strings.Builder
	b.WriteString("This is a bounded repair_structured_output attempt.\n")
	b.WriteString("Fix the malformed structured task result and return only JSON.\n")
	b.WriteString("Do not add markdown fences, commentary, or tool calls.\n\n")
	b.WriteString("Structured task result contract:\n")
	b.WriteString(strings.TrimSpace(runtimeStructuredTaskResultContract(cfg)))
	b.WriteString("\n\n")
	b.WriteString("Context:\n")
	b.WriteString(fmt.Sprintf("- task_id: %s\n", strings.TrimSpace(task.TaskID)))
	if strings.TrimSpace(task.Title) != "" {
		b.WriteString(fmt.Sprintf("- task_title: %s\n", strings.TrimSpace(task.Title)))
	}
	if strings.TrimSpace(session.SessionID) != "" {
		b.WriteString(fmt.Sprintf("- session_id: %s\n", strings.TrimSpace(session.SessionID)))
	}
	if strings.TrimSpace(runID) != "" {
		b.WriteString(fmt.Sprintf("- run_id: %s\n", strings.TrimSpace(runID)))
	}
	b.WriteString("\nValidation error:\n")
	b.WriteString(strings.TrimSpace(parseErr.Error()))
	b.WriteString("\n\nOriginal output:\n")
	b.WriteString(strings.TrimSpace(raw))
	b.WriteString("\n")
	return b.String()
}

func structuredOutputRepairVerification(raw string, repairInfo *structuredOutputRepairInfo) map[string]any {
	verification := map[string]any{
		"repair_action": structuredOutputRepairAction,
		"failure_code":  "",
		"status":        "valid",
		"attempted":     false,
		"attempts":      0,
		"parse_error":   "",
		"initial_raw":   truncate(strings.TrimSpace(raw), 1200),
		"repair_output": "",
		"repair_error":  "",
	}
	if repairInfo == nil {
		return verification
	}

	verification["attempted"] = repairInfo.Attempted
	verification["attempts"] = repairInfo.Attempts
	verification["parse_error"] = truncate(strings.TrimSpace(repairInfo.ParseError), 600)
	verification["repair_output"] = truncate(strings.TrimSpace(repairInfo.RepairOutput), 1200)
	verification["repair_error"] = truncate(strings.TrimSpace(repairInfo.RepairError), 600)
	if repairInfo.Attempted {
		verification["failure_code"] = structuredOutputFailureCodeInvalid
		if strings.TrimSpace(repairInfo.RepairError) != "" {
			verification["status"] = "repair_failed"
		} else {
			verification["status"] = "repaired"
		}
	}
	return verification
}

func structuredOutputFailureResult(task WorkspaceTaskRecord, raw string, repairInfo *structuredOutputRepairInfo, cause error) StructuredTaskResult {
	parts := []string{
		"Structured output repair failed",
	}
	if repairInfo != nil && strings.TrimSpace(repairInfo.ParseError) != "" {
		parts = append(parts, "parse error: "+strings.TrimSpace(repairInfo.ParseError))
	}
	if repairInfo != nil && strings.TrimSpace(repairInfo.RepairError) != "" {
		parts = append(parts, "repair error: "+strings.TrimSpace(repairInfo.RepairError))
	} else if cause != nil {
		parts = append(parts, "repair error: "+strings.TrimSpace(cause.Error()))
	}
	summary := strings.Join(parts, "; ")
	details := strings.Join([]string{
		"bounded repair action: " + structuredOutputRepairAction,
		"initial output: " + truncate(strings.TrimSpace(raw), 1200),
	}, "\n")
	if repairInfo != nil && strings.TrimSpace(repairInfo.RepairOutput) != "" {
		details += "\nrepaired output: " + truncate(strings.TrimSpace(repairInfo.RepairOutput), 1200)
	}
	return StructuredTaskResult{
		Outcome:   "failed",
		Summary:   summary,
		Details:   details,
		BlockedOn: normalizedBlockedRefs(nil, firstNonEmpty(summary, task.Title, task.TaskID)),
	}
}

func (r *Runtime) executeTaskCycle(ctx context.Context, task WorkspaceTaskRecord) error {
	session, runID, err := r.ensureSessionForTask(ctx, task)
	if err != nil {
		return err
	}
	r.logTaskCycleStartMemory(task, session, runID)
	r.recordTaskCycleStart(task, time.Now().UTC())
	r.clearPolicySignals()
	if err := r.refreshToolPlane(ctx); err != nil {
		log.Printf("[tool-plane] refresh routed tool plane degraded: %v", err)
	}
	workPacket := r.currentWorkPacketCopy()
	if err := r.refreshCompletionCoordinationResponse(ctx, task, *session, runID); err != nil && ctx.Err() == nil {
		log.Printf("[runtime] completion coordination response refresh failed for task %s: %v", task.TaskID, err)
	}
	if changed, err := r.refreshInboxDrainAdvisory(ctx); err != nil && ctx.Err() == nil {
		log.Printf("[runtime] inbox drain advisory refresh failed for task %s: %v", task.TaskID, err)
	} else if changed {
		r.invalidateFocus()
	}
	runSnapshot, runSnapshotPath, err := r.captureRunCapabilitySnapshot(task, *session, runID, workPacket)
	if err != nil {
		return err
	}
	initialPhaseRecords := taskCycleInitialPhaseRecords(r.cfg, task, *session, runID, workPacket)

	if _, err := r.client.WriteExecutionStep(ctx, ExecutionStepWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		Phase:       "PLAN",
		Title:       firstNonEmpty(task.Title, "Plan task "+task.TaskID),
		Summary:     firstNonEmpty(task.Description, "RNAR planning step"),
		Status:      "COMPLETED",
		SortOrder:   10,
		Evidence:    prependCapabilitySnapshotEvidence(runSnapshot.SnapshotID, nil),
		Verification: map[string]any{
			"capability_snapshot_id":     runSnapshot.SnapshotID,
			"capability_snapshot_ref":    capabilitySnapshotEvidenceRef(runSnapshot.SnapshotID),
			"capability_snapshot_path":   runSnapshotPath,
			"capability_snapshot":        runSnapshot,
			"prompt_capability_evidence": daemonPromptCapabilityEvidence(runSnapshot, runSnapshotPath),
			"real_llm_pilot_profile":     buildRealLLMPilotProfileEvidence(r.cfg),
			"repo_worktree_evidence":     buildRepoWorktreeEvidence(r.cfg, time.Now().UTC()),
			"repo_merge_queue_evidence":  buildRepoMergeQueueEvidence(r.cfg, task, *session, runID),
			"task_cycle_phase_records":   initialPhaseRecords,
			"task_cycle_phase_record":    lastTaskCyclePhaseRecord(initialPhaseRecords),
		},
	}); err != nil {
		return err
	}

	trace := &TaskRunTrace{}
	specPack := appendSpecPack(r.activeCapabilitySnapshotPromptProjection(runSnapshot), r.buildDaemonSpecPack(ctx, &task))
	hydration := r.taskHydration(ctx, &task)
	taskCtx := AgentTaskContext{
		Mode:                     "daemon",
		WorkspaceID:              r.cfg.WorkspaceID,
		AgentID:                  r.cfg.AgentID,
		SessionID:                session.SessionID,
		RunID:                    runID,
		CapabilitySnapshotID:     runSnapshot.SnapshotID,
		CapabilitySnapshotSchema: runSnapshot.Schema,
		Summary:                  r.currentSummary(),
		SpecPack:                 specPack,
		Task:                     &task,
		Hydration:                hydration,
		Packet:                   workPacket,
		AdvisorySignals:          r.scratch.AdvisorySignals,
		ReflectionSignals:        r.scratch.ReflectionSignals,
		ToolLoopLimit:            r.cfg.MaxToolLoopIterations,
	}
	// If the previous cycle for this task hit a transient provider
	// timeout, surface the persisted compact resume summary at the front of the
	// advisory signals so the model resumes from the prior progress instead of
	// rediscovering it. It is cleared after this cycle so it cannot leak forward.
	if resumeAdvisory := r.timeoutResumeAdvisoryForTask(task); resumeAdvisory != "" {
		taskCtx.AdvisorySignals = append([]string{resumeAdvisory}, taskCtx.AdvisorySignals...)
		if err := r.clearTimeoutResumeSummary(ctx, task.TaskID); err != nil {
			log.Printf("[runtime] clear transient timeout resume summary failed for task %s: %v", strings.TrimSpace(task.TaskID), err)
		}
	}
	if hydration == nil {
		taskCtx.Snapshot = &r.bootstrap.Snapshot
	}
	if preflightResult, ok := patchQueueSupersedeStewardshipPreflightBlockResult(task); ok {
		return r.blockTaskCycle(ctx, task, *session, runID, r.normalizeAutonomyResult(preflightResult), workPacket, trace)
	}
	toolExecutor := func(toolCtx context.Context, registry *ToolRegistry, call ToolCall) ToolResult {
		return r.taskCycleToolExecutorWithTrace(toolCtx, registry, call, task, workPacket, trace)
	}
	r.telicSnapshotBaseline(ctx, task)
	finalRaw, err := r.agent.ExecuteTaskWithHooks(ctx, r.cycleObjective(task, *session), taskCtx, toolExecutor, &taskLoopObserver{ctx: ctx, trace: trace, runtime: r})
	if err != nil {
		failCtx, cancelFail := taskCycleFailureContext(ctx)
		defer cancelFail()
		if isTransientYieldableExecError(ctx, err) {
			return r.yieldTransientProviderTimeoutTaskCycle(failCtx, task, *session, runID, err, workPacket, trace)
		}
		if recovered, recoveryErr := r.recoverProjectImplementationExecutionError(failCtx, task, *session, runID, err, workPacket); recovered {
			if recoveryErr != nil {
				return recoveryErr
			}
			return nil
		}
		failErr := r.failTaskCycle(failCtx, task, *session, runID, err, workPacket, trace)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			if failErr != nil {
				return fmt.Errorf("%w: task cycle timed out; failure terminalization failed: %v", errRuntimePlannerWorkCycleTimeout, failErr)
			}
			if clearErr := r.clearActiveState(failCtx, "Task cycle timed out and was terminalized"); clearErr != nil {
				return fmt.Errorf("%w: task cycle timed out; active-state cleanup failed: %v", errRuntimePlannerWorkCycleTimeout, clearErr)
			}
			return fmt.Errorf("%w: task cycle timed out and was terminalized: %v", errRuntimePlannerWorkCycleTimeout, err)
		}
		return failErr
	}

	result, repairInfo, resolveErr := r.resolveStructuredTaskResult(ctx, task, *session, runID, finalRaw)
	executeStatus := "FAILED"
	executeTitle := "Repair structured output"
	if resolveErr == nil {
		executeStatus = stepStatusForOutcome(result.Outcome)
		executeTitle = "Execution cycle"
	}
	assistantTurns := trace.AssistantTurns
	if repairInfo != nil {
		assistantTurns += repairInfo.Attempts
	}
	if resolveErr != nil {
		result = structuredOutputFailureResult(task, finalRaw, repairInfo, resolveErr)
	}
	result = r.applyPolicySignals(result)
	result = r.normalizeAutonomyResult(result)
	result = authorityPreemptionYieldResult(task, result, trace)
	if result.Summary == "" {
		result.Summary = firstNonEmpty(task.Title, task.TaskID)
	}
	if resolveErr == nil {
		if err := r.applyNoopContinueGuard(ctx, task, *session, runID, &result, trace); err != nil {
			return err
		}
		result = r.normalizeAutonomyResult(result)
		if patchQueueSupersedeStewardshipToolFailed(task, result, trace) {
			result = r.normalizeAutonomyResult(blockedPatchQueueSupersedeStewardshipResult(result))
		}
		if patchQueueSupersedeStewardshipInvalidEvidenceObserved(task, result) {
			result = r.normalizeAutonomyResult(blockedPatchQueueSupersedeStewardshipResult(result))
		}
		executeStatus = stepStatusForOutcome(result.Outcome)
		executeTitle = "Execution cycle"
	}
	outcomeBeforeEvidenceGates := normalizeOutcome(result.Outcome)
	evidence := append([]string(nil), trace.ToolCalls...)
	if repairInfo != nil && repairInfo.Attempted {
		evidence = append(evidence, structuredOutputRepairAction)
	}
	evidence = prependCapabilitySnapshotEvidence(runSnapshot.SnapshotID, evidence)
	if _, err := r.client.WriteExecutionStep(ctx, ExecutionStepWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		Phase:       "EXECUTE",
		Title:       executeTitle,
		Summary:     result.Summary,
		Status:      executeStatus,
		SortOrder:   20,
		Evidence:    evidence,
		Verification: map[string]any{
			"assistant_turns":            assistantTurns,
			"capability_snapshot_id":     runSnapshot.SnapshotID,
			"capability_snapshot_ref":    capabilitySnapshotEvidenceRef(runSnapshot.SnapshotID),
			"capability_snapshot_path":   runSnapshotPath,
			"capability_snapshot":        runSnapshot,
			"prompt_capability_evidence": daemonPromptCapabilityEvidence(runSnapshot, runSnapshotPath),
			"raw_result":                 truncate(strings.TrimSpace(finalRaw), 1200),
			"local_reflection_expected":  runtimeTrustFirst(r.cfg),
			"public_reflection":          result.Reflection,
			"public_reflection_present":  result.Reflection != nil,
			"structured_output_repair":   structuredOutputRepairVerification(finalRaw, repairInfo),
			"task_cycle_phase_records": taskCycleProgressPhaseRecords(
				r.cfg,
				task,
				*session,
				runID,
				workPacket,
				&result,
				trace,
			),
			"task_cycle_phase_record": taskCyclePhaseRecordEnvelope(
				r.cfg,
				task,
				*session,
				runID,
				"PROGRESS_RECORDED",
				"EXECUTE",
				workPacket,
				&result,
				trace,
				6,
			),
		},
	}); err != nil {
		return err
	}

	if result.Outcome == "completed" {
		gatedResult, _, err := r.enforceCompletionCoordinationGate(ctx, task, *session, runID, result, trace, workPacket)
		if err != nil {
			return err
		}
		result = gatedResult
	}

	if result.Outcome == "completed" {
		gatedResult, _, err := r.enforceProjectPatchQueueIntegrationAssemblyGate(task, result, trace)
		if err != nil {
			return err
		}
		result = gatedResult
	}

	if result.Outcome == "completed" {
		gatedResult, _, err := r.enforceProjectImplementationCompletionEvidenceGate(ctx, task, result)
		if err != nil {
			return err
		}
		result = gatedResult
	}
	if normalizeOutcome(result.Outcome) == "blocked" {
		gatedResult, _, err := r.enforceProjectImplementationSelfActionableGitEvidenceBlock(ctx, task, result)
		if err != nil {
			return err
		}
		result = gatedResult
	}
	if normalizeOutcome(result.Outcome) == "failed" {
		gatedResult, _, err := r.enforceProjectImplementationRecoverableFailureGate(ctx, task, result)
		if err != nil {
			return err
		}
		result = gatedResult
	}
	if resolveErr == nil &&
		outcomeBeforeEvidenceGates != "continue" &&
		taskCycleIsSelfActionableToolTransitionNoop(task, result, trace) {
		if err := r.applyNoopContinueGuard(ctx, task, *session, runID, &result, trace); err != nil {
			return err
		}
		result = r.normalizeAutonomyResult(result)
	}
	if resolveErr == nil {
		if err := r.applyRequiredTransitionExecutionGate(ctx, task, *session, runID, &result, trace, workPacket); err != nil {
			return err
		}
		result = r.normalizeAutonomyResult(result)
		result = completeRecordedAuthorityTransitionDenial(task, result, trace, workPacket)
		result = completeRecordedAuthorityTransitionApplied(task, result, trace, workPacket)
		result = completeProjectPatchQueueIntegrationTerminalReceipt(task, result, trace)
		result = completeProjectClaimRepairReceipt(task, result, trace)
		result = completeSideEffectResolutionSuccessorReceipt(task, result, trace)
		result = completeSideEffectClassifierSuccessorHandoff(task, result, trace)
		result = completeSideEffectClassifierTerminalDecision(task, result, trace)
		result = r.normalizeAutonomyResult(result)
		if result.Outcome == "completed" {
			gatedResult, err := r.enforcePostReceiptTerminalCompletionEvidenceGate(ctx, task, *session, runID, result, trace, workPacket)
			if err != nil {
				return err
			}
			result = r.normalizeAutonomyResult(gatedResult)
		}
	}

	if result.Outcome == "completed" {
		drifted, err := r.shouldSuppressStaleCompletionTransition(ctx, task)
		if err != nil {
			return fmt.Errorf("verify completion authority before materialization: %w", err)
		}
		if drifted {
			return r.fencedLateCompletionCycle(ctx, task, *session, runID, result, "before materialization", workPacket)
		}
	}

	if resolveErr == nil && (normalizeOutcome(result.Outcome) == "continue" || (result.RequiresHuman && normalizeOutcome(result.Outcome) != "completed")) {
		if err := r.materializeCycle(ctx, task, *session, runID, result); err != nil {
			return err
		}
	}
	r.recordTaskCycleProgress(result, trace, repairInfo, time.Now().UTC())
	if resolveErr != nil {
		if failedPatchQueueSupersedeStewardshipTask(task, result) {
			return r.blockTaskCycle(ctx, task, *session, runID, blockedPatchQueueSupersedeStewardshipResult(result), workPacket, trace)
		}
		if failedPatchQueueConvergenceTask(task, result, repairInfo) {
			return r.blockTaskCycle(ctx, task, *session, runID, blockedPatchQueueConvergenceFailureResult(result), workPacket, trace)
		}
		return r.releaseFailedTaskCycle(ctx, task, *session, runID, result, workPacket, trace)
	}
	if result.RequiresHuman && normalizeOutcome(result.Outcome) != "completed" {
		return r.decisionTaskCycle(ctx, task, *session, runID, result)
	}

	switch result.Outcome {
	case "completed":
		return r.completeTaskCycle(ctx, task, *session, runID, result, workPacket, trace)
	case "blocked":
		return r.blockTaskCycle(ctx, task, *session, runID, result, workPacket, trace)
	case "failed":
		return r.failedTaskCycle(ctx, task, *session, runID, result, workPacket, trace)
	default:
		return r.continueTaskCycle(ctx, task, *session, runID, result, trace)
	}
}

func taskCycleFailureContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return runtimeBudgetCleanupContext()
	}
	return ctx, func() {}
}

func isTransientProviderTimeoutError(err error) bool {
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	text := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(text, "llm provider call timed out") ||
		strings.Contains(text, "codex exec timed out")
}

func isTransientProviderCapacityError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(strings.TrimSpace(err.Error()))
	if text == "" {
		return false
	}
	switch {
	case strings.Contains(text, "selected model is at capacity"),
		strings.Contains(text, "model is at capacity"),
		strings.Contains(text, "provider is at capacity"),
		strings.Contains(text, "server is at capacity"),
		strings.Contains(text, "server is overloaded"),
		strings.Contains(text, "temporarily overloaded"),
		strings.Contains(text, "temporarily unavailable"),
		strings.Contains(text, "rate limit exceeded"),
		strings.Contains(text, "rate limited"),
		strings.Contains(text, "too many requests"):
		return true
	default:
		return false
	}
}

// isTransientYieldableExecError generalizes CR-41 (provider timeout) to ALL transient/recoverable
// execution-cycle errors that should YIELD (release the claim for retry) instead of being normalized
// to a durable operator-block:
//   - a transient budget reserve/capture RPC blip (errRuntimeBudgetExhausted wrapping a
//     transport / HTTP 5xx / deadline signature). Genuine exhaustion (daily/weekly token, usage
//     exceeded reserved, account unavailable) lacks that signature, so isTransientRuntimeBudgetCleanupError
//     returns false for it and it correctly still BLOCKS.
//   - CW-A3: any other transient backend RPC error (transport / read body / HTTP 5xx), or an inner RPC
//     deadline that is NOT the task cycle's own deadline.
//
// The task cycle's OWN deadline must terminalize (never infinite-yield), unless the execution error
// carries an explicit provider-transient marker. Provider calls can consume the same outer deadline;
// those still release for retry instead of becoming durable operator blockers.
func isTransientYieldableExecError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if isTransientProviderTimeoutError(err) || isTransientProviderCapacityError(err) {
		return true
	}
	if ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return false
	}
	return isTransientRuntimeBudgetCleanupError(err)
}

func (r *Runtime) recoverProjectImplementationExecutionError(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, execErr error, workPacket *AgentWorkPacket) (bool, error) {
	if r == nil || !runtimeProjectCompletionEvidenceGateRequired(task) {
		return false, nil
	}
	detail, ok, err := r.recoverableProjectImplementationWorkDetail(ctx, task)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	result := r.normalizeAutonomyResult(recoverableProjectImplementationExecutionErrorResult(execErr, detail))
	if _, err := r.client.WriteExecutionStep(ctx, ExecutionStepWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		Phase:       "RECOVER",
		Title:       "Recover project execution error",
		Summary:     result.Summary,
		Status:      "ACTIVE",
		SortOrder:   25,
		Evidence:    phaseEvidenceRefs(&result),
		Verification: map[string]any{
			"recovery":                "recoverable_project_work",
			"execution_error":         strings.TrimSpace(fmt.Sprint(execErr)),
			"task_cycle_phase_record": taskCyclePhaseRecordEnvelope(r.cfg, task, session, runID, "PROGRESS_RECORDED", "RECOVER", workPacket, &result, nil, 7),
		},
	}); err != nil {
		return true, err
	}
	if err := r.materializeCycle(ctx, task, session, runID, result); err != nil {
		return true, err
	}
	if err := r.continueTaskCycle(ctx, task, session, runID, result, nil); err != nil {
		return true, err
	}
	return true, nil
}

func (r *Runtime) policyAwareToolExecutor(ctx context.Context, registry *ToolRegistry, call ToolCall) ToolResult {
	if !taskCycleRequiredToolGuardApplied(ctx) {
		if result, ok := r.activeTaskBoundRequiredToolSubstituteToolResult(call.Function.Name); ok {
			return result
		}
	}
	capability := toolCapabilityForName(call.Function.Name)
	if reason, disabled := capabilityDisabledReasonForTool(r.cfg, call.Function.Name); disabled {
		r.recordPolicySignal(ToolPolicySignal{
			ToolName:    call.Function.Name,
			Verdict:     "DENY",
			Capability:  capability,
			Description: fmt.Sprintf("runtime capability snapshot disabled tool %s (%s)", call.Function.Name, reason),
		})
		if runtimeTrustFirst(r.cfg) {
			return r.executeTrustFirstAdvisoryTool(ctx, registry, call, fmt.Sprintf("runtime capability snapshot marked tool %s disabled (%s); executed anyway in trust-first mode", call.Function.Name, reason))
		}
		return ToolResult{Output: fmt.Sprintf("runtime capability snapshot disabled tool %s (%s); use repo authority patch flow instead", call.Function.Name, reason), IsError: true}
	}
	check, err := r.client.CheckPolicy(ctx, PolicyCheckInput{
		WorkspaceID: r.cfg.WorkspaceID,
		SubjectType: "agent",
		SubjectID:   r.cfg.AgentID,
		Capability:  capability,
		ToolID:      call.Function.Name,
	})
	if err != nil {
		if runtimeTrustFirst(r.cfg) {
			r.recordPolicySignal(ToolPolicySignal{
				ToolName:    call.Function.Name,
				Verdict:     "POLICY_UNAVAILABLE",
				Capability:  capability,
				Description: fmt.Sprintf("policy check unavailable for tool %s: %v", call.Function.Name, err),
			})
			return r.executeTrustFirstAdvisoryTool(ctx, registry, call, fmt.Sprintf("policy check failed for %s (%v); executed anyway in trust-first mode", call.Function.Name, err))
		}
		return ToolResult{Output: fmt.Sprintf("policy check failed for %s: %v", call.Function.Name, err), IsError: true}
	}
	switch strings.ToUpper(strings.TrimSpace(check.Verdict)) {
	case "DENY":
		r.recordPolicySignal(ToolPolicySignal{
			ToolName:    call.Function.Name,
			Verdict:     "DENY",
			Capability:  capability,
			Description: fmt.Sprintf("policy denied tool %s", call.Function.Name),
		})
		if runtimeTrustFirst(r.cfg) {
			return r.executeTrustFirstAdvisoryTool(ctx, registry, call, fmt.Sprintf("policy denied tool %s for capability %s; executed anyway in trust-first mode", call.Function.Name, capability))
		}
		return ToolResult{Output: fmt.Sprintf("policy denied tool %s; do not retry blindly", call.Function.Name), IsError: true}
	case "REQUIRE_APPROVAL":
		r.recordPolicySignal(ToolPolicySignal{
			ToolName:    call.Function.Name,
			Verdict:     "REQUIRE_APPROVAL",
			Capability:  capability,
			Description: fmt.Sprintf("policy requires approval for tool %s", call.Function.Name),
		})
		if runtimeTrustFirst(r.cfg) {
			return r.executeTrustFirstAdvisoryTool(ctx, registry, call, fmt.Sprintf("policy requires approval for tool %s; executed anyway in trust-first mode", call.Function.Name))
		}
		return ToolResult{Output: fmt.Sprintf("policy requires approval for tool %s; ask for approval instead of retrying", call.Function.Name), IsError: true}
	default:
		if capability == "tool.call" {
			return r.postProcessToolResult(call, defaultToolLoopExecutor(ctx, registry, call))
		}
	}

	check, err = r.client.CheckPolicy(ctx, PolicyCheckInput{
		WorkspaceID: r.cfg.WorkspaceID,
		SubjectType: "agent",
		SubjectID:   r.cfg.AgentID,
		Capability:  "tool.call",
		ToolID:      call.Function.Name,
	})
	if err != nil {
		if runtimeTrustFirst(r.cfg) {
			r.recordPolicySignal(ToolPolicySignal{
				ToolName:    call.Function.Name,
				Verdict:     "POLICY_UNAVAILABLE",
				Capability:  "tool.call",
				Description: fmt.Sprintf("tool.call policy check unavailable for tool %s: %v", call.Function.Name, err),
			})
			return r.executeTrustFirstAdvisoryTool(ctx, registry, call, fmt.Sprintf("tool.call policy check failed for %s (%v); executed anyway in trust-first mode", call.Function.Name, err))
		}
		return ToolResult{Output: fmt.Sprintf("policy check failed for %s: %v", call.Function.Name, err), IsError: true}
	}
	switch strings.ToUpper(strings.TrimSpace(check.Verdict)) {
	case "DENY":
		r.recordPolicySignal(ToolPolicySignal{
			ToolName:    call.Function.Name,
			Verdict:     "DENY",
			Capability:  "tool.call",
			Description: fmt.Sprintf("policy denied tool %s", call.Function.Name),
		})
		if runtimeTrustFirst(r.cfg) {
			return r.executeTrustFirstAdvisoryTool(ctx, registry, call, fmt.Sprintf("policy denied tool %s for tool.call; executed anyway in trust-first mode", call.Function.Name))
		}
		return ToolResult{Output: fmt.Sprintf("policy denied tool %s; do not retry blindly", call.Function.Name), IsError: true}
	case "REQUIRE_APPROVAL":
		r.recordPolicySignal(ToolPolicySignal{
			ToolName:    call.Function.Name,
			Verdict:     "REQUIRE_APPROVAL",
			Capability:  "tool.call",
			Description: fmt.Sprintf("policy requires approval for tool %s", call.Function.Name),
		})
		if runtimeTrustFirst(r.cfg) {
			return r.executeTrustFirstAdvisoryTool(ctx, registry, call, fmt.Sprintf("policy requires approval for tool %s; executed anyway in trust-first mode", call.Function.Name))
		}
		return ToolResult{Output: fmt.Sprintf("policy requires approval for tool %s; ask for approval instead of retrying", call.Function.Name), IsError: true}
	default:
		return r.postProcessToolResult(call, defaultToolLoopExecutor(ctx, registry, call))
	}
}

func (r *Runtime) executeTrustFirstAdvisoryTool(ctx context.Context, registry *ToolRegistry, call ToolCall, advisory string) ToolResult {
	result := r.postProcessToolResult(call, defaultToolLoopExecutor(ctx, registry, call))
	advisory = strings.TrimSpace(advisory)
	if advisory == "" {
		return result
	}
	suffix := "trust_first policy advisory: " + advisory
	if strings.TrimSpace(result.Output) == "" {
		result.Output = suffix
	} else if !strings.Contains(result.Output, suffix) {
		result.Output = strings.TrimRight(result.Output, "\r\n") + "\n" + suffix
	}
	return result
}

func (r *Runtime) taskCycleToolExecutor(ctx context.Context, registry *ToolRegistry, call ToolCall) ToolResult {
	return r.taskCycleToolExecutorWithTrace(ctx, registry, call, WorkspaceTaskRecord{}, nil, nil)
}

func (r *Runtime) taskCycleToolExecutorWithTrace(ctx context.Context, registry *ToolRegistry, call ToolCall, task WorkspaceTaskRecord, workPacket *AgentWorkPacket, trace *TaskRunTrace) ToolResult {
	toolName := strings.TrimSpace(call.Function.Name)
	if trigger, activeTaskID, ok := r.pendingAuthorityTransitionPreemptsActiveTask(); ok {
		r.recordPolicySignal(ToolPolicySignal{
			ToolName:    toolName,
			Verdict:     "YIELD",
			Capability:  toolCapabilityForName(toolName),
			Description: fmt.Sprintf("pending authority transition %s preempts active task %s", firstNonEmpty(strings.TrimSpace(trigger.TaskID), "unknown"), firstNonEmpty(activeTaskID, "unknown")),
		})
		return authorityPreemptionYieldToolResult(toolName, trigger, activeTaskID)
	}
	if requiredTool, ok := requiredTransitionReceiptPreemptsToolCall(task, workPacket, trace); ok {
		r.recordPolicySignal(ToolPolicySignal{
			ToolName:    toolName,
			Verdict:     "YIELD",
			Capability:  toolCapabilityForName(toolName),
			Description: fmt.Sprintf("required transition receipt %s already recorded; blocking further tool call %s so the task can terminalize", requiredTool, firstNonEmpty(toolName, "unknown")),
		})
		return requiredTransitionReceiptYieldToolResult(toolName, requiredTool)
	}
	if requiredTool, reason, ok := taskBoundRequiredToolPreemptsSubstituteToolCall(task, trace, toolName); ok {
		r.recordPolicySignal(ToolPolicySignal{
			ToolName:    toolName,
			Verdict:     "YIELD",
			Capability:  toolCapabilityForName(toolName),
			Description: fmt.Sprintf("required task-bound tool %s preempts substitute tool %s: %s", requiredTool, firstNonEmpty(toolName, "unknown"), reason),
		})
		return taskBoundRequiredToolSubstituteYieldToolResult(toolName, requiredTool, reason)
	}
	if err := r.keepaliveTaskLoopSession(ctx, "before_tool", toolName, true); err != nil {
		return taskLoopSessionFenceToolResult(toolName, err)
	}

	result := r.policyAwareToolExecutor(context.WithValue(ctx, taskCycleRequiredToolGuardContextKey{}, true), registry, call)
	if err := r.keepaliveTaskLoopSession(ctx, "after_tool", toolName, true); err != nil {
		if result.IsError {
			result.Output = strings.TrimRight(result.Output, "\r\n") + "\n" + taskLoopSessionFenceMessage(toolName, err)
			return result
		}
		return taskLoopSessionFenceToolResult(toolName, err)
	}
	return result
}

func (r *Runtime) keepaliveTaskLoopSession(ctx context.Context, phase, detail string, strict bool) error {
	if r == nil || r.cfg.Mode != RuntimeModeDaemon {
		return nil
	}
	session := r.currentSessionCopy()
	if session == nil {
		if strict {
			return fmt.Errorf("active task-loop session is missing before %s", firstNonEmpty(strings.TrimSpace(detail), strings.TrimSpace(phase), "tool execution"))
		}
		return nil
	}
	if !isSessionRunnable(session) {
		if strict {
			return fmt.Errorf("active task-loop session %s is not runnable: status=%s", firstNonEmpty(strings.TrimSpace(session.SessionID), "unknown"), firstNonEmpty(strings.TrimSpace(session.Status), "unknown"))
		}
		return nil
	}

	phase = firstNonEmpty(strings.TrimSpace(phase), "progress")
	detail = strings.TrimSpace(detail)
	summary := "Task loop progress: " + phase
	if detail != "" {
		summary += " " + detail
	}
	keepalive := true
	state, err := r.client.SessionEvent(ctx, "agent.session.keepalive", SessionEventInput{
		WorkspaceID:       r.cfg.WorkspaceID,
		SessionID:         session.SessionID,
		AgentID:           r.cfg.AgentID,
		TaskID:            session.TaskID,
		Summary:           summary,
		Status:            firstNonEmpty(strings.TrimSpace(session.Status), "ACTIVE"),
		OwnerScope:        "task/session",
		KeepSessionActive: &keepalive,
		UpdatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		if r.handleInactiveSessionKeepalive(ctx, session, err) {
			return fmt.Errorf("active task-loop session %s is no longer active: %w", firstNonEmpty(strings.TrimSpace(session.SessionID), "unknown"), err)
		}
		return fmt.Errorf("active task-loop keepalive failed for %s: %w", firstNonEmpty(detail, phase), err)
	}
	if strings.TrimSpace(state.SessionID) != "" {
		r.mu.Lock()
		if r.activeSession != nil && strings.TrimSpace(r.activeSession.SessionID) == strings.TrimSpace(state.SessionID) {
			stateCopy := state
			r.activeSession = &stateCopy
		}
		r.mu.Unlock()
	}
	return nil
}

func taskLoopSessionFenceToolResult(toolName string, err error) ToolResult {
	return ToolResult{
		Output:  taskLoopSessionFenceMessage(toolName, err),
		IsError: true,
	}
}

func taskLoopSessionFenceMessage(toolName string, err error) string {
	return fmt.Sprintf("active task-loop session fence blocked tool %s: %v. Stop local work immediately and return a blocked result instead of retrying tool calls.", firstNonEmpty(strings.TrimSpace(toolName), "unknown"), err)
}

func (r *Runtime) peerRequestToolExecutor(ctx context.Context, registry *ToolRegistry, call ToolCall) ToolResult {
	toolName := strings.TrimSpace(call.Function.Name)
	if !peerRequestReadOnlyToolAllowed(toolName) {
		if r != nil {
			r.recordPolicySignal(ToolPolicySignal{
				ToolName:    toolName,
				Verdict:     "DENY",
				Capability:  toolCapabilityForName(toolName),
				Description: fmt.Sprintf("model.ask read-only context blocked tool %s", toolName),
			})
		}
		return ToolResult{
			Output:  fmt.Sprintf("model.ask peer request context is read-only; blocked tool %s. Do not implement, write files, run shell, submit tasks, mutate project state, materialize checkouts, or commit branches while answering. If work is needed, reply that you must claim the relevant task through the normal planner/work loop first.", toolName),
			IsError: true,
		}
	}
	return r.policyAwareToolExecutor(ctx, registry, call)
}

func peerRequestReadOnlyToolAllowed(toolName string) bool {
	switch strings.TrimSpace(toolName) {
	case "workspace_doc_get",
		"read_file",
		"list_directory",
		"memory_read",
		"memory_search",
		"memory_coherence_read",
		"memory_promotion_read",
		"coalition_status":
		return true
	default:
		return false
	}
}

func (r *Runtime) postProcessToolResult(call ToolCall, result ToolResult) ToolResult {
	if out, ok := r.telicSensor(call, result); ok {
		return out
	}
	if !result.IsError || strings.TrimSpace(call.Function.Name) != "shell" {
		return result
	}
	guidance := runtimeShellFailureAdvisory()
	if !strings.Contains(result.Output, "Shell is trusted local execution") {
		result.Output = strings.TrimRight(result.Output, "\r\n") + "\n" + guidance
	}
	if r != nil {
		r.mu.Lock()
		if len(r.scratch.AdvisorySignals) == 0 || r.scratch.AdvisorySignals[len(r.scratch.AdvisorySignals)-1] != guidance {
			r.scratch.AdvisorySignals = append(r.scratch.AdvisorySignals, guidance)
			if len(r.scratch.AdvisorySignals) > 5 {
				r.scratch.AdvisorySignals = r.scratch.AdvisorySignals[len(r.scratch.AdvisorySignals)-5:]
			}
		}
		r.mu.Unlock()
	}
	return result
}

func (r *Runtime) materializeCycle(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult) error {
	artifactRefs := resultArtifactRefs(result)
	payload := map[string]any{
		"status":        coordinationStatusForResult(result),
		"next_action":   result.NextAction,
		"task_ids":      []string{task.TaskID},
		"doc_keys":      relatedDocKeys(r.cfg.AgentID, result),
		"artifact_refs": artifactRefs,
		"blocked_on":    result.BlockedOn,
		"owner_action":  result.OwnerAction,
		"human_reason":  result.HumanReason,
		"notes":         firstNonEmpty(result.Details, result.Summary),
	}

	if result.Materialize.DocKey != "" {
		docTitle, docContent, expectedSHA := r.prepareMaterializedDocWrite(ctx, task, session, runID, result)
		if err := r.putDocWithRetryExpected(ctx, result.Materialize.DocKey, docTitle, docContent, expectedSHA); err != nil {
			return err
		}
		if err := r.writePrimaryArtifact(ctx, task, runID, result); err != nil {
			return err
		}
	}

	if err := r.syncCurrentContextDoc(ctx, &task, &session, &result); err != nil {
		return err
	}
	if err := r.syncClaimedWorkDoc(ctx, &task, &session, runID, &result); err != nil {
		return err
	}
	r.logTaskResultMemory(task, session, runID, result)

	rawPayload, _ := json.Marshal(payload)
	if err := r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID:   r.cfg.WorkspaceID,
		AgentID:       r.cfg.AgentID,
		UpdateType:    updateTypeForResult(result),
		Summary:       result.Summary,
		PayloadJSON:   string(rawPayload),
		RequiresHuman: result.RequiresHuman,
	}); err != nil {
		return err
	}

	var memoryRecord WorkspaceMemoryRecord
	if strings.TrimSpace(result.MemoryBody) != "" {
		memoryBody := strings.TrimSpace(result.MemoryBody)
		if len(memoryBody) > r.cfg.MaxResultMemoryBody {
			memoryBody = strings.TrimSpace(memoryBody[:r.cfg.MaxResultMemoryBody]) + "\n...[truncated]"
		}
		if r.cfg.Mode != RuntimeModeDaemon {
			record, err := r.client.WriteMemory(ctx, WorkspaceMemoryWriteInput{
				WorkspaceID: r.cfg.WorkspaceID,
				MemoryType:  firstNonEmpty(result.MemoryType, "NOTE"),
				Title:       firstNonEmpty(result.MemoryTitle, result.Summary),
				Body:        memoryBody,
				Summary:     result.Summary,
				AgentID:     r.cfg.AgentID,
				SessionID:   session.SessionID,
				TaskID:      task.TaskID,
				SourceKind:  "reflection",
				SourceID:    runID,
				Importance:  0.65,
				Confidence:  0.55,
			})
			if err != nil {
				return err
			}
			memoryRecord = record
			if claimType := claimTypeForMemoryType(result.MemoryType); claimType != "" {
				evidence := []string{"run:" + runID}
				if result.Materialize.DocKey != "" {
					evidence = append(evidence, "doc:"+result.Materialize.DocKey)
				}
				if err := r.client.WriteClaim(ctx, KnowledgeClaimWriteInput{
					WorkspaceID: r.cfg.WorkspaceID,
					ClaimType:   claimType,
					Status:      "ACTIVE",
					Subject:     firstNonEmpty(result.MemoryTitle, result.Summary),
					Body:        memoryBody,
					Summary:     result.Summary,
					Confidence:  0.55,
					SourceKind:  "reflection",
					SourceID:    runID,
					MemoryID:    memoryRecord.MemoryID,
					TaskID:      task.TaskID,
					SessionID:   session.SessionID,
					AgentID:     r.cfg.AgentID,
					Evidence:    evidence,
					Tags:        []string{"rnar", strings.ToLower(claimType)},
				}); err != nil {
					return err
				}
			}
			r.markMemoryPromotionsPromoted(runID)
		} else {
			log.Printf("[memory] daemon mode skipped canonical memory materialization for run=%s task=%s", runID, task.TaskID)
		}
	}
	if err := r.syncOperatorQueue(ctx, task, session, result); err != nil {
		return err
	}
	if r.cfg.Mode != RuntimeModeDaemon {
		if err := r.flushPendingMemoryPromotions(ctx, r.cfg.MaxPromotionSyncBatch); err != nil {
			log.Printf("[memory] promotion flush degraded: %v", err)
		}
	}

	r.markHydrationStale()
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.LastSummary = result.Summary
	})
}

func (r *Runtime) continueTaskCycle(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, trace *TaskRunTrace) error {
	updatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	// G-SUB-1: if this cycle touched project files and left the owned checkout
	// dirty, surface a deterministic "commit your uncommitted work" directive
	// into the durable continuation state. Computed before status/run writes so
	// the session summary and immediate-resume signature reflect the required
	// next transition, not only the model's softer continuation text.
	commitDirective := ""
	if traceTouchedProjectFiles(trace) {
		commitDirective = r.projectImplementationUncommittedCommitDirective(ctx, task)
		if commitDirective != "" {
			result.NextAction = commitDirective
		}
	}
	fanoutDirective := ""
	if traceNeedsProjectImplementationFanoutDirective(trace) {
		fanoutDirective = r.projectImplementationFanoutDirective(ctx, task)
	}
	// AG-01/AG-02: deterministic publication nudge on the continue path. When the checkout
	// is not dirty (commit directive did not fire), surface the next owned publication step
	// (committed->review_ready, or review_ready->patch_queue_submit) so an agent that committed
	// then keeps yielding continue is not left to stall. Advisory only; never blocks.
	publicationDirective := ""
	if commitDirective == "" && runtimeProjectCompletionEvidenceGateRequired(task) {
		publicationDirective = r.projectImplementationPublicationDirective(ctx, task)
		if publicationDirective != "" {
			result.NextAction = publicationDirective
		}
	}
	state, err := r.client.SessionEvent(ctx, "agent.session.status", sessionEventWithTaskRunTrace(SessionEventInput{
		WorkspaceID:         r.cfg.WorkspaceID,
		SessionID:           session.SessionID,
		AgentID:             r.cfg.AgentID,
		TaskID:              task.TaskID,
		Summary:             firstNonEmpty(result.NextAction, result.Summary),
		Status:              "ACTIVE",
		OwnerScope:          "task/session",
		KeepSessionActive:   boolPtr(true),
		RelatedArtifactRefs: resultArtifactRefs(result),
		UpdatedAt:           updatedAt,
	}, trace))
	if err != nil {
		if isInactiveSessionKeepaliveError(err) {
			summary := "Dropped blocked transition for inactive session " + firstNonEmpty(strings.TrimSpace(session.SessionID), "unknown") + ": " + strings.TrimSpace(err.Error())
			log.Printf("[runtime] %s", summary)
			r.logSessionTransitionMemory("session_block_suppressed", task.TaskID, session.SessionID, "ENDED", summary, "", "", result.BlockedOn)
			if clearErr := r.clearActiveState(ctx, summary); clearErr != nil {
				return clearErr
			}
			return r.refreshBootstrap(ctx)
		}
		return err
	}
	if _, err := r.client.WriteExecutionRun(ctx, ExecutionRunWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		TaskID:      task.TaskID,
		SessionID:   session.SessionID,
		AgentID:     r.cfg.AgentID,
		Title:       firstNonEmpty(task.Title, "Task "+task.TaskID),
		Summary:     firstNonEmpty(result.NextAction, result.Summary),
		Status:      "ACTIVE",
	}); err != nil {
		return err
	}

	r.mu.Lock()
	r.activeSession = &state
	r.activeTask = &task
	r.activeRunID = runID
	r.scratch.ActiveTaskID = task.TaskID
	r.scratch.ActiveSessionID = state.SessionID
	r.scratch.ActiveRunID = runID
	if commitDirective != "" {
		r.scratch.AdvisorySignals = appendCappedAdvisorySignal(r.scratch.AdvisorySignals, commitDirective)
	}
	if fanoutDirective != "" {
		r.scratch.AdvisorySignals = appendCappedAdvisorySignal(r.scratch.AdvisorySignals, fanoutDirective)
	}
	if publicationDirective != "" {
		r.scratch.AdvisorySignals = appendCappedAdvisorySignal(r.scratch.AdvisorySignals, publicationDirective)
	}
	r.setContinuationHoldLocked(task, state, runID, result, time.Now().UTC())
	resumeQueued := r.maybeQueueImmediateProjectContinuationResumeLocked(task, state, runID, result, trace, time.Now().UTC())
	r.invalidateFocusLocked()
	stateCopy := r.scratch
	r.mu.Unlock()
	r.logSessionTransitionMemory("session_continue", task.TaskID, state.SessionID, state.Status, firstNonEmpty(result.NextAction, result.Summary), "", "", nil)
	if err := r.saveScratchState(ctx, stateCopy); err != nil {
		return err
	}
	if resumeQueued {
		r.markHydrationStale()
		r.wakePlanner()
	}
	return nil
}

func (r *Runtime) decisionTaskCycle(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult) error {
	updatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	state, err := r.client.SessionEvent(ctx, "agent.session.decision_needed", SessionEventInput{
		WorkspaceID:         r.cfg.WorkspaceID,
		SessionID:           session.SessionID,
		AgentID:             r.cfg.AgentID,
		TaskID:              task.TaskID,
		Summary:             firstNonEmpty(result.OwnerAction, result.Summary),
		Status:              "WAITING_DECISION",
		OwnerScope:          "task/session",
		DecisionNeededFrom:  firstNonEmpty(r.cfg.OwnerUserID, "human"),
		DecisionType:        firstNonEmpty(result.DecisionType, "unknown"),
		KeepSessionActive:   boolPtr(true),
		RelatedDocKeys:      relatedDocKeys(r.cfg.AgentID, result),
		RelatedArtifactRefs: resultArtifactRefs(result),
		UpdatedAt:           updatedAt,
	})
	if err != nil {
		return err
	}
	if _, err := r.client.WriteExecutionRun(ctx, ExecutionRunWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		TaskID:      task.TaskID,
		SessionID:   session.SessionID,
		AgentID:     r.cfg.AgentID,
		Title:       firstNonEmpty(task.Title, "Task "+task.TaskID),
		Summary:     firstNonEmpty(result.OwnerAction, result.Summary),
		Status:      "BLOCKED",
		Outcome:     "WAITING_DECISION",
	}); err != nil {
		return err
	}

	r.mu.Lock()
	r.activeSession = &state
	r.activeTask = &task
	r.activeRunID = runID
	r.scratch.ActiveTaskID = task.TaskID
	r.scratch.ActiveSessionID = state.SessionID
	r.scratch.ActiveRunID = runID
	r.clearContinuationHoldLocked()
	r.invalidateFocusLocked()
	stateCopy := r.scratch
	r.mu.Unlock()
	r.logSessionTransitionMemory("session_decision_needed", task.TaskID, state.SessionID, state.Status, firstNonEmpty(result.OwnerAction, result.Summary), result.DecisionType, "", result.BlockedOn)
	if err := r.saveScratchState(ctx, stateCopy); err != nil {
		return err
	}
	return r.refreshBootstrap(ctx)
}

func (r *Runtime) completeTaskCycle(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, workPacket *AgentWorkPacket, trace *TaskRunTrace) error {
	drifted, err := r.shouldSuppressStaleCompletionTransition(ctx, task)
	if err != nil {
		return fmt.Errorf("verify completion authority before terminal transition: %w", err)
	}
	if drifted {
		return r.fencedLateCompletionCycle(ctx, task, session, runID, result, "before terminal transition", workPacket)
	}

	if err := r.client.CompleteTask(ctx, TaskCompleteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		TaskID:      task.TaskID,
		Summary:     result.Summary,
	}); err != nil {
		return err
	}
	materializationOutcome := "COMPLETED"
	materializationSummary := result.Summary
	if err := r.materializeCycle(ctx, task, session, runID, result); err != nil {
		materializationOutcome = "COMPLETION_MATERIALIZATION_DEGRADED"
		materializationSummary = fmt.Sprintf("%s (completion materialization degraded: %v)", firstNonEmpty(result.Summary, "task completed"), err)
		log.Printf("[runtime] completion materialization degraded after terminal transition for task %s: %v", task.TaskID, err)
		if _, stepErr := r.client.WriteExecutionStep(ctx, ExecutionStepWriteInput{
			WorkspaceID: r.cfg.WorkspaceID,
			RunID:       runID,
			Phase:       "MATERIALIZE",
			Title:       "Completion materialization degraded",
			Summary:     materializationSummary,
			Status:      "DEGRADED",
			SortOrder:   35,
			Evidence:    prependCapabilitySnapshotEvidence(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID), phaseEvidenceRefs(&result)),
			Verification: map[string]any{
				"capability_snapshot_id":      daemonRunCapabilitySnapshotID(r.cfg, task, session, runID),
				"capability_snapshot_ref":     capabilitySnapshotEvidenceRef(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID)),
				"terminal_transition_applied": true,
				"materialization_applied":     false,
				"error":                       err.Error(),
				"task_cycle_phase_records": taskCycleTerminalPhaseRecords(
					r.cfg,
					task,
					session,
					runID,
					workPacket,
					&result,
					nil,
				),
				"task_cycle_phase_record": taskCyclePhaseRecordEnvelope(
					r.cfg,
					task,
					session,
					runID,
					"RESULT_RECORDED",
					"VERIFY",
					workPacket,
					&result,
					nil,
					7,
				),
			},
		}); stepErr != nil {
			log.Printf("[runtime] failed to record completion materialization degradation for task %s: %v", task.TaskID, stepErr)
		}
	}
	r.maybeRequestVisualFailureRevisionFollowup(ctx, task, result)
	if materializationOutcome == "COMPLETED" {
		if _, err := r.client.WriteExecutionStep(ctx, ExecutionStepWriteInput{
			WorkspaceID: r.cfg.WorkspaceID,
			RunID:       runID,
			Phase:       "VERIFY",
			Title:       "Verify completion",
			Summary:     result.Summary,
			Status:      "COMPLETED",
			SortOrder:   30,
			Evidence:    prependCapabilitySnapshotEvidence(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID), phaseEvidenceRefs(&result)),
			Verification: map[string]any{
				"capability_snapshot_id":  daemonRunCapabilitySnapshotID(r.cfg, task, session, runID),
				"capability_snapshot_ref": capabilitySnapshotEvidenceRef(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID)),
				"task_cycle_phase_records": taskCycleTerminalPhaseRecords(
					r.cfg,
					task,
					session,
					runID,
					workPacket,
					&result,
					nil,
				),
				"task_cycle_phase_record": taskCyclePhaseRecordEnvelope(
					r.cfg,
					task,
					session,
					runID,
					"TERMINAL_TRANSITIONED",
					"VERIFY",
					workPacket,
					&result,
					nil,
					8,
				),
			},
		}); err != nil {
			return err
		}
	}
	if _, err := r.client.SessionEvent(ctx, "agent.session.end", sessionEventWithTaskRunTrace(SessionEventInput{
		WorkspaceID:         r.cfg.WorkspaceID,
		SessionID:           session.SessionID,
		AgentID:             r.cfg.AgentID,
		TaskID:              task.TaskID,
		Summary:             materializationSummary,
		Status:              "ENDED",
		OwnerScope:          "task/session",
		KeepSessionActive:   boolPtr(false),
		RelatedDocKeys:      relatedDocKeys(r.cfg.AgentID, result),
		RelatedArtifactRefs: resultArtifactRefs(result),
		UpdatedAt:           time.Now().UTC().Format(time.RFC3339Nano),
	}, trace)); err != nil {
		return err
	}
	r.logSessionTransitionMemory("session_end_completed", task.TaskID, session.SessionID, "ENDED", materializationSummary, "", "", nil)
	if _, err := r.client.WriteExecutionRun(ctx, ExecutionRunWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		TaskID:      task.TaskID,
		SessionID:   session.SessionID,
		AgentID:     r.cfg.AgentID,
		Title:       firstNonEmpty(task.Title, "Task "+task.TaskID),
		Summary:     materializationSummary,
		Status:      "COMPLETED",
		Outcome:     materializationOutcome,
	}); err != nil {
		return err
	}
	if err := r.clearActiveState(ctx, materializationSummary); err != nil {
		return err
	}
	return r.refreshBootstrap(ctx)
}

func (r *Runtime) fencedLateCompletionCycle(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, phase string, workPacket *AgentWorkPacket) error {
	summary := firstNonEmpty(result.Summary, "task cycle completed")
	driftSummary := "Dropped late completion after ownership drift: " + summary
	log.Printf("[runtime] suppressing late completion for task %s after ownership drift (%s)", task.TaskID, strings.TrimSpace(phase))
	terminalRecords := taskCycleTerminalPhaseRecords(
		r.cfg,
		task,
		session,
		runID,
		workPacket,
		&result,
		nil,
	)
	markTerminalCanonicalMutationApplied(terminalRecords, false)
	if _, err := r.client.WriteExecutionStep(ctx, ExecutionStepWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		Phase:       "VERIFY",
		Title:       "Fence late completion",
		Summary:     driftSummary,
		Status:      "BLOCKED",
		SortOrder:   30,
		Evidence:    prependCapabilitySnapshotEvidence(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID), phaseEvidenceRefs(&result)),
		Verification: map[string]any{
			"capability_snapshot_id":     daemonRunCapabilitySnapshotID(r.cfg, task, session, runID),
			"capability_snapshot_ref":    capabilitySnapshotEvidenceRef(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID)),
			"late_completion":            true,
			"canonical_mutation_applied": false,
			"fence":                      "ownership_drift",
			"phase":                      strings.TrimSpace(phase),
			"task_cycle_phase_records":   terminalRecords,
			"task_cycle_phase_record":    lastTaskCyclePhaseRecord(terminalRecords),
		},
	}); err != nil {
		return err
	}
	r.logSessionTransitionMemory("session_completion_suppressed", task.TaskID, session.SessionID, "ENDED", driftSummary, "", "", nil)
	if _, err := r.client.WriteExecutionRun(ctx, ExecutionRunWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		TaskID:      task.TaskID,
		SessionID:   session.SessionID,
		AgentID:     r.cfg.AgentID,
		Title:       firstNonEmpty(task.Title, "Task "+task.TaskID),
		Summary:     driftSummary,
		Status:      "BLOCKED",
		Outcome:     "FENCED_LATE",
	}); err != nil {
		return err
	}
	if err := r.clearActiveState(ctx, driftSummary); err != nil {
		return err
	}
	return r.refreshBootstrap(ctx)
}

func (r *Runtime) blockTaskCycle(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, workPacket *AgentWorkPacket, trace *TaskRunTrace) error {
	drifted, err := r.shouldSuppressStaleBlockedTransition(ctx, task)
	if err != nil {
		log.Printf("[runtime] unable to verify task ownership before block transition for %s: %v", task.TaskID, err)
	} else if drifted {
		summary := firstNonEmpty(result.Summary, "task cycle blocked")
		driftSummary := "Dropped stale blocked transition after ownership drift: " + summary
		log.Printf("[runtime] suppressing stale block transition for task %s after ownership drift", task.TaskID)
		r.logSessionTransitionMemory("session_block_suppressed", task.TaskID, session.SessionID, "ENDED", driftSummary, "", "", result.BlockedOn)
		if err := r.clearActiveState(ctx, driftSummary); err != nil {
			return err
		}
		return r.refreshBootstrap(ctx)
	}
	if completedResult, ok := routineBlockedResultAsCompletedTask(task, result); ok {
		return r.completeTaskCycle(ctx, task, session, runID, completedResult, workPacket, trace)
	}
	if shouldEndRoutineDependencyBlockedSession(result) {
		return r.endRoutineDependencyBlockedTaskCycle(ctx, task, session, runID, result, workPacket, trace)
	}
	r.maybeRequestVisualFailureRevisionFollowup(ctx, task, result)

	updatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	state, err := r.client.SessionEvent(ctx, "agent.session.blocked", sessionEventWithTaskRunTrace(SessionEventInput{
		WorkspaceID:         r.cfg.WorkspaceID,
		SessionID:           session.SessionID,
		AgentID:             r.cfg.AgentID,
		TaskID:              task.TaskID,
		Summary:             result.Summary,
		Status:              "BLOCKED",
		OwnerScope:          "task/session",
		BlockedOn:           normalizedBlockedRefs(result.BlockedOn, result.Summary),
		KeepSessionActive:   boolPtr(false),
		RelatedDocKeys:      relatedDocKeys(r.cfg.AgentID, result),
		RelatedArtifactRefs: resultArtifactRefs(result),
		UpdatedAt:           updatedAt,
	}, trace))
	if err != nil {
		if isInactiveSessionKeepaliveError(err) {
			summary := "Parked blocked task after inactive session " + firstNonEmpty(strings.TrimSpace(session.SessionID), "unknown") + ": " + strings.TrimSpace(err.Error())
			log.Printf("[runtime] %s", summary)
			r.logSessionTransitionMemory("session_block_suppressed", task.TaskID, session.SessionID, "ENDED", summary, "", "", result.BlockedOn)
			if blockErr := r.client.BlockTask(ctx, TaskBlockInput{
				WorkspaceID: r.cfg.WorkspaceID,
				AgentID:     r.cfg.AgentID,
				TaskID:      task.TaskID,
				Reason:      firstNonEmpty(result.Summary, summary),
			}); blockErr != nil {
				return fmt.Errorf("park blocked task after inactive session: %w", blockErr)
			}
			if refreshErr := r.refreshTensionProjectionAfterBlock(ctx); refreshErr != nil {
				log.Printf("[runtime] tension refresh after inactive-session block degraded for task %s: %v", task.TaskID, refreshErr)
			}
			if clearErr := r.clearActiveState(ctx, summary); clearErr != nil {
				return clearErr
			}
			return r.refreshBootstrap(ctx)
		}
		return err
	}
	if err := r.client.BlockTask(ctx, TaskBlockInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		TaskID:      task.TaskID,
		Reason:      result.Summary,
	}); err != nil {
		return err
	}
	materializationSummary := r.materializeAfterTerminalTransition(ctx, task, session, runID, result, workPacket, "blocked")
	if _, err := r.client.WriteExecutionRun(ctx, ExecutionRunWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		TaskID:      task.TaskID,
		SessionID:   session.SessionID,
		AgentID:     r.cfg.AgentID,
		Title:       firstNonEmpty(task.Title, "Task "+task.TaskID),
		Summary:     materializationSummary,
		Status:      "BLOCKED",
		Outcome:     "BLOCKED",
	}); err != nil {
		return err
	}
	if _, err := r.client.WriteExecutionStep(ctx, ExecutionStepWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		Phase:       "VERIFY",
		Title:       "Record blocked result",
		Summary:     materializationSummary,
		Status:      "BLOCKED",
		SortOrder:   30,
		Evidence:    prependCapabilitySnapshotEvidence(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID), phaseEvidenceRefs(&result)),
		Verification: map[string]any{
			"capability_snapshot_id":  daemonRunCapabilitySnapshotID(r.cfg, task, session, runID),
			"capability_snapshot_ref": capabilitySnapshotEvidenceRef(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID)),
			"task_cycle_phase_records": taskCycleTerminalPhaseRecords(
				r.cfg,
				task,
				session,
				runID,
				workPacket,
				&result,
				nil,
			),
			"task_cycle_phase_record": taskCyclePhaseRecordEnvelope(
				r.cfg,
				task,
				session,
				runID,
				"TERMINAL_TRANSITIONED",
				"VERIFY",
				workPacket,
				&result,
				nil,
				8,
			),
		},
	}); err != nil {
		return err
	}
	if err := r.refreshTensionProjectionAfterBlock(ctx); err != nil {
		log.Printf("[runtime] tension refresh after blocked transition degraded for task %s: %v", task.TaskID, err)
	}

	r.logSessionTransitionMemory("session_blocked", task.TaskID, state.SessionID, state.Status, result.Summary, "", "", result.BlockedOn)
	if err := r.clearActiveState(ctx, materializationSummary); err != nil {
		return err
	}
	return r.refreshBootstrap(ctx)
}

func routineBlockedResultAsCompletedTask(task WorkspaceTaskRecord, result StructuredTaskResult) (StructuredTaskResult, bool) {
	if normalizeOutcome(result.Outcome) != "blocked" || result.RequiresHuman || shouldSurfaceOperatorQueue(result) {
		return StructuredTaskResult{}, false
	}
	reason := ""
	if routinePatchQueueSupersedeParkedBlock(result) {
		reason = "patch queue supersede stewardship parked without retrying the stale task"
	} else if _, ok := visualFailureRevisionFollowupArgs(task, result); ok {
		reason = "visual validation produced actionable negative evidence for a repair lane"
	}
	if reason == "" {
		return StructuredTaskResult{}, false
	}
	completed := result
	completed.Outcome = "completed"
	completed.RequiresHuman = false
	completed.OwnerAction = ""
	completed.HumanReason = ""
	completed.Details = appendAutonomyNote(completed.Details, "blocked result closed current task: "+reason)
	completed.NextAction = firstNonEmpty(strings.TrimSpace(completed.NextAction), "Continue from the newly surfaced frontier task or fresh same-head validation evidence.")
	return completed, true
}

func routinePatchQueueSupersedeParkedBlock(result StructuredTaskResult) bool {
	for _, item := range result.BlockedOn {
		if isRoutineSessionEndingBlocker(item) && normalizeBlockedKind(item.Kind, item.Detail) == "patch_queue_supersede" {
			return true
		}
	}
	text := strings.ToLower(strings.Join([]string{result.Summary, result.Details, result.NextAction}, "\n"))
	return (strings.Contains(text, "patch queue supersede") || strings.Contains(text, "patch_queue_supersede")) &&
		(strings.Contains(text, "parked") || strings.Contains(text, "do not retry"))
}

func (r *Runtime) maybeRequestVisualFailureRevisionFollowup(ctx context.Context, task WorkspaceTaskRecord, result StructuredTaskResult) {
	if r == nil || r.client == nil {
		return
	}
	followupArgs, ok := visualFailureRevisionFollowupArgs(task, result)
	if !ok {
		return
	}
	followup := NewProjectPatchQueueFollowupTool(r.client, r.cfg.WorkspaceID, r.cfg.AgentID, r.cfg.OwnerUserID).Execute(ctx, followupArgs)
	if followup == nil {
		log.Printf("[runtime] visual failure revision follow-up returned no result for task %s", strings.TrimSpace(task.TaskID))
	} else if followup.IsError {
		log.Printf("[runtime] visual failure revision follow-up degraded for task %s: %s", strings.TrimSpace(task.TaskID), strings.TrimSpace(followup.Output))
	} else {
		log.Printf("[runtime] visual failure revision follow-up requested for task %s: %s", strings.TrimSpace(task.TaskID), strings.TrimSpace(followup.Output))
	}
}

func visualFailureRevisionFollowupArgs(task WorkspaceTaskRecord, result StructuredTaskResult) (map[string]any, bool) {
	text := strings.TrimSpace(strings.Join([]string{
		result.Summary,
		result.Details,
		result.NextAction,
		result.Materialize.DocTitle,
		result.Materialize.DocContent,
	}, "\n"))
	lower := strings.ToLower(text)
	if lower == "" || !strings.Contains(lower, "rhizome_visual_acceptance_v1") || !visualFailureTextHasFailVerdict(lower) {
		return nil, false
	}
	if !visualFailureTextNamesProductRepair(lower) {
		return nil, false
	}
	projectID := firstNonEmpty(strings.TrimSpace(task.ProjectID), visualFailureDocField(text, "project_id"))
	queueID := visualFailureDocField(text, "queue_id")
	itemID := visualFailureDocField(text, "item_id")
	if itemID == "" {
		itemID = visualFailureDocField(text, "patch_item_id")
	}
	branchID := visualFailureDocField(text, "branch_id")
	if projectID == "" || (queueID == "" && itemID == "" && branchID == "") {
		return nil, false
	}
	docKey := strings.TrimSpace(result.Materialize.DocKey)
	headSHA := visualFailureDocField(text, "head_sha")
	if headSHA == "" {
		headSHA = visualFailureDocField(text, "candidate_head_sha")
	}
	if headSHA == "" {
		headSHA = visualFailureDocField(text, "observed_head_sha")
	}
	extra := strings.TrimSpace(strings.Join([]string{
		"visual_failure_doc_key: " + docKey,
		"visual_failure_summary: " + firstNonEmpty(strings.TrimSpace(result.Summary), "visual acceptance failed"),
		"visual_failure_next_action: " + strings.TrimSpace(result.NextAction),
		"visual_failure_branch_id: " + branchID,
		"visual_failure_head_sha: " + headSHA,
	}, "\n"))
	args := map[string]any{
		"project_id":    projectID,
		"followup_kind": "revision",
		"extra_context": extra,
	}
	if queueID != "" {
		args["queue_id"] = queueID
	}
	if itemID != "" {
		args["item_id"] = itemID
	}
	if queueID == "" && itemID == "" && branchID != "" {
		args["branch_id"] = branchID
	}
	return args, true
}

func visualFailureTextHasFailVerdict(lower string) bool {
	compact := strings.NewReplacer(" ", "", "\t", "", "\r", "", "\n", "", `"`, "", "'", "", "`", "").Replace(strings.ToLower(lower))
	for _, marker := range []string{
		"visual_verdict:fail",
		"visual_verdict=fail",
		"visualverdict:fail",
		"visualverdict=fail",
		"verdict:fail",
		"verdict=fail",
	} {
		if strings.Contains(compact, marker) {
			return true
		}
	}
	return false
}

func visualFailureTextNamesProductRepair(lower string) bool {
	signals := []string{
		"board-first",
		"board first",
		"board is pushed",
		"below the first viewport",
		"below first viewport",
		"below the fold",
		"blocking findings",
		"clipping",
		"control/header density",
		"excessive chrome",
		"excessively sparse",
		"grid appears",
		"giant sparse",
		"implementation/revision lane",
		"implementation repair",
		"narrow initial state does not meet",
		"narrow layout",
		"narrow viewport",
		"narrow viewport board",
		"not sufficiently board-first",
		"primary board is",
		"primary flow is not sufficiently board-first",
		"promote the board",
		"pushed too far down",
		"reduce top-of-screen chrome",
		"repair the layout",
		"responsive layout breaks",
		"responsive repair",
		"silly",
		"tiny grid",
		"top-heavy layout",
		"unusable",
		"visual dominance",
		"wide past the viewport",
		"horizontal overflow",
		"overflows horizontally",
		"document scroll",
		"viewport width",
		"do not accept",
	}
	for _, signal := range signals {
		if strings.Contains(lower, signal) {
			return true
		}
	}
	return false
}

func visualFailureDocField(content, field string) string {
	field = strings.ToLower(strings.TrimSpace(field))
	if field == "" {
		return ""
	}
	for _, line := range strings.Split(content, "\n") {
		clean := strings.TrimSpace(line)
		clean = strings.TrimSpace(strings.TrimPrefix(clean, "-"))
		lower := strings.ToLower(clean)
		prefix := field + ":"
		if !strings.HasPrefix(lower, prefix) {
			continue
		}
		value := strings.TrimSpace(clean[len(prefix):])
		value = strings.Trim(value, "`'\" ")
		fields := strings.Fields(value)
		if len(fields) == 0 {
			return value
		}
		return strings.Trim(fields[0], "`'\",.;")
	}
	return ""
}

func (r *Runtime) endRoutineDependencyBlockedTaskCycle(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, workPacket *AgentWorkPacket, trace *TaskRunTrace) error {
	if err := r.client.BlockTask(ctx, TaskBlockInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		TaskID:      task.TaskID,
		Reason:      result.Summary,
	}); err != nil {
		return err
	}
	materializationSummary := r.materializeAfterTerminalTransition(ctx, task, session, runID, result, workPacket, "dependency block")
	if _, err := r.client.WriteExecutionRun(ctx, ExecutionRunWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		TaskID:      task.TaskID,
		SessionID:   session.SessionID,
		AgentID:     r.cfg.AgentID,
		Title:       firstNonEmpty(task.Title, "Task "+task.TaskID),
		Summary:     materializationSummary,
		Status:      "BLOCKED",
		Outcome:     "BLOCKED",
	}); err != nil {
		return err
	}
	if _, err := r.client.WriteExecutionStep(ctx, ExecutionStepWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		Phase:       "VERIFY",
		Title:       "Record dependency block",
		Summary:     materializationSummary,
		Status:      "BLOCKED",
		SortOrder:   30,
		Evidence:    prependCapabilitySnapshotEvidence(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID), phaseEvidenceRefs(&result)),
		Verification: map[string]any{
			"capability_snapshot_id":  daemonRunCapabilitySnapshotID(r.cfg, task, session, runID),
			"capability_snapshot_ref": capabilitySnapshotEvidenceRef(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID)),
			"operator_queue_required": false,
			"session_terminal_state":  "ENDED",
			"task_cycle_phase_records": taskCycleTerminalPhaseRecords(
				r.cfg,
				task,
				session,
				runID,
				workPacket,
				&result,
				nil,
			),
			"task_cycle_phase_record": taskCyclePhaseRecordEnvelope(
				r.cfg,
				task,
				session,
				runID,
				"TERMINAL_TRANSITIONED",
				"VERIFY",
				workPacket,
				&result,
				nil,
				8,
			),
		},
	}); err != nil {
		return err
	}
	if err := r.refreshTensionProjectionAfterBlock(ctx); err != nil {
		log.Printf("[runtime] tension refresh after dependency block degraded for task %s: %v", task.TaskID, err)
	}
	if _, err := r.client.SessionEvent(ctx, "agent.session.end", sessionEventWithTaskRunTrace(SessionEventInput{
		WorkspaceID:         r.cfg.WorkspaceID,
		SessionID:           session.SessionID,
		AgentID:             r.cfg.AgentID,
		TaskID:              task.TaskID,
		Summary:             materializationSummary,
		Status:              "ENDED",
		OwnerScope:          "task/session",
		BlockedOn:           normalizedBlockedRefs(result.BlockedOn, result.Summary),
		KeepSessionActive:   boolPtr(false),
		RelatedDocKeys:      relatedDocKeys(r.cfg.AgentID, result),
		RelatedArtifactRefs: resultArtifactRefs(result),
		UpdatedAt:           time.Now().UTC().Format(time.RFC3339Nano),
	}, trace)); err != nil {
		if isInactiveSessionKeepaliveError(err) {
			summary := "Ended dependency-blocked task after inactive session " + firstNonEmpty(strings.TrimSpace(session.SessionID), "unknown") + ": " + strings.TrimSpace(err.Error())
			log.Printf("[runtime] %s", summary)
			r.logSessionTransitionMemory("session_dependency_block_end_suppressed", task.TaskID, session.SessionID, "ENDED", summary, "", "", result.BlockedOn)
			if clearErr := r.clearActiveState(ctx, summary); clearErr != nil {
				return clearErr
			}
			return r.refreshBootstrap(ctx)
		}
		return err
	}
	r.recordStrategyBlockerFingerprint(ctx, task, result)
	r.logSessionTransitionMemory("session_end_dependency_blocked", task.TaskID, session.SessionID, "ENDED", materializationSummary, "", "", result.BlockedOn)
	if err := r.clearActiveState(ctx, materializationSummary); err != nil {
		return err
	}
	return r.refreshBootstrap(ctx)
}

// timeoutResumeSummaryMaxBytes bounds the persisted timeout summary so a
// timeout never bloats scratch state. 2KB is the plan ceiling.
const timeoutResumeSummaryMaxBytes = 2048

// buildTimeoutResumeSummary builds the compact, bounded resume summary
// persisted on a transient provider timeout so the NEXT cycle for the same task
// carries it (in the prompt) instead of rediscovering the same context blind.
// It records: task id, iteration count, last tools called + outcome digests,
// the last assistant intent line, and the timeout cause. The whole string is
// capped at timeoutResumeSummaryMaxBytes.
func buildTimeoutResumeSummary(task WorkspaceTaskRecord, trace *TaskRunTrace, execErr error) string {
	var b strings.Builder
	b.WriteString("SYSTEM TIMEOUT RESUME SUMMARY: the previous cycle for this task hit a transient provider timeout and was released for retry. ")
	b.WriteString("Resume from this state instead of rebuilding context from scratch.\n")
	b.WriteString("- task_id: " + firstNonEmpty(strings.TrimSpace(task.TaskID), "unknown") + "\n")
	iterations := 0
	if trace != nil {
		iterations = trace.AssistantTurns
	}
	b.WriteString(fmt.Sprintf("- iterations_before_timeout: %d\n", iterations))
	if trace != nil && len(trace.ToolReceipts) > 0 {
		b.WriteString("- last_tools (newest last, tool=outcome_digest):")
		start := 0
		if len(trace.ToolReceipts) > 5 {
			start = len(trace.ToolReceipts) - 5
		}
		for _, receipt := range trace.ToolReceipts[start:] {
			name := firstNonEmpty(strings.TrimSpace(receipt.ToolName), "tool")
			status := "ok"
			if receipt.IsError {
				status = "err"
			}
			b.WriteString(fmt.Sprintf(" %s=%s/%s", name, status, shortHash(receipt.Output)))
		}
		b.WriteString("\n")
	} else if trace != nil && len(trace.ToolCalls) > 0 {
		b.WriteString("- last_tools: " + truncate(oneLine(strings.Join(trace.ToolCalls, ", ")), 240) + "\n")
	}
	if intent := lastAssistantIntentLine(trace); intent != "" {
		b.WriteString("- last_intent: " + truncate(oneLine(intent), 320) + "\n")
	}
	if cause := strings.TrimSpace(fmt.Sprint(execErr)); cause != "" {
		b.WriteString("- timeout_cause: " + truncate(oneLine(cause), 300) + "\n")
	}
	b.WriteString("Next move: continue the in-progress work using a concrete tool action; do not restart discovery.")
	out := strings.TrimSpace(b.String())
	if len(out) > timeoutResumeSummaryMaxBytes {
		out = out[:timeoutResumeSummaryMaxBytes]
	}
	return out
}

// lastAssistantIntentLine returns the most recent non-empty successful tool
// receipt's first output line as a proxy for the last assistant intent. Tool
// receipts are the durable trace we keep; the bare assistant prose is not
// retained on the trace, so we surface the freshest successful tool outcome
// line as the resumable intent anchor.
func lastAssistantIntentLine(trace *TaskRunTrace) string {
	if trace == nil {
		return ""
	}
	for i := len(trace.ToolReceipts) - 1; i >= 0; i-- {
		receipt := trace.ToolReceipts[i]
		if receipt.IsError {
			continue
		}
		line := strings.TrimSpace(receipt.Output)
		if line == "" {
			continue
		}
		if idx := strings.IndexByte(line, '\n'); idx >= 0 {
			line = line[:idx]
		}
		return strings.TrimSpace(receipt.ToolName + ": " + line)
	}
	return ""
}

// persistTimeoutResumeSummary stores the bounded resume summary in scratch
// state keyed by the timed-out task so the next cycle that re-claims the same
// task can surface it in the prompt. It uses the existing scratch persistence
// channel (no new DB table). Persisted BEFORE active state is cleared so it
// survives the timeout teardown.
func (r *Runtime) persistTimeoutResumeSummary(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, trace *TaskRunTrace, execErr error) error {
	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" {
		return nil
	}
	summary := buildTimeoutResumeSummary(task, trace, execErr)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		count := 1
		if strings.TrimSpace(state.TimeoutResumeTaskID) == taskID && state.TimeoutResumeCount > 0 {
			count = state.TimeoutResumeCount + 1
		}
		state.TimeoutResumeTaskID = taskID
		state.TimeoutResumeSessionID = strings.TrimSpace(session.SessionID)
		state.TimeoutResumeRunID = strings.TrimSpace(runID)
		state.TimeoutResumeSummary = summary
		state.TimeoutResumeAt = now
		state.TimeoutResumeCount = count
	})
}

// timeoutResumeAdvisoryForTask returns the persisted resume summary if it
// belongs to the given task, so the cycle assembly can inject it into the
// prompt advisory signals. Returns "" when there is no pending summary for this
// task.
func (r *Runtime) timeoutResumeAdvisoryForTask(task WorkspaceTaskRecord) string {
	if r == nil {
		return ""
	}
	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if strings.TrimSpace(r.scratch.TimeoutResumeTaskID) != taskID {
		return ""
	}
	return strings.TrimSpace(r.scratch.TimeoutResumeSummary)
}

// clearTimeoutResumeSummary clears the persisted resume summary once the
// task has been resumed (or terminalized), so a stale summary cannot leak into
// an unrelated later cycle.
func (r *Runtime) clearTimeoutResumeSummary(ctx context.Context, taskID string) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil
	}
	r.mu.Lock()
	stale := strings.TrimSpace(r.scratch.TimeoutResumeTaskID) == taskID && strings.TrimSpace(r.scratch.TimeoutResumeSummary) != ""
	r.mu.Unlock()
	if !stale {
		return nil
	}
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.TimeoutResumeTaskID = ""
		state.TimeoutResumeSessionID = ""
		state.TimeoutResumeRunID = ""
		state.TimeoutResumeSummary = ""
		state.TimeoutResumeAt = ""
		state.TimeoutResumeCount = 0
	})
}

func (r *Runtime) yieldTransientProviderTimeoutTaskCycle(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, execErr error, workPacket *AgentWorkPacket, trace *TaskRunTrace) error {
	if r != nil && r.pinnedTaskAllows(task.TaskID) && r.pinnedTaskID() != "" {
		return r.retainPinnedTransientProviderTimeoutTaskCycle(ctx, task, session, runID, execErr, workPacket, trace)
	}
	detail := strings.TrimSpace(fmt.Sprint(execErr))
	summary := "Transient LLM provider timeout; released task claim for retry"
	if detail != "" {
		summary += ": " + truncate(oneLine(detail), 300)
	}
	// Persist a compact, bounded resume summary keyed by this task before
	// teardown so the next cycle that re-claims the task carries it in the
	// prompt instead of rebuilding the same expensive context blind. The timeout
	// path still releases the task and ends the session below (unchanged).
	if err := r.persistTimeoutResumeSummary(ctx, task, session, runID, trace, execErr); err != nil {
		log.Printf("[runtime] persist transient timeout resume summary failed for task %s: %v", firstNonEmpty(strings.TrimSpace(task.TaskID), "unknown"), err)
	}
	result := StructuredTaskResult{
		Outcome:   "failed",
		Summary:   summary,
		Details:   detail,
		BlockedOn: []BlockedRef{{Kind: "runtime", Detail: detail}},
	}
	r.recordTaskCycleProgress(result, nil, nil, time.Now().UTC())

	if _, err := r.client.WriteExecutionRun(ctx, ExecutionRunWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		TaskID:      task.TaskID,
		SessionID:   session.SessionID,
		AgentID:     r.cfg.AgentID,
		Title:       firstNonEmpty(task.Title, "Task "+task.TaskID),
		Summary:     summary,
		Status:      "TIMED_OUT",
		Outcome:     "TRANSIENT_PROVIDER_TIMEOUT",
	}); err != nil {
		return err
	}
	terminalRecords := taskCycleTerminalPhaseRecords(r.cfg, task, session, runID, workPacket, &result, nil)
	markTerminalCanonicalMutationApplied(terminalRecords, true)
	if _, err := r.client.WriteExecutionStep(ctx, ExecutionStepWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		Phase:       "VERIFY",
		Title:       "Yield transient provider timeout",
		Summary:     summary,
		Status:      "TIMED_OUT",
		SortOrder:   30,
		Evidence:    prependCapabilitySnapshotEvidence(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID), nil),
		Verification: map[string]any{
			"capability_snapshot_id":       daemonRunCapabilitySnapshotID(r.cfg, task, session, runID),
			"capability_snapshot_ref":      capabilitySnapshotEvidenceRef(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID)),
			"transient_provider_timeout":   true,
			"operator_queue_required":      false,
			"task_claim_transition":        "release_for_retry",
			"session_terminal_state":       "ENDED",
			"execution_error":              truncate(detail, 1200),
			"task_cycle_phase_records":     terminalRecords,
			"task_cycle_phase_record":      lastTaskCyclePhaseRecord(terminalRecords),
			"canonical_mutation_applied":   true,
			"provider_timeout_retry_class": "transient_llm_provider_call",
		},
	}); err != nil {
		return err
	}
	if _, err := r.client.SessionEvent(ctx, "agent.session.end", sessionEventWithTaskRunTrace(SessionEventInput{
		WorkspaceID:       r.cfg.WorkspaceID,
		SessionID:         session.SessionID,
		AgentID:           r.cfg.AgentID,
		TaskID:            task.TaskID,
		Summary:           summary,
		Status:            "ENDED",
		OwnerScope:        "task/session",
		KeepSessionActive: boolPtr(false),
		UpdatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	}, trace)); err != nil {
		if !isInactiveSessionKeepaliveError(err) {
			return err
		}
		log.Printf("[runtime] transient provider timeout session end observed inactive session %s: %v", firstNonEmpty(strings.TrimSpace(session.SessionID), "unknown"), err)
	}
	if err := r.client.ReleaseTask(ctx, TaskReleaseInput{
		WorkspaceID:           r.cfg.WorkspaceID,
		AgentID:               r.cfg.AgentID,
		TaskID:                task.TaskID,
		Reason:                summary,
		SessionTransitionKind: "reclaim_release",
	}); err != nil {
		return err
	}
	r.logSessionTransitionMemory("session_end_transient_provider_timeout", task.TaskID, session.SessionID, "ENDED", summary, "", "", nil)
	if err := r.clearActiveState(ctx, summary); err != nil {
		return err
	}
	return r.refreshBootstrap(ctx)
}

func (r *Runtime) retainPinnedTransientProviderTimeoutTaskCycle(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, execErr error, workPacket *AgentWorkPacket, trace *TaskRunTrace) error {
	detail := strings.TrimSpace(fmt.Sprint(execErr))
	summary := "Transient LLM provider timeout; pinned task retained for retry"
	if detail != "" {
		summary += ": " + truncate(oneLine(detail), 300)
	}
	if err := r.persistTimeoutResumeSummary(ctx, task, session, runID, trace, execErr); err != nil {
		log.Printf("[runtime] persist pinned transient timeout resume summary failed for task %s: %v", firstNonEmpty(strings.TrimSpace(task.TaskID), "unknown"), err)
	}
	result := StructuredTaskResult{
		Outcome:   "failed",
		Summary:   summary,
		Details:   detail,
		BlockedOn: []BlockedRef{{Kind: "runtime", Detail: detail}},
	}
	r.recordTaskCycleProgress(result, nil, nil, time.Now().UTC())

	if _, err := r.client.WriteExecutionRun(ctx, ExecutionRunWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		TaskID:      task.TaskID,
		SessionID:   session.SessionID,
		AgentID:     r.cfg.AgentID,
		Title:       firstNonEmpty(task.Title, "Task "+task.TaskID),
		Summary:     summary,
		Status:      "TIMED_OUT",
		Outcome:     "TRANSIENT_PROVIDER_TIMEOUT",
	}); err != nil {
		return err
	}
	terminalRecords := taskCycleTerminalPhaseRecords(r.cfg, task, session, runID, workPacket, &result, nil)
	markTerminalCanonicalMutationApplied(terminalRecords, true)
	if _, err := r.client.WriteExecutionStep(ctx, ExecutionStepWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		Phase:       "VERIFY",
		Title:       "Retain pinned task after transient provider timeout",
		Summary:     summary,
		Status:      "TIMED_OUT",
		SortOrder:   30,
		Evidence:    prependCapabilitySnapshotEvidence(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID), nil),
		Verification: map[string]any{
			"capability_snapshot_id":       daemonRunCapabilitySnapshotID(r.cfg, task, session, runID),
			"capability_snapshot_ref":      capabilitySnapshotEvidenceRef(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID)),
			"transient_provider_timeout":   true,
			"operator_queue_required":      false,
			"operator_authored_harness":    true,
			"pinned_task_id":               r.pinnedTaskID(),
			"task_claim_transition":        "retain_claim_for_retry",
			"session_terminal_state":       "ACTIVE",
			"execution_error":              truncate(detail, 1200),
			"task_cycle_phase_records":     terminalRecords,
			"task_cycle_phase_record":      lastTaskCyclePhaseRecord(terminalRecords),
			"canonical_mutation_applied":   true,
			"provider_timeout_retry_class": "transient_llm_provider_call",
		},
	}); err != nil {
		return err
	}
	keepalive := true
	if _, err := r.client.SessionEvent(ctx, "agent.session.keepalive", sessionEventWithTaskRunTrace(SessionEventInput{
		WorkspaceID:       r.cfg.WorkspaceID,
		SessionID:         session.SessionID,
		AgentID:           r.cfg.AgentID,
		TaskID:            task.TaskID,
		Summary:           summary,
		Status:            firstNonEmpty(strings.TrimSpace(session.Status), "ACTIVE"),
		OwnerScope:        "task/session",
		KeepSessionActive: &keepalive,
		UpdatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
	}, trace)); err != nil {
		if !isInactiveSessionKeepaliveError(err) {
			return err
		}
		log.Printf("[runtime] pinned transient provider timeout keepalive observed inactive session %s: %v", firstNonEmpty(strings.TrimSpace(session.SessionID), "unknown"), err)
	}
	r.mu.Lock()
	taskCopy := task
	sessionCopy := session
	r.activeTask = &taskCopy
	r.activeSession = &sessionCopy
	r.activeRunID = runID
	r.scratch.ActiveTaskID = task.TaskID
	r.scratch.ActiveSessionID = session.SessionID
	r.scratch.ActiveRunID = runID
	r.scratch.LastSummary = summary
	r.invalidateFocusLocked()
	stateCopy := r.scratch
	r.mu.Unlock()
	if err := r.saveScratchState(ctx, stateCopy); err != nil {
		return err
	}
	r.logSessionTransitionMemory("session_keepalive_pinned_transient_provider_timeout", task.TaskID, session.SessionID, firstNonEmpty(strings.TrimSpace(session.Status), "ACTIVE"), summary, "", "", result.BlockedOn)
	return nil
}

func (r *Runtime) materializeAfterTerminalTransition(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, workPacket *AgentWorkPacket, terminalKind string) string {
	summary := firstNonEmpty(result.Summary, "task result recorded")
	if err := r.materializeCycle(ctx, task, session, runID, result); err != nil {
		terminalKind = strings.TrimSpace(terminalKind)
		if terminalKind == "" {
			terminalKind = "terminal"
		}
		degradedSummary := fmt.Sprintf("%s (%s materialization degraded: %v)", summary, terminalKind, err)
		log.Printf("[runtime] %s materialization degraded after terminal transition for task %s: %v", terminalKind, task.TaskID, err)
		if _, stepErr := r.client.WriteExecutionStep(ctx, ExecutionStepWriteInput{
			WorkspaceID: r.cfg.WorkspaceID,
			RunID:       runID,
			Phase:       "MATERIALIZE",
			Title:       strings.Title(terminalKind) + " materialization degraded",
			Summary:     degradedSummary,
			Status:      "DEGRADED",
			SortOrder:   35,
			Evidence:    prependCapabilitySnapshotEvidence(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID), phaseEvidenceRefs(&result)),
			Verification: map[string]any{
				"capability_snapshot_id":      daemonRunCapabilitySnapshotID(r.cfg, task, session, runID),
				"capability_snapshot_ref":     capabilitySnapshotEvidenceRef(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID)),
				"terminal_transition_applied": true,
				"materialization_applied":     false,
				"terminal_kind":               terminalKind,
				"error":                       err.Error(),
				"task_cycle_phase_records": taskCycleTerminalPhaseRecords(
					r.cfg,
					task,
					session,
					runID,
					workPacket,
					&result,
					nil,
				),
				"task_cycle_phase_record": taskCyclePhaseRecordEnvelope(
					r.cfg,
					task,
					session,
					runID,
					"RESULT_RECORDED",
					"MATERIALIZE",
					workPacket,
					&result,
					nil,
					7,
				),
			},
		}); stepErr != nil {
			log.Printf("[runtime] failed to record %s materialization degradation for task %s: %v", terminalKind, task.TaskID, stepErr)
		}
		return degradedSummary
	}
	return summary
}

func (r *Runtime) refreshTensionProjectionAfterBlock(ctx context.Context) error {
	if r == nil || r.client == nil || r.cfg.Mode != RuntimeModeDaemon {
		return nil
	}
	workspaceID := strings.TrimSpace(r.cfg.WorkspaceID)
	agentID := strings.TrimSpace(r.cfg.AgentID)
	if workspaceID == "" || agentID == "" {
		return nil
	}
	refreshCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	return r.client.RefreshTensions(refreshCtx, TensionRefreshInput{
		WorkspaceID:  workspaceID,
		ActorID:      agentID,
		Limit:        100,
		ClusterLimit: 20,
	})
}

const runtimeTensionProjectionRefreshCadence = 2 * time.Minute

func (r *Runtime) maybeRefreshTensionProjectionOnPlannerCadence(ctx context.Context) {
	if r == nil || r.client == nil || r.cfg.Mode != RuntimeModeDaemon {
		return
	}
	workspaceID := strings.TrimSpace(r.cfg.WorkspaceID)
	agentID := strings.TrimSpace(r.cfg.AgentID)
	if workspaceID == "" || agentID == "" {
		return
	}
	now := time.Now().UTC()
	r.mu.Lock()
	if !r.lastTensionProjectionRefresh.IsZero() && now.Sub(r.lastTensionProjectionRefresh) < runtimeTensionProjectionRefreshCadence {
		r.mu.Unlock()
		return
	}
	r.lastTensionProjectionRefresh = now
	r.mu.Unlock()

	refreshCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := r.client.RefreshTensions(refreshCtx, TensionRefreshInput{
		WorkspaceID:  workspaceID,
		ActorID:      agentID,
		Limit:        100,
		ClusterLimit: 20,
	}); err != nil {
		log.Printf("[runtime] tension refresh on planner cadence degraded: %v", err)
	}
}

func (r *Runtime) shouldSuppressStaleBlockedTransition(ctx context.Context, task WorkspaceTaskRecord) (bool, error) {
	return r.shouldSuppressStaleTaskTransition(ctx, task, taskAllowsBlockedTransition)
}

func (r *Runtime) shouldSuppressStaleCompletionTransition(ctx context.Context, task WorkspaceTaskRecord) (bool, error) {
	return r.shouldSuppressStaleTaskTransition(ctx, task, taskAllowsCompletionTransition)
}

func (r *Runtime) shouldSuppressStaleTaskTransition(ctx context.Context, task WorkspaceTaskRecord, allows func(WorkspaceTaskRecord, string) bool) (bool, error) {
	tasks, err := r.client.ListTasks(ctx, r.cfg.WorkspaceID)
	if err != nil {
		return false, err
	}
	taskID := strings.TrimSpace(task.TaskID)
	for _, current := range tasks {
		if strings.TrimSpace(current.TaskID) != taskID {
			continue
		}
		return !allows(current, r.cfg.AgentID), nil
	}
	return true, nil
}

func taskAllowsBlockedTransition(task WorkspaceTaskRecord, agentID string) bool {
	switch strings.ToUpper(strings.TrimSpace(task.Status)) {
	case "RESOLVED", "FAILED", "CANCELLED", "COMPLETED":
		return false
	}
	claimStatus := taskClaimStatus(task)
	return isClaimOwnedBy(task, agentID) && (claimStatus == "CLAIMED" || claimStatus == "BLOCKED")
}

func taskAllowsCompletionTransition(task WorkspaceTaskRecord, agentID string) bool {
	if taskAllowsOwnedClaimedTransition(task, agentID) {
		return true
	}
	return taskAllowsSideEffectClassifierBlockedSuccessorCompletion(task, agentID)
}

func taskAllowsOwnedClaimedTransition(task WorkspaceTaskRecord, agentID string) bool {
	switch strings.ToUpper(strings.TrimSpace(task.Status)) {
	case "RESOLVED", "FAILED", "CANCELLED", "COMPLETED":
		return false
	}
	return isClaimOwnedBy(task, agentID) && taskClaimStatus(task) == "CLAIMED"
}

func taskAllowsSideEffectClassifierBlockedSuccessorCompletion(task WorkspaceTaskRecord, agentID string) bool {
	switch strings.ToUpper(strings.TrimSpace(task.Status)) {
	case "RESOLVED", "FAILED", "CANCELLED", "COMPLETED":
		return false
	}
	if !taskLooksSideEffectClassificationParent(task) || !isClaimOwnedBy(task, agentID) || taskClaimStatus(task) != "BLOCKED" {
		return false
	}
	return strings.Contains(strings.ToLower(strings.TrimSpace(pointerValue(task.ClaimSummary))), "waiting_on_side_effect_resolution_successors")
}

func (r *Runtime) failedTaskCycle(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, workPacket *AgentWorkPacket, trace *TaskRunTrace) error {
	if failedPatchQueueSupersedeStewardshipTask(task, result) {
		return r.blockTaskCycle(ctx, task, session, runID, blockedPatchQueueSupersedeStewardshipResult(result), workPacket, trace)
	}
	return r.releaseFailedTaskCycle(ctx, task, session, runID, result, workPacket, trace)
}

func failedPatchQueueSupersedeStewardshipTask(task WorkspaceTaskRecord, result StructuredTaskResult) bool {
	if normalizeOutcome(result.Outcome) != "failed" {
		return false
	}
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		task.ProjectLane,
		strings.Join(task.Tags, " "),
		result.Summary,
		result.Details,
		result.NextAction,
	}, "\n"))
	return strings.Contains(text, "supersede") &&
		(strings.Contains(text, "patch_queue") || strings.Contains(text, "patch queue") || strings.Contains(text, "patchq"))
}

func failedPatchQueueConvergenceTask(task WorkspaceTaskRecord, result StructuredTaskResult, repairInfo *structuredOutputRepairInfo) bool {
	if normalizeOutcome(result.Outcome) != "failed" {
		return false
	}
	if repairInfo == nil || !repairInfo.Attempted || (strings.TrimSpace(repairInfo.ParseError) == "" && strings.TrimSpace(repairInfo.RepairError) == "") {
		return false
	}
	refs := idleReflectionPatchQueueTaskRefsFromTask(task)
	if refs.PatchQueueTask {
		return true
	}
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		task.ProjectLane,
		strings.Join(task.Tags, " "),
		result.Summary,
		result.Details,
		result.NextAction,
	}, "\n"))
	if !containsAnySignal(text, []string{"patch_queue", "patch queue", "patchq", "project_patch_queue"}) {
		return false
	}
	return containsAnySignal(text, []string{"integrat", "review", "accepted candidate", "queue item", "candidate"})
}

func blockedPatchQueueConvergenceFailureResult(result StructuredTaskResult) StructuredTaskResult {
	result.Outcome = "blocked"
	if strings.TrimSpace(result.Summary) == "" {
		result.Summary = "Patch queue convergence task failed before producing a valid structured result"
	}
	if strings.TrimSpace(result.NextAction) == "" {
		result.NextAction = "Inspect the structured_output_repair evidence and create or claim the visible patch-queue continuation instead of retrying the same malformed cycle."
	}
	result.BlockedOn = normalizedBlockedRefs(result.BlockedOn, result.Summary)
	return result
}

func patchQueueSupersedeStewardshipToolFailed(task WorkspaceTaskRecord, result StructuredTaskResult, trace *TaskRunTrace) bool {
	if normalizeOutcome(result.Outcome) == "blocked" || !patchQueueSupersedeStewardshipTaskText(task, result) {
		return false
	}
	if trace == nil || len(trace.FailedToolCalls) == 0 {
		return false
	}
	failed := false
	for _, tool := range trace.FailedToolCalls {
		if strings.TrimSpace(tool) == "project_patch_queue_lifecycle" {
			failed = true
			break
		}
	}
	if !failed {
		return false
	}
	for _, tool := range trace.SuccessfulToolCalls {
		if strings.TrimSpace(tool) == "project_patch_queue_lifecycle" {
			return false
		}
	}
	return true
}

func patchQueueSupersedeStewardshipTaskText(task WorkspaceTaskRecord, result StructuredTaskResult) bool {
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		task.ProjectLane,
		strings.Join(task.Tags, " "),
		result.Summary,
		result.Details,
		result.NextAction,
	}, "\n"))
	return strings.Contains(text, "supersede") &&
		(strings.Contains(text, "patch_queue") || strings.Contains(text, "patch queue") || strings.Contains(text, "patchq"))
}

func patchQueueSupersedeStewardshipInvalidEvidenceObserved(task WorkspaceTaskRecord, result StructuredTaskResult) bool {
	if normalizeOutcome(result.Outcome) == "blocked" || !patchQueueSupersedeStewardshipTaskText(task, result) {
		return false
	}
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		task.ProjectLane,
		strings.Join(task.Tags, " "),
		result.Summary,
		result.Details,
		result.NextAction,
	}, "\n"))
	if patchQueueSupersedeStewardshipTextHasExplicitNegativeVerdict(text) {
		return true
	}
	if !strings.Contains(text, "evidence_doc_key") {
		return false
	}
	return containsAny(text,
		"claimed_work",
		"current_context",
		"agent state",
		"claimed work ledger",
		"active_claimed_work",
		"not validation evidence",
		"not a validation artifact",
		"not positive same-head validation evidence",
		"required evidence",
		"not confirmed",
		"not bound",
		"does not bind",
		"missing exact",
		"without exact",
		"cannot call project_patch_queue_lifecycle",
		"do not call project_patch_queue_lifecycle",
		"cannot safely call",
		"safe to call: false",
	)
}

func patchQueueSupersedeStewardshipPreflightBlockResult(task WorkspaceTaskRecord) (StructuredTaskResult, bool) {
	if !patchQueueSupersedeStewardshipTaskText(task, StructuredTaskResult{}) {
		return StructuredTaskResult{}, false
	}
	evidenceDocKey := patchQueueSupersedeStewardshipEvidenceDocKey(task)
	if evidenceDocKey == "" || !patchQueueSupersedeStewardshipEvidenceKeyLooksAgentState(evidenceDocKey) {
		return StructuredTaskResult{}, false
	}
	summary := fmt.Sprintf("Patch queue supersede stewardship parked before tool loop: evidence_doc_key %s is agent state, not positive same-head validation evidence.", evidenceDocKey)
	return blockedPatchQueueSupersedeStewardshipResult(StructuredTaskResult{
		Outcome:    "blocked",
		Summary:    summary,
		Details:    "Stale supersede stewardship tasks must not enter the LLM/tool loop when their own task brief points at claimed_work/current_context agent state. Wait for fresh queue-facing validation evidence and a new frontier task.",
		NextAction: "Wait for fresh positive same-head validation evidence in a task/review/visual acceptance doc; do not retry this stale supersede stewardship task.",
	}), true
}

func patchQueueSupersedeStewardshipEvidenceDocKey(task WorkspaceTaskRecord) string {
	text := strings.Join([]string{task.Description, task.Title}, "\n")
	for _, line := range strings.Split(text, "\n") {
		clean := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "-"))
		lower := strings.ToLower(clean)
		for _, marker := range []string{"evidence_doc_key:", "evidence_doc_key="} {
			if idx := strings.Index(lower, marker); idx >= 0 {
				value := strings.TrimSpace(clean[idx+len(marker):])
				value = strings.Trim(value, "`'\" ")
				fields := strings.Fields(value)
				if len(fields) > 0 {
					return strings.Trim(fields[0], "`'\",.;")
				}
				return value
			}
		}
	}
	return ""
}

func patchQueueSupersedeStewardshipEvidenceKeyLooksAgentState(docKey string) bool {
	key := strings.ToLower(strings.TrimSpace(docKey))
	if key == "" {
		return false
	}
	if key == "claimed_work" || key == "current_context" {
		return true
	}
	return strings.Contains(key, ".claimed_work") || strings.Contains(key, ".current_context") ||
		strings.Contains(key, "claimed_work") || strings.Contains(key, "current_context")
}

func patchQueueSupersedeStewardshipTextHasExplicitNegativeVerdict(text string) bool {
	compact := strings.ToLower(strings.NewReplacer(
		" ", "",
		"\t", "",
		"\r", "",
		"\n", "",
		"\"", "",
		"'", "",
		"`", "",
	).Replace(text))
	fields := []string{"visual_verdict", "visualverdict", "validation_verdict", "validationverdict", "acceptance_verdict", "acceptanceverdict"}
	values := []string{"block", "blocked", "blocker", "fail", "failed", "reject", "rejected"}
	for _, field := range fields {
		for _, value := range values {
			if strings.Contains(compact, field+":"+value) || strings.Contains(compact, field+"="+value) {
				return true
			}
		}
	}
	return false
}

func blockedPatchQueueSupersedeStewardshipResult(result StructuredTaskResult) StructuredTaskResult {
	result.Outcome = "blocked"
	result.Summary = firstNonEmpty(
		strings.TrimSpace(result.Summary),
		"Patch queue supersede stewardship failed deterministically and was parked.",
	)
	result.NextAction = firstNonEmpty(
		strings.TrimSpace(result.NextAction),
		"Wait for fresh positive same-head validation evidence or a new frontier task; do not retry this stale supersede stewardship task.",
	)
	result.RequiresHuman = false
	result.OwnerAction = ""
	result.HumanReason = ""
	result.BlockedOn = []BlockedRef{{
		Kind:   "patch_queue_supersede",
		Detail: "patch queue supersede stewardship parked; wait for fresh positive same-head validation evidence; do not retry this stale task",
	}}
	return result
}

func (r *Runtime) releaseFailedTaskCycle(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, workPacket *AgentWorkPacket, trace *TaskRunTrace) error {
	if err := r.client.ReleaseTask(ctx, TaskReleaseInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		TaskID:      task.TaskID,
		Reason:      result.Summary,
	}); err != nil {
		return err
	}
	materializationSummary := r.materializeAfterTerminalTransition(ctx, task, session, runID, result, workPacket, "failed")
	if _, err := r.client.SessionEvent(ctx, "agent.session.end", sessionEventWithTaskRunTrace(SessionEventInput{
		WorkspaceID:         r.cfg.WorkspaceID,
		SessionID:           session.SessionID,
		AgentID:             r.cfg.AgentID,
		TaskID:              task.TaskID,
		Summary:             materializationSummary,
		Status:              "ENDED",
		OwnerScope:          "task/session",
		KeepSessionActive:   boolPtr(false),
		RelatedDocKeys:      relatedDocKeys(r.cfg.AgentID, result),
		RelatedArtifactRefs: resultArtifactRefs(result),
		UpdatedAt:           time.Now().UTC().Format(time.RFC3339Nano),
	}, trace)); err != nil {
		return err
	}
	r.logSessionTransitionMemory("session_end_failed", task.TaskID, session.SessionID, "ENDED", materializationSummary, "", "", result.BlockedOn)
	if _, err := r.client.WriteExecutionRun(ctx, ExecutionRunWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		TaskID:      task.TaskID,
		SessionID:   session.SessionID,
		AgentID:     r.cfg.AgentID,
		Title:       firstNonEmpty(task.Title, "Task "+task.TaskID),
		Summary:     materializationSummary,
		Status:      "FAILED",
		Outcome:     "FAILED",
	}); err != nil {
		return err
	}
	if _, err := r.client.WriteExecutionStep(ctx, ExecutionStepWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		Phase:       "VERIFY",
		Title:       "Record failed result",
		Summary:     materializationSummary,
		Status:      "FAILED",
		SortOrder:   30,
		Evidence:    prependCapabilitySnapshotEvidence(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID), phaseEvidenceRefs(&result)),
		Verification: map[string]any{
			"capability_snapshot_id":  daemonRunCapabilitySnapshotID(r.cfg, task, session, runID),
			"capability_snapshot_ref": capabilitySnapshotEvidenceRef(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID)),
			"task_cycle_phase_records": taskCycleTerminalPhaseRecords(
				r.cfg,
				task,
				session,
				runID,
				workPacket,
				&result,
				nil,
			),
			"task_cycle_phase_record": taskCyclePhaseRecordEnvelope(
				r.cfg,
				task,
				session,
				runID,
				"TERMINAL_TRANSITIONED",
				"VERIFY",
				workPacket,
				&result,
				nil,
				8,
			),
		},
	}); err != nil {
		return err
	}
	if err := r.clearActiveState(ctx, materializationSummary); err != nil {
		return err
	}
	return r.refreshBootstrap(ctx)
}

func (r *Runtime) failTaskCycle(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, execErr error, workPacket *AgentWorkPacket, trace *TaskRunTrace) error {
	result := StructuredTaskResult{
		Outcome:   "blocked",
		Summary:   fmt.Sprintf("Execution failed: %v", execErr),
		Details:   execErr.Error(),
		BlockedOn: []BlockedRef{{Kind: "runtime", Detail: execErr.Error()}},
	}
	result = r.normalizeAutonomyResult(result)
	r.recordTaskCycleProgress(result, nil, nil, time.Now().UTC())
	// C-P1-2: thread the cycle trace into the block so token usage from a failed cycle
	// (tool-loop guard, execution errors) is still checkpointed to the session totals and
	// budget ledger. Without it, the most expensive failure path (a 30-iteration tool loop
	// that then errors) recorded zero usage and silently undercounted campaign cost.
	return r.blockTaskCycle(ctx, task, session, runID, result, workPacket, trace)
}

func (r *Runtime) clearActiveState(ctx context.Context, summary string) error {
	r.mu.Lock()
	r.activeTask = nil
	r.activeSession = nil
	r.clearHydrationLocked()
	r.activeWorkPacket = nil
	r.activeRunID = ""
	r.scratch.ActiveTaskID = ""
	r.scratch.ActiveSessionID = ""
	r.scratch.ActiveRunID = ""
	r.clearContinuationHoldLocked()
	clearNoopContinue(&r.scratch)
	r.scratch.LastSummary = summary
	r.invalidateFocusLocked()
	stateCopy := r.scratch
	r.mu.Unlock()
	if err := r.saveScratchState(ctx, stateCopy); err != nil {
		return err
	}
	return r.syncPresenceDocs(ctx)
}

func (r *Runtime) putDocWithRetry(ctx context.Context, docKey, title, content string) error {
	return r.putDocWithRetryExpected(ctx, docKey, title, content, "")
}

func (r *Runtime) putDocWithRetryExpected(ctx context.Context, docKey, title, content, expectedOverride string) error {
	if strings.TrimSpace(docKey) == "" || strings.TrimSpace(content) == "" {
		return nil
	}
	attemptLimit := docConflictRetryLimit(r.cfg)

	var expected string
	if strings.TrimSpace(expectedOverride) != "" {
		expected = strings.TrimSpace(expectedOverride)
	} else {
		r.mu.Lock()
		if r.scratch.DocSHAs != nil {
			expected = r.scratch.DocSHAs[docKey]
		}
		r.mu.Unlock()
	}

	for attempt := 1; attempt <= attemptLimit; attempt++ {
		sha, err := r.client.PutDoc(ctx, WorkspaceDocPutInput{
			WorkspaceID: r.cfg.WorkspaceID,
			DocKey:      docKey,
			Title:       title,
			Content:     content,
			UpdatedBy:   r.cfg.AgentID,
			ExpectedSHA: expected,
		})
		if err == nil {
			if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
				if state.DocSHAs == nil {
					state.DocSHAs = map[string]string{}
				}
				state.DocSHAs[docKey] = sha
			}); err != nil {
				return err
			}
			r.rememberMemoryDocVersion(docKey, sha)
			r.invalidateMemory(LocalMemoryInvalidationInput{
				Reasons: []string{"doc updated"},
				DocKeys: []string{docKey},
				GuardChanges: []LocalMemoryGuardChange{{
					GuardType:      "doc_sha",
					Ref:            docKey,
					CurrentVersion: sha,
				}},
			})
			return nil
		}
		if !isDocumentConflictError(err) {
			return err
		}
		if attempt == attemptLimit {
			return fmt.Errorf("document conflict retry ceiling reached after %d attempts for %s: %w", attemptLimit, docKey, err)
		}
		doc, ok, getErr := r.client.GetDoc(ctx, r.cfg.WorkspaceID, docKey)
		if getErr != nil {
			return getErr
		}
		if !ok {
			return fmt.Errorf("document conflict retry ceiling reached after %d attempts for %s: remote doc missing", attemptLimit, docKey)
		}
		content = mergeDocContents(doc.Content, content, r.cfg.AgentID)
		expected = doc.SHA
	}
	return fmt.Errorf("document conflict retry ceiling reached after %d attempts for %s", attemptLimit, docKey)
}

func (r *Runtime) prepareMaterializedDocWrite(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult) (string, string, string) {
	docKey := strings.TrimSpace(result.Materialize.DocKey)
	title := firstNonEmpty(result.Materialize.DocTitle, docKey)
	content := strings.TrimSpace(result.Materialize.DocContent)
	if r == nil || r.client == nil || docKey == "" || content == "" || !materializedDocIncomingLooksThin(content) {
		return title, content, ""
	}
	doc, ok, err := r.client.GetDoc(ctx, r.cfg.WorkspaceID, docKey)
	if err != nil || !ok {
		if err != nil {
			log.Printf("[runtime] materialize doc preservation check degraded for %s task=%s session=%s run=%s: %v", docKey, strings.TrimSpace(task.TaskID), strings.TrimSpace(session.SessionID), strings.TrimSpace(runID), err)
		}
		return title, content, ""
	}
	protected, applied := protectMaterializedDocContent(docKey, doc.Content, content, r.cfg.AgentID, task.TaskID, session.SessionID, runID, result.Summary, result.Outcome)
	if !applied {
		return title, content, ""
	}
	return firstNonEmpty(doc.Title, title), protected, doc.SHA
}

func materializedDocIncomingLooksThin(content string) bool {
	content = strings.TrimSpace(content)
	if content == "" {
		return false
	}
	if len(content) <= 240 {
		return true
	}
	if len(content) <= 700 && markdownStructureScore(content) <= 1 {
		return true
	}
	return false
}

func protectMaterializedDocContent(docKey, existingContent, incomingContent, agentID, taskID, sessionID, runID, summary, outcome string) (string, bool) {
	existingContent = strings.TrimSpace(existingContent)
	incomingContent = strings.TrimSpace(incomingContent)
	if !shouldProtectMaterializedDocContent(docKey, existingContent, incomingContent) {
		return incomingContent, false
	}
	marker := fmt.Sprintf("<!-- rhizome-final-materialize:%s:%s -->", sanitizeDocKeySegment(firstNonEmpty(agentID, "agent")), sanitizeDocKeySegment(firstNonEmpty(runID, "run")))
	if strings.Contains(existingContent, marker) {
		return existingContent, true
	}
	var b strings.Builder
	b.WriteString(existingContent)
	b.WriteString("\n\n---\n\n")
	b.WriteString(marker)
	b.WriteString("\n## Final Materialization Note\n\n")
	if agentID = strings.TrimSpace(agentID); agentID != "" {
		b.WriteString("- agent_id: " + agentID + "\n")
	}
	if taskID = strings.TrimSpace(taskID); taskID != "" {
		b.WriteString("- task_id: " + taskID + "\n")
	}
	if sessionID = strings.TrimSpace(sessionID); sessionID != "" {
		b.WriteString("- session_id: " + sessionID + "\n")
	}
	if runID = strings.TrimSpace(runID); runID != "" {
		b.WriteString("- run_id: " + runID + "\n")
	}
	if outcome = strings.TrimSpace(outcome); outcome != "" {
		b.WriteString("- outcome: " + normalizeOutcome(outcome) + "\n")
	}
	if summary = strings.TrimSpace(summary); summary != "" {
		b.WriteString("- summary: " + summary + "\n")
	}
	b.WriteString("\n")
	b.WriteString(incomingContent)
	return strings.TrimSpace(b.String()), true
}

func shouldProtectMaterializedDocContent(docKey, existingContent, incomingContent string) bool {
	docKey = strings.ToLower(strings.TrimSpace(docKey))
	existingContent = strings.TrimSpace(existingContent)
	incomingContent = strings.TrimSpace(incomingContent)
	if existingContent == "" || incomingContent == "" || existingContent == incomingContent || strings.Contains(incomingContent, existingContent) {
		return false
	}
	if len(existingContent) < 500 {
		return false
	}
	if len(incomingContent) >= len(existingContent)/2 {
		return false
	}
	if markdownStructureScore(incomingContent) >= markdownStructureScore(existingContent) && len(incomingContent) > 700 {
		return false
	}
	if strings.HasPrefix(docKey, "agent.") {
		return false
	}
	return materializedDocIncomingLooksThin(incomingContent)
}

func markdownStructureScore(content string) int {
	score := 0
	for _, line := range strings.Split(strings.TrimSpace(content), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "#"):
			score += 2
		case strings.HasPrefix(line, "- "), strings.HasPrefix(line, "* "), strings.HasPrefix(line, "1. "):
			score++
		case strings.Contains(line, ":") && len(line) <= 120:
			score++
		}
		if score >= 8 {
			return score
		}
	}
	return score
}

const defaultDocConflictRetryLimit = 4

func docConflictRetryLimit(cfg RuntimeConfig) int {
	limit := cfg.MaxProviderRetryAttempts
	if limit <= 0 {
		limit = defaultProviderRetryLimit
	}
	if limit < defaultDocConflictRetryLimit {
		return defaultDocConflictRetryLimit
	}
	return limit
}

func isDocumentConflictError(err error) bool {
	if err == nil {
		return false
	}
	if isRhizomeRPCErrorCode(err, rhizomeRPCCodeDocumentConflict) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "document conflict")
}

func (r *Runtime) writePrimaryArtifact(ctx context.Context, task WorkspaceTaskRecord, runID string, result StructuredTaskResult) error {
	if strings.TrimSpace(result.Materialize.DocKey) == "" {
		return nil
	}
	artifactRef := "doc:" + strings.TrimSpace(result.Materialize.DocKey)
	metadata, _ := json.Marshal(map[string]any{
		"task_id":    strings.TrimSpace(task.TaskID),
		"run_id":     strings.TrimSpace(runID),
		"doc_key":    strings.TrimSpace(result.Materialize.DocKey),
		"summary":    strings.TrimSpace(result.Summary),
		"outcome":    normalizeOutcome(result.Outcome),
		"agent_id":   strings.TrimSpace(r.cfg.AgentID),
		"created_at": time.Now().UTC().Format(time.RFC3339Nano),
	})
	artifact, err := r.client.WriteArtifact(ctx, WorkspaceArtifactWriteInput{
		WorkspaceID:  r.cfg.WorkspaceID,
		TaskID:       task.TaskID,
		Title:        firstNonEmpty(result.Materialize.DocTitle, result.Materialize.DocKey),
		ArtifactRef:  artifactRef,
		Kind:         "workspace_doc",
		ContentType:  "text/markdown",
		CreatedBy:    r.cfg.AgentID,
		MetadataJSON: string(metadata),
	})
	if err != nil {
		return err
	}
	if strings.TrimSpace(artifact.ArtifactRef) == "" {
		artifact.ArtifactRef = artifactRef
	}
	if artifactID := strings.TrimSpace(artifact.ArtifactID); artifactID != "" {
		r.rememberMemoryArtifactVersion(artifact.ArtifactRef, artifactID)
		r.invalidateMemory(LocalMemoryInvalidationInput{
			Reasons:      []string{"artifact updated"},
			ArtifactRefs: []string{artifact.ArtifactRef},
			GuardChanges: []LocalMemoryGuardChange{{
				GuardType:      "artifact_version",
				Ref:            artifact.ArtifactRef,
				CurrentVersion: artifactID,
			}},
		})
	}
	return nil
}

func resultArtifactRefs(result StructuredTaskResult) []ArtifactRef {
	if strings.TrimSpace(result.Materialize.DocKey) == "" {
		return nil
	}
	return []ArtifactRef{{
		Ref:         "doc:" + strings.TrimSpace(result.Materialize.DocKey),
		Kind:        "workspace_doc",
		ContentType: "text/markdown",
	}}
}

func buildTaskCycleObjective(cfg RuntimeConfig, task WorkspaceTaskRecord, session AgentSessionStateRecord) string {
	var b strings.Builder
	if runtimeTrustFirst(cfg) {
		policy := metacognitionPolicyForRuntimeConfig(cfg)
		b.WriteString("Work one useful trust-first autonomy cycle on the claimed task.\n")
		b.WriteString("Use tools when needed, verify fresh reality, bypass stale blockers where possible, and include public reflection.\n")
		b.WriteString(fmt.Sprintf("Metacognition scope=%s idle_policy=%s. %s\n\n", policy.ReflectionScope, policy.IdleActionPolicy, policy.ScopeDescription))
	} else {
		b.WriteString("Work one useful RNAR cycle on the claimed task.\n")
		b.WriteString("Use tools when needed. Avoid filler. Preserve coordination discipline.\n\n")
	}
	b.WriteString(fmt.Sprintf("task_id=%s\n", task.TaskID))
	b.WriteString(fmt.Sprintf("session_id=%s\n", session.SessionID))
	if task.Title != "" {
		b.WriteString(fmt.Sprintf("title=%s\n", task.Title))
	}
	if task.Description != "" {
		b.WriteString("description:\n")
		b.WriteString(strings.TrimSpace(task.Description))
		b.WriteString("\n")
	}
	b.WriteString("\n")
	b.WriteString(strings.TrimSpace(runtimeStructuredTaskResultContract(cfg)))
	return b.String()
}

func renderAgentContextDoc(task WorkspaceTaskRecord, session AgentSessionStateRecord, result StructuredTaskResult) string {
	var b strings.Builder
	b.WriteString("# Agent Current Context\n\n")
	b.WriteString(fmt.Sprintf("- agent_id: %s\n", session.AgentID))
	b.WriteString(fmt.Sprintf("- session_id: %s\n", session.SessionID))
	b.WriteString(fmt.Sprintf("- task_id: %s\n", task.TaskID))
	b.WriteString(fmt.Sprintf("- task_title: %s\n", firstNonEmpty(task.Title, task.TaskID)))
	b.WriteString(fmt.Sprintf("- outcome: %s\n", result.Outcome))
	b.WriteString(fmt.Sprintf("- summary: %s\n", result.Summary))
	if result.NextAction != "" {
		b.WriteString(fmt.Sprintf("- next_action: %s\n", result.NextAction))
	}
	if result.RequiresHuman {
		b.WriteString(fmt.Sprintf("- human_reason: %s\n", result.HumanReason))
		b.WriteString(fmt.Sprintf("- owner_action: %s\n", result.OwnerAction))
	}
	if len(result.BlockedOn) > 0 {
		b.WriteString("\n## Blocked On\n")
		for _, item := range result.BlockedOn {
			b.WriteString(fmt.Sprintf("- %s: %s\n", item.Kind, item.Detail))
		}
	}
	if result.Details != "" {
		b.WriteString("\n## Details\n")
		b.WriteString(strings.TrimSpace(result.Details))
		b.WriteString("\n")
	}
	return b.String()
}

func renderActiveAgentContextDoc(cfg RuntimeConfig, task WorkspaceTaskRecord, session AgentSessionStateRecord, lastSummary string) string {
	var b strings.Builder
	b.WriteString("# Agent Current Context\n\n")
	b.WriteString(fmt.Sprintf("- agent_id: %s\n", firstNonEmpty(session.AgentID, cfg.AgentID)))
	b.WriteString(fmt.Sprintf("- session_id: %s\n", session.SessionID))
	b.WriteString(fmt.Sprintf("- task_id: %s\n", task.TaskID))
	b.WriteString(fmt.Sprintf("- task_title: %s\n", firstNonEmpty(task.Title, task.TaskID)))
	b.WriteString("- outcome: active\n")
	b.WriteString(fmt.Sprintf("- summary: %s\n", taskActivationSummary(task, lastSummary)))
	b.WriteString("- next_action: Continue the active task cycle.\n")
	return b.String()
}

func renderIdleAgentContextDoc(cfg RuntimeConfig, lastSummary string) string {
	var b strings.Builder
	b.WriteString("# Agent Current Context\n\n")
	b.WriteString(fmt.Sprintf("- agent_id: %s\n", cfg.AgentID))
	b.WriteString("- session_id: (none)\n")
	b.WriteString("- task_id: (none)\n")
	b.WriteString("- outcome: idle\n")
	b.WriteString(fmt.Sprintf("- summary: %s\n", firstNonEmpty(lastSummary, "idle")))
	if runtimeTrustFirst(cfg) {
		policy := metacognitionPolicyForRuntimeConfig(cfg)
		b.WriteString(fmt.Sprintf("- reflection_scope: %s\n", policy.ReflectionScope))
		b.WriteString(fmt.Sprintf("- idle_action_policy: %s\n", policy.IdleActionPolicy))
		b.WriteString(fmt.Sprintf("- next_action: %s\n", renderIdleNextActionForPolicy(policy)))
	} else {
		b.WriteString("- next_action: Wait for a fresh task claim.\n")
	}
	return b.String()
}

func normalizedBlockedRefs(items []BlockedRef, fallback string) []BlockedRef {
	if len(items) > 0 {
		return items
	}
	return []BlockedRef{{Kind: "unknown", Detail: firstNonEmpty(fallback, "blocked")}}
}

func stepStatusForOutcome(outcome string) string {
	switch normalizeOutcome(outcome) {
	case "completed":
		return "COMPLETED"
	case "blocked":
		return "BLOCKED"
	case "failed":
		return "FAILED"
	default:
		return "ACTIVE"
	}
}

func agentContextDocKey(agentID string) string {
	return "agent." + strings.TrimSpace(agentID) + ".current_context"
}

func claimedWorkDocKey(agentID string) string {
	return "agent." + strings.TrimSpace(agentID) + ".claimed_work"
}

func relatedDocKeys(agentID string, result StructuredTaskResult) []string {
	keys := []string{agentContextDocKey(agentID), claimedWorkDocKey(agentID)}
	if strings.TrimSpace(result.Materialize.DocKey) != "" {
		keys = append(keys, strings.TrimSpace(result.Materialize.DocKey))
	}
	return keys
}

func mergeDocContents(serverContent, localContent, agentID string) string {
	serverContent = strings.TrimSpace(serverContent)
	localContent = strings.TrimSpace(localContent)
	switch {
	case localContent == "":
		return serverContent
	case serverContent == "":
		return localContent
	case serverContent == localContent:
		return localContent
	case strings.Contains(serverContent, localContent):
		return serverContent
	case strings.Contains(localContent, serverContent):
		return localContent
	default:
		return serverContent + "\n\n---\nMerged local update from " + strings.TrimSpace(agentID) + ":\n\n" + localContent
	}
}

func (r *Runtime) syncOperatorQueue(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, result StructuredTaskResult) error {
	switch {
	case shouldSurfaceOperatorQueue(result):
		if gateType, ok := externalGateTypeForResult(result); ok {
			activeQueueKey := externalGateQueueKey(gateType, externalGateRequestKey(task.TaskID))
			if err := r.client.RequestExternalGate(ctx, ExternalGateRequestInput{
				WorkspaceID:       r.cfg.WorkspaceID,
				RequestKey:        externalGateRequestKey(task.TaskID),
				GateType:          gateType,
				Title:             firstNonEmpty(result.OwnerAction, result.Summary),
				Summary:           result.Summary,
				Details:           firstNonEmpty(result.HumanReason, result.Details),
				AssignedTo:        r.cfg.OwnerUserID,
				Urgency:           queueUrgencyForPriority(task.Priority),
				SourceKind:        "session",
				SourceID:          session.SessionID,
				TaskID:            task.TaskID,
				SessionID:         session.SessionID,
				AgentID:           r.cfg.AgentID,
				KeepSessionActive: boolPtr(true),
			}); err != nil {
				return err
			}
			return r.resolveHumanQueueKeys(ctx, task.TaskID, result.Summary, activeQueueKey)
		}
		return r.upsertDecisionQueue(ctx, OperatorQueueUpsertInput{
			WorkspaceID:       r.cfg.WorkspaceID,
			QueueKey:          operatorQueueKey(task.TaskID, "decision"),
			QueueType:         "DECISION",
			Title:             firstNonEmpty(result.OwnerAction, result.Summary),
			Summary:           result.Summary,
			Details:           firstNonEmpty(result.HumanReason, result.Details),
			AssignedTo:        r.cfg.OwnerUserID,
			Urgency:           queueUrgencyForPriority(task.Priority),
			SourceKind:        "session",
			SourceID:          session.SessionID,
			TaskID:            task.TaskID,
			SessionID:         session.SessionID,
			AgentID:           r.cfg.AgentID,
			KeepSessionActive: boolPtr(true),
		})
	default:
		return r.resolveHumanQueueKeys(ctx, task.TaskID, result.Summary, "")
	}
}

func (r *Runtime) upsertDecisionQueue(ctx context.Context, input OperatorQueueUpsertInput) error {
	hydrated, ok, err := r.client.GetOperatorQueue(ctx, strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(input.QueueID), strings.TrimSpace(input.QueueKey))
	if err != nil {
		if errors.Is(err, errOperatorQueueLookupUnavailable) {
			return fmt.Errorf("strict operator queue hydration unavailable for %s: %w", strings.TrimSpace(input.QueueKey), err)
		}
		return err
	}
	if ok {
		input.CurrentRevision = hydrated.Revision
		input.CurrentUpdatedAt = hydrated.UpdatedAt
	}
	return r.client.UpsertOperatorQueue(ctx, input)
}

func (r *Runtime) resolveHumanQueueKeys(ctx context.Context, taskID, resolution string, skipKeys ...string) error {
	skip := make(map[string]struct{}, len(skipKeys))
	for _, key := range skipKeys {
		if trimmed := strings.TrimSpace(key); trimmed != "" {
			skip[trimmed] = struct{}{}
		}
	}
	for _, key := range humanGateQueueKeys(taskID) {
		if _, ok := skip[key]; ok {
			continue
		}
		if err := r.resolveOperatorQueue(ctx, key, resolution); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) resolveOperatorQueue(ctx context.Context, queueKey, resolution string) error {
	err := r.client.ResolveOperatorQueue(ctx, OperatorQueueResolveInput{
		WorkspaceID: r.cfg.WorkspaceID,
		QueueKey:    queueKey,
		Status:      "RESOLVED",
		ResolvedBy:  r.cfg.AgentID,
		Resolution:  resolution,
	})
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "not found") {
		return nil
	}
	return err
}

func operatorQueueKey(taskID, suffix string) string {
	return "rnar.task." + strings.TrimSpace(taskID) + "." + strings.TrimSpace(suffix)
}

func externalGateRequestKey(taskID string) string {
	return "rnar.task." + strings.TrimSpace(taskID)
}

func externalGateTypeForResult(result StructuredTaskResult) (string, bool) {
	switch inferDecisionType(result) {
	case "credential":
		return "CREDENTIAL_AUTH", true
	case "payment":
		return "PAYMENT_BILLING", true
	case "approval":
		return "EXPLICIT_APPROVAL", true
	default:
		return "", false
	}
}

func humanGateQueueKeys(taskID string) []string {
	keys := []string{
		operatorQueueKey(taskID, "decision"),
		operatorQueueKey(taskID, "blocker"),
	}
	requestKey := externalGateRequestKey(taskID)
	for _, gateType := range []string{"CREDENTIAL_AUTH", "PAYMENT_BILLING", "EXPLICIT_APPROVAL"} {
		keys = append(keys, externalGateQueueKey(gateType, requestKey))
	}
	return keys
}

func (r *Runtime) syncClaimedWorkDoc(ctx context.Context, task *WorkspaceTaskRecord, session *AgentSessionStateRecord, runID string, result *StructuredTaskResult) error {
	title := "Agent Claimed Work Ledger"
	content := renderClaimedWorkDoc(r.cfg, task, session, runID, result, r.currentSummary())
	return r.putDocWithRetry(ctx, claimedWorkDocKey(r.cfg.AgentID), title, content)
}

func (r *Runtime) syncCurrentContextDoc(ctx context.Context, task *WorkspaceTaskRecord, session *AgentSessionStateRecord, result *StructuredTaskResult) error {
	title := "Agent Current Context"
	lastSummary := r.currentSummary()
	var content string
	switch {
	case task == nil || session == nil:
		content = renderIdleAgentContextDoc(r.cfg, lastSummary)
	case result != nil:
		content = renderAgentContextDoc(*task, *session, *result)
	default:
		content = renderActiveAgentContextDoc(r.cfg, *task, *session, lastSummary)
	}
	return r.putDocWithRetry(ctx, agentContextDocKey(r.cfg.AgentID), title, content)
}

func (r *Runtime) syncPresenceDocs(ctx context.Context) error {
	var taskCopy *WorkspaceTaskRecord
	var sessionCopy *AgentSessionStateRecord
	runID := ""
	r.mu.Lock()
	if r.activeTask != nil {
		copy := *r.activeTask
		taskCopy = &copy
	}
	if r.activeSession != nil {
		copy := *r.activeSession
		sessionCopy = &copy
	}
	runID = strings.TrimSpace(r.activeRunID)
	r.mu.Unlock()

	var firstErr error
	if err := r.syncCurrentContextDoc(ctx, taskCopy, sessionCopy, nil); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := r.syncClaimedWorkDoc(ctx, taskCopy, sessionCopy, runID, nil); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func renderClaimedWorkDoc(cfg RuntimeConfig, task *WorkspaceTaskRecord, session *AgentSessionStateRecord, runID string, result *StructuredTaskResult, lastSummary string) string {
	var b strings.Builder
	summary := strings.TrimSpace(lastSummary)
	if task != nil && session != nil && result == nil {
		summary = taskActivationSummary(*task, summary)
	}
	b.WriteString("# Claimed Work Ledger\n\n")
	b.WriteString(fmt.Sprintf("- agent_id: %s\n", cfg.AgentID))
	b.WriteString(fmt.Sprintf("- workspace_id: %s\n", cfg.WorkspaceID))
	b.WriteString(fmt.Sprintf("- updated_at: %s\n", time.Now().UTC().Format(time.RFC3339)))
	b.WriteString(fmt.Sprintf("- last_summary: %s\n", firstNonEmpty(summary, "idle")))
	if task == nil || session == nil {
		b.WriteString("- active_claimed_work: none\n")
		return b.String()
	}

	b.WriteString("\n## Active Claim\n")
	b.WriteString(fmt.Sprintf("- task_id: %s\n", task.TaskID))
	b.WriteString(fmt.Sprintf("- task_title: %s\n", firstNonEmpty(task.Title, task.TaskID)))
	b.WriteString(fmt.Sprintf("- session_id: %s\n", session.SessionID))
	b.WriteString(fmt.Sprintf("- run_id: %s\n", strings.TrimSpace(runID)))
	b.WriteString(fmt.Sprintf("- state: %s\n", firstNonEmpty(session.Status, "ACTIVE")))
	b.WriteString(fmt.Sprintf("- claimed_by: %s\n", cfg.AgentID))
	if result != nil {
		b.WriteString(fmt.Sprintf("- outcome: %s\n", result.Outcome))
		b.WriteString(fmt.Sprintf("- summary: %s\n", result.Summary))
		if result.NextAction != "" {
			b.WriteString(fmt.Sprintf("- expected_next_action: %s\n", result.NextAction))
		}
		if result.RequiresHuman {
			b.WriteString(fmt.Sprintf("- owner_action: %s\n", result.OwnerAction))
			b.WriteString(fmt.Sprintf("- human_reason: %s\n", result.HumanReason))
		}
		if len(result.BlockedOn) > 0 {
			b.WriteString("\n## Open Blockers\n")
			for _, item := range result.BlockedOn {
				b.WriteString(fmt.Sprintf("- %s: %s\n", item.Kind, item.Detail))
			}
		}
	}
	b.WriteString("\n## Related Docs\n")
	for _, key := range relatedDocKeys(cfg.AgentID, firstNonEmptyResult(result)) {
		b.WriteString(fmt.Sprintf("- %s\n", key))
	}
	return b.String()
}

func firstNonEmptyResult(result *StructuredTaskResult) StructuredTaskResult {
	if result == nil {
		return StructuredTaskResult{}
	}
	return *result
}

func (r *Runtime) recordPolicySignal(signal ToolPolicySignal) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.policySignals = append(r.policySignals, signal)
}

func (r *Runtime) clearPolicySignals() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.policySignals = nil
}

func (r *Runtime) applyPolicySignals(result StructuredTaskResult) StructuredTaskResult {
	r.mu.Lock()
	signals := append([]ToolPolicySignal(nil), r.policySignals...)
	r.policySignals = nil
	r.mu.Unlock()
	if len(signals) == 0 {
		return result
	}
	if runtimeTrustFirst(r.cfg) {
		var advisories []string
		for _, signal := range signals {
			advisories = append(advisories, strings.TrimSpace(signal.Description))
		}
		advisory := "Trust-first policy advisory telemetry: " + strings.Join(uniqueTrimmedCSVStrings(advisories), "; ")
		result.Details = strings.TrimSpace(firstNonEmpty(result.Details, result.Summary) + "\n\n" + advisory)
		return result
	}
	primary := signals[0]
	if primary.Verdict == "REQUIRE_APPROVAL" {
		result.Outcome = "blocked"
		result.RequiresHuman = true
		result.DecisionType = "approval"
		result.OwnerAction = firstNonEmpty(result.OwnerAction, "Approve tool "+primary.ToolName)
		result.HumanReason = firstNonEmpty(result.HumanReason, primary.Description)
		result.BlockedOn = normalizedBlockedRefs(result.BlockedOn, primary.Description)
		if result.Summary == "" {
			result.Summary = primary.Description
		}
		return result
	}
	result.Outcome = "blocked"
	result.BlockedOn = normalizedBlockedRefs(result.BlockedOn, primary.Description)
	if result.Summary == "" {
		result.Summary = primary.Description
	}
	return result
}

func taskCyclePhaseRecordEnvelope(cfg RuntimeConfig, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID, phaseName, sourceStepPhase string, work *AgentWorkPacket, result *StructuredTaskResult, trace *TaskRunTrace, phaseSequence int) map[string]any {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	selectionKey := firstNonEmpty(
		strings.TrimSpace(workTypeForTaskCycle(work)),
		strings.TrimSpace(workWhyNowForTaskCycle(work)),
		strings.TrimSpace(workTransitionForTaskCycle(work)),
		strings.TrimSpace(session.SessionID),
		strings.TrimSpace(task.TaskID),
	)
	selectionBasis := strings.Join([]string{
		strings.TrimSpace(cfg.WorkspaceID),
		strings.TrimSpace(cfg.AgentID),
		strings.TrimSpace(task.TaskID),
		strings.TrimSpace(session.SessionID),
		strings.TrimSpace(runID),
		selectionKey,
		strings.TrimSpace(phaseName),
		strings.TrimSpace(sourceStepPhase),
	}, "|")
	selectionID := "sel_" + shortHash(selectionBasis)
	checkpointID := "chk_" + shortHash(selectionBasis+"|"+phaseName+"|"+sourceStepPhase)

	toolCallNames := []string{}
	if trace != nil {
		for _, callName := range trace.ToolCalls {
			if trimmed := strings.TrimSpace(callName); trimmed != "" {
				toolCallNames = append(toolCallNames, trimmed)
			}
		}
	}

	phaseRecord := map[string]any{
		"schema":                    "task_cycle_phase_record.v1",
		"task_cycle_id":             firstNonEmpty(strings.TrimSpace(runID), strings.TrimSpace(session.SessionID), strings.TrimSpace(task.TaskID)),
		"phase_record_id":           "tpr_" + shortHash(selectionBasis+"|"+phaseName+"|"+sourceStepPhase),
		"phase_sequence":            phaseSequence,
		"phase_name":                strings.TrimSpace(phaseName),
		"workspace_id":              strings.TrimSpace(cfg.WorkspaceID),
		"principal_type":            "agent",
		"principal_id":              strings.TrimSpace(cfg.AgentID),
		"agent_id":                  strings.TrimSpace(cfg.AgentID),
		"task_id":                   strings.TrimSpace(task.TaskID),
		"selection_id":              selectionID,
		"selection_idempotency_key": selectionKey,
		"claim": map[string]any{
			"claim_id":              "",
			"claim_result":          taskCycleClaimResult(task, cfg.AgentID, work),
			"claim_term":            "",
			"claim_status_at_start": taskClaimStatus(task),
			"claimed_by_agent_id":   taskClaimAgentID(task),
		},
		"session": map[string]any{
			"session_id":              strings.TrimSpace(session.SessionID),
			"session_term":            "",
			"session_status_at_start": strings.TrimSpace(session.Status),
		},
		"run": map[string]any{
			"run_id":              strings.TrimSpace(runID),
			"run_term":            "",
			"run_status_at_start": "ACTIVE",
		},
		"basis": map[string]any{
			"capability_snapshot_id":     daemonRunCapabilitySnapshotID(cfg, task, session, runID),
			"capability_snapshot_schema": "daemon_capability_snapshot.v1",
			"prompt_hash":                "sha256:" + shortHash(strings.TrimSpace(task.Title)+"|"+strings.TrimSpace(task.Description)+"|"+strings.TrimSpace(session.SessionID)),
			"objective_hash":             "sha256:" + shortHash(buildTaskCycleObjective(cfg, task, session)),
			"authority_term":             "",
			"authority_term_status":      "unavailable_to_agent",
			"authority_basis":            strings.TrimSpace(session.OwnerScope),
		},
		"checkpoint": map[string]any{
			"checkpoint_id":     checkpointID,
			"progress_cursor":   firstNonEmpty(strings.TrimSpace(runID), strings.TrimSpace(task.TaskID)),
			"progress_summary":  firstNonEmpty(strings.TrimSpace(task.Title), strings.TrimSpace(task.Description)),
			"last_durable_step": taskCycleDurableStepName(phaseName, sourceStepPhase),
		},
		"operations": map[string]any{
			"operation_ids":            []string{},
			"operation_statuses":       []string{},
			"tool_call_names":          toolCallNames,
			"operation_ledger_status":  "pending_A6.2",
			"operation_evidence_basis": "tool names only; durable operation ids are produced by A6.2",
		},
		"structured_result": map[string]any{},
		"terminal_transition": map[string]any{
			"status":                     "non_terminal",
			"transition_at":              "",
			"canonical_mutation_applied": false,
			"terminal_evidence_refs":     []string{},
		},
		"source_step_phase": strings.TrimSpace(sourceStepPhase),
		"created_at":        now,
		"updated_at":        now,
	}
	if result != nil {
		evidenceRefs := phaseEvidenceRefs(result)
		phaseRecord["structured_result"] = map[string]any{
			"outcome":       strings.TrimSpace(result.Outcome),
			"summary":       strings.TrimSpace(result.Summary),
			"next_action":   strings.TrimSpace(result.NextAction),
			"blocked_on":    normalizedBlockedRefs(result.BlockedOn, result.Summary),
			"artifacts":     resultArtifactRefs(*result),
			"evidence_refs": evidenceRefs,
		}
		phaseRecord["terminal_transition"] = map[string]any{
			"status":                     terminalStatusForResult(result.Outcome),
			"transition_at":              now,
			"canonical_mutation_applied": strings.EqualFold(strings.TrimSpace(phaseName), "TERMINAL_TRANSITIONED"),
			"terminal_evidence_refs":     evidenceRefs,
		}
		phaseRecord["checkpoint"] = map[string]any{
			"checkpoint_id":     checkpointID,
			"progress_cursor":   firstNonEmpty(strings.TrimSpace(runID), strings.TrimSpace(task.TaskID)),
			"progress_summary":  firstNonEmpty(strings.TrimSpace(result.Summary), strings.TrimSpace(task.Title), strings.TrimSpace(task.TaskID)),
			"last_durable_step": taskCycleDurableStepName(phaseName, sourceStepPhase),
		}
	}
	return phaseRecord
}

func taskCycleProgressPhaseRecords(cfg RuntimeConfig, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, work *AgentWorkPacket, result *StructuredTaskResult, trace *TaskRunTrace) []map[string]any {
	return []map[string]any{
		taskCyclePhaseRecordEnvelope(cfg, task, session, runID, "CHECKPOINT_SAVED", "EXECUTE", work, result, trace, 5),
		taskCyclePhaseRecordEnvelope(cfg, task, session, runID, "PROGRESS_RECORDED", "EXECUTE", work, result, trace, 6),
	}
}

func taskCycleTerminalPhaseRecords(cfg RuntimeConfig, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, work *AgentWorkPacket, result *StructuredTaskResult, trace *TaskRunTrace) []map[string]any {
	return []map[string]any{
		taskCyclePhaseRecordEnvelope(cfg, task, session, runID, "RESULT_RECORDED", "VERIFY", work, result, trace, 7),
		taskCyclePhaseRecordEnvelope(cfg, task, session, runID, "TERMINAL_TRANSITIONED", "VERIFY", work, result, trace, 8),
	}
}

func markTerminalCanonicalMutationApplied(records []map[string]any, applied bool) {
	for _, record := range records {
		phaseName, _ := record["phase_name"].(string)
		if !strings.EqualFold(strings.TrimSpace(phaseName), "TERMINAL_TRANSITIONED") {
			continue
		}
		transition, ok := record["terminal_transition"].(map[string]any)
		if !ok {
			continue
		}
		transition["canonical_mutation_applied"] = applied
	}
}

func phaseEvidenceRefs(result *StructuredTaskResult) []string {
	if result == nil {
		return nil
	}
	refs := resultArtifactRefs(*result)
	if len(refs) == 0 {
		return nil
	}
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		if trimmed := strings.TrimSpace(ref.Ref); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func taskCycleDurableStepName(phaseName, sourceStepPhase string) string {
	switch strings.ToUpper(strings.TrimSpace(phaseName)) {
	case "CHECKPOINT_SAVED", "PROGRESS_RECORDED", "RESULT_RECORDED", "TERMINAL_TRANSITIONED":
		return strings.TrimSpace(phaseName)
	default:
		return strings.TrimSpace(sourceStepPhase)
	}
}

func taskCycleInitialPhaseRecords(cfg RuntimeConfig, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, work *AgentWorkPacket) []map[string]any {
	return []map[string]any{
		taskCyclePhaseRecordEnvelope(cfg, task, session, runID, "WORK_SELECTED", "PLAN", work, nil, nil, 1),
		taskCyclePhaseRecordEnvelope(cfg, task, session, runID, taskCycleClaimPhaseName(task, cfg.AgentID, work), "PLAN", work, nil, nil, 2),
		taskCyclePhaseRecordEnvelope(cfg, task, session, runID, "SESSION_BOUND", "PLAN", work, nil, nil, 3),
		taskCyclePhaseRecordEnvelope(cfg, task, session, runID, "RUN_BOUND", "PLAN", work, nil, nil, 4),
	}
}

func lastTaskCyclePhaseRecord(records []map[string]any) map[string]any {
	if len(records) == 0 {
		return map[string]any{}
	}
	return records[len(records)-1]
}

func taskCycleClaimPhaseName(task WorkspaceTaskRecord, agentID string, work *AgentWorkPacket) string {
	if strings.EqualFold(taskCycleClaimResult(task, agentID, work), "reused") {
		return "CLAIM_REUSED"
	}
	return "CLAIM_ACQUIRED"
}

func taskCycleClaimResult(task WorkspaceTaskRecord, agentID string, work *AgentWorkPacket) string {
	claimAction := ""
	sessionAction := ""
	if work != nil {
		claimAction = strings.TrimSpace(work.ClaimAction)
		sessionAction = strings.TrimSpace(work.SessionAction)
	}
	switch claimAction {
	case "reuse_claim":
		if strings.EqualFold(taskClaimStatus(task), "CLAIMED") && isClaimOwnedBy(task, agentID) {
			return "reused"
		}
		switch sessionAction {
		case "reuse_active", "resume_inactive":
			return "reused"
		default:
			return "acquired"
		}
	case "claim_required":
		return "acquired"
	}
	return "acquired"
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return fmt.Sprintf("%x", sum[:8])
}

func taskClaimAgentID(task WorkspaceTaskRecord) string {
	if task.ClaimAgentID == nil {
		return ""
	}
	return strings.TrimSpace(*task.ClaimAgentID)
}

func workTypeForTaskCycle(work *AgentWorkPacket) string {
	if work == nil {
		return ""
	}
	return strings.TrimSpace(work.WorkType)
}

func workWhyNowForTaskCycle(work *AgentWorkPacket) string {
	if work == nil {
		return ""
	}
	return strings.TrimSpace(work.WhyNow)
}

func workTransitionForTaskCycle(work *AgentWorkPacket) string {
	if work == nil {
		return ""
	}
	return strings.TrimSpace(work.PreferredTransition)
}

func terminalStatusForResult(outcome string) string {
	switch strings.ToLower(strings.TrimSpace(outcome)) {
	case "completed":
		return "completed"
	case "blocked":
		return "blocked"
	case "failed":
		return "failed"
	case "cancelled", "canceled":
		return "canceled"
	default:
		return "non_terminal"
	}
}
