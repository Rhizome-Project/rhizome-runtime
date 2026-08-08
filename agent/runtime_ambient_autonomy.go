package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

const (
	ambientAutonomyCooldown          = 5 * time.Minute
	ambientAutonomyMaxToolIterations = 12
)

type ambientAutonomyDisposition struct {
	OwnsNoWork       bool
	Ran              bool
	CoolingDown      bool
	SubmittedTask    bool
	SubmittedTaskIDs []string
}

func (r *Runtime) ambientAutonomyRunnable() bool {
	if !r.ambientAutonomyConfigured() {
		return false
	}
	return !r.runtimePaused()
}

func (r *Runtime) ambientAutonomyConfigured() bool {
	return r != nil &&
		r.cfg.Mode == RuntimeModeDaemon &&
		r.client != nil &&
		r.agent != nil &&
		r.agent.LLM != nil &&
		strings.TrimSpace(r.cfg.WorkspaceID) != "" &&
		strings.TrimSpace(r.cfg.AgentID) != ""
}

func (r *Runtime) ambientAutonomyTarget(base idleReflectionTarget, snapshot WorkspaceSnapshot, workspaceID string, now time.Time, policy AgentMetacognitionPolicy) idleReflectionTarget {
	if strings.TrimSpace(base.Key) != "" {
		return base
	}
	if !r.ambientAutonomyConfigured() {
		return idleReflectionTarget{}
	}
	agentID := strings.TrimSpace(r.cfg.AgentID)
	if agentID == "" {
		return idleReflectionTarget{}
	}
	policy = describeMetacognitionPolicy(policy)
	openTasks := make([]WorkspaceTaskRecord, 0, len(snapshot.Tasks))
	for _, task := range snapshot.Tasks {
		if taskSubmitTaskIsTerminal(task) || isIdleReflectionTask(task) {
			continue
		}
		openTasks = append(openTasks, task)
	}
	if idleReflectionHasUnprojectedOpenWork(openTasks) {
		return idleReflectionTarget{}
	}

	projectID := dominantIdleReflectionProjectID(openTasks, snapshot.Projects)
	projectTitle := projectTitleByID(snapshot.Projects, projectID)
	projectLane := "qa"
	scopeKind := "workspace"
	scopeID := firstNonEmpty(strings.TrimSpace(workspaceID), strings.TrimSpace(snapshot.Workspace.WorkspaceID), "workspace")
	if policy.ReflectionScope == reflectionScopeLocal || !policy.CanOpenReflectionTasks {
		scopeKind = "agent"
		scopeID = agentID
		projectLane = "self-check"
	} else if strings.TrimSpace(projectID) != "" {
		scopeKind = "project"
		scopeID = strings.TrimSpace(projectID)
	}

	target := idleReflectionTarget{
		Key:              "ambient:" + sanitizeRefSegment(agentID+"-"+scopeKind+"-"+scopeID),
		ScopeKind:        scopeKind,
		ScopeID:          scopeID,
		ProjectID:        projectID,
		ProjectTitle:     projectTitle,
		ProjectLane:      projectLane,
		ReflectionScope:  policy.ReflectionScope,
		IdleActionPolicy: policy.IdleActionPolicy,
		Title:            ambientAutonomyTitle(policy, scopeKind, scopeID),
		Description:      ambientAutonomyDescription(scopeKind, scopeID, openTasks, policy),
	}
	for _, task := range openTasks {
		taskID := strings.TrimSpace(task.TaskID)
		if taskID == "" {
			continue
		}
		if target.ProjectID != "" && strings.TrimSpace(task.ProjectID) != target.ProjectID {
			continue
		}
		target.OpenTaskIDs = append(target.OpenTaskIDs, taskID)
		if taskClaimStatus(task) == "BLOCKED" || strings.EqualFold(strings.TrimSpace(task.Status), "BLOCKED") {
			target.BlockedTaskIDs = append(target.BlockedTaskIDs, taskID)
		}
		if len(target.AnchorTaskIDs) < 8 {
			target.AnchorTaskIDs = append(target.AnchorTaskIDs, taskID)
		}
	}
	return target
}

func (r *Runtime) maybeRunAmbientAutonomy(ctx context.Context, target idleReflectionTarget, work AgentWorkNextResult, trigger pendingWorkTrigger, now time.Time) (ambientAutonomyDisposition, error) {
	disposition := ambientAutonomyDisposition{}
	if !r.ambientAutonomyRunnable() || normalizeWorkTrigger(trigger.Trigger) != "" || strings.TrimSpace(target.Key) == "" {
		return disposition, nil
	}
	disposition.OwnsNoWork = true
	if r.ambientAutonomyCoolingDown(target.Key, now) {
		disposition.CoolingDown = true
		return disposition, nil
	}
	if err := r.markAmbientAutonomyStarted(ctx, target, now); err != nil {
		return disposition, err
	}
	cycleCtx, cancelCycle := r.backgroundCycleContext(ctx)
	defer cancelCycle()
	r.recordLoopStarted("ambient_autonomy", now)

	if err := r.refreshToolPlane(cycleCtx); err != nil {
		log.Printf("[ambient-autonomy] tool plane refresh degraded: %v", err)
	}

	snapshot := r.bootstrapSnapshotCopy()
	specPack := appendSpecPack(r.activeCapabilitySnapshotPromptProjectionForRequest(), r.buildDaemonSpecPack(cycleCtx, nil))
	specPack = appendSpecPack(specPack, r.ambientProjectDocsSpecPack(cycleCtx, target))
	trace := &TaskRunTrace{}
	taskCtx := AgentTaskContext{
		Mode:              "daemon",
		WorkspaceID:       r.cfg.WorkspaceID,
		AgentID:           r.cfg.AgentID,
		Summary:           r.currentSummary(),
		SpecPack:          specPack,
		Snapshot:          &snapshot,
		Packet:            r.currentWorkPacketCopy(),
		AdvisorySignals:   r.scratch.AdvisorySignals,
		ReflectionSignals: r.scratch.ReflectionSignals,
		ToolLoopLimit:     ambientAutonomyToolLoopLimit(r.cfg),
	}

	objective := buildAmbientAutonomyObjective(r.cfg, target, work, now)
	ambientAgent := *r.agent
	ambientAgent.registry = r.agent.registry.Filter(ambientAutonomyToolAllowed)
	policy := metacognitionPolicyForRuntimeConfig(r.cfg)
	taskSubmitCount := 0
	submittedTaskIDs := []string{}
	executor := func(execCtx context.Context, registry *ToolRegistry, call ToolCall) ToolResult {
		result := r.ambientAutonomyToolExecutorWithLimits(execCtx, registry, call, target, policy, &taskSubmitCount)
		if strings.EqualFold(strings.TrimSpace(call.Function.Name), "task_submit") && !result.IsError {
			if taskID := ambientAutonomySubmittedTaskID(result.Output); taskID != "" {
				submittedTaskIDs = append(submittedTaskIDs, taskID)
			}
		}
		return result
	}
	raw, execErr := ambientAgent.ExecuteTaskWithHooks(cycleCtx, objective, taskCtx, executor, &taskLoopObserver{trace: trace})
	timedOut := errors.Is(cycleCtx.Err(), context.DeadlineExceeded)
	if timedOut && execErr == nil {
		execErr = fmt.Errorf("ambient_autonomy cycle timed out after %s: %w", runtimePlannerCycleTimeout(r.cfg), context.DeadlineExceeded)
	}
	result, parseSalvaged, parseErr := parseStructuredTaskResultStrictOrFirstObject(raw)
	parseError := ""
	if execErr != nil {
		result = ambientAutonomyFailureResult(target, raw, execErr)
	} else if parseErr != nil {
		parseError = parseErr.Error()
		result = ambientAutonomyFailureResult(target, raw, parseErr)
	} else {
		result = r.applyPolicySignals(result)
		result = r.normalizeAutonomyResult(result)
	}
	if strings.TrimSpace(result.Summary) == "" {
		result.Summary = firstNonEmpty(target.Title, "ambient autonomy heartbeat")
	}
	materializeCtx := ctx
	if timedOut {
		timeoutErr := fmt.Errorf("ambient_autonomy cycle timed out after %s", runtimePlannerCycleTimeout(r.cfg))
		r.recordBackgroundCycleTimeoutSignal(ctx, "ambient_autonomy", "SYSTEM LIVENESS: ambient_autonomy_timeout target_key="+strings.TrimSpace(target.Key), timeoutErr, now)
		materializeCtx = context.Background()
	} else if execErr != nil || parseErr != nil {
		r.recordLoopFailure("ambient_autonomy", firstNonNilError(execErr, parseErr), now)
	} else {
		r.recordLoopSuccess("ambient_autonomy", now)
	}
	if err := r.materializeAmbientAutonomyResult(materializeCtx, target, result, raw, trace, now, parseSalvaged, parseError); err != nil {
		return disposition, err
	}
	disposition.Ran = true
	disposition.SubmittedTask = taskSubmitCount > 0
	disposition.SubmittedTaskIDs = uniqueTrimmedAmbientAutonomyStrings(submittedTaskIDs)
	if disposition.SubmittedTask && len(disposition.SubmittedTaskIDs) > 0 {
		if err := r.setPendingWorkTrigger(ctx, "request_resume", disposition.SubmittedTaskIDs[0], ""); err != nil {
			return disposition, err
		}
	}
	return disposition, nil
}

func (r *Runtime) ambientAutonomyCoolingDown(targetKey string, now time.Time) bool {
	if r == nil {
		return false
	}
	targetKey = strings.TrimSpace(targetKey)
	r.mu.Lock()
	lastKey := strings.TrimSpace(r.scratch.AmbientAutonomyKey)
	lastAtRaw := strings.TrimSpace(r.scratch.AmbientAutonomyAt)
	r.mu.Unlock()
	if targetKey == "" || lastKey != targetKey || lastAtRaw == "" {
		return false
	}
	lastAt, err := time.Parse(time.RFC3339Nano, lastAtRaw)
	if err != nil {
		return false
	}
	return now.Sub(lastAt) < ambientAutonomyCooldown
}

func (r *Runtime) markAmbientAutonomyStarted(ctx context.Context, target idleReflectionTarget, now time.Time) error {
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		key := strings.TrimSpace(target.Key)
		if strings.TrimSpace(state.AmbientAutonomyKey) == key {
			state.AmbientAutonomyCount++
		} else {
			state.AmbientAutonomyKey = key
			state.AmbientAutonomyCount = 1
		}
		state.AmbientAutonomyAt = now.UTC().Format(time.RFC3339Nano)
		state.AmbientAutonomySummary = "ambient autonomy heartbeat started"
		state.AmbientAutonomyOutcome = "active"
		state.AmbientAutonomyDocKey = ""
		state.LastSummary = firstNonEmpty(state.LastSummary, "ambient autonomy heartbeat")
	})
}

func (r *Runtime) bootstrapSnapshotCopy() WorkspaceSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bootstrap.Snapshot
}

func (r *Runtime) ambientProjectDocsSpecPack(ctx context.Context, target idleReflectionTarget) string {
	if r == nil || r.client == nil {
		return ""
	}
	projectID := strings.TrimSpace(target.ProjectID)
	if projectID == "" {
		return ""
	}
	keys := []string{
		"project." + projectID + ".reflection_board",
		"project." + projectID + ".product_contract",
		"project." + projectID + ".plan_review",
	}
	keys = append(keys, target.RecentEvidenceDocKeys...)
	var b strings.Builder
	seen := map[string]struct{}{}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		doc, ok, err := r.client.GetDoc(ctx, r.cfg.WorkspaceID, key)
		if err != nil {
			log.Printf("[ambient-autonomy] project doc %s unavailable: %v", key, err)
			continue
		}
		if !ok || strings.TrimSpace(doc.Content) == "" {
			continue
		}
		if b.Len() == 0 {
			b.WriteString("Ambient Project Docs:\n")
		}
		b.WriteString("- " + key + " (" + firstNonEmpty(doc.Title, "untitled") + "):\n")
		b.WriteString(truncate(strings.TrimSpace(doc.Content), 1800))
		b.WriteString("\n")
	}
	return b.String()
}

func ambientAutonomyToolLoopLimit(cfg RuntimeConfig) int {
	cfg.ApplyDefaults()
	if cfg.MaxToolLoopIterations <= 0 {
		return ambientAutonomyMaxToolIterations
	}
	return min(cfg.MaxToolLoopIterations, ambientAutonomyMaxToolIterations)
}

func (r *Runtime) ambientAutonomyToolExecutor(ctx context.Context, registry *ToolRegistry, call ToolCall) ToolResult {
	return r.ambientAutonomyToolExecutorWithLimits(ctx, registry, call, idleReflectionTarget{}, metacognitionPolicyForRuntimeConfig(r.cfg), nil)
}

func (r *Runtime) ambientAutonomyToolExecutorWithLimits(ctx context.Context, registry *ToolRegistry, call ToolCall, target idleReflectionTarget, policy AgentMetacognitionPolicy, taskSubmitCount *int) ToolResult {
	toolName := strings.TrimSpace(call.Function.Name)
	if !ambientAutonomyToolAllowed(toolName) {
		if r != nil {
			r.recordPolicySignal(ToolPolicySignal{
				ToolName:    toolName,
				Verdict:     "DENY",
				Capability:  toolCapabilityForName(toolName),
				Description: fmt.Sprintf("ambient no-task heartbeat blocked authority-bearing tool %s", toolName),
			})
		}
		return ToolResult{
			Output:  fmt.Sprintf("ambient autonomy heartbeat blocked tool %s. Create or claim a concrete task before durable project/service authority changes, source edits, project role changes, budget mutations, paid resource evidence, branch lifecycle evidence, or patch-queue actions.", firstNonEmpty(toolName, "unknown")),
			IsError: true,
		}
	}
	if toolName == "agent_request" && !ambientAutonomyAgentRequestAllowed(call) {
		return ToolResult{Output: "ambient autonomy heartbeat blocked broad agent_request/model.ask. From no-task mode, create a bounded task first or use request_kind=delegate_task with an exact existing task_id; broad peer discussion belongs inside a concrete claimed task.", IsError: true}
	}
	if toolName == "task_submit" {
		policy = describeMetacognitionPolicy(policy)
		if idleReflectionHasActiveNonIdleWork(target) {
			return ToolResult{Output: "ambient autonomy heartbeat blocked task_submit because active non-idle project work already has ownership; publish evidence/memory or wait for that task to produce review/blocker state instead of creating a competing lane.", IsError: true}
		}
		if !policy.CanOpenReflectionTasks || policy.MaxNewTasksPerIdleCycle <= 0 {
			return ToolResult{Output: "ambient autonomy heartbeat blocked task_submit by metacognition policy; publish evidence or memory instead.", IsError: true}
		}
		if taskSubmitCount != nil && *taskSubmitCount >= policy.MaxNewTasksPerIdleCycle {
			return ToolResult{Output: fmt.Sprintf("ambient autonomy heartbeat blocked task_submit after reaching max_new_tasks_per_idle_cycle=%d", policy.MaxNewTasksPerIdleCycle), IsError: true}
		}
		call = ambientAutonomyCallWithDeterministicTaskID(target, call)
		result := r.policyAwareToolExecutor(ctx, registry, call)
		if taskSubmitCount != nil && !result.IsError {
			*taskSubmitCount++
		}
		return result
	}
	return r.policyAwareToolExecutor(ctx, registry, call)
}

func ambientAutonomyAgentRequestAllowed(call ToolCall) bool {
	var args map[string]any
	if err := json.Unmarshal([]byte(call.Function.Arguments), &args); err != nil || args == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(stringArg(args, "request_kind")), "delegate_task") &&
		strings.TrimSpace(stringArg(args, "task_id")) != ""
}

func ambientAutonomyCallWithDeterministicTaskID(target idleReflectionTarget, call ToolCall) ToolCall {
	if !strings.EqualFold(strings.TrimSpace(call.Function.Name), "task_submit") {
		return call
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(call.Function.Arguments), &args); err != nil || args == nil {
		return call
	}
	if strings.TrimSpace(stringArg(args, "task_id")) != "" {
		return call
	}
	if strings.TrimSpace(stringArg(args, "project_id")) == "" && strings.TrimSpace(target.ProjectID) != "" {
		args["project_id"] = strings.TrimSpace(target.ProjectID)
	}
	if strings.TrimSpace(stringArg(args, "project_lane")) == "" && strings.TrimSpace(target.ProjectLane) != "" {
		args["project_lane"] = strings.TrimSpace(target.ProjectLane)
	}
	// R65 creation-side anchor: ambient autonomy must NOT mint implementation-lane work. New
	// implementation work is spec-rooted and is born ONLY from the spec-seed or from coverage-gap
	// detection (the lead's gap task) - never from proactive ambient minting. Anchoring "blocking" on
	// lane-label was defeated precisely because the model picks project_lane=implementation here; so if
	// it did, downgrade to a safe non-implementation lane. The task then can never be born as spec-rooted
	// product work (and the deterministic id carries the task-ambient-* provenance the terminal-side
	// classifier treats as non-blocking - two layers, W2.1 pattern).
	if runtimeProjectLaneRequiresImplementationGate(strings.TrimSpace(stringArg(args, "project_lane"))) {
		safeLane := strings.TrimSpace(target.ProjectLane)
		if safeLane == "" || runtimeProjectLaneRequiresImplementationGate(safeLane) {
			safeLane = "qa"
		}
		args["project_lane"] = safeLane
	}
	args["task_id"] = ambientAutonomyDeterministicTaskID(target, args)
	raw, err := json.Marshal(args)
	if err != nil {
		return call
	}
	call.Function.Arguments = string(raw)
	return call
}

func ambientAutonomyDeterministicTaskID(target idleReflectionTarget, args map[string]any) string {
	scope := compactRefSegment("ambient", firstNonEmpty(target.ProjectID, target.ScopeID, target.Key, "workspace"))
	seed := strings.Join([]string{
		strings.TrimSpace(target.Key),
		strings.TrimSpace(target.ProjectID),
		strings.TrimSpace(target.ScopeKind),
		strings.TrimSpace(target.ScopeID),
		strings.TrimSpace(stringArg(args, "project_lane")),
		strings.TrimSpace(stringArg(args, "task_kind")),
		strings.TrimSpace(stringArg(args, "title")),
		strings.TrimSpace(stringArg(args, "description")),
	}, "|")
	return sanitizeRefSegment("task-ambient-" + scope + "-" + shortHash(seed))
}

func ambientAutonomySubmittedTaskID(output string) string {
	var payload struct {
		TaskID         string `json:"task_id"`
		ExistingTaskID string `json:"existing_task_id"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(firstNonEmpty(payload.TaskID, payload.ExistingTaskID))
}

func uniqueTrimmedAmbientAutonomyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func ambientAutonomyToolAllowed(toolName string) bool {
	switch strings.TrimSpace(toolName) {
	case "read_file",
		"list_directory",
		"memory_read",
		"memory_write",
		"daily_note",
		"memory_search",
		"memory_reinforce",
		"memory_coherence_read",
		"memory_promotion_read",
		"workspace_doc_get",
		"workspace_doc_put",
		"project_patch_queue_list",
		"browser_session",
		"browser_visual_probe",
		"agent_request",
		"coalition_seek",
		"coalition_status",
		"reviewer_route",
		"reviewer_scarcity",
		"service_direction_list",
		"service_direction_get",
		"service_candidate_list",
		"service_candidate_get",
		"service_run_list",
		"service_run_get",
		"service_coordination_get",
		"budget_account_get",
		"budget_ledger_list",
		"budget_reservations_list",
		"task_submit":
		return true
	default:
		return false
	}
}

func buildAmbientAutonomyObjective(cfg RuntimeConfig, target idleReflectionTarget, work AgentWorkNextResult, now time.Time) string {
	policy := metacognitionPolicyForRuntimeConfig(cfg)
	var b strings.Builder
	b.WriteString("Autonomous ambient heartbeat: you are running, awake, and currently have no assigned runnable task.\n")
	b.WriteString("Do not wait passively. Inspect the visible Rhizome context, your local memory packet, shared docs, recent activity, open/closed tasks, project coordination, and other agents' posture. Decide for yourself whether the system needs reflection, evidence, a workspace doc update, a bounded task_submit follow-up, or a delegate_task request for an exact existing task.\n")
	b.WriteString("You must produce one concrete outcome: join or extend existing work, create a bounded follow-up task when policy allows, or record fresh durable evidence explaining why no action is useful now.\n\n")
	b.WriteString("Authority boundary for this heartbeat:\n")
	b.WriteString("- You may read, inspect, create/update workspace docs, write a structured final materialization doc, submit concrete follow-up tasks when the metacognition policy allows it, and use agent_request only as delegate_task with an exact existing task_id.\n")
	b.WriteString("- Use service/project read surfaces for context, but do not bootstrap projects, assign roles, transition phases, create/update service directions/candidates/runs, or record approvals/resources/spend/revenue/outcomes from a no-task heartbeat. Write a doc and submit a bounded task first.\n")
	b.WriteString("- Do not create, raise, reserve, spend, release, or refund budgets from a no-task heartbeat. Budget authority starts only inside a concrete task/run with the right account binding.\n")
	b.WriteString("- Generic host shell is not part of this no-task heartbeat. Browser_visual_probe and browser_session are allowed for trusted visual/UX reality checks when an existing runnable surface or dev-server command is visible; for other local command execution, create or claim a task first.\n")
	b.WriteString("- Do not mutate product source files, materialize/register project checkouts, publish branch lifecycle evidence, commit branches, or integrate/materialize patch queues from this no-task heartbeat. Create or claim a task first.\n")
	b.WriteString("- Keep the cycle bounded: after roughly 4-6 useful tool calls, stop inspecting and return the final JSON with the freshest evidence and next move.\n")
	b.WriteString("- If you create no task, explain the fresh evidence that made passivity acceptable; otherwise the runtime may escalate this heartbeat into a duty-bearing idle task so the gap can be handled with full task authority.\n")
	b.WriteString("- Prefer durable facts in workspace docs/final materialize and local memory fields over private path references.\n\n")
	b.WriteString("Metacognition profile:\n")
	b.WriteString("- reflection_scope: " + policy.ReflectionScope + "\n")
	b.WriteString("- idle_action_policy: " + policy.IdleActionPolicy + "\n")
	b.WriteString(fmt.Sprintf("- can_open_reflection_tasks: %t\n", policy.CanOpenReflectionTasks))
	b.WriteString(fmt.Sprintf("- max_new_tasks_per_idle_cycle: %d\n", policy.MaxNewTasksPerIdleCycle))
	b.WriteString("- scope_guidance: " + policy.ScopeDescription + "\n")
	b.WriteString("- idle_guidance: " + policy.IdleBehaviorDescription + "\n\n")
	b.WriteString("Heartbeat target:\n")
	b.WriteString("- target_key: " + strings.TrimSpace(target.Key) + "\n")
	b.WriteString("- scope_kind: " + firstNonEmpty(target.ScopeKind, "workspace") + "\n")
	b.WriteString("- scope_id: " + strings.TrimSpace(target.ScopeID) + "\n")
	if strings.TrimSpace(target.ProjectID) != "" {
		b.WriteString("- project_id: " + strings.TrimSpace(target.ProjectID) + "\n")
		b.WriteString("- reflection_board_doc_key: project." + strings.TrimSpace(target.ProjectID) + ".reflection_board\n")
		b.WriteString("- product_contract_doc_key: project." + strings.TrimSpace(target.ProjectID) + ".product_contract\n")
		b.WriteString("- plan_review_doc_key: project." + strings.TrimSpace(target.ProjectID) + ".plan_review\n")
	}
	if strings.TrimSpace(work.Reason) != "" || strings.TrimSpace(work.ProfileGateSummary) != "" {
		b.WriteString("- no_work_reason: " + oneLine(firstNonEmpty(work.ProfileGateSummary, work.ProfileGateReason, work.Reason)) + "\n")
	}
	if work.Packet != nil && work.Packet.Gate != nil {
		b.WriteString("- work_gate_state: " + strings.TrimSpace(work.Packet.Gate.GateState) + "\n")
		b.WriteString("- work_gate_type: " + strings.TrimSpace(work.Packet.Gate.GateType) + "\n")
		b.WriteString("- work_gate_summary: " + oneLine(work.Packet.Gate.Summary) + "\n")
	}
	if len(target.OpenTaskIDs) > 0 {
		b.WriteString("- open_task_ids: " + strings.Join(target.OpenTaskIDs, ", ") + "\n")
	}
	if len(target.BlockedTaskIDs) > 0 {
		b.WriteString("- blocked_task_ids: " + strings.Join(target.BlockedTaskIDs, ", ") + "\n")
	}
	if len(target.ActiveTaskIDs) > 0 {
		b.WriteString("- active_task_ids: " + strings.Join(target.ActiveTaskIDs, ", ") + "\n")
		b.WriteString("- active_work_rule: do not create a competing idle/reflection/task_submit lane while these tasks have active ownership; publish evidence or wait unless you can name a concrete blocker.\n")
	}
	if len(target.StaleTaskIDs) > 0 {
		b.WriteString("- ignored_stale_task_ids: " + strings.Join(target.StaleTaskIDs, ", ") + "\n")
	}
	if strings.TrimSpace(target.Description) != "" {
		b.WriteString("\nTarget guidance:\n")
		b.WriteString(strings.TrimSpace(target.Description))
		b.WriteString("\n")
	}
	b.WriteString("\nReturn a strict structured task result JSON object. Use materialize.doc_key/doc_title/doc_content for your durable heartbeat note when you learn anything non-trivial. Use memory_title/memory_body/memory_type for your local note. Current time: ")
	b.WriteString(now.UTC().Format(time.RFC3339Nano))
	b.WriteString("\n\n")
	b.WriteString(strings.TrimSpace(runtimeStructuredTaskResultContract(cfg)))
	return b.String()
}

func ambientAutonomyTitle(policy AgentMetacognitionPolicy, scopeKind, scopeID string) string {
	policy = describeMetacognitionPolicy(policy)
	switch {
	case policy.ReflectionScope == reflectionScopeLocal:
		return "Ambient self-check: inspect local lane and nearby context"
	case policy.ReflectionScope == reflectionScopeArtifact:
		return "Ambient QA heartbeat: inspect evidence and review gaps"
	case strings.EqualFold(scopeKind, "project"):
		return "Ambient project heartbeat: inspect quality state and next improvements"
	default:
		return "Ambient workspace heartbeat: inspect context and useful next work"
	}
}

func ambientAutonomyDescription(scopeKind, scopeID string, openTasks []WorkspaceTaskRecord, policy AgentMetacognitionPolicy) string {
	policy = describeMetacognitionPolicy(policy)
	var b strings.Builder
	b.WriteString("This is a no-task autonomous heartbeat, not an assigned implementation task.\n")
	b.WriteString("Scope: " + firstNonEmpty(scopeKind, "workspace"))
	if strings.TrimSpace(scopeID) != "" {
		b.WriteString(" / " + strings.TrimSpace(scopeID))
	}
	b.WriteString("\n")
	b.WriteString("- Inspect whether formal completion actually matches the product contract, recent evidence, and remaining risks.\n")
	b.WriteString("- If another agent already opened a reflection direction, join or critique it instead of duplicating it.\n")
	if policy.CanOpenReflectionTasks {
		b.WriteString(fmt.Sprintf("- If a concrete gap emerges, create at most %d bounded task_submit follow-up(s) in the correct lane.\n", policy.MaxNewTasksPerIdleCycle))
	} else {
		b.WriteString("- Keep initiative local: publish evidence or memory, and request/flag broader work instead of opening broad reflection tasks.\n")
	}
	if len(openTasks) > 0 {
		b.WriteString("\nVisible open task hints:\n")
		for _, task := range takeIdleReflectionTasks(openTasks, 8) {
			b.WriteString(fmt.Sprintf("- %s [%s/%s]: %s\n", strings.TrimSpace(task.TaskID), strings.TrimSpace(task.Status), taskClaimStatus(task), firstNonEmpty(task.Title, task.Description)))
		}
	}
	return b.String()
}

func ambientAutonomyFailureResult(target idleReflectionTarget, raw string, err error) StructuredTaskResult {
	details := ""
	if err != nil {
		details = err.Error()
	}
	if trimmed := strings.TrimSpace(raw); trimmed != "" {
		details = strings.TrimSpace(details + "\nraw_result: " + truncate(trimmed, 800))
	}
	return StructuredTaskResult{
		Outcome: "failed",
		Summary: firstNonEmpty(target.Title, "ambient autonomy heartbeat failed"),
		Details: details,
		BlockedOn: []BlockedRef{{
			Kind:   "runtime",
			Detail: firstNonEmpty(details, "ambient autonomy cycle failed"),
		}},
	}
}

func (r *Runtime) materializeAmbientAutonomyResult(ctx context.Context, target idleReflectionTarget, result StructuredTaskResult, raw string, trace *TaskRunTrace, now time.Time, parseSalvaged bool, parseError string) error {
	docKey := strings.TrimSpace(result.Materialize.DocKey)
	if docKey != "" {
		title := firstNonEmpty(result.Materialize.DocTitle, result.Summary, docKey)
		content := strings.TrimSpace(result.Materialize.DocContent)
		if content == "" {
			content = renderAmbientAutonomyDoc(target, result, trace, now)
		}
		if err := r.putDocWithRetry(ctx, docKey, title, content); err != nil {
			return err
		}
	}

	r.logAmbientAutonomyMemory(target, result, trace, ambientAutonomyCycleID(r.cfg.AgentID, target.Key, now))

	payload := map[string]any{
		"status":             coordinationStatusForResult(result),
		"next_action":        result.NextAction,
		"target_key":         strings.TrimSpace(target.Key),
		"scope_kind":         strings.TrimSpace(target.ScopeKind),
		"scope_id":           strings.TrimSpace(target.ScopeID),
		"project_id":         strings.TrimSpace(target.ProjectID),
		"doc_keys":           ambientAutonomyDocKeys(r.cfg.AgentID, result),
		"blocked_on":         result.BlockedOn,
		"owner_action":       result.OwnerAction,
		"notes":              firstNonEmpty(result.Details, result.Summary),
		"assistant_turns":    0,
		"tool_calls":         []string{},
		"parse_salvaged":     parseSalvaged,
		"raw_result_preview": truncate(strings.TrimSpace(raw), 800),
	}
	if strings.TrimSpace(parseError) != "" {
		payload["parse_error"] = strings.TrimSpace(parseError)
	}
	if trace != nil {
		payload["assistant_turns"] = trace.AssistantTurns
		payload["tool_calls"] = append([]string(nil), trace.ToolCalls...)
		payload["successful_tool_calls"] = append([]string(nil), trace.SuccessfulToolCalls...)
		payload["failed_tool_calls"] = append([]string(nil), trace.FailedToolCalls...)
	}
	rawPayload, _ := json.Marshal(payload)
	if err := r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID:   r.cfg.WorkspaceID,
		AgentID:       r.cfg.AgentID,
		UpdateType:    "ambient_reflection",
		Summary:       result.Summary,
		PayloadJSON:   string(rawPayload),
		RequiresHuman: result.RequiresHuman,
	}); err != nil {
		return err
	}

	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.AmbientAutonomyKey = strings.TrimSpace(target.Key)
		state.AmbientAutonomyAt = now.UTC().Format(time.RFC3339Nano)
		state.AmbientAutonomySummary = strings.TrimSpace(result.Summary)
		state.AmbientAutonomyOutcome = normalizeOutcome(result.Outcome)
		state.AmbientAutonomyDocKey = docKey
		state.LastSummary = firstNonEmpty(strings.TrimSpace(result.Summary), state.LastSummary)
	}); err != nil {
		return err
	}
	if err := r.syncPresenceDocs(ctx); err != nil {
		log.Printf("[ambient-autonomy] presence doc sync degraded: %v", err)
	}
	return nil
}

func renderAmbientAutonomyDoc(target idleReflectionTarget, result StructuredTaskResult, trace *TaskRunTrace, now time.Time) string {
	var b strings.Builder
	b.WriteString("# Ambient Autonomy Heartbeat\n\n")
	b.WriteString("- created_at: " + now.UTC().Format(time.RFC3339Nano) + "\n")
	b.WriteString("- target_key: " + strings.TrimSpace(target.Key) + "\n")
	b.WriteString("- scope_kind: " + strings.TrimSpace(target.ScopeKind) + "\n")
	b.WriteString("- scope_id: " + strings.TrimSpace(target.ScopeID) + "\n")
	if strings.TrimSpace(target.ProjectID) != "" {
		b.WriteString("- project_id: " + strings.TrimSpace(target.ProjectID) + "\n")
	}
	b.WriteString("- outcome: " + normalizeOutcome(result.Outcome) + "\n")
	b.WriteString("- summary: " + strings.TrimSpace(result.Summary) + "\n")
	if strings.TrimSpace(result.NextAction) != "" {
		b.WriteString("- next_action: " + strings.TrimSpace(result.NextAction) + "\n")
	}
	if trace != nil {
		b.WriteString(fmt.Sprintf("- assistant_turns: %d\n", trace.AssistantTurns))
		if len(trace.ToolCalls) > 0 {
			b.WriteString("- tool_calls: " + strings.Join(trace.ToolCalls, ", ") + "\n")
		}
	}
	if strings.TrimSpace(result.Details) != "" {
		b.WriteString("\n## Details\n\n")
		b.WriteString(strings.TrimSpace(result.Details))
		b.WriteString("\n")
	}
	if result.Reflection != nil {
		b.WriteString("\n## Reflection\n\n")
		b.WriteString("- current_intent: " + strings.TrimSpace(result.Reflection.CurrentIntent) + "\n")
		b.WriteString("- fresh_evidence: " + strings.TrimSpace(result.Reflection.FreshEvidence) + "\n")
		b.WriteString("- blocker_freshness: " + strings.TrimSpace(result.Reflection.BlockerFreshness) + "\n")
		b.WriteString("- next_useful_move: " + strings.TrimSpace(result.Reflection.NextUsefulMove) + "\n")
	}
	return b.String()
}

func (r *Runtime) logAmbientAutonomyMemory(target idleReflectionTarget, result StructuredTaskResult, trace *TaskRunTrace, sourceID string) {
	focus := r.currentFocusCopy()
	clusterID, tensionID := currentFocusAnchors(focus)
	docKeys := ambientAutonomyDocKeys(r.cfg.AgentID, result)
	metadata := map[string]any{
		"target_key":       strings.TrimSpace(target.Key),
		"scope_kind":       strings.TrimSpace(target.ScopeKind),
		"scope_id":         strings.TrimSpace(target.ScopeID),
		"project_id":       strings.TrimSpace(target.ProjectID),
		"reflection_scope": strings.TrimSpace(target.ReflectionScope),
		"idle_policy":      strings.TrimSpace(target.IdleActionPolicy),
	}
	if trace != nil {
		metadata["assistant_turns"] = trace.AssistantTurns
		metadata["tool_calls"] = append([]string(nil), trace.ToolCalls...)
	}
	rawMeta, _ := json.Marshal(metadata)
	nodeType := localMemoryNodeEpisodePack
	switch strings.ToUpper(strings.TrimSpace(result.MemoryType)) {
	case "DECISION":
		nodeType = localMemoryNodeDecision
	case "PROCEDURE":
		nodeType = localMemoryNodeProcedure
	case "ANTI_PROCEDURE":
		nodeType = localMemoryNodeAntiProcedure
	case "INCIDENT":
		nodeType = localMemoryNodeBlocker
	}
	r.logMemoryEvent(LocalMemoryEvent{
		NodeType:       nodeType,
		EventKind:      "ambient_autonomy",
		Summary:        firstNonEmpty(strings.TrimSpace(result.MemoryTitle), strings.TrimSpace(result.Summary)),
		Details:        firstNonEmpty(strings.TrimSpace(result.MemoryBody), strings.TrimSpace(result.Details), strings.TrimSpace(result.NextAction)),
		TensionID:      tensionID,
		ProtoClusterID: clusterID,
		DocKeys:        docKeys,
		Outcome:        normalizeOutcome(result.Outcome),
		SourceID:       sourceID,
		RequiresHuman:  result.RequiresHuman,
		MetadataJSON:   string(rawMeta),
	})
}

func ambientAutonomyDocKeys(agentID string, result StructuredTaskResult) []string {
	keys := []string{agentContextDocKey(agentID), claimedWorkDocKey(agentID)}
	if strings.TrimSpace(result.Materialize.DocKey) != "" {
		keys = append(keys, strings.TrimSpace(result.Materialize.DocKey))
	}
	return uniqueTrimmedCSVStrings(keys)
}

func ambientAutonomyCycleID(agentID, targetKey string, now time.Time) string {
	return "ambient_" + shortHash(strings.Join([]string{
		strings.TrimSpace(agentID),
		strings.TrimSpace(targetKey),
		now.UTC().Format(time.RFC3339Nano),
	}, "|"))
}

func (r *Runtime) ambientAutonomyStatusSnapshot(now time.Time) map[string]any {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	r.mu.Lock()
	paused := r.scratch.ControlPaused
	key := strings.TrimSpace(r.scratch.AmbientAutonomyKey)
	atRaw := strings.TrimSpace(r.scratch.AmbientAutonomyAt)
	summary := strings.TrimSpace(r.scratch.AmbientAutonomySummary)
	outcome := strings.TrimSpace(r.scratch.AmbientAutonomyOutcome)
	docKey := strings.TrimSpace(r.scratch.AmbientAutonomyDocKey)
	count := r.scratch.AmbientAutonomyCount
	r.mu.Unlock()
	status := map[string]any{
		"enabled":             r.ambientAutonomyConfigured() && !paused,
		"cooldown_seconds":    int(ambientAutonomyCooldown.Seconds()),
		"last_key":            key,
		"last_at":             atRaw,
		"last_summary":        summary,
		"last_outcome":        outcome,
		"last_doc_key":        docKey,
		"cycle_count_for_key": count,
	}
	if atRaw != "" {
		if at, err := time.Parse(time.RFC3339Nano, atRaw); err == nil {
			next := at.Add(ambientAutonomyCooldown)
			status["next_due_at"] = next.UTC().Format(time.RFC3339Nano)
			status["cooling_down"] = now.Before(next)
		}
	}
	return status
}
