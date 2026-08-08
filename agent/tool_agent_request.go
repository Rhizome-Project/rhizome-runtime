package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
)

type AgentRequestTool struct {
	client      *RhizomeClient
	workspaceID string
	agentID     string
}

func NewAgentRequestTool(client *RhizomeClient, workspaceID, agentID string) *AgentRequestTool {
	return &AgentRequestTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
	}
}

func (t *AgentRequestTool) Name() string { return "agent_request" }

func (t *AgentRequestTool) Description() string {
	return "Ask a peer agent to answer a bounded read-only question/review, hand off a real Rhizome task, or wake an authority-bearing transition through Rhizome agent.request. For executable work, set request_kind=delegate_task or authority_transition and task_id so the peer runtime wakes its planner on that task. Do not ask peers to run shell/browser/checkouts or durable mutations through question/review model.ask; create a concrete task first. Peer agents cannot read your private local workdir; include needed content or a workspace_doc doc_key."
}

func (t *AgentRequestTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"to_agent_id": map[string]any{
				"type":        "string",
				"description": "Target peer agent ID.",
			},
			"prompt": map[string]any{
				"type":        "string",
				"description": "Concrete bounded request for the peer agent. Do not reference private local files unless you also include the content/excerpt or a canonical workspace_doc doc_key.",
			},
			"request_kind": map[string]any{
				"type":        "string",
				"enum":        []string{"question", "review", "delegate_task", "authority_transition"},
				"description": "Intent of this request. Use delegate_task when asking a peer to claim and execute a Rhizome task through its normal planner loop; use authority_transition when an authorized peer must apply a durable boundary/role/claim transition; use review for read-only peer review only; use question for information exchange.",
			},
			"task_id": map[string]any{
				"type":        "string",
				"description": "Rhizome task id to wake on when request_kind=delegate_task or authority_transition. Optional for question/review.",
			},
			"timeout_sec": map[string]any{
				"type":        "integer",
				"description": "Optional wait budget in seconds. Defaults to 120.",
			},
			"wait_for_response": map[string]any{
				"type":        "boolean",
				"description": "Whether to wait for the peer response before returning. Defaults to true.",
			},
			"wait_for_claim": map[string]any{
				"type":        "boolean",
				"description": "For delegate_task requests, wait for durable task claim evidence after the peer ACK. Defaults to true when wait_for_response is true.",
			},
		},
		"required": []string{"to_agent_id", "prompt"},
	}
}

func (t *AgentRequestTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "agent_request is disabled: missing client or runtime identity", IsError: true}
	}

	toAgentID := strings.TrimSpace(stringArg(args, "to_agent_id"))
	prompt := strings.TrimSpace(stringArg(args, "prompt"))
	requestKind := normalizeAgentRequestKind(stringArg(args, "request_kind"))
	taskID := strings.TrimSpace(stringArg(args, "task_id"))
	timeoutSec := intArg(args["timeout_sec"], 120)
	waitForResponse := boolArg(args, "wait_for_response", true)

	if toAgentID == "" {
		return &ToolResult{Output: "to_agent_id is required", IsError: true}
	}
	if prompt == "" {
		return &ToolResult{Output: "prompt is required", IsError: true}
	}
	if agentRequestHasUnresolvedToolPlaceholder(prompt) || agentRequestHasUnresolvedToolPlaceholder(taskID) {
		return &ToolResult{Output: "agent_request blocked: unresolved tool-result placeholder detected. Resolve the real task_id/doc_key from the previous tool output or workspace state before sending coordination evidence.", IsError: true}
	}
	if strings.EqualFold(toAgentID, t.agentID) {
		return &ToolResult{Output: "agent_request cannot target the current agent; use local tools or direct reasoning instead", IsError: true}
	}
	if agentRequestNeedsSharedArtifact(prompt) {
		return &ToolResult{Output: "agent_request blocked: the prompt appears to ask a peer to inspect a private local file path. Peer agents cannot read your workdir. First store the artifact or review target with workspace_doc_put and reference its doc_key, or include the relevant file content/excerpt directly in the prompt; if the artifact is missing, surface a dependency/tension instead of retrying the same local path.", IsError: true}
	}
	if requestKind == "" {
		requestKind = inferAgentRequestKind(prompt, taskID)
	}
	executableRequest := requestKind == agentRequestKindDelegateTask || requestKind == agentRequestKindAuthorityTransition
	if !executableRequest && agentRequestRequiresWorkLoop(prompt) {
		return &ToolResult{Output: "agent_request blocked: this question/review asks the peer to run a work-loop action such as checkout, shell/browser smoke, screenshot capture, evidence publication, or durable authority mutation. model.ask is read-only. Create a concrete peer-owned task with task_submit, then send agent_request with request_kind=delegate_task or authority_transition and that task_id; if this is your current claimed lane, publish the blocker/next_action and finish or block the task instead of delegating the same claim.", IsError: true}
	}
	if executableRequest {
		if taskID == "" {
			taskID = firstAgentRequestTaskID(prompt)
		}
		if taskID == "" {
			return &ToolResult{Output: fmt.Sprintf("agent_request %s requires task_id so the peer runtime can wake its planner on the executable task", requestKind), IsError: true}
		}
		if output, handled := t.executableTaskPreflight(ctx, requestKind, taskID, toAgentID); handled {
			return output
		}
	}
	if timeoutSec <= 0 {
		timeoutSec = 120
	}
	waitForClaim := boolArg(args, "wait_for_claim", agentRequestDefaultWaitForClaim(requestKind, waitForResponse))
	totalDeadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)

	payloadBody := map[string]any{
		"prompt": prompt,
	}
	if requestKind != "" {
		payloadBody["request_kind"] = requestKind
	}
	if taskID != "" {
		payloadBody["task_id"] = taskID
	}
	payload, err := json.Marshal(payloadBody)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("failed to encode agent request payload: %v", err), IsError: true}
	}

	created, err := t.client.RequestAgent(ctx, AgentRequestInput{
		WorkspaceID: t.workspaceID,
		FromAgentID: t.agentID,
		ToAgentID:   toAgentID,
		Method:      "model.ask",
		PayloadJSON: string(payload),
		TimeoutSec:  timeoutSec,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("failed to create peer request: %v", err), IsError: true}
	}
	if !waitForResponse {
		output := fmt.Sprintf("peer request queued: request_id=%s target=%s status=%s", created.RequestID, toAgentID, firstNonEmpty(created.Status, "PENDING"))
		if requestKind == agentRequestKindDelegateTask {
			output += "; delegate_task is not covered until the target publishes claim_admitted/branch_bound/product evidence"
		} else if requestKind == agentRequestKindAuthorityTransition {
			output += "; authority_transition is not covered until the target reaches claim_admitted and produces a project_role_assign applied/denied receipt or typed terminal blocker"
		}
		return &ToolResult{Output: output}
	}

	record, err := waitForManagedAgentRequestResult(ctx, t.client, t.workspaceID, created.RequestID, time.Duration(timeoutSec)*time.Second)
	if err != nil {
		detail := firstNonEmpty(formatAgentRequestResponse(record), err.Error())
		return &ToolResult{Output: fmt.Sprintf("peer request did not complete cleanly: %s", detail), IsError: true}
	}
	if status := strings.ToUpper(strings.TrimSpace(record.Status)); status == "FAILED" || status == "TIMEOUT" || status == "CANCELLED" || status == "REJECTED" {
		return &ToolResult{Output: fmt.Sprintf("peer request finished with status %s: %s", status, firstNonEmpty(formatAgentRequestResponse(record), "no response body")), IsError: true}
	}

	output := fmt.Sprintf("peer response from %s:\n%s", toAgentID, firstNonEmpty(formatAgentRequestResponse(record), "request completed"))
	if (requestKind == agentRequestKindDelegateTask || requestKind == agentRequestKindAuthorityTransition) && waitForClaim {
		claimWait := time.Until(totalDeadline)
		if claimWait <= 0 {
			return &ToolResult{Output: output + "\n" + agentRequestClaimWaitFailurePrefix(requestKind) + " peer ACK consumed the request timeout before claim evidence could be checked", IsError: true}
		}
		claimSummary, claimErr := waitForDelegatedTaskClaimEvidence(ctx, t.client, t.workspaceID, taskID, toAgentID, claimWait, requestKind == agentRequestKindAuthorityTransition)
		if claimErr != nil {
			return &ToolResult{Output: output + "\n" + agentRequestClaimWaitFailurePrefix(requestKind) + " " + claimErr.Error(), IsError: true}
		}
		if activeLaneSummary, ok := strings.CutPrefix(claimSummary, delegatedTaskActiveLaneCoveragePrefix); ok {
			if requestKind == agentRequestKindAuthorityTransition {
				return &ToolResult{Output: output + "\nAuthority transition admission not confirmed: alternate active-lane coverage is not a target-task authority receipt: " + strings.TrimSpace(activeLaneSummary) + ". Authority transitions require the target task to reach claim_admitted and later produce project_role_assign applied/denied evidence or a typed terminal blocker.", IsError: true}
			} else {
				output += "\nDelegated task alternate coverage confirmed: " + strings.TrimSpace(activeLaneSummary) + ". The sidecar remains unclaimed; branch/product evidence must come from the active lane before treating work as complete."
			}
		} else if requestKind == agentRequestKindAuthorityTransition {
			output += "\nAuthority transition claim confirmed: " + claimSummary + ". A project_role_assign applied/denied receipt or typed terminal blocker is still required before treating the transition as complete."
		} else {
			output += "\nDelegated task claim confirmed: " + claimSummary + ". Branch/product evidence is still required before treating the lane as complete."
		}
	}
	return &ToolResult{Output: output}
}

func agentRequestDefaultWaitForClaim(requestKind string, waitForResponse bool) bool {
	if !waitForResponse {
		return false
	}
	switch requestKind {
	case agentRequestKindDelegateTask, agentRequestKindAuthorityTransition:
		return true
	default:
		return false
	}
}

func agentRequestClaimWaitFailurePrefix(requestKind string) string {
	if requestKind == agentRequestKindAuthorityTransition {
		return "Authority transition admission not confirmed:"
	}
	return "Delegated task coverage not confirmed:"
}

func (t *AgentRequestTool) delegateTaskPreflight(ctx context.Context, taskID, toAgentID string) (*ToolResult, bool) {
	return t.executableTaskPreflight(ctx, agentRequestKindDelegateTask, taskID, toAgentID)
}

func (t *AgentRequestTool) executableTaskPreflight(ctx context.Context, requestKind, taskID, toAgentID string) (*ToolResult, bool) {
	if t == nil || t.client == nil {
		return nil, false
	}
	workspaceID := strings.TrimSpace(t.workspaceID)
	taskID = strings.TrimSpace(taskID)
	toAgentID = strings.TrimSpace(toAgentID)
	if workspaceID == "" || taskID == "" {
		return nil, false
	}
	bundle, err := t.client.HydrateTask(ctx, TaskHydrationInput{
		WorkspaceID:      workspaceID,
		TaskID:           taskID,
		UpdatesLimit:     8,
		ArtifactLimit:    0,
		RelatedTaskLimit: 0,
	})
	label := firstNonEmpty(strings.TrimSpace(requestKind), agentRequestKindDelegateTask)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("agent_request %s blocked: task %s could not be verified before handoff: %v. Do not send optimistic delegation evidence; retry hydration or create a visible task first.", label, taskID, err), IsError: true}, true
	}
	if bundle.WorkspaceTask == nil || strings.TrimSpace(bundle.WorkspaceTask.TaskID) == "" {
		return &ToolResult{Output: fmt.Sprintf("agent_request %s blocked: task %s does not exist or is not visible in workspace %s. Create the real peer-owned lane with task_submit before delegating it.", label, taskID, workspaceID), IsError: true}, true
	}
	task := *bundle.WorkspaceTask
	if taskSubmitTaskIsTerminal(task) {
		if delegatedTaskOwnershipObserved(task, toAgentID) {
			claimStatus := taskClaimStatus(task)
			status := strings.TrimSpace(task.Status)
			if gap := delegatedTaskProjectCoverageGap(task); gap != "" {
				return &ToolResult{Output: fmt.Sprintf("agent_request %s blocked: task %s already has terminal ownership by %s, but the claim is not sufficient coverage: %s. Inspect its result/evidence docs or create a narrower follow-up instead of treating the lane as covered.", label, taskID, toAgentID, gap), IsError: true}, true
			}
			return &ToolResult{Output: fmt.Sprintf("Delegated task already has target terminal ownership: task_id=%s claim_agent_id=%s claim_status=%s task_status=%s. Treat this only as receipt of the delegated lane; branch/product/evidence docs still determine whether follow-up work is needed.", taskID, toAgentID, firstNonEmpty(claimStatus, "<empty>"), firstNonEmpty(status, "<empty>"))}, true
		}
		return &ToolResult{Output: fmt.Sprintf("agent_request %s blocked: task %s is already terminal with status %s. Inspect its result/evidence docs; create a new follow-up task only if current evidence is absent or stale.", label, taskID, firstNonEmpty(strings.TrimSpace(task.Status), "<empty>")), IsError: true}, true
	}
	if requestKind == agentRequestKindDelegateTask && taskRecordIsPathlessSideEffectFoundationCarrier(task) {
		return &ToolResult{Output: fmt.Sprintf("agent_request delegate_task blocked: task %s is a no-path ABPC side-effect foundation carrier. It has side-effect refs but no dirty-path/write-scope identity, so no peer can safely claim it as executable work. Run side_effect_resolve again with fresh active checkout dirty_paths/branch identity, rebind to the active owner, or publish a typed terminal blocker instead of delegating this stale carrier.", taskID), IsError: true}, true
	}
	if requestKind == agentRequestKindDelegateTask {
		if message, ok := runtimeSwitchTerminalBlockerMessage(task); ok {
			return &ToolResult{Output: "agent_request delegate_task blocked: " + message, IsError: true}, true
		}
	}
	if requestKind == agentRequestKindAuthorityTransition {
		if blocker := authorityTransitionDedicatedTaskBlocker(task); blocker != "" {
			return &ToolResult{Output: fmt.Sprintf("agent_request authority_transition blocked: %s. Create or reuse the canonical lead/role-scope repair task first, then send authority_transition with that dedicated task_id.", blocker), IsError: true}, true
		}
		if authorityTransitionTaskHasTerminalBlocker(task) {
			return &ToolResult{Output: "agent_request authority_transition blocked: " + authorityTransitionTerminalBlockerMessage(task), IsError: true}, true
		}
		if coverage, ok := delegatedTaskActiveLaneCoverageFromUpdates(bundle.Updates, taskID, toAgentID); ok {
			return &ToolResult{Output: fmt.Sprintf("agent_request authority_transition blocked: task %s was previously routed only to active lane %s/session %s (coverage_state=%s). Authority transitions require the target task itself to be queued and executed; create or reuse a concrete authority task and do not treat request_resume as transition evidence.", taskID, firstNonEmpty(coverage.ActiveTaskID, "<unknown>"), firstNonEmpty(coverage.ActiveSessionID, "<unknown>"), firstNonEmpty(coverage.CoverageState, "active_task_resume_only")), IsError: true}, true
		}
	}
	claimAgentID := strings.TrimSpace(pointerValue(task.ClaimAgentID))
	claimStatus := taskClaimStatus(task)
	claimActive := taskClaimStatusIsActiveOwnership(claimStatus)
	if requestKind == agentRequestKindAuthorityTransition {
		claimActive = taskClaimStatusIsActiveAuthorityTransitionOwnership(task)
	}
	if output, handled := t.executableTaskClaimPreflight(requestKind, task, taskID, toAgentID, claimAgentID, claimStatus, claimActive); handled {
		return output, true
	}
	if requestKind == agentRequestKindAuthorityTransition {
		return nil, false
	}
	if coverage, ok := delegatedTaskActiveLaneCoverageFromUpdates(bundle.Updates, taskID, toAgentID); ok {
		return &ToolResult{Output: fmt.Sprintf("Delegated task already routed to active lane: task_id=%s target=%s active_task_id=%s active_session_id=%s delegation_state=%s coverage_state=%s. Do not retry the sidecar delegation; wait for the active lane to publish branch/product evidence or create a new follow-up only after current evidence is stale.", taskID, firstNonEmpty(toAgentID, coverage.TargetAgentID, "<unknown>"), firstNonEmpty(coverage.ActiveTaskID, "<unknown>"), firstNonEmpty(coverage.ActiveSessionID, "<unknown>"), firstNonEmpty(coverage.DelegationState, "active_lane_resume_queued"), firstNonEmpty(coverage.CoverageState, "active_task_resume_only"))}, true
	}
	switch {
	case claimAgentID == "" || !claimActive:
		if blocker, ok := delegatedTaskProjectOverlapBlockerFromUpdates(bundle.Updates, taskID); ok {
			return &ToolResult{Output: fmt.Sprintf("agent_request delegate_task blocked: recent project-claim overlap already says task %s is not a safe handoff target: %s. Do not retry another peer on the same stale task; wait for/inspect the active overlapping lane or create a narrower non-overlapping follow-up.", taskID, blocker), IsError: true}, true
		}
		return nil, false
	case strings.EqualFold(claimAgentID, toAgentID):
		if gap := delegatedTaskProjectCoverageGap(task); gap != "" {
			return &ToolResult{Output: fmt.Sprintf("agent_request delegate_task blocked: task %s is already claimed by %s, but the claim is not sufficient coverage: %s. Wait for branch/product evidence or create a narrower follow-up instead of treating the lane as covered.", taskID, toAgentID, gap), IsError: true}, true
		}
		return &ToolResult{Output: fmt.Sprintf("Delegated task already has target ownership: task_id=%s claim_agent_id=%s claim_status=%s task_status=%s. Treat this only as coverage of the claim; branch/product/evidence still must be published by that lane.", taskID, toAgentID, firstNonEmpty(claimStatus, "<empty>"), firstNonEmpty(strings.TrimSpace(task.Status), "<empty>"))}, true
	case strings.EqualFold(claimAgentID, strings.TrimSpace(t.agentID)):
		return &ToolResult{Output: fmt.Sprintf("agent_request delegate_task blocked: task %s is already claimed by the current agent (%s, claim_status=%s). Do not ask a peer to execute your claimed lane through model.ask or delegate_task. If an enabled local tool or installed tool bundle can satisfy the lane, use that tool yourself and publish its evidence. If peer execution is genuinely needed, create a separate peer-owned validation/review task with task_submit and delegate that new task_id; otherwise publish your blocker/next_action and finish or block this task.", taskID, claimAgentID, firstNonEmpty(claimStatus, "<empty>")), IsError: true}, true
	default:
		return &ToolResult{Output: fmt.Sprintf("agent_request delegate_task blocked: task %s is already claimed by %s, not %s (claim_status=%s). Delegate to the current owner, wait for its evidence, or create a separate non-overlapping follow-up task instead of manufacturing duplicate coverage.", taskID, claimAgentID, toAgentID, firstNonEmpty(claimStatus, "<empty>")), IsError: true}, true
	}
}

func (t *AgentRequestTool) executableTaskClaimPreflight(requestKind string, task WorkspaceTaskRecord, taskID, toAgentID, claimAgentID, claimStatus string, claimActive bool) (*ToolResult, bool) {
	if !claimActive || strings.TrimSpace(claimAgentID) == "" {
		return nil, false
	}
	taskID = strings.TrimSpace(taskID)
	toAgentID = strings.TrimSpace(toAgentID)
	claimAgentID = strings.TrimSpace(claimAgentID)
	claimStatus = firstNonEmpty(strings.TrimSpace(claimStatus), "<empty>")
	if requestKind == agentRequestKindAuthorityTransition {
		switch {
		case strings.EqualFold(claimAgentID, toAgentID):
			return &ToolResult{Output: fmt.Sprintf("Authority transition already has target ownership: task_id=%s claim_agent_id=%s claim_status=%s. Do not send another authority_transition request for the same task; wait for the target task to produce a project_role_assign applied/denied receipt or a typed terminal blocker.", taskID, toAgentID, claimStatus)}, true
		case strings.EqualFold(claimAgentID, strings.TrimSpace(t.agentID)):
			return &ToolResult{Output: fmt.Sprintf("agent_request authority_transition blocked: task %s is already claimed by the current agent (%s, claim_status=%s). Do not ask a peer or strategic lead to execute your current claimed task as an authority transition. Create or reuse the dedicated lead-owned authority task (for role/scope repairs, call project_role_assign from the non-lead context so it submits a task-role-scope-* request) and send that task_id, or publish a blocker/next_action for this claim.", taskID, claimAgentID, claimStatus), IsError: true}, true
		default:
			return &ToolResult{Output: fmt.Sprintf("agent_request authority_transition blocked: task %s is already claimed by %s, not %s (claim_status=%s). Authority transitions must target an unclaimed/dedicated authority task or the current owner of that authority task; do not use another agent's claimed lane as the transition carrier.", taskID, claimAgentID, toAgentID, claimStatus), IsError: true}, true
		}
	}
	switch {
	case strings.EqualFold(claimAgentID, toAgentID):
		if gap := delegatedTaskProjectCoverageGap(task); gap != "" {
			return &ToolResult{Output: fmt.Sprintf("agent_request delegate_task blocked: task %s is already claimed by %s, but the claim is not sufficient coverage: %s. Wait for branch/product evidence or create a narrower follow-up instead of treating the lane as covered.", taskID, toAgentID, gap), IsError: true}, true
		}
		return &ToolResult{Output: fmt.Sprintf("Delegated task already has target ownership: task_id=%s claim_agent_id=%s claim_status=%s. Treat this only as coverage of the claim; branch/product/evidence still must be published by that lane.", taskID, toAgentID, claimStatus)}, true
	case strings.EqualFold(claimAgentID, strings.TrimSpace(t.agentID)):
		return &ToolResult{Output: fmt.Sprintf("agent_request delegate_task blocked: task %s is already claimed by the current agent (%s, claim_status=%s). Do not ask a peer to execute your claimed lane through model.ask or delegate_task. If an enabled local tool or installed tool bundle can satisfy the lane, use that tool yourself and publish its evidence. If peer execution is genuinely needed, create a separate peer-owned validation/review task with task_submit and delegate that new task_id; otherwise publish your blocker/next_action and finish or block this task.", taskID, claimAgentID, claimStatus), IsError: true}, true
	default:
		return &ToolResult{Output: fmt.Sprintf("agent_request delegate_task blocked: task %s is already claimed by %s, not %s (claim_status=%s). Delegate to the current owner, wait for its evidence, or create a separate non-overlapping follow-up task instead of manufacturing duplicate coverage.", taskID, claimAgentID, toAgentID, claimStatus), IsError: true}, true
	}
}

func agentRequestRequiresWorkLoop(prompt string) bool {
	lower := strings.ToLower(strings.TrimSpace(prompt))
	if lower == "" {
		return false
	}
	if agentRequestHasScreenshotAction(lower) {
		return true
	}
	for _, marker := range []string{
		"browser-based visual qa",
		"browser smoke/visual qa",
		"browser smoke",
		"visual qa",
		"run playwright",
		"playwright",
		"start the app",
		"launch the app",
		"run the app",
		"materialize checkout",
		"create checkout",
		"publish evidence",
		"publish packet",
		"workspace_doc_put",
		"screenshot refs",
		"viewport matrix",
		"run a real browser",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	action := containsAnySignal(lower, []string{
		"run ",
		"execute ",
		"start ",
		"launch ",
		"capture ",
		"materialize ",
		"checkout ",
		"npm ",
		"npx ",
		"shell",
		"browser",
	})
	workTarget := containsAnySignal(lower, []string{
		"checkout",
		"browser",
		"screenshot",
		"smoke",
		"playwright",
		"vite",
		"npm",
		"server",
		"localhost",
		"evidence",
		"packet",
		"viewport",
		"artifact",
	})
	return action && workTarget
}

func agentRequestHasScreenshotAction(text string) bool {
	text = strings.NewReplacer("don't", "do not", "dont", "do not").Replace(text)
	words := strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	for i, word := range words {
		if word == "screenshot" && !agentRequestActionIsNegated(words, i) &&
			(i == 0 || words[i-1] == "please" || (i >= 2 && (words[i-2] == "can" || words[i-2] == "could" || words[i-2] == "would") && words[i-1] == "you")) {
			return true
		}
		if word != "take" && word != "capture" && word != "grab" && word != "snap" {
			continue
		}
		if agentRequestActionIsNegated(words, i) {
			continue
		}
		j := i + 1
		for j < len(words) {
			switch words[j] {
			case "a", "an", "the", "another", "app", "browser", "current", "fresh", "final", "result", "desktop", "mobile", "new":
				j++
			default:
				goto target
			}
		}
	target:
		if j < len(words) && words[j] == "screenshot" {
			return true
		}
		if j+1 < len(words) && words[j] == "screen" && words[j+1] == "shot" {
			return true
		}
	}
	return false
}

func agentRequestActionIsNegated(words []string, actionIndex int) bool {
	start := actionIndex - 3
	if start < 0 {
		start = 0
	}
	for _, word := range words[start:actionIndex] {
		if word == "not" || word == "never" || word == "without" {
			return true
		}
	}
	return false
}

func waitForDelegatedTaskClaimEvidence(ctx context.Context, client *RhizomeClient, workspaceID, taskID, agentID string, timeout time.Duration, authorityTransition bool) (string, error) {
	if client == nil {
		return "", fmt.Errorf("missing Rhizome client")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" || taskID == "" || agentID == "" {
		return "", fmt.Errorf("missing workspace_id, task_id, or target agent_id")
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	deadline := time.Now().Add(timeout)
	lastState := "not checked"
	for {
		bundle, err := client.HydrateTask(ctx, TaskHydrationInput{
			WorkspaceID:      workspaceID,
			TaskID:           taskID,
			UpdatesLimit:     8,
			ArtifactLimit:    0,
			RelatedTaskLimit: 0,
		})
		if err != nil {
			lastState = "hydrate failed: " + err.Error()
		} else if bundle.WorkspaceTask == nil || strings.TrimSpace(bundle.WorkspaceTask.TaskID) == "" {
			lastState = "task not visible"
		} else {
			task := *bundle.WorkspaceTask
			claimAgentID := strings.TrimSpace(pointerValue(task.ClaimAgentID))
			claimStatus := taskClaimStatus(task)
			status := strings.TrimSpace(task.Status)
			lastState = fmt.Sprintf("task_status=%s claim_agent_id=%s claim_status=%s", firstNonEmpty(status, "<empty>"), firstNonEmpty(claimAgentID, "<empty>"), firstNonEmpty(claimStatus, "<empty>"))
			if coverage, ok := delegatedTaskActiveLaneCoverageFromUpdates(bundle.Updates, taskID, agentID); ok {
				return delegatedTaskActiveLaneCoveragePrefix + coverage.Summary(taskID, agentID), nil
			}
			if blocker, ok := delegatedTaskBlockerFromUpdates(bundle.Updates, taskID, agentID); ok {
				return "", fmt.Errorf("target reported delegated task blocker before claim_admitted: %s; last_state=%s", blocker, lastState)
			}
			if delegatedTaskOwnershipObserved(task, agentID) {
				if gap := delegatedTaskProjectCoverageGap(task); gap != "" {
					return "", fmt.Errorf("task %s claim by %s is not sufficient coverage: %s (%s)", taskID, agentID, gap, lastState)
				}
				return fmt.Sprintf("task_id=%s claim_agent_id=%s claim_status=%s task_status=%s", taskID, agentID, firstNonEmpty(claimStatus, "<empty>"), firstNonEmpty(status, "<empty>")), nil
			}
			if taskSubmitTaskIsTerminal(task) {
				return "", fmt.Errorf("task %s became terminal before target claim (%s)", taskID, lastState)
			}
			claimActive := taskClaimStatusIsActiveOwnership(claimStatus)
			if authorityTransition {
				claimActive = taskClaimStatusIsActiveAuthorityTransitionOwnership(task)
			}
			if claimAgentID != "" && claimAgentID != agentID && claimActive {
				return "", fmt.Errorf("task %s was claimed by %s instead of %s (%s)", taskID, claimAgentID, agentID, lastState)
			}
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "", fmt.Errorf("timed out waiting for claim_admitted on task %s by %s; last_state=%s", taskID, agentID, lastState)
		}
		sleepFor := 2 * time.Second
		if remaining < sleepFor {
			sleepFor = remaining
		}
		timer := time.NewTimer(sleepFor)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return "", ctx.Err()
		case <-timer.C:
		}
	}
}

const delegatedTaskActiveLaneCoveragePrefix = "active_lane_resume:"

type delegatedTaskActiveLaneCoverage struct {
	DelegationState string
	CoverageState   string
	TargetAgentID   string
	ActiveTaskID    string
	ActiveSessionID string
	SummaryText     string
}

func (c delegatedTaskActiveLaneCoverage) Summary(taskID, agentID string) string {
	parts := []string{
		"delegated_task_id=" + strings.TrimSpace(taskID),
		"target=" + firstNonEmpty(strings.TrimSpace(c.TargetAgentID), strings.TrimSpace(agentID), "<unknown>"),
		"active_task_id=" + firstNonEmpty(strings.TrimSpace(c.ActiveTaskID), "<unknown>"),
	}
	if sessionID := strings.TrimSpace(c.ActiveSessionID); sessionID != "" {
		parts = append(parts, "active_session_id="+sessionID)
	}
	parts = append(parts,
		"delegation_state="+firstNonEmpty(strings.TrimSpace(c.DelegationState), "active_lane_resume_queued"),
		"coverage_state="+firstNonEmpty(strings.TrimSpace(c.CoverageState), "active_task_resume_only"),
	)
	if summary := strings.TrimSpace(c.SummaryText); summary != "" {
		parts = append(parts, "summary="+summary)
	}
	return strings.Join(parts, " ")
}

func delegatedTaskActiveLaneCoverageFromUpdates(updates []AgentUpdateRecord, taskID, agentID string) (delegatedTaskActiveLaneCoverage, bool) {
	taskID = strings.TrimSpace(taskID)
	agentID = strings.TrimSpace(agentID)
	for _, update := range updates {
		var payload map[string]any
		if strings.TrimSpace(update.PayloadJSON) == "" || json.Unmarshal([]byte(update.PayloadJSON), &payload) != nil {
			continue
		}
		if !delegatedTaskUpdateMatchesTask(payload, taskID) {
			continue
		}
		targetAgentID := firstNonEmpty(stringMapValue(payload, "to_agent_id"), strings.TrimSpace(update.AgentID))
		if agentID != "" && targetAgentID != "" && targetAgentID != agentID {
			continue
		}
		state := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "delegation_state")))
		coverageState := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "coverage_state")))
		if state != "active_lane_resume_queued" || coverageState != "active_task_resume_only" {
			continue
		}
		activeTaskID := strings.TrimSpace(stringMapValue(payload, "active_task_id"))
		if activeTaskID == "" {
			continue
		}
		return delegatedTaskActiveLaneCoverage{
			DelegationState: state,
			CoverageState:   coverageState,
			TargetAgentID:   targetAgentID,
			ActiveTaskID:    activeTaskID,
			ActiveSessionID: strings.TrimSpace(stringMapValue(payload, "active_session_id")),
			SummaryText:     firstNonEmpty(stringMapValue(payload, "summary"), strings.TrimSpace(update.Summary)),
		}, true
	}
	return delegatedTaskActiveLaneCoverage{}, false
}

func delegatedTaskOwnershipObserved(task WorkspaceTaskRecord, agentID string) bool {
	if !isClaimOwnedBy(task, agentID) {
		return false
	}
	switch taskClaimStatus(task) {
	case "CLAIMED", "COMPLETED", "BLOCKED", "FAILED", "CANCELLED", "CANCELED":
		return true
	}
	return taskSubmitTaskIsTerminal(task)
}

func delegatedTaskProjectCoverageGap(task WorkspaceTaskRecord) string {
	if strings.TrimSpace(task.ProjectID) == "" || !runtimeProjectTaskRequiresImplementationGate(task) {
		return ""
	}
	var missing []string
	if taskPointerValue(task.ClaimBranchID) == "" {
		missing = append(missing, "branch_bound")
	}
	if taskPointerValue(task.ClaimCheckoutID) == "" {
		missing = append(missing, "checkout_bound")
	}
	if taskPointerValue(task.ClaimWriteScopeJSON) == "" {
		missing = append(missing, "write_scope")
	}
	if len(missing) == 0 {
		return ""
	}
	return "project implementation delegation requires claim_admitted with " + strings.Join(missing, ", ")
}

func delegatedTaskBlockerFromUpdates(updates []AgentUpdateRecord, taskID, agentID string) (string, bool) {
	taskID = strings.TrimSpace(taskID)
	agentID = strings.TrimSpace(agentID)
	for _, update := range updates {
		var payload map[string]any
		if strings.TrimSpace(update.PayloadJSON) == "" || json.Unmarshal([]byte(update.PayloadJSON), &payload) != nil {
			continue
		}
		if !delegatedTaskUpdateMatchesTask(payload, taskID) {
			continue
		}
		targetAgentID := firstNonEmpty(stringMapValue(payload, "to_agent_id"), strings.TrimSpace(update.AgentID))
		if agentID != "" && targetAgentID != "" && targetAgentID != agentID {
			continue
		}
		state := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "delegation_state")))
		if delegatedTaskProjectClaimOverlapPayload(payload) && !delegatedTaskProjectClaimOverlapCurrent(update, payload, time.Now().UTC()) {
			continue
		}
		if delegatedTaskBlockerState(state) || delegatedTaskProjectClaimOverlapPayload(payload) {
			return formatDelegatedTaskBlocker(update, payload, taskID, agentID, targetAgentID, state), true
		}
	}
	return "", false
}

func delegatedTaskProjectOverlapBlockerFromUpdates(updates []AgentUpdateRecord, taskID string) (string, bool) {
	taskID = strings.TrimSpace(taskID)
	for _, update := range updates {
		var payload map[string]any
		if strings.TrimSpace(update.PayloadJSON) == "" || json.Unmarshal([]byte(update.PayloadJSON), &payload) != nil {
			continue
		}
		if !delegatedTaskUpdateMatchesTask(payload, taskID) || !delegatedTaskProjectClaimOverlapPayload(payload) {
			continue
		}
		if !delegatedTaskProjectClaimOverlapCurrent(update, payload, time.Now().UTC()) {
			continue
		}
		state := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "delegation_state")))
		targetAgentID := firstNonEmpty(stringMapValue(payload, "to_agent_id"), strings.TrimSpace(update.AgentID))
		return formatDelegatedTaskBlocker(update, payload, taskID, "", targetAgentID, state), true
	}
	return "", false
}

func delegatedTaskUpdateMatchesTask(payload map[string]any, taskID string) bool {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return true
	}
	seenTaskField := false
	for _, key := range []string{"task_id", "delegated_task_id", "blocked_task_id"} {
		got := strings.TrimSpace(stringMapValue(payload, key))
		if got == "" {
			continue
		}
		seenTaskField = true
		if got == taskID {
			return true
		}
	}
	if raw, ok := payload["task_ids"].([]any); ok {
		seenTaskField = true
		for _, item := range raw {
			got, _ := item.(string)
			if strings.TrimSpace(got) == taskID {
				return true
			}
		}
	}
	return !seenTaskField
}

func delegatedTaskProjectClaimOverlapPayload(payload map[string]any) bool {
	state := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "delegation_state")))
	holdKind := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "hold_kind")))
	coverageState := strings.ToLower(strings.TrimSpace(stringMapValue(payload, "coverage_state")))
	switch state {
	case "project_claim_overlap", "delegation_project_claim_overlap", "claim_deferred_project_overlap", "delegation_claim_deferred_project_overlap":
		return true
	}
	if holdKind == "project_claim_overlap" {
		return true
	}
	return coverageState == "covered_by_active_overlapping_claim"
}

func delegatedTaskProjectClaimOverlapCurrent(update AgentUpdateRecord, payload map[string]any, now time.Time) bool {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	for _, key := range []string{"hold_until", "expires_at"} {
		if expiresAt, ok := parseRFC3339Nano(strings.TrimSpace(stringMapValue(payload, key))); ok && expiresAt != nil {
			return now.Before(expiresAt.UTC())
		}
	}
	if createdAt, ok := parseRFC3339Nano(strings.TrimSpace(update.CreatedAt)); ok && createdAt != nil {
		return now.Sub(createdAt.UTC()) <= projectClaimAdmissionOverlapHoldDuration
	}
	return true
}

func delegatedTaskBlockerState(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "blocked_dependency", "delegation_blocked_dependency",
		"blocked_profile", "delegation_blocked_profile",
		"blocked_role", "delegation_blocked_role",
		"blocked_project_gate", "delegation_blocked_project_gate",
		"blocked_validation_artifact", "delegation_blocked_validation_artifact",
		"blocked_targeted_delegation", "delegation_blocked_targeted_delegation",
		"blocked_owner_bound", "delegation_blocked_owner_bound",
		"claim_rejected", "delegation_failed":
		return true
	default:
		return false
	}
}

func formatDelegatedTaskBlocker(update AgentUpdateRecord, payload map[string]any, taskID, agentID, targetAgentID, state string) string {
	state = strings.ToLower(strings.TrimSpace(state))
	if state == "" {
		state = firstNonEmpty(
			strings.ToLower(strings.TrimSpace(stringMapValue(payload, "hold_kind"))),
			strings.ToLower(strings.TrimSpace(stringMapValue(payload, "coverage_state"))),
			"delegation_failed",
		)
	}
	summary := firstNonEmpty(stringMapValue(payload, "summary"), strings.TrimSpace(update.Summary), state)
	reason := firstNonEmpty(stringMapValue(payload, "work_reason"), stringMapValue(payload, "profile_gate_reason"), stringMapValue(payload, "gate_summary"), stringMapValue(payload, "observed_error"))
	if reason != "" && !strings.Contains(summary, reason) {
		summary += " (" + reason + ")"
	}
	var details []string
	for _, key := range []string{"coverage_state", "hold_kind", "conflict_owner_agent_id", "conflict_task_id", "conflict_branch_id", "repair_task_id", "recommended_next_action"} {
		if value := strings.TrimSpace(stringMapValue(payload, key)); value != "" {
			details = append(details, key+"="+value)
		}
	}
	if len(details) > 0 {
		summary += " [" + strings.Join(details, "; ") + "]"
	}
	return fmt.Sprintf("delegation_state=%s target=%s task_id=%s summary=%s", state, firstNonEmpty(targetAgentID, agentID, "<unknown>"), taskID, summary)
}

var (
	agentRequestLocalArtifactPattern = regexp.MustCompile(`(?i)([a-z]:\\|/(?:[\w.\- ]+[/\\])*[\w.\- ]+\.(?:html|css|js|jsx|ts|tsx|go|py|md|json|yaml|yml|txt|png|jpg|jpeg|gif|webp|svg)\b|(?:^|\s)[\w.\-]+[/\\][\w.\- /\\]*\.(?:html|css|js|jsx|ts|tsx|go|py|md|json|yaml|yml|txt|png|jpg|jpeg|gif|webp|svg)\b)`)
	agentRequestDocContextPattern    = regexp.MustCompile(`(?i)(\b(?:doc_key|artifact_ref)\s*[:=]\s*[a-z0-9._:/-]{3,}|\bdoc:[a-z0-9._:/-]{3,})`)
	agentRequestInlineContentPattern = regexp.MustCompile(`(?is)\b(?:file content|content|excerpt)\s*:\s*(?:` + "```" + `.+` + "```" + `|.{120,})`)
	agentRequestFencePattern         = regexp.MustCompile(`(?s)` + "```" + `\s*([^\s].{80,}?)\s*` + "```")
)

func agentRequestNeedsSharedArtifact(prompt string) bool {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" || !agentRequestContainsPrivateArtifactReference(prompt) {
		return false
	}
	if agentRequestHasSharedArtifactContext(prompt) {
		return false
	}
	return true
}

func agentRequestHasUnresolvedToolPlaceholder(text string) bool {
	normalized := strings.ToUpper(strings.TrimSpace(text))
	return strings.Contains(normalized, "__FROM_PREVIOUS") ||
		strings.Contains(normalized, "PREVIOUS_TOOL_RESULT") ||
		strings.Contains(normalized, "<TASK_ID>") ||
		strings.Contains(normalized, "{{TASK_ID}}")
}

func agentRequestContainsPrivateArtifactReference(prompt string) bool {
	for _, loc := range agentRequestLocalArtifactPattern.FindAllStringIndex(prompt, -1) {
		start := trimMatchLeadingSpace(prompt, loc[0], loc[1])
		if start >= 2 && strings.HasSuffix(prompt[:start], ":/") {
			continue
		}
		return true
	}
	return false
}

func trimMatchLeadingSpace(s string, start, end int) int {
	for start < end {
		switch s[start] {
		case ' ', '\t', '\r', '\n':
			start++
		default:
			return start
		}
	}
	return start
}

func agentRequestHasSharedArtifactContext(prompt string) bool {
	if agentRequestDocContextPattern.MatchString(prompt) || agentRequestInlineContentPattern.MatchString(prompt) {
		return true
	}
	if agentRequestFencePattern.MatchString(prompt) {
		return true
	}
	lower := strings.ToLower(prompt)
	for _, marker := range []string{"<!doctype html", "<html", "package main", "func ", "function ", "const ", "class "} {
		if markerHasSubstantialTrailingContent(lower, marker, 80) {
			return true
		}
	}
	return false
}

func markerHasSubstantialTrailingContent(s, marker string, minChars int) bool {
	idx := strings.Index(s, marker)
	if idx < 0 {
		return false
	}
	trailing := strings.TrimSpace(s[idx+len(marker):])
	return len(trailing) >= minChars
}

func intArg(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		if parsed, err := typed.Int64(); err == nil {
			return int(parsed)
		}
	}
	return fallback
}

func boolArg(args map[string]any, key string, fallback bool) bool {
	value, ok := args[key]
	if !ok {
		return fallback
	}
	typed, ok := value.(bool)
	if !ok {
		return fallback
	}
	return typed
}
