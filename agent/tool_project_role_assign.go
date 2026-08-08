package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type ProjectRoleAssignTool struct {
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	runtimeBinding func() AgentRuntimeBinding
}

const (
	projectRoleAssignBoundaryTaskStateLive            = "live"
	projectRoleAssignBoundaryTaskStateTerminalBlocker = "terminal_blocker"
)

func NewProjectRoleAssignTool(client *RhizomeClient, workspaceID, agentID string) *ProjectRoleAssignTool {
	return &ProjectRoleAssignTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
	}
}

func (t *ProjectRoleAssignTool) Name() string { return "project_role_assign" }

func (t *ProjectRoleAssignTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectRoleAssignTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectRoleAssignTool) Description() string {
	return "Assign a non-lead project role and write scope to an agent so project implementation, review, or integration work can be claimed through Rhizome admission."
}

func (t *ProjectRoleAssignTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id": map[string]any{"type": "string", "description": "Project id."},
			"agent_id":   map[string]any{"type": "string", "description": "Agent receiving the project role."},
			"role_type": map[string]any{
				"type":        "string",
				"description": "Role type: PLANNER, IMPLEMENTER, REVIEWER, INTEGRATOR, or OBSERVER. Use project_bootstrap/lead tools for STRATEGIC_LEAD.",
			},
			"write_scope_json": map[string]any{
				"type":        "string",
				"description": "JSON write scope. IMPLEMENTER roles require non-empty paths, for example {\"paths\":[\"src/**\",\"tests/**\",\"package.json\",\"tsconfig*.json\",\"vite.config.*\"]}.",
			},
			"summary": map[string]any{
				"type":        "string",
				"description": "Short coordination note explaining why this role/scope is assigned.",
			},
			"boundary_transition_key": map[string]any{
				"type":        "string",
				"description": "ABPC authority-transition dedup key from a project_role_scope_authority_transition task. Pass this through when present so denials and receipts bind to the original side effect.",
			},
			"side_effect_refs": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Side-effect refs from the authority-transition task, if any.",
			},
			"active_task_id": map[string]any{
				"type":        "string",
				"description": "Original owner lane task id from the authority-transition task, if any.",
			},
			"branch_id": map[string]any{
				"type":        "string",
				"description": "Original branch id from the authority-transition task, if any.",
			},
			"branch_name": map[string]any{
				"type":        "string",
				"description": "Original branch name from the authority-transition task, if any.",
			},
		},
		"required": []string{"project_id", "agent_id", "role_type"},
	}
}

func (t *ProjectRoleAssignTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_role_assign is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	projectID, err := resolveProjectToolActiveProjectID("project_role_assign", t.workspaceID, projectToolRefArg(args, "project_id"), t.runtimeBinding)
	targetAgentID := strings.TrimSpace(stringArg(args, "agent_id"))
	roleType := normalizeProjectRoleAssignToolRole(stringArg(args, "role_type"))
	writeScopeJSON := strings.TrimSpace(stringArg(args, "write_scope_json"))
	summary := strings.TrimSpace(stringArg(args, "summary"))
	transitionMeta := projectRoleAssignBoundaryTransitionMetaFromArgs(args)
	suppressDenialUpdate := boolArg(args, "suppress_denial_update", false)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	if targetAgentID == "" {
		return &ToolResult{Output: "agent_id is required", IsError: true}
	}
	if roleType == "" {
		return &ToolResult{Output: "role_type must be one of PLANNER, IMPLEMENTER, REVIEWER, INTEGRATOR, or OBSERVER", IsError: true}
	}
	if writeScopeJSON != "" && !json.Valid([]byte(writeScopeJSON)) {
		return &ToolResult{Output: "write_scope_json must be valid JSON", IsError: true}
	}
	if roleType == "IMPLEMENTER" && projectRoleAssignWriteScopeEmpty(writeScopeJSON) {
		return &ToolResult{Output: "IMPLEMENTER role requires non-empty write_scope_json paths, for example {\"paths\":[\"web/**\"]}", IsError: true}
	}

	coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_role_assign failed reading coordination for %s: %v", projectID, err), IsError: true}
	}
	if coordination.StrategicLead == nil || strings.TrimSpace(coordination.StrategicLead.AgentID) != t.agentID {
		if role, ok := projectRoleAssignRequestedRoleSatisfied(coordination, targetAgentID, roleType, writeScopeJSON); ok {
			if projectRoleAssignBoundaryTransitionMetaPresent(transitionMeta) {
				if applied, ok := t.projectRoleAssignAlreadySatisfiedBoundaryReceipt(ctx, coordination, projectID, targetAgentID, roleType, writeScopeJSON, role, transitionMeta); ok {
					return applied
				}
				if projectRoleAssignBoundaryTransitionRequiresLiveBinding(transitionMeta) {
					return t.queueProjectRoleAssignLeadRequest(ctx, coordination, projectID, targetAgentID, roleType, writeScopeJSON, summary, transitionMeta)
				}
			}
			return projectRoleAssignAlreadySatisfiedResult(projectID, targetAgentID, roleType, writeScopeJSON, role)
		}
		return t.queueProjectRoleAssignLeadRequest(ctx, coordination, projectID, targetAgentID, roleType, writeScopeJSON, summary, transitionMeta)
	}
	if projectRoleAssignBoundaryTransitionMetaPresent(transitionMeta) {
		if applied, ok := t.projectRoleAssignExistingAppliedBoundaryReceipt(ctx, coordination, "", projectID, targetAgentID, roleType, writeScopeJSON, transitionMeta); ok {
			return applied
		}
	}
	if blocked, ok := t.projectRoleAssignReadyBranchRebindBlockedResult(ctx, coordination, projectID, targetAgentID, roleType, writeScopeJSON, transitionMeta); ok {
		return blocked
	}

	assignResult, err := t.client.AssignProjectRoleWithResult(ctx, ProjectRoleAssignInput{
		WorkspaceID:    t.workspaceID,
		ProjectID:      projectID,
		ActorID:        t.agentID,
		AgentID:        targetAgentID,
		RoleType:       roleType,
		WriteScopeJSON: writeScopeJSON,
		Summary:        summary,
	})
	if err != nil {
		if projectRoleAssignLooksLikeOverlapConflict(err) {
			return t.projectRoleAssignOverlapDeniedResult(ctx, projectID, targetAgentID, roleType, writeScopeJSON, transitionMeta, suppressDenialUpdate, err)
		}
		return &ToolResult{Output: fmt.Sprintf("project_role_assign failed assigning %s to %s on %s: %v", roleType, targetAgentID, projectID, err), IsError: true}
	}
	role := assignResult.Role
	payload := map[string]any{
		"project_id":       projectID,
		"role_id":          role.RoleID,
		"agent_id":         role.AgentID,
		"role_type":        role.RoleType,
		"write_scope_json": role.WriteScopeJSON,
		"status":           role.Status,
		"actor_id":         t.agentID,
		"guidance":         "Role assignment is durable. The target agent can claim matching project work once phase and repository gates are open.",
	}
	if assignResult.ActiveClaimRebind != nil {
		payload["active_claim_rebind"] = assignResult.ActiveClaimRebind
		if rebindState := strings.ToLower(strings.TrimSpace(stringMapValue(assignResult.ActiveClaimRebind, "state"))); rebindState != "" && rebindState != "updated" {
			payload["guidance"] = "Role assignment persisted, but active_claim_rebind did not update the live branch/claim. Treat the repair as unresolved until branch registry and task claim scope are verified or the stale lane is released."
			raw, _ := json.MarshalIndent(payload, "", "  ")
			return &ToolResult{Output: string(raw), IsError: true}
		}
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_role_assign completed role %s for %s", role.RoleID, targetAgentID)}
	}
	return &ToolResult{Output: string(raw)}
}

type projectRoleAssignBoundaryTransitionMeta struct {
	BoundaryTransitionKey string
	SideEffectRefs        []string
	ActiveTaskID          string
	BranchID              string
	BranchName            string
}

func projectRoleAssignBoundaryTransitionMetaFromArgs(args map[string]any) projectRoleAssignBoundaryTransitionMeta {
	return projectRoleAssignBoundaryTransitionMeta{
		BoundaryTransitionKey: strings.TrimSpace(stringArg(args, "boundary_transition_key")),
		SideEffectRefs:        uniqueNonEmptyStrings(firstNonEmptyStringSlice(stringSliceArg(args, "side_effect_refs"), stringSliceArg(args, "side_effect_ref"))),
		ActiveTaskID:          strings.TrimSpace(stringArg(args, "active_task_id")),
		BranchID:              strings.TrimSpace(stringArg(args, "branch_id")),
		BranchName:            strings.TrimSpace(stringArg(args, "branch_name")),
	}
}

func projectRoleAssignBoundaryTransitionMetaPresent(meta projectRoleAssignBoundaryTransitionMeta) bool {
	return strings.TrimSpace(meta.BoundaryTransitionKey) != "" ||
		len(meta.SideEffectRefs) > 0 ||
		strings.TrimSpace(meta.ActiveTaskID) != "" ||
		strings.TrimSpace(meta.BranchID) != ""
}

func projectRoleAssignBoundaryTransitionRequiresLiveBinding(meta projectRoleAssignBoundaryTransitionMeta) bool {
	return strings.TrimSpace(meta.ActiveTaskID) != "" || strings.TrimSpace(meta.BranchID) != ""
}

func (t *ProjectRoleAssignTool) queueProjectRoleAssignLeadRequest(ctx context.Context, coordination ProjectCoordinationRecord, projectID, targetAgentID, roleType, writeScopeJSON, summary string, transitionMeta projectRoleAssignBoundaryTransitionMeta) *ToolResult {
	leadAgentID := projectCoordinationStrategicLeadAgentID(coordination)
	if leadAgentID == "" {
		return &ToolResult{Output: fmt.Sprintf("project_role_assign requires an active strategic lead lease for %s. Do not retry project_role_assign from a non-lead context; record one blocker/tension and wait for a strategic lead to claim or renew the project lead lease.", projectID), IsError: true}
	}
	if leadAgentID == t.agentID {
		return &ToolResult{Output: fmt.Sprintf("project_role_assign requires this agent to hold the active strategic lead lease for %s, but coordination did not expose a matching active lease; refresh project coordination before retrying.", projectID), IsError: true}
	}

	fingerprint := strings.Join([]string{projectID, targetAgentID, roleType, projectRoleAssignNormalizedWriteScopeKey(writeScopeJSON)}, "\x00")
	if strings.TrimSpace(transitionMeta.BoundaryTransitionKey) != "" {
		fingerprint = strings.Join([]string{projectID, targetAgentID, roleType, strings.TrimSpace(transitionMeta.BoundaryTransitionKey)}, "\x00")
	}
	taskID := "task-role-scope-" + shortRefHash(fingerprint)
	title := "Resolve project role/scope request for " + targetAgentID
	description := renderProjectRoleAssignLeadRequestDescription(projectID, t.agentID, targetAgentID, roleType, writeScopeJSON, summary, transitionMeta)
	if projectRoleAssignBoundaryTransitionMetaPresent(transitionMeta) {
		if applied, ok := t.projectRoleAssignExistingAppliedBoundaryReceipt(ctx, coordination, taskID, projectID, targetAgentID, roleType, writeScopeJSON, transitionMeta); ok {
			return applied
		}
		if existingTask, state, ok := t.projectRoleAssignExistingBoundaryTransitionTask(ctx, taskID, projectID, targetAgentID, roleType, transitionMeta); ok {
			if state == projectRoleAssignBoundaryTaskStateTerminalBlocker {
				return projectRoleAssignAuthorityTransitionTerminalBlockerResult(existingTask, projectID, leadAgentID, t.agentID, targetAgentID, roleType, writeScopeJSON, transitionMeta)
			}
			return projectRoleAssignLeadRequestQueuedResult(projectRoleAssignLeadRequestQueued{
				ProjectID:                projectID,
				LeadAgentID:              leadAgentID,
				RequesterID:              t.agentID,
				TargetAgentID:            targetAgentID,
				RoleType:                 roleType,
				WriteScopeJSON:           writeScopeJSON,
				TaskID:                   strings.TrimSpace(existingTask.TaskID),
				TaskCreated:              false,
				TransitionAlreadyPending: true,
				BoundaryTransitionKey:    transitionMeta.BoundaryTransitionKey,
				Warning:                  "matching role/scope authority transition already exists; lead wake request was not re-sent to avoid duplicate repair nudges",
			})
		}
	}
	requiresGate := false
	taskCreated := true
	taskResult, submitErr := t.client.SubmitTask(ctx, TaskSubmitInput{
		WorkspaceID:         t.workspaceID,
		TaskID:              taskID,
		OwnerUserID:         t.agentID,
		Priority:            "high",
		Title:               title,
		Description:         description,
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "coordination",
		RequiresProjectGate: &requiresGate,
		TaskRequirements:    projectRoleAssignLeadTaskRequirements(projectID, t.agentID, targetAgentID, roleType, writeScopeJSON, summary, transitionMeta),
		Tags:                []string{"project-role-scope", "strategic-lead", "coordination", "blocker-unblock"},
		LinkedBy:            t.agentID,
	})
	if submitErr != nil {
		if !isDuplicateTaskSubmitError(submitErr) {
			return &ToolResult{Output: fmt.Sprintf("project_role_assign cannot be run by non-lead agent %s and failed to create lead coordination task for %s: %v. Do not retry project_role_assign blindly; record one blocker/tension for strategic lead %s.", t.agentID, projectID, submitErr, leadAgentID), IsError: true}
		}
		if applied, ok := t.projectRoleAssignExistingAppliedBoundaryReceipt(ctx, coordination, taskID, projectID, targetAgentID, roleType, writeScopeJSON, transitionMeta); ok {
			return applied
		}
		if existingTask, state, ok := t.projectRoleAssignExistingBoundaryTransitionTask(ctx, taskID, projectID, targetAgentID, roleType, transitionMeta); ok {
			if state == projectRoleAssignBoundaryTaskStateTerminalBlocker {
				return projectRoleAssignAuthorityTransitionTerminalBlockerResult(existingTask, projectID, leadAgentID, t.agentID, targetAgentID, roleType, writeScopeJSON, transitionMeta)
			}
			return projectRoleAssignLeadRequestQueuedResult(projectRoleAssignLeadRequestQueued{
				ProjectID:                projectID,
				LeadAgentID:              leadAgentID,
				RequesterID:              t.agentID,
				TargetAgentID:            targetAgentID,
				RoleType:                 roleType,
				WriteScopeJSON:           writeScopeJSON,
				TaskID:                   strings.TrimSpace(existingTask.TaskID),
				TaskCreated:              false,
				TransitionAlreadyPending: true,
				BoundaryTransitionKey:    transitionMeta.BoundaryTransitionKey,
				Warning:                  "role/scope authority transition already exists; lead wake request was not re-sent to avoid duplicate repair nudges",
			})
		}
		return projectRoleAssignLeadRequestQueuedResult(projectRoleAssignLeadRequestQueued{
			ProjectID:                projectID,
			LeadAgentID:              leadAgentID,
			RequesterID:              t.agentID,
			TargetAgentID:            targetAgentID,
			RoleType:                 roleType,
			WriteScopeJSON:           writeScopeJSON,
			TaskID:                   taskID,
			TaskCreated:              false,
			TransitionAlreadyPending: true,
			BoundaryTransitionKey:    transitionMeta.BoundaryTransitionKey,
			Warning:                  "role/scope authority transition already exists; lead wake request was not re-sent to avoid duplicate repair nudges",
		})
	} else if strings.TrimSpace(taskResult.TaskID) != "" {
		taskID = strings.TrimSpace(taskResult.TaskID)
	}

	requestID := ""
	requestStatus := ""
	requestPayload := map[string]any{
		"prompt":       fmt.Sprintf("A non-lead agent (%s) needs durable project role/scope correction on project %s. Lead coordination task %s records the request. Queue this as an authority transition in your normal work loop; if the requested role/scope is valid, execute project_role_assign so project_agent_roles, task_claims.write_scope_json, and project_branch_registry.write_scope_json are durably rebound. A chat-only answer is not a transition receipt.\n\nRequested target agent: %s\nRequested role_type: %s\nRequested write_scope_json: %s\nRequested summary: %s", t.agentID, projectID, taskID, targetAgentID, roleType, firstNonEmpty(writeScopeJSON, "{}"), firstNonEmpty(summary, "role/scope alignment requested by blocked peer")),
		"request_kind": "authority_transition",
		"task_id":      taskID,
		"lead_task_id": taskID,
	}
	if strings.TrimSpace(transitionMeta.BoundaryTransitionKey) != "" {
		requestPayload["boundary_transition_key"] = strings.TrimSpace(transitionMeta.BoundaryTransitionKey)
	}
	rawPayload, marshalErr := json.Marshal(requestPayload)
	if marshalErr == nil {
		created, requestErr := t.client.RequestAgent(ctx, AgentRequestInput{
			WorkspaceID: t.workspaceID,
			FromAgentID: t.agentID,
			ToAgentID:   leadAgentID,
			Method:      "model.ask",
			PayloadJSON: string(rawPayload),
			TimeoutSec:  60,
		})
		if requestErr != nil {
			return projectRoleAssignLeadRequestQueuedResult(projectRoleAssignLeadRequestQueued{
				ProjectID:      projectID,
				LeadAgentID:    leadAgentID,
				RequesterID:    t.agentID,
				TargetAgentID:  targetAgentID,
				RoleType:       roleType,
				WriteScopeJSON: writeScopeJSON,
				TaskID:         taskID,
				TaskCreated:    taskCreated,
				Warning:        "coordination task exists, but direct lead wake request failed: " + requestErr.Error(),
			})
		}
		requestID = created.RequestID
		requestStatus = created.Status
	}

	return projectRoleAssignLeadRequestQueuedResult(projectRoleAssignLeadRequestQueued{
		ProjectID:             projectID,
		LeadAgentID:           leadAgentID,
		RequesterID:           t.agentID,
		TargetAgentID:         targetAgentID,
		RoleType:              roleType,
		WriteScopeJSON:        writeScopeJSON,
		TaskID:                taskID,
		TaskCreated:           taskCreated,
		RequestID:             requestID,
		RequestStatus:         requestStatus,
		DelegatedToLead:       false,
		BoundaryTransitionKey: transitionMeta.BoundaryTransitionKey,
	})
}

type projectRoleAssignLeadRequestQueued struct {
	ProjectID                string
	LeadAgentID              string
	RequesterID              string
	TargetAgentID            string
	RoleType                 string
	WriteScopeJSON           string
	TaskID                   string
	TaskCreated              bool
	RequestID                string
	RequestStatus            string
	DelegatedToLead          bool
	Warning                  string
	TransitionAlreadyPending bool
	BoundaryTransitionKey    string
}

func projectRoleAssignLeadRequestQueuedResult(result projectRoleAssignLeadRequestQueued) *ToolResult {
	payload := map[string]any{
		"project_id":         result.ProjectID,
		"lead_agent_id":      result.LeadAgentID,
		"requester_id":       result.RequesterID,
		"target_agent_id":    result.TargetAgentID,
		"role_type":          result.RoleType,
		"write_scope_json":   result.WriteScopeJSON,
		"lead_task_id":       result.TaskID,
		"task_created":       result.TaskCreated,
		"lead_request_id":    result.RequestID,
		"lead_request_state": result.RequestStatus,
		"delegated_to_lead":  result.DelegatedToLead,
		"lead_notified":      strings.TrimSpace(result.RequestID) != "",
		"do_not_retry":       true,
		"next_action":        "Stop retrying project_role_assign from this non-lead context. Record one blocker/tension if needed; strategic lead coordination should resolve the role/scope request durably.",
	}
	if strings.TrimSpace(result.BoundaryTransitionKey) != "" {
		payload["boundary_transition_key"] = strings.TrimSpace(result.BoundaryTransitionKey)
	}
	if result.TransitionAlreadyPending {
		payload["transition_already_pending"] = true
		payload["existing_task_id"] = result.TaskID
		payload["lead_task_id"] = result.TaskID
		payload["next_transition"] = "wait_or_assist_existing_authority_transition"
		payload["next_action"] = "Reuse the existing role/scope authority transition instead of creating or waking a parallel task."
	}
	if strings.TrimSpace(result.Warning) != "" {
		payload["warning"] = result.Warning
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_role_assign request queued for strategic lead %s on task %s; do not retry from non-lead context", result.LeadAgentID, result.TaskID)}
	}
	return &ToolResult{Output: string(raw)}
}

func projectRoleAssignAuthorityTransitionTerminalBlockerResult(task WorkspaceTaskRecord, projectID, leadAgentID, requesterID, targetAgentID, roleType, writeScopeJSON string, meta projectRoleAssignBoundaryTransitionMeta) *ToolResult {
	payload := map[string]any{
		"project_id":                  strings.TrimSpace(projectID),
		"lead_agent_id":               strings.TrimSpace(leadAgentID),
		"requester_id":                strings.TrimSpace(requesterID),
		"target_agent_id":             strings.TrimSpace(targetAgentID),
		"role_type":                   strings.TrimSpace(roleType),
		"write_scope_json":            strings.TrimSpace(writeScopeJSON),
		"lead_task_id":                strings.TrimSpace(task.TaskID),
		"existing_task_id":            strings.TrimSpace(task.TaskID),
		"task_status":                 strings.TrimSpace(task.Status),
		"claim_agent_id":              strings.TrimSpace(pointerValue(task.ClaimAgentID)),
		"claim_status":                firstNonEmpty(taskClaimStatus(task), "<empty>"),
		"transition_terminal_blocker": true,
		"lead_notified":               false,
		"do_not_retry":                true,
		"next_transition":             "inspect_existing_authority_terminal_blocker_or_create_corrected_successor",
		"next_action":                 "Inspect the existing authority transition terminal blocker/evidence. If the transition is still required, create a corrected successor/follow-up authority task instead of reusing or waking this blocked carrier.",
		"warning":                     "matching role/scope authority transition has a blocked claim; it is not a live pending transition",
	}
	if summary := strings.TrimSpace(pointerValue(task.ClaimSummary)); summary != "" {
		payload["claim_summary"] = summary
	}
	if strings.TrimSpace(meta.BoundaryTransitionKey) != "" {
		payload["boundary_transition_key"] = strings.TrimSpace(meta.BoundaryTransitionKey)
	}
	if len(meta.SideEffectRefs) > 0 {
		payload["side_effect_refs"] = append([]string(nil), meta.SideEffectRefs...)
	}
	if activeTaskID := strings.TrimSpace(meta.ActiveTaskID); activeTaskID != "" {
		payload["active_task_id"] = activeTaskID
	}
	if branchID := strings.TrimSpace(meta.BranchID); branchID != "" {
		payload["branch_id"] = branchID
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_role_assign found blocked authority transition task %s; inspect its terminal blocker before retrying", strings.TrimSpace(task.TaskID))}
	}
	return &ToolResult{Output: string(raw)}
}

func projectRoleAssignLeadTaskRequirements(projectID, requesterID, targetAgentID, roleType, writeScopeJSON, summary string, meta projectRoleAssignBoundaryTransitionMeta) map[string]any {
	req := map[string]any{
		"schema":             "project_role_scope_authority_transition.v1",
		"project_id":         strings.TrimSpace(projectID),
		"requester_agent_id": strings.TrimSpace(requesterID),
		"target_agent_id":    strings.TrimSpace(targetAgentID),
		"role_type":          strings.TrimSpace(roleType),
		"write_scope_json":   strings.TrimSpace(writeScopeJSON),
		"request_summary":    strings.TrimSpace(summary),
		"allowed_denial_states": []string{
			"boundary_expansion_denied_overlap",
			"role_scope_conflict",
			"transition_pending_authorized_mutation",
		},
	}
	if strings.TrimSpace(meta.BoundaryTransitionKey) != "" {
		req["boundary_transition_key"] = strings.TrimSpace(meta.BoundaryTransitionKey)
		req["dedup_key"] = strings.TrimSpace(meta.BoundaryTransitionKey)
	}
	if len(meta.SideEffectRefs) > 0 {
		req["side_effect_refs"] = meta.SideEffectRefs
	}
	if strings.TrimSpace(meta.ActiveTaskID) != "" {
		req["active_task_id"] = strings.TrimSpace(meta.ActiveTaskID)
	}
	if strings.TrimSpace(meta.BranchID) != "" {
		req["branch_id"] = strings.TrimSpace(meta.BranchID)
	}
	if strings.TrimSpace(meta.BranchName) != "" {
		req["branch_name"] = strings.TrimSpace(meta.BranchName)
	}
	return req
}

func projectRoleAssignLooksLikeOverlapConflict(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "write_scope_json overlaps active claim") ||
		strings.Contains(text, "write_scope_json overlaps live branch") ||
		strings.Contains(text, "project role write scope conflict")
}

func (t *ProjectRoleAssignTool) projectRoleAssignOverlapDeniedResult(ctx context.Context, projectID, targetAgentID, roleType, writeScopeJSON string, meta projectRoleAssignBoundaryTransitionMeta, suppressUpdate bool, err error) *ToolResult {
	payload := projectRoleAssignOverlapDeniedPayload(projectID, targetAgentID, roleType, writeScopeJSON, meta, err)
	if !suppressUpdate {
		payload["denial_recorded"] = true
		if updateErr := t.postProjectRoleAssignOverlapDenial(ctx, payload); updateErr != nil {
			payload["denial_recorded"] = false
			payload["denial_record_error"] = updateErr.Error()
		}
	}
	raw, marshalErr := json.MarshalIndent(payload, "", "  ")
	if marshalErr != nil {
		return &ToolResult{Output: "project_role_assign denied boundary expansion because the requested write_scope_json overlaps a live owner lane", IsError: true}
	}
	return &ToolResult{Output: string(raw), IsError: true}
}

func projectRoleAssignOverlapDeniedPayload(projectID, targetAgentID, roleType, writeScopeJSON string, meta projectRoleAssignBoundaryTransitionMeta, err error) map[string]any {
	payload := map[string]any{
		"schema":                    "project_role_scope_authority_denial.v1",
		"project_id":                strings.TrimSpace(projectID),
		"agent_id":                  strings.TrimSpace(targetAgentID),
		"role_type":                 strings.TrimSpace(roleType),
		"write_scope_json":          strings.TrimSpace(writeScopeJSON),
		"boundary_transition_state": "boundary_expansion_denied_overlap",
		"transition_executed":       false,
		"transition_denied":         true,
		"denial_reason":             "overlaps_live_owner_lane",
		"error":                     err.Error(),
		"allowed_next":              []string{"wait_for_conflicting_lane_publication", "request_split", "request_merge_or_adopt", "reassign_downstream_after_owner", "ask_conflicting_owner_release_scope"},
		"preferred_transition":      "wait_or_split_existing_owner_lane",
		"guidance":                  "Do not retry the same boundary expansion. Reuse the existing authority transition and sequence, split, merge/adopt, or narrow ownership around the live overlapping lane.",
	}
	if strings.TrimSpace(meta.BoundaryTransitionKey) != "" {
		payload["boundary_transition_key"] = strings.TrimSpace(meta.BoundaryTransitionKey)
	}
	if len(meta.SideEffectRefs) > 0 {
		payload["side_effect_refs"] = append([]string(nil), meta.SideEffectRefs...)
	}
	if strings.TrimSpace(meta.ActiveTaskID) != "" {
		payload["active_task_id"] = strings.TrimSpace(meta.ActiveTaskID)
	}
	if strings.TrimSpace(meta.BranchID) != "" {
		payload["branch_id"] = strings.TrimSpace(meta.BranchID)
	}
	if strings.TrimSpace(meta.BranchName) != "" {
		payload["branch_name"] = strings.TrimSpace(meta.BranchName)
	}
	if taskID := projectRoleAssignExtractTokenAfter(err.Error(), "task_id="); taskID != "" {
		payload["conflicting_task_id"] = taskID
	}
	if agentID := projectRoleAssignExtractTokenAfter(err.Error(), "agent_id="); agentID != "" {
		payload["conflicting_owner_agent_id"] = agentID
	}
	if branchID := projectRoleAssignExtractTokenAfter(err.Error(), "branch_id="); branchID != "" {
		payload["conflicting_branch_id"] = branchID
	}
	if activeTaskID := projectRoleAssignExtractTokenAfter(err.Error(), "active_task_id="); activeTaskID != "" {
		payload["conflicting_active_task_id"] = activeTaskID
	}
	return payload
}

func (t *ProjectRoleAssignTool) projectRoleAssignReadyBranchRebindBlockedResult(ctx context.Context, coordination ProjectCoordinationRecord, projectID, targetAgentID, roleType, writeScopeJSON string, meta projectRoleAssignBoundaryTransitionMeta) (*ToolResult, bool) {
	branch, ok := projectRoleAssignReadyBranchNeedsExplicitContinuation(coordination, projectID, targetAgentID, writeScopeJSON, meta)
	if !ok || roleType != "IMPLEMENTER" {
		return nil, false
	}
	payload := map[string]any{
		"schema":                    "project_role_scope_authority_denial.v1",
		"project_id":                strings.TrimSpace(projectID),
		"agent_id":                  strings.TrimSpace(targetAgentID),
		"role_type":                 strings.TrimSpace(roleType),
		"write_scope_json":          strings.TrimSpace(writeScopeJSON),
		"boundary_transition_state": "ready_for_review_branch_rebind_blocked",
		"transition_executed":       false,
		"transition_denied":         true,
		"denial_reason":             "ready_for_review_branch_requires_revision_or_patch_queue_followup",
		"branch_id":                 strings.TrimSpace(branch.BranchID),
		"branch_name":               strings.TrimSpace(branch.BranchName),
		"branch_status":             strings.TrimSpace(branch.Status),
		"branch_write_scope_json":   strings.TrimSpace(branch.WriteScopeJSON),
		"active_task_id":            strings.TrimSpace(firstNonEmpty(meta.ActiveTaskID, branch.ActiveTaskID)),
		"allowed_next":              []string{"project_patch_queue_followup", "create_revision_lane", "release_ready_branch", "wait_for_patch_queue_decision"},
		"preferred_transition":      "project_patch_queue_followup_or_revision_lane",
		"guidance":                  "Do not widen a READY_FOR_REVIEW branch-bound lane in place. Route through patch-queue follow-up, revision, release, or a fresh owner lane with explicit lineage.",
		"denial_recorded":           true,
	}
	if strings.TrimSpace(meta.BoundaryTransitionKey) != "" {
		payload["boundary_transition_key"] = strings.TrimSpace(meta.BoundaryTransitionKey)
	}
	if len(meta.SideEffectRefs) > 0 {
		payload["side_effect_refs"] = append([]string(nil), meta.SideEffectRefs...)
	}
	rawPayload, _ := json.Marshal(payload)
	if err := t.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID:   t.workspaceID,
		AgentID:       t.agentID,
		UpdateType:    "boundary_transition_denial",
		Summary:       fmt.Sprintf("project_role_assign denied ready-for-review branch rebind for %s on %s", targetAgentID, projectID),
		PayloadJSON:   string(rawPayload),
		RequiresHuman: false,
	}); err != nil {
		payload["denial_recorded"] = false
		payload["denial_record_error"] = err.Error()
	}
	raw, marshalErr := json.MarshalIndent(payload, "", "  ")
	if marshalErr != nil {
		return &ToolResult{Output: "project_role_assign denied ready-for-review branch rebind", IsError: true}, true
	}
	return &ToolResult{Output: string(raw), IsError: true}, true
}

func projectRoleAssignReadyBranchNeedsExplicitContinuation(coordination ProjectCoordinationRecord, projectID, targetAgentID, requestedScopeJSON string, meta projectRoleAssignBoundaryTransitionMeta) (ProjectBranchRecord, bool) {
	requestedPaths := projectRoleAssignNormalizedWriteScopePaths(requestedScopeJSON)
	if len(requestedPaths) == 0 {
		return ProjectBranchRecord{}, false
	}
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.BranchID) == "" {
			continue
		}
		if strings.TrimSpace(projectID) != "" && strings.TrimSpace(branch.ProjectID) != "" && strings.TrimSpace(branch.ProjectID) != strings.TrimSpace(projectID) {
			continue
		}
		if strings.TrimSpace(targetAgentID) != "" && strings.TrimSpace(branch.AgentID) != strings.TrimSpace(targetAgentID) {
			continue
		}
		if strings.TrimSpace(meta.BranchID) != "" && strings.TrimSpace(branch.BranchID) != strings.TrimSpace(meta.BranchID) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(branch.Status), "READY_FOR_REVIEW") {
			continue
		}
		if strings.TrimSpace(meta.ActiveTaskID) != "" && strings.TrimSpace(branch.ActiveTaskID) != "" && strings.TrimSpace(branch.ActiveTaskID) != strings.TrimSpace(meta.ActiveTaskID) {
			continue
		}
		if projectRoleAssignScopeCovers(projectRoleAssignNormalizedWriteScopePaths(branch.WriteScopeJSON), requestedPaths) {
			activeTaskID := strings.TrimSpace(firstNonEmpty(meta.ActiveTaskID, branch.ActiveTaskID))
			if activeTaskID != "" {
				if task, ok := projectRoleAssignFindWorkspaceTask(coordination.Tasks, activeTaskID); ok {
					if !projectRoleAssignClaimScopeCovers(task, targetAgentID, branch.BranchID, requestedPaths) {
						return branch, true
					}
				}
			}
			continue
		}
		return branch, true
	}
	return ProjectBranchRecord{}, false
}

func (t *ProjectRoleAssignTool) postProjectRoleAssignOverlapDenial(ctx context.Context, payload map[string]any) error {
	if t == nil || t.client == nil {
		return fmt.Errorf("missing client")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode role/scope denial payload: %w", err)
	}
	return t.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID:   t.workspaceID,
		AgentID:       t.agentID,
		UpdateType:    "boundary_transition_denial",
		Summary:       fmt.Sprintf("project_role_assign denied overlap for %s on %s", stringMapValue(payload, "agent_id"), stringMapValue(payload, "project_id")),
		PayloadJSON:   string(raw),
		RequiresHuman: false,
	})
}

func projectRoleAssignExtractTokenAfter(text, marker string) string {
	idx := strings.Index(text, marker)
	if idx < 0 {
		return ""
	}
	value := strings.TrimSpace(text[idx+len(marker):])
	if value == "" {
		return ""
	}
	end := len(value)
	for i, r := range value {
		if r == ';' || r == ',' || r == ')' || r == '(' || r == '\n' || r == '\r' || r == '\t' || r == ' ' {
			end = i
			break
		}
	}
	return strings.Trim(value[:end], "`'\"")
}

func projectRoleAssignRequestedRoleSatisfied(coordination ProjectCoordinationRecord, targetAgentID, roleType, requestedScopeJSON string) (ProjectRoleRecord, bool) {
	targetAgentID = strings.TrimSpace(targetAgentID)
	roleType = strings.ToUpper(strings.TrimSpace(roleType))
	requestedPaths := projectRoleAssignNormalizedWriteScopePaths(requestedScopeJSON)
	for _, role := range coordination.Roles {
		if strings.TrimSpace(role.AgentID) != targetAgentID {
			continue
		}
		if strings.ToUpper(strings.TrimSpace(role.RoleType)) != roleType {
			continue
		}
		if strings.ToUpper(strings.TrimSpace(role.Status)) != "ACTIVE" {
			continue
		}
		if roleType == "IMPLEMENTER" {
			if len(requestedPaths) == 0 {
				continue
			}
			if projectRoleAssignScopeCovers(projectRoleAssignNormalizedWriteScopePaths(role.WriteScopeJSON), requestedPaths) {
				return role, true
			}
			continue
		}
		if len(requestedPaths) == 0 || projectRoleAssignScopeCovers(projectRoleAssignNormalizedWriteScopePaths(role.WriteScopeJSON), requestedPaths) {
			return role, true
		}
	}
	return ProjectRoleRecord{}, false
}

func projectRoleAssignAlreadySatisfiedResult(projectID, targetAgentID, roleType, requestedScopeJSON string, role ProjectRoleRecord) *ToolResult {
	payload := map[string]any{
		"project_id":                strings.TrimSpace(projectID),
		"agent_id":                  strings.TrimSpace(targetAgentID),
		"role_id":                   strings.TrimSpace(role.RoleID),
		"role_type":                 firstNonEmpty(strings.TrimSpace(role.RoleType), strings.TrimSpace(roleType)),
		"write_scope_json":          strings.TrimSpace(role.WriteScopeJSON),
		"requested_write_scope":     strings.TrimSpace(requestedScopeJSON),
		"already_satisfied":         true,
		"do_not_retry":              true,
		"canonical_mutation_needed": false,
		"guidance":                  "An active project role already covers the requested role/scope. Refresh project coordination and continue the owned task instead of queueing another strategic-lead correction.",
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_role_assign already satisfied by active role %s for %s", role.RoleID, targetAgentID)}
	}
	return &ToolResult{Output: string(raw)}
}

func (t *ProjectRoleAssignTool) projectRoleAssignAlreadySatisfiedBoundaryReceipt(ctx context.Context, coordination ProjectCoordinationRecord, projectID, targetAgentID, roleType, writeScopeJSON string, role ProjectRoleRecord, meta projectRoleAssignBoundaryTransitionMeta) (*ToolResult, bool) {
	if t == nil || t.client == nil || !projectRoleAssignBoundaryTransitionRequiresLiveBinding(meta) {
		return nil, false
	}
	requestedPaths := projectRoleAssignNormalizedWriteScopePaths(writeScopeJSON)
	if len(requestedPaths) == 0 {
		return nil, false
	}
	if !projectRoleAssignBranchScopeCovers(coordination, meta.BranchID, targetAgentID, meta.ActiveTaskID, requestedPaths) {
		return nil, false
	}
	tasks, err := t.client.ListTasks(ctx, t.workspaceID)
	if err != nil {
		return nil, false
	}
	claimTask, ok := projectRoleAssignFindWorkspaceTask(tasks, meta.ActiveTaskID)
	if !ok || !projectRoleAssignLiveClaimScopeCovers(claimTask, targetAgentID, meta.BranchID, requestedPaths) {
		return nil, false
	}
	branchScope := projectRoleAssignBranchWriteScopeJSON(coordination, meta.BranchID)
	claimScope := strings.TrimSpace(pointerValue(claimTask.ClaimWriteScopeJSON))
	payload := map[string]any{
		"project_id":                strings.TrimSpace(projectID),
		"agent_id":                  strings.TrimSpace(targetAgentID),
		"role_id":                   strings.TrimSpace(role.RoleID),
		"role_type":                 firstNonEmpty(strings.TrimSpace(role.RoleType), strings.TrimSpace(roleType)),
		"write_scope_json":          firstNonEmpty(branchScope, claimScope, strings.TrimSpace(role.WriteScopeJSON), strings.TrimSpace(writeScopeJSON)),
		"requested_write_scope":     strings.TrimSpace(writeScopeJSON),
		"already_satisfied":         true,
		"canonical_mutation_needed": false,
		"boundary_transition_state": "role_scope_already_satisfied",
		"active_task_id":            strings.TrimSpace(meta.ActiveTaskID),
		"branch_id":                 strings.TrimSpace(meta.BranchID),
		"branch_name":               strings.TrimSpace(meta.BranchName),
		"side_effect_refs":          append([]string(nil), meta.SideEffectRefs...),
		"active_claim_rebind": map[string]any{
			"state":                   "already_applied",
			"task_id":                 strings.TrimSpace(meta.ActiveTaskID),
			"branch_id":               strings.TrimSpace(meta.BranchID),
			"claim_write_scope_json":  claimScope,
			"branch_write_scope_json": branchScope,
		},
		"guidance": "An active role, branch boundary, and task claim boundary already cover the requested side-effect bucket. Treat this as a no-op durable boundary receipt and continue the owner lane.",
	}
	if key := strings.TrimSpace(meta.BoundaryTransitionKey); key != "" {
		payload["boundary_transition_key"] = key
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: "project_role_assign boundary role/scope already satisfied with live branch and claim binding"}, true
	}
	return &ToolResult{Output: string(raw)}, true
}

func (t *ProjectRoleAssignTool) projectRoleAssignExistingAppliedBoundaryReceipt(ctx context.Context, coordination ProjectCoordinationRecord, taskID, projectID, targetAgentID, roleType, writeScopeJSON string, meta projectRoleAssignBoundaryTransitionMeta) (*ToolResult, bool) {
	if t == nil || t.client == nil || strings.TrimSpace(t.workspaceID) == "" {
		return nil, false
	}
	taskID = strings.TrimSpace(taskID)
	projectID = strings.TrimSpace(projectID)
	targetAgentID = strings.TrimSpace(targetAgentID)
	roleType = strings.TrimSpace(roleType)
	if projectID == "" || targetAgentID == "" {
		return nil, false
	}
	if strings.TrimSpace(meta.BranchID) == "" || strings.TrimSpace(meta.ActiveTaskID) == "" {
		return nil, false
	}
	requestedPaths := projectRoleAssignNormalizedWriteScopePaths(writeScopeJSON)
	if len(requestedPaths) == 0 {
		return nil, false
	}
	tasks, err := t.client.ListTasks(ctx, t.workspaceID)
	if err != nil {
		return nil, false
	}
	authorityTask, ok := projectRoleAssignFindAppliedBoundaryAuthorityTask(tasks, taskID, meta, projectID, targetAgentID, roleType)
	if !ok {
		return nil, false
	}
	if !projectRoleAssignBranchScopeCovers(coordination, meta.BranchID, targetAgentID, meta.ActiveTaskID, requestedPaths) {
		return nil, false
	}
	claimTask, ok := projectRoleAssignFindWorkspaceTask(tasks, meta.ActiveTaskID)
	if !ok || !projectRoleAssignClaimScopeCovers(claimTask, targetAgentID, meta.BranchID, requestedPaths) {
		return nil, false
	}
	branchScope := projectRoleAssignBranchWriteScopeJSON(coordination, meta.BranchID)
	claimScope := strings.TrimSpace(pointerValue(claimTask.ClaimWriteScopeJSON))
	payload := map[string]any{
		"project_id":                   projectID,
		"agent_id":                     targetAgentID,
		"role_type":                    roleType,
		"write_scope_json":             firstNonEmpty(branchScope, claimScope, strings.TrimSpace(writeScopeJSON)),
		"requested_write_scope":        strings.TrimSpace(writeScopeJSON),
		"authority_transition_applied": true,
		"transition_executed":          true,
		"already_satisfied":            true,
		"canonical_mutation_needed":    false,
		"boundary_transition_state":    "authority_transition_applied",
		"existing_task_id":             strings.TrimSpace(authorityTask.TaskID),
		"lead_task_id":                 strings.TrimSpace(authorityTask.TaskID),
		"active_task_id":               strings.TrimSpace(meta.ActiveTaskID),
		"branch_id":                    strings.TrimSpace(meta.BranchID),
		"branch_name":                  strings.TrimSpace(meta.BranchName),
		"side_effect_refs":             append([]string(nil), meta.SideEffectRefs...),
		"active_claim_rebind": map[string]any{
			"state":                   "already_applied",
			"task_id":                 strings.TrimSpace(meta.ActiveTaskID),
			"branch_id":               strings.TrimSpace(meta.BranchID),
			"claim_write_scope_json":  claimScope,
			"branch_write_scope_json": branchScope,
		},
		"guidance": "An existing role/scope authority transition is already applied: the terminal authority task, branch boundary, and task claim boundary cover the requested side-effect bucket. Treat this as the side-effect decision receipt and continue the owner lane instead of creating a generic record-decision task.",
	}
	if taskID != "" && strings.TrimSpace(authorityTask.TaskID) != taskID {
		payload["superseded_task_id"] = taskID
		payload["stale_authority_task_id"] = taskID
		payload["guidance"] = "An older role/scope authority transition is already applied and covers this duplicate request. Treat the current authority task as superseded by the applied receipt, do not run project_role_assign again, and continue the owner lane."
	}
	if strings.TrimSpace(meta.BoundaryTransitionKey) != "" {
		payload["boundary_transition_key"] = strings.TrimSpace(meta.BoundaryTransitionKey)
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: "project_role_assign authority transition already applied"}, true
	}
	return &ToolResult{Output: string(raw)}, true
}

func projectRoleAssignFindAppliedBoundaryAuthorityTask(tasks []WorkspaceTaskRecord, preferredTaskID string, meta projectRoleAssignBoundaryTransitionMeta, projectID, targetAgentID, roleType string) (WorkspaceTaskRecord, bool) {
	preferredTaskID = strings.TrimSpace(preferredTaskID)
	if preferredTaskID != "" {
		if task, ok := projectRoleAssignFindWorkspaceTask(tasks, preferredTaskID); ok && projectRoleAssignAuthorityTaskApplied(task, meta, projectID, targetAgentID, roleType) {
			return task, true
		}
	}
	for _, task := range tasks {
		if preferredTaskID != "" && strings.TrimSpace(task.TaskID) == preferredTaskID {
			continue
		}
		if projectRoleAssignAuthorityTaskApplied(task, meta, projectID, targetAgentID, roleType) {
			return task, true
		}
	}
	return WorkspaceTaskRecord{}, false
}

func (t *ProjectRoleAssignTool) projectRoleAssignExistingLiveBoundaryTransitionTask(ctx context.Context, preferredTaskID, projectID, targetAgentID, roleType string, meta projectRoleAssignBoundaryTransitionMeta) (WorkspaceTaskRecord, bool) {
	task, state, ok := t.projectRoleAssignExistingBoundaryTransitionTask(ctx, preferredTaskID, projectID, targetAgentID, roleType, meta)
	return task, ok && state == projectRoleAssignBoundaryTaskStateLive
}

func (t *ProjectRoleAssignTool) projectRoleAssignExistingBoundaryTransitionTask(ctx context.Context, preferredTaskID, projectID, targetAgentID, roleType string, meta projectRoleAssignBoundaryTransitionMeta) (WorkspaceTaskRecord, string, bool) {
	if t == nil || t.client == nil || strings.TrimSpace(t.workspaceID) == "" || !projectRoleAssignBoundaryTransitionMetaPresent(meta) {
		return WorkspaceTaskRecord{}, "", false
	}
	tasks, err := t.client.ListTasks(ctx, t.workspaceID)
	if err != nil {
		return WorkspaceTaskRecord{}, "", false
	}
	return projectRoleAssignFindBoundaryAuthorityTask(tasks, preferredTaskID, meta, projectID, targetAgentID, roleType)
}

func projectRoleAssignFindLiveBoundaryAuthorityTask(tasks []WorkspaceTaskRecord, preferredTaskID string, meta projectRoleAssignBoundaryTransitionMeta, projectID, targetAgentID, roleType string) (WorkspaceTaskRecord, bool) {
	task, state, ok := projectRoleAssignFindBoundaryAuthorityTask(tasks, preferredTaskID, meta, projectID, targetAgentID, roleType)
	return task, ok && state == projectRoleAssignBoundaryTaskStateLive
}

func projectRoleAssignFindBoundaryAuthorityTask(tasks []WorkspaceTaskRecord, preferredTaskID string, meta projectRoleAssignBoundaryTransitionMeta, projectID, targetAgentID, roleType string) (WorkspaceTaskRecord, string, bool) {
	preferredTaskID = strings.TrimSpace(preferredTaskID)
	if preferredTaskID != "" {
		if task, ok := projectRoleAssignFindWorkspaceTask(tasks, preferredTaskID); ok {
			if state, matched := projectRoleAssignAuthorityTaskState(task, meta, projectID, targetAgentID, roleType); matched {
				return task, state, true
			}
		}
	}
	for _, task := range tasks {
		if preferredTaskID != "" && strings.TrimSpace(task.TaskID) == preferredTaskID {
			continue
		}
		if state, matched := projectRoleAssignAuthorityTaskState(task, meta, projectID, targetAgentID, roleType); matched {
			return task, state, true
		}
	}
	return WorkspaceTaskRecord{}, "", false
}

func projectRoleAssignFindWorkspaceTask(tasks []WorkspaceTaskRecord, taskID string) (WorkspaceTaskRecord, bool) {
	taskID = strings.TrimSpace(taskID)
	for _, task := range tasks {
		if strings.TrimSpace(task.TaskID) == taskID {
			return task, true
		}
	}
	return WorkspaceTaskRecord{}, false
}

func projectRoleAssignAuthorityTaskApplied(task WorkspaceTaskRecord, meta projectRoleAssignBoundaryTransitionMeta, projectID, targetAgentID, roleType string) bool {
	status := strings.ToUpper(strings.TrimSpace(task.Status))
	if status != "RESOLVED" && status != "COMPLETED" && status != "DONE" {
		return false
	}
	if task.ClaimStatus != nil {
		claimStatus := strings.ToUpper(strings.TrimSpace(*task.ClaimStatus))
		if claimStatus != "" && claimStatus != "COMPLETED" && claimStatus != "DONE" {
			return false
		}
	}
	return projectRoleAssignAuthorityTaskMatchesTransition(task, meta, projectID, targetAgentID, roleType)
}

func projectRoleAssignAuthorityTaskState(task WorkspaceTaskRecord, meta projectRoleAssignBoundaryTransitionMeta, projectID, targetAgentID, roleType string) (string, bool) {
	if !projectRoleAssignAuthorityTaskMatchesTransition(task, meta, projectID, targetAgentID, roleType) {
		return "", false
	}
	if taskClaimStatus(task) == "BLOCKED" {
		return projectRoleAssignBoundaryTaskStateTerminalBlocker, true
	}
	if projectRoleAssignAuthorityTaskLive(task, meta, projectID, targetAgentID, roleType) {
		return projectRoleAssignBoundaryTaskStateLive, true
	}
	return "", false
}

func projectRoleAssignAuthorityTaskLive(task WorkspaceTaskRecord, meta projectRoleAssignBoundaryTransitionMeta, projectID, targetAgentID, roleType string) bool {
	if taskClaimStatus(task) == "BLOCKED" {
		return false
	}
	status := strings.ToUpper(strings.TrimSpace(task.Status))
	switch status {
	case "PENDING", "RUNNING", "CLAIMED", "ACTIVE", "BLOCKED":
	default:
		return false
	}
	return projectRoleAssignAuthorityTaskMatchesTransition(task, meta, projectID, targetAgentID, roleType)
}

func projectRoleAssignAuthorityTaskMatchesTransition(task WorkspaceTaskRecord, meta projectRoleAssignBoundaryTransitionMeta, projectID, targetAgentID, roleType string) bool {
	var req map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(task.TaskRequirementsJSON)), &req); err != nil {
		return false
	}
	if strings.TrimSpace(stringMapValue(req, "schema")) != "project_role_scope_authority_transition.v1" {
		return false
	}
	if strings.TrimSpace(meta.BoundaryTransitionKey) == "" && len(meta.SideEffectRefs) == 0 {
		return false
	}
	if projectID := strings.TrimSpace(projectID); projectID != "" {
		reqProjectID := strings.TrimSpace(stringMapValue(req, "project_id"))
		if reqProjectID != "" && reqProjectID != projectID {
			return false
		}
	}
	if targetAgentID := strings.TrimSpace(targetAgentID); targetAgentID != "" {
		reqTargetAgentID := strings.TrimSpace(stringMapValue(req, "target_agent_id"))
		if reqTargetAgentID != "" && reqTargetAgentID != targetAgentID {
			return false
		}
	}
	if roleType := strings.ToUpper(strings.TrimSpace(roleType)); roleType != "" {
		reqRoleType := strings.ToUpper(strings.TrimSpace(stringMapValue(req, "role_type")))
		if reqRoleType != "" && reqRoleType != roleType {
			return false
		}
	}
	if key := strings.TrimSpace(meta.BoundaryTransitionKey); key != "" {
		reqKey := firstNonEmpty(strings.TrimSpace(stringMapValue(req, "boundary_transition_key")), strings.TrimSpace(stringMapValue(req, "dedup_key")))
		if reqKey != key {
			return false
		}
	}
	if activeTaskID := strings.TrimSpace(meta.ActiveTaskID); activeTaskID != "" && strings.TrimSpace(stringMapValue(req, "active_task_id")) != activeTaskID {
		return false
	}
	if branchID := strings.TrimSpace(meta.BranchID); branchID != "" && strings.TrimSpace(stringMapValue(req, "branch_id")) != branchID {
		return false
	}
	if len(meta.SideEffectRefs) > 0 {
		reqRefs := uniqueNonEmptyStrings(firstNonEmptyStringSlice(stringSliceFromAny(req["side_effect_refs"]), stringSliceFromAny(req["side_effect_ref"])))
		if len(reqRefs) == 0 || !projectRoleAssignStringSetCovers(reqRefs, meta.SideEffectRefs) {
			return false
		}
	}
	return true
}

func projectRoleAssignBranchScopeCovers(coordination ProjectCoordinationRecord, branchID, targetAgentID, activeTaskID string, requestedPaths []string) bool {
	branchID = strings.TrimSpace(branchID)
	targetAgentID = strings.TrimSpace(targetAgentID)
	activeTaskID = strings.TrimSpace(activeTaskID)
	if branchID == "" || len(requestedPaths) == 0 {
		return false
	}
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.BranchID) != branchID {
			continue
		}
		if projectBranchTerminalState(branch.Status) {
			return false
		}
		if targetAgentID != "" && strings.TrimSpace(branch.AgentID) != "" && strings.TrimSpace(branch.AgentID) != targetAgentID {
			return false
		}
		if activeTaskID != "" && strings.TrimSpace(branch.ActiveTaskID) != "" && strings.TrimSpace(branch.ActiveTaskID) != activeTaskID {
			return false
		}
		return projectRoleAssignScopeCovers(projectRoleAssignNormalizedWriteScopePaths(branch.WriteScopeJSON), requestedPaths)
	}
	return false
}

func projectRoleAssignBranchWriteScopeJSON(coordination ProjectCoordinationRecord, branchID string) string {
	branchID = strings.TrimSpace(branchID)
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.BranchID) == branchID {
			return strings.TrimSpace(branch.WriteScopeJSON)
		}
	}
	return ""
}

func projectRoleAssignClaimScopeCovers(task WorkspaceTaskRecord, targetAgentID, branchID string, requestedPaths []string) bool {
	return projectRoleAssignClaimScopeCoversWithStatus(task, targetAgentID, branchID, requestedPaths, map[string]bool{
		"CLAIMED":   true,
		"RUNNING":   true,
		"ACTIVE":    true,
		"BLOCKED":   true,
		"COMPLETED": true,
		"DONE":      true,
		"RELEASED":  true,
	})
}

func projectRoleAssignLiveClaimScopeCovers(task WorkspaceTaskRecord, targetAgentID, branchID string, requestedPaths []string) bool {
	return projectRoleAssignClaimScopeCoversWithStatus(task, targetAgentID, branchID, requestedPaths, map[string]bool{
		"CLAIMED": true,
		"RUNNING": true,
		"ACTIVE":  true,
		"BLOCKED": true,
	})
}

func projectRoleAssignClaimScopeCoversWithStatus(task WorkspaceTaskRecord, targetAgentID, branchID string, requestedPaths []string, allowedStatuses map[string]bool) bool {
	if len(requestedPaths) == 0 {
		return false
	}
	if targetAgentID = strings.TrimSpace(targetAgentID); targetAgentID != "" && task.ClaimAgentID != nil && strings.TrimSpace(*task.ClaimAgentID) != targetAgentID {
		return false
	}
	if branchID = strings.TrimSpace(branchID); branchID != "" {
		if task.ClaimBranchID == nil || strings.TrimSpace(*task.ClaimBranchID) != branchID {
			return false
		}
	}
	if task.ClaimStatus == nil {
		return false
	}
	status := strings.ToUpper(strings.TrimSpace(*task.ClaimStatus))
	if !allowedStatuses[status] {
		return false
	}
	return projectRoleAssignScopeCovers(projectRoleAssignNormalizedWriteScopePaths(pointerValue(task.ClaimWriteScopeJSON)), requestedPaths)
}

func projectRoleAssignStringSetCovers(have, want []string) bool {
	set := map[string]struct{}{}
	for _, value := range have {
		if value = strings.TrimSpace(value); value != "" {
			set[value] = struct{}{}
		}
	}
	for _, value := range want {
		if value = strings.TrimSpace(value); value != "" {
			if _, ok := set[value]; !ok {
				return false
			}
		}
	}
	return true
}

func renderProjectRoleAssignLeadRequestDescription(projectID, requesterID, targetAgentID, roleType, writeScopeJSON, summary string, meta projectRoleAssignBoundaryTransitionMeta) string {
	var b strings.Builder
	b.WriteString("# Strategic Lead Role/Scope Request\n\n")
	b.WriteString("- project_id: " + strings.TrimSpace(projectID) + "\n")
	b.WriteString("- requested_by: " + strings.TrimSpace(requesterID) + "\n")
	b.WriteString("- target_agent_id: " + strings.TrimSpace(targetAgentID) + "\n")
	b.WriteString("- requested_role_type: " + strings.TrimSpace(roleType) + "\n")
	b.WriteString("- requested_write_scope_json: " + firstNonEmpty(strings.TrimSpace(writeScopeJSON), "{}") + "\n")
	if strings.TrimSpace(meta.BoundaryTransitionKey) != "" {
		b.WriteString("- boundary_transition_key: " + strings.TrimSpace(meta.BoundaryTransitionKey) + "\n")
	}
	if len(meta.SideEffectRefs) > 0 {
		b.WriteString("- side_effect_refs: " + strings.Join(meta.SideEffectRefs, ", ") + "\n")
	}
	if strings.TrimSpace(meta.ActiveTaskID) != "" {
		b.WriteString("- active_task_id: " + strings.TrimSpace(meta.ActiveTaskID) + "\n")
	}
	if strings.TrimSpace(meta.BranchID) != "" {
		b.WriteString("- branch_id: " + strings.TrimSpace(meta.BranchID) + "\n")
	}
	if strings.TrimSpace(meta.BranchName) != "" {
		b.WriteString("- branch_name: " + strings.TrimSpace(meta.BranchName) + "\n")
	}
	if strings.TrimSpace(summary) != "" {
		b.WriteString("- requester_summary: " + strings.TrimSpace(summary) + "\n")
	}
	b.WriteString("\n## Required Lead Action\n")
	b.WriteString("Review the current project role/task matrix. If the request is correct, run project_role_assign from the active strategic lead planner context. Preserve boundary_transition_key, side_effect_refs, active_task_id, branch_id, and branch_name from this task when calling the tool so any overlap denial becomes durable routing state for the original lane. If the request exposes a task/role mismatch, update the role assignment or create a corrected task/delegation so implementation agents do not work in isolated duplicate directions.\n")
	b.WriteString("\n## Anti-Loop Rule\n")
	b.WriteString("Do not solve this only by replying to a model.ask peer request. The durable fix is a project role/task update, revised task assignment, or explicit blocker decision.\n")
	return strings.TrimSpace(b.String())
}

func normalizeProjectRoleAssignToolRole(value string) string {
	roleType := strings.ToUpper(strings.TrimSpace(value))
	roleType = strings.ReplaceAll(roleType, "-", "_")
	roleType = strings.ReplaceAll(roleType, " ", "_")
	switch roleType {
	case "PLANNER", "IMPLEMENTER", "REVIEWER", "INTEGRATOR", "OBSERVER":
		return roleType
	default:
		return ""
	}
}

func projectRoleAssignWriteScopeEmpty(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return true
	}
	return len(projectRoleAssignWriteScopePaths(raw)) == 0
}

func projectRoleAssignWriteScopePaths(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil
	}
	seen := map[string]struct{}{}
	var paths []string
	add := func(value string) {
		value = strings.TrimSpace(strings.ReplaceAll(value, `\`, `/`))
		value = strings.Trim(value, "/")
		if value == "" || value == "." {
			return
		}
		value = strings.ToLower(value)
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		paths = append(paths, value)
	}
	var walk func(any)
	walk = func(value any) {
		switch typed := value.(type) {
		case string:
			add(typed)
		case []any:
			for _, item := range typed {
				walk(item)
			}
		}
	}
	if object, ok := decoded.(map[string]any); ok {
		for _, key := range []string{"paths", "files", "path_prefixes", "write_paths", "scopes"} {
			walk(object[key])
		}
		return paths
	}
	walk(decoded)
	return paths
}

func projectRoleAssignNormalizedWriteScopePaths(raw string) []string {
	paths := projectRoleAssignWriteScopePaths(raw)
	sort.Strings(paths)
	return paths
}

func projectRoleAssignNormalizedWriteScopeKey(raw string) string {
	return strings.Join(projectRoleAssignNormalizedWriteScopePaths(raw), "\n")
}

func projectRoleAssignScopeCovers(activePaths, requestedPaths []string) bool {
	if len(requestedPaths) == 0 {
		return true
	}
	if len(activePaths) == 0 {
		return false
	}
	for _, requested := range requestedPaths {
		if !projectRoleAssignPathCovered(activePaths, requested) {
			return false
		}
	}
	return true
}

func projectRoleAssignPathCovered(activePaths []string, requested string) bool {
	requested = strings.Trim(strings.ToLower(strings.TrimSpace(requested)), "/")
	if requested == "" {
		return true
	}
	for _, active := range activePaths {
		active = strings.Trim(strings.ToLower(strings.TrimSpace(active)), "/")
		if active == "" {
			continue
		}
		if active == "*" || active == "**" || active == requested || strings.HasPrefix(requested, active+"/") || scopePathCovers(active, requested) {
			return true
		}
	}
	return false
}
