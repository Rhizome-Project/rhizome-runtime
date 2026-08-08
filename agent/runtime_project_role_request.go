package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const projectRoleRequestMethod = "project.role.request"

type projectRoleRequestPayload struct {
	ProjectID      string `json:"project_id"`
	TaskID         string `json:"task_id,omitempty"`
	RoleType       string `json:"role_type"`
	WriteScopeJSON string `json:"write_scope_json"`
	Reason         string `json:"reason,omitempty"`
	RequestKey     string `json:"request_key,omitempty"`
}

func projectCoordinationStrategicLeadAgentID(coordination ProjectCoordinationRecord) string {
	if coordination.StrategicLead == nil {
		return ""
	}
	return strings.TrimSpace(coordination.StrategicLead.AgentID)
}

func (r *Runtime) handleProjectClaimAdmissionError(ctx context.Context, task WorkspaceTaskRecord, err error) (bool, error) {
	if handled, handleErr := r.handleProjectClaimOverlapAdmissionError(ctx, task, err); handled {
		return true, handleErr
	}
	var missing *projectClaimRoleMissingError
	if !errors.As(err, &missing) || missing == nil {
		return false, nil
	}
	if strings.TrimSpace(missing.AgentID) == "" {
		missing.AgentID = strings.TrimSpace(r.cfg.AgentID)
	}
	if strings.TrimSpace(missing.ProjectID) == "" {
		missing.ProjectID = strings.TrimSpace(task.ProjectID)
	}
	if strings.TrimSpace(missing.TaskID) == "" {
		missing.TaskID = strings.TrimSpace(task.TaskID)
	}
	if strings.TrimSpace(missing.RoleType) == "" {
		missing.RoleType = firstNonEmpty(runtimeProjectClaimRequiredRoleType(task.ProjectLane), "IMPLEMENTER")
	}
	if strings.TrimSpace(missing.WriteScopeJSON) == "" {
		missing.WriteScopeJSON = defaultProjectRoleRequestWriteScope
	}
	if strings.TrimSpace(missing.LeadAgentID) == "" && r.client != nil && strings.TrimSpace(missing.ProjectID) != "" {
		coordination, coordErr := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, missing.ProjectID)
		if coordErr == nil {
			missing.LeadAgentID = projectCoordinationStrategicLeadAgentID(coordination)
		}
	}
	requestKey := projectRoleRequestKey(r.cfg.WorkspaceID, missing.ProjectID, missing.TaskID, missing.AgentID, missing.RoleType)
	if missing.RoleType == "IMPLEMENTER" && projectRoleAssignWriteScopeEmpty(missing.WriteScopeJSON) {
		if r.projectRoleRequestAlreadySent(requestKey, 10*time.Minute) {
			return true, nil
		}
		summary := fmt.Sprintf("Cannot auto-request IMPLEMENTER role for task %s because no bounded write_scope_json is available for agent %s", missing.TaskID, missing.AgentID)
		if err := r.postProjectRoleRequestUpdate(ctx, "issue", summary, missing, "", false); err != nil {
			return true, err
		}
		return true, r.recordProjectRoleRequestScratch(ctx, requestKey, "", missing)
	}
	if strings.TrimSpace(missing.LeadAgentID) == "" {
		if r.projectRoleRequestAlreadySent(requestKey, 10*time.Minute) {
			return true, nil
		}
		summary := fmt.Sprintf("Cannot claim project task %s: no active strategic lead can assign %s role for agent %s", missing.TaskID, missing.RoleType, missing.AgentID)
		if err := r.postProjectRoleRequestUpdate(ctx, "issue", summary, missing, "", true); err != nil {
			return true, err
		}
		return true, r.recordProjectRoleRequestScratch(ctx, requestKey, "", missing)
	}
	if strings.TrimSpace(missing.LeadAgentID) == strings.TrimSpace(r.cfg.AgentID) {
		if runtimeTrustFirst(r.cfg) {
			if ok, reason := r.trustFirstMissingRoleAutoAssignAllowed(ctx, task, missing); ok {
				role, assignErr := r.client.AssignProjectRole(ctx, ProjectRoleAssignInput{
					WorkspaceID:    r.cfg.WorkspaceID,
					ProjectID:      missing.ProjectID,
					ActorID:        r.cfg.AgentID,
					AgentID:        missing.AgentID,
					RoleType:       missing.RoleType,
					WriteScopeJSON: missing.WriteScopeJSON,
					Summary:        fmt.Sprintf("Trust-first auto-remediated bounded missing project role for task %s after claim admission failed.", missing.TaskID),
				})
				if assignErr != nil {
					return true, assignErr
				}
				summary := fmt.Sprintf("Trust-first assigned bounded %s role %s to %s for task %s after project claim admission requested a task-derived write scope.", role.RoleType, role.RoleID, role.AgentID, missing.TaskID)
				if err := r.postProjectRoleRequestUpdate(ctx, "coordination", summary+" "+reason, missing, role.RoleID, false); err != nil {
					return true, err
				}
				r.markBootstrapStale()
				r.wakePlanner()
				return true, nil
			}
			summary := fmt.Sprintf("Trust-first recorded advisory %s role request for %s on task %s without automatic project role assignment; task-local write_scope_hints and self-selection remain primary.", missing.RoleType, missing.AgentID, missing.TaskID)
			if err := r.postProjectRoleRequestUpdate(ctx, "coordination", summary, missing, "", false); err != nil {
				return true, err
			}
			r.markBootstrapStale()
			r.wakePlanner()
			return true, nil
		}
		role, assignErr := r.client.AssignProjectRole(ctx, ProjectRoleAssignInput{
			WorkspaceID:    r.cfg.WorkspaceID,
			ProjectID:      missing.ProjectID,
			ActorID:        r.cfg.AgentID,
			AgentID:        missing.AgentID,
			RoleType:       missing.RoleType,
			WriteScopeJSON: missing.WriteScopeJSON,
			Summary:        fmt.Sprintf("Auto-remediated missing project role for task %s after claim admission failed.", missing.TaskID),
		})
		if assignErr != nil {
			return true, assignErr
		}
		summary := fmt.Sprintf("Assigned %s role %s to %s for task %s after project claim admission requested a write scope.", role.RoleType, role.RoleID, role.AgentID, missing.TaskID)
		if err := r.postProjectRoleRequestUpdate(ctx, "coordination", summary, missing, role.RoleID, false); err != nil {
			return true, err
		}
		r.markBootstrapStale()
		r.wakePlanner()
		return true, nil
	}
	if r.projectRoleRequestAlreadySent(requestKey, 10*time.Minute) {
		return true, nil
	}
	payload := projectRoleRequestPayload{
		ProjectID:      missing.ProjectID,
		TaskID:         missing.TaskID,
		RoleType:       missing.RoleType,
		WriteScopeJSON: missing.WriteScopeJSON,
		Reason:         fmt.Sprintf("Agent %s cannot claim project task %s until strategic lead assigns an active %s role.", missing.AgentID, missing.TaskID, missing.RoleType),
		RequestKey:     requestKey,
	}
	raw, marshalErr := json.Marshal(payload)
	if marshalErr != nil {
		return true, marshalErr
	}
	created, reqErr := r.client.RequestAgent(ctx, AgentRequestInput{
		WorkspaceID: r.cfg.WorkspaceID,
		FromAgentID: missing.AgentID,
		ToAgentID:   missing.LeadAgentID,
		Method:      projectRoleRequestMethod,
		PayloadJSON: string(raw),
		TimeoutSec:  600,
	})
	if reqErr != nil {
		return true, reqErr
	}
	if err := r.recordProjectRoleRequestScratch(ctx, requestKey, created.RequestID, missing); err != nil {
		return true, err
	}
	summary := fmt.Sprintf("Requested %s role/write scope from strategic lead %s for task %s after project claim admission blocked.", missing.RoleType, missing.LeadAgentID, missing.TaskID)
	if err := r.postProjectRoleRequestUpdate(ctx, "coordination", summary, missing, created.RequestID, false); err != nil {
		return true, err
	}
	return true, nil
}

func (r *Runtime) maybeRequestProjectRoleScopeFromWork(ctx context.Context, work AgentWorkNextResult) (bool, error) {
	if !agentWorkProjectRoleScopeRequired(work) {
		return false, nil
	}
	// R67 finalization-liveness freeze: a work-driven role-scope request is for acquiring IMPLEMENTER
	// scope to do (more) implementation work. When the project is convergeable (no spec-rooted blocking
	// task remains -> product is spec-complete), no new implementation scope is needed and minting
	// role-scope churn only starves the lead's convergence. Suppress it. This is the routine role-scope
	// routing only; the side-effect boundary-transition path (a possible real blocker) enters via
	// project_role_assign directly, not here, so it is unaffected. Errs toward allow (loaded clear frontier).
	if coordination, ok := projectCoordinationFromWork(&work); ok {
		if runtimeProjectFinalizationConvergeable(coordination, strings.TrimSpace(projectClaimRepairBlockedTaskFromWork(work).ProjectID)) {
			return true, nil
		}
	}
	task := projectClaimRepairBlockedTaskFromWork(work)
	if strings.TrimSpace(task.TaskID) == "" || strings.TrimSpace(task.ProjectID) == "" {
		return true, r.postProjectRoleScopeMalformedWorkUpdate(ctx, work, task)
	}
	roleType := runtimeProjectClaimRequiredRoleType(task.ProjectLane)
	if roleType == "" {
		return false, nil
	}
	writeScopeJSON := r.inferProjectTaskWriteScopeJSON(ctx, task, &work)
	if strings.TrimSpace(writeScopeJSON) == "" {
		writeScopeJSON = defaultProjectRoleRequestWriteScope
	}
	leadID := ""
	if coordination, ok := projectCoordinationFromWork(&work); ok {
		leadID = projectCoordinationStrategicLeadAgentID(coordination)
	}
	return r.handleProjectClaimAdmissionError(ctx, task, &projectClaimRoleMissingError{
		ProjectID:      strings.TrimSpace(task.ProjectID),
		TaskID:         strings.TrimSpace(task.TaskID),
		AgentID:        strings.TrimSpace(r.cfg.AgentID),
		LeadAgentID:    leadID,
		RoleType:       roleType,
		WriteScopeJSON: writeScopeJSON,
	})
}

func agentWorkProjectRoleScopeRequired(work AgentWorkNextResult) bool {
	if agentWorkProjectRoleScopeRequiredType(work.Reason) {
		return true
	}
	if work.Packet == nil {
		return false
	}
	return agentWorkProjectRoleScopeRequiredType(work.Packet.WorkType) ||
		agentWorkProjectRoleScopeRequiredType(work.Packet.CoordinationState) ||
		(work.Packet.Gate != nil && agentWorkProjectRoleScopeRequiredType(work.Packet.Gate.GateType))
}

func agentWorkProjectRoleScopeRequiredType(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "project_role_lane_required", "project_role_targeted_delegation", "project_targeted_delegation_required":
		return true
	default:
		return false
	}
}

func (r *Runtime) postProjectRoleScopeMalformedWorkUpdate(ctx context.Context, work AgentWorkNextResult, task WorkspaceTaskRecord) error {
	if r == nil || r.client == nil {
		return nil
	}
	payload := map[string]any{
		"role_request_state": "malformed_work_packet",
		"work_reason":        strings.TrimSpace(work.Reason),
		"task_id":            strings.TrimSpace(task.TaskID),
		"project_id":         strings.TrimSpace(task.ProjectID),
		"agent_id":           strings.TrimSpace(r.cfg.AgentID),
		"summary":            "project role/scope repair could not materialize because work.next omitted a concrete task/project anchor",
	}
	raw, _ := json.Marshal(payload)
	return r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		UpdateType:  "issue",
		Summary:     "Project role/scope repair work packet was missing concrete task/project anchors.",
		PayloadJSON: string(raw),
	})
}

func (r *Runtime) handleProjectRoleRequest(ctx context.Context, request AgentRequestRecord, status map[string]any) error {
	var payload projectRoleRequestPayload
	if err := json.Unmarshal([]byte(strings.TrimSpace(request.Payload)), &payload); err != nil {
		status["status"] = "error"
		status["details"] = "invalid project.role.request payload: " + err.Error()
		return nil
	}
	payload.ProjectID = strings.TrimSpace(payload.ProjectID)
	payload.TaskID = strings.TrimSpace(payload.TaskID)
	payload.RoleType = normalizeProjectRoleRequestRoleType(payload.RoleType)
	payload.WriteScopeJSON = strings.TrimSpace(payload.WriteScopeJSON)
	if payload.ProjectID == "" {
		status["status"] = "error"
		status["details"] = "project_id is required"
		return nil
	}
	if payload.RoleType == "" {
		status["status"] = "error"
		status["details"] = "role_type must be one of IMPLEMENTER, REVIEWER, INTEGRATOR, PLANNER, or OBSERVER"
		return nil
	}
	if payload.WriteScopeJSON == "" {
		payload.WriteScopeJSON = defaultProjectRoleRequestWriteScope
	}
	if !json.Valid([]byte(payload.WriteScopeJSON)) {
		status["status"] = "error"
		status["details"] = "write_scope_json must be valid JSON"
		return nil
	}
	if payload.RoleType == "IMPLEMENTER" && projectRoleAssignWriteScopeEmpty(payload.WriteScopeJSON) {
		status["status"] = "error"
		status["details"] = "IMPLEMENTER role requires non-empty write_scope_json paths"
		return nil
	}
	coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, payload.ProjectID)
	if err != nil {
		status["status"] = "error"
		status["details"] = "project coordination read failed: " + err.Error()
		return nil
	}
	leadID := projectCoordinationStrategicLeadAgentID(coordination)
	if leadID == "" || leadID != strings.TrimSpace(r.cfg.AgentID) {
		status["status"] = "error"
		status["details"] = fmt.Sprintf("agent %s is not active strategic lead for project %s", r.cfg.AgentID, payload.ProjectID)
		return nil
	}
	targetAgentID := strings.TrimSpace(request.FromAgentID)
	if targetAgentID == "" {
		status["status"] = "error"
		status["details"] = "request.from_agent_id is required"
		return nil
	}
	if payload.RoleType == "IMPLEMENTER" {
		if existing, ok := activeNonImplementerProjectRoleForAgent(coordination, targetAgentID); ok {
			existingRoleType := strings.ToUpper(strings.TrimSpace(existing.RoleType))
			if existingRoleType == "" {
				existingRoleType = "UNKNOWN"
			}
			summary := fmt.Sprintf("Refused automatic IMPLEMENTER role request for %s because the agent already has active %s role %s on project %s.", targetAgentID, existingRoleType, existing.RoleID, payload.ProjectID)
			status["status"] = "role_mismatch"
			status["summary"] = summary
			status["details"] = "Automatic claim-admission remediation will not promote an agent that already has a non-IMPLEMENTER project role. The strategic lead must deliberately revise the role/task matrix with project_role_assign if this agent should implement code."
			status["project_id"] = payload.ProjectID
			status["agent_id"] = targetAgentID
			status["blocked_role_type"] = payload.RoleType
			status["existing_role_id"] = existing.RoleID
			status["existing_role_type"] = existingRoleType
			if err := r.postProjectRoleRequestUpdate(ctx, "issue", summary, &projectClaimRoleMissingError{
				ProjectID:      payload.ProjectID,
				TaskID:         payload.TaskID,
				AgentID:        targetAgentID,
				LeadAgentID:    r.cfg.AgentID,
				RoleType:       payload.RoleType,
				WriteScopeJSON: payload.WriteScopeJSON,
			}, existing.RoleID, false); err != nil {
				return err
			}
			return nil
		}
	}
	trustFirstAutoAssignReason := ""
	if runtimeTrustFirst(r.cfg) && !projectRoleRequestExplicitRepair(payload.Reason) {
		if ok, reason := r.trustFirstProjectRoleRequestAutoAssignAllowed(ctx, payload); ok {
			trustFirstAutoAssignReason = reason
		} else {
			if reason == "" {
				reason = "requested role/scope is advisory in trust_first mode"
			}
			summary := fmt.Sprintf("Trust-first recorded advisory %s role request for %s on project %s without automatic project role assignment.", payload.RoleType, targetAgentID, payload.ProjectID)
			status["status"] = "advisory_recorded"
			status["summary"] = summary
			status["project_id"] = payload.ProjectID
			status["agent_id"] = targetAgentID
			status["role_type"] = payload.RoleType
			status["write_scope_json"] = payload.WriteScopeJSON
			status["auto_assign_blocker"] = reason
			status["guidance"] = "In trust_first, task-local write_scope_hints, semantic fit, and branch evidence remain primary; bounded task-scoped requests can be auto-provisioned by the active lead, while broad or unverified requests stay advisory."
			if err := r.postProjectRoleRequestUpdate(ctx, "coordination", summary, &projectClaimRoleMissingError{
				ProjectID:      payload.ProjectID,
				TaskID:         payload.TaskID,
				AgentID:        targetAgentID,
				LeadAgentID:    r.cfg.AgentID,
				RoleType:       payload.RoleType,
				WriteScopeJSON: payload.WriteScopeJSON,
			}, "", false); err != nil {
				return err
			}
			r.markBootstrapStale()
			return nil
		}
	}
	role, err := r.client.AssignProjectRole(ctx, ProjectRoleAssignInput{
		WorkspaceID:    r.cfg.WorkspaceID,
		ProjectID:      payload.ProjectID,
		ActorID:        r.cfg.AgentID,
		AgentID:        targetAgentID,
		RoleType:       payload.RoleType,
		WriteScopeJSON: payload.WriteScopeJSON,
		Summary:        firstNonEmpty(payload.Reason, fmt.Sprintf("Role requested by %s for task %s.", targetAgentID, payload.TaskID)),
	})
	if err != nil {
		status["status"] = "error"
		status["details"] = "project role assignment failed: " + err.Error()
		return nil
	}
	status["status"] = "assigned"
	status["summary"] = fmt.Sprintf("assigned %s role %s to %s", role.RoleType, role.RoleID, role.AgentID)
	status["project_id"] = role.ProjectID
	status["role_id"] = role.RoleID
	status["role_type"] = role.RoleType
	status["agent_id"] = role.AgentID
	status["write_scope_json"] = role.WriteScopeJSON
	if trustFirstAutoAssignReason != "" {
		status["trust_first_auto_assign"] = "bounded_task_scope"
		status["trust_first_auto_assign_reason"] = trustFirstAutoAssignReason
	}
	if err := r.postProjectRoleRequestUpdate(ctx, "coordination", status["summary"].(string), &projectClaimRoleMissingError{
		ProjectID:      payload.ProjectID,
		TaskID:         payload.TaskID,
		AgentID:        targetAgentID,
		LeadAgentID:    r.cfg.AgentID,
		RoleType:       payload.RoleType,
		WriteScopeJSON: payload.WriteScopeJSON,
	}, role.RoleID, false); err != nil {
		return err
	}
	r.markBootstrapStale()
	return nil
}

func (r *Runtime) trustFirstMissingRoleAutoAssignAllowed(ctx context.Context, task WorkspaceTaskRecord, missing *projectClaimRoleMissingError) (bool, string) {
	if missing == nil {
		return false, "missing role payload is empty"
	}
	payload := projectRoleRequestPayload{
		ProjectID:      strings.TrimSpace(missing.ProjectID),
		TaskID:         strings.TrimSpace(missing.TaskID),
		RoleType:       normalizeProjectRoleRequestRoleType(missing.RoleType),
		WriteScopeJSON: strings.TrimSpace(missing.WriteScopeJSON),
	}
	if strings.TrimSpace(payload.TaskID) == "" {
		return false, "task_id is required to prove a task-scoped role request"
	}
	if payload.RoleType != "IMPLEMENTER" {
		return false, "non-IMPLEMENTER trust_first role requests require explicit assignment or server fit-based claim admission"
	}
	if projectRoleAssignWriteScopeEmpty(payload.WriteScopeJSON) {
		return false, "write_scope_json is empty"
	}
	if projectWriteScopeJSONIsBroad(payload.WriteScopeJSON) {
		return false, "write_scope_json is broad"
	}
	taskScopeJSON := r.inferProjectTaskWriteScopeJSON(ctx, task, nil)
	if strings.TrimSpace(taskScopeJSON) == "" {
		return false, "task-derived write_scope_json is unavailable"
	}
	if projectWriteScopeJSONIsBroad(taskScopeJSON) {
		return false, "task-derived write_scope_json is broad"
	}
	taskPaths := projectRoleAssignWriteScopePaths(taskScopeJSON)
	requestedPaths := projectRoleAssignWriteScopePaths(payload.WriteScopeJSON)
	if len(taskPaths) == 0 || len(requestedPaths) == 0 {
		return false, "task or request write_scope_json has no concrete paths"
	}
	if !projectRoleAssignScopeCovers(taskPaths, requestedPaths) {
		return false, "requested write_scope_json is not covered by task-derived write_scope_hints"
	}
	return true, "trust_first_auto_assign=bounded_task_scope"
}

func (r *Runtime) trustFirstProjectRoleRequestAutoAssignAllowed(ctx context.Context, payload projectRoleRequestPayload) (bool, string) {
	if strings.TrimSpace(payload.TaskID) == "" {
		return false, "task_id is required to prove a task-scoped role request"
	}
	if payload.RoleType != "IMPLEMENTER" {
		return false, "non-IMPLEMENTER trust_first role requests require explicit assignment or server fit-based claim admission"
	}
	if projectRoleAssignWriteScopeEmpty(payload.WriteScopeJSON) {
		return false, "write_scope_json is empty"
	}
	if projectWriteScopeJSONIsBroad(payload.WriteScopeJSON) {
		return false, "write_scope_json is broad"
	}
	taskScopeJSON, reason := r.trustFirstProjectRoleRequestTaskWriteScopeJSON(ctx, payload)
	if strings.TrimSpace(taskScopeJSON) == "" {
		return false, reason
	}
	if projectWriteScopeJSONIsBroad(taskScopeJSON) {
		return false, "task-derived write_scope_json is broad"
	}
	taskPaths := projectRoleAssignWriteScopePaths(taskScopeJSON)
	requestedPaths := projectRoleAssignWriteScopePaths(payload.WriteScopeJSON)
	if len(taskPaths) == 0 || len(requestedPaths) == 0 {
		return false, "task or request write_scope_json has no concrete paths"
	}
	if !projectRoleAssignScopeCovers(taskPaths, requestedPaths) {
		return false, "requested write_scope_json is not covered by task-derived write_scope_hints"
	}
	return true, "requested IMPLEMENTER scope is bounded and covered by task-derived write_scope_hints"
}

func (r *Runtime) trustFirstProjectRoleRequestTaskWriteScopeJSON(ctx context.Context, payload projectRoleRequestPayload) (string, string) {
	task, reason := r.trustFirstProjectRoleRequestHydratedTask(ctx, payload)
	if strings.TrimSpace(task.TaskID) == "" {
		return "", reason
	}
	scopeJSON := r.inferProjectTaskWriteScopeJSON(ctx, task.WorkspaceTaskRecord, &AgentWorkNextResult{Hydration: task.Hydration})
	if strings.TrimSpace(scopeJSON) == "" {
		return "", "task hydration did not expose concrete write_scope_hints"
	}
	return scopeJSON, ""
}

type hydratedProjectRoleRequestTask struct {
	WorkspaceTaskRecord
	Hydration *TaskHydrationBundle
}

func (r *Runtime) trustFirstProjectRoleRequestHydratedTask(ctx context.Context, payload projectRoleRequestPayload) (hydratedProjectRoleRequestTask, string) {
	if r == nil || r.client == nil {
		return hydratedProjectRoleRequestTask{}, "runtime client is unavailable"
	}
	taskID := strings.TrimSpace(payload.TaskID)
	if taskID == "" {
		return hydratedProjectRoleRequestTask{}, "task_id is required"
	}
	bundle, err := r.client.HydrateTask(ctx, TaskHydrationInput{
		WorkspaceID:      r.cfg.WorkspaceID,
		TaskID:           taskID,
		DocKeys:          defaultHydrationDocKeys(taskID),
		IncludeAllDocs:   boolPtr(false),
		UpdatesLimit:     5,
		ArtifactLimit:    5,
		RelatedTaskLimit: 5,
	})
	if err != nil {
		return hydratedProjectRoleRequestTask{}, "task hydration failed: " + err.Error()
	}
	task := WorkspaceTaskRecord{
		TaskID:      taskID,
		ProjectID:   strings.TrimSpace(payload.ProjectID),
		ProjectLane: "implementation",
	}
	if bundle.WorkspaceTask != nil {
		task = *bundle.WorkspaceTask
	} else if strings.TrimSpace(bundle.Task.TaskID) != "" {
		task = workspaceTaskRecordFromTaskStatus(bundle.Task)
	}
	if strings.TrimSpace(task.TaskID) == "" {
		task.TaskID = taskID
	}
	if strings.TrimSpace(task.ProjectID) == "" {
		task.ProjectID = strings.TrimSpace(payload.ProjectID)
	}
	if projectID := strings.TrimSpace(payload.ProjectID); projectID != "" && strings.TrimSpace(task.ProjectID) != "" && strings.TrimSpace(task.ProjectID) != projectID {
		return hydratedProjectRoleRequestTask{}, "task belongs to a different project"
	}
	return hydratedProjectRoleRequestTask{WorkspaceTaskRecord: task, Hydration: &bundle}, ""
}

func projectRoleRequestExplicitRepair(reason string) bool {
	reason = strings.ToLower(strings.TrimSpace(reason))
	if reason == "" {
		return false
	}
	return containsAnySignal(reason, []string{
		"stale role",
		"stale project role",
		"mismatched role",
		"role mismatch",
		"scope mismatch",
		"scope repair",
		"role repair",
		"repair role",
		"repair scope",
		"explicit role repair",
		"project claim repair",
	})
}

func activeNonImplementerProjectRoleForAgent(coordination ProjectCoordinationRecord, agentID string) (ProjectRoleRecord, bool) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return ProjectRoleRecord{}, false
	}
	for _, role := range activeProjectRolesForAgent(coordination, agentID) {
		roleType := strings.ToUpper(strings.TrimSpace(role.RoleType))
		if roleType != "" && roleType != "IMPLEMENTER" {
			return role, true
		}
	}
	return ProjectRoleRecord{}, false
}

func activeProjectRolesForAgent(coordination ProjectCoordinationRecord, agentID string) []ProjectRoleRecord {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil
	}
	seen := map[string]bool{}
	roles := make([]ProjectRoleRecord, 0, len(coordination.Roles)+1)
	appendRole := func(role ProjectRoleRecord) {
		if !strings.EqualFold(strings.TrimSpace(role.AgentID), agentID) || !strings.EqualFold(strings.TrimSpace(role.Status), "ACTIVE") {
			return
		}
		key := strings.TrimSpace(role.RoleID)
		if key == "" {
			key = strings.TrimSpace(role.AgentID) + ":" + strings.TrimSpace(role.RoleType)
		}
		if seen[key] {
			return
		}
		seen[key] = true
		roles = append(roles, role)
	}
	if coordination.StrategicLead != nil {
		appendRole(*coordination.StrategicLead)
	}
	for _, role := range coordination.Roles {
		appendRole(role)
	}
	return roles
}

func normalizeProjectRoleRequestRoleType(value string) string {
	roleType := strings.ToUpper(strings.TrimSpace(value))
	roleType = strings.ReplaceAll(roleType, "-", "_")
	roleType = strings.ReplaceAll(roleType, " ", "_")
	switch roleType {
	case "IMPLEMENTER", "REVIEWER", "INTEGRATOR", "PLANNER", "OBSERVER":
		return roleType
	default:
		return ""
	}
}

func projectRoleRequestKey(workspaceID, projectID, taskID, agentID, roleType string) string {
	parts := []string{
		"project_role_request",
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(projectID),
		strings.TrimSpace(taskID),
		strings.TrimSpace(agentID),
		strings.TrimSpace(roleType),
	}
	return strings.Join(parts, ":")
}

func (r *Runtime) projectRoleRequestAlreadySent(requestKey string, maxAge time.Duration) bool {
	requestKey = strings.TrimSpace(requestKey)
	if requestKey == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if strings.TrimSpace(r.scratch.ProjectRoleRequestKey) != requestKey {
		return false
	}
	if maxAge <= 0 {
		return true
	}
	at, ok := parseRFC3339Nano(strings.TrimSpace(r.scratch.ProjectRoleRequestAt))
	if !ok || at == nil {
		return true
	}
	return time.Since(*at) < maxAge
}

func (r *Runtime) recordProjectRoleRequestScratch(ctx context.Context, requestKey, requestID string, missing *projectClaimRoleMissingError) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.ProjectRoleRequestKey = strings.TrimSpace(requestKey)
		state.ProjectRoleRequestID = strings.TrimSpace(requestID)
		state.ProjectRoleRequestAt = now
		state.LastSummary = fmt.Sprintf("Waiting for project role %s from strategic lead %s for task %s.", missing.RoleType, missing.LeadAgentID, missing.TaskID)
	})
}

func (r *Runtime) postProjectRoleRequestUpdate(ctx context.Context, updateType, summary string, missing *projectClaimRoleMissingError, ref string, requiresHuman bool) error {
	if r == nil || r.client == nil {
		return nil
	}
	payload := map[string]any{
		"project_id":       strings.TrimSpace(missing.ProjectID),
		"task_id":          strings.TrimSpace(missing.TaskID),
		"agent_id":         strings.TrimSpace(missing.AgentID),
		"lead_agent_id":    strings.TrimSpace(missing.LeadAgentID),
		"role_type":        strings.TrimSpace(missing.RoleType),
		"write_scope_json": strings.TrimSpace(missing.WriteScopeJSON),
		"ref":              strings.TrimSpace(ref),
	}
	raw, _ := json.Marshal(payload)
	return r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID:   r.cfg.WorkspaceID,
		AgentID:       r.cfg.AgentID,
		UpdateType:    firstNonEmpty(strings.TrimSpace(updateType), "coordination"),
		Summary:       summary,
		PayloadJSON:   string(raw),
		RequiresHuman: requiresHuman,
	})
}
