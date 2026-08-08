package main

import (
	"encoding/json"
	"strings"
)

const (
	projectRoleScopeAuthorityTransitionSchema = "project_role_scope_authority_transition.v1"
	projectRoleScopeAuthorityTransitionKind   = "project_role_scope_authority_transition"
	projectRoleScopeAuthorityTransitionTool   = "project_role_assign"

	authorityTransitionTerminalBlockerSchema      = "authority_transition_terminal_blocker.v1"
	authorityTransitionGenericRequiredTransition  = "dedicated_authority_transition_receipt"
	authorityTransitionGenericTerminalBlockerKind = "no_fresh_claimable_authority_transition_path"
	authorityTransitionGenericSafetyInvariant     = "no_completion_without_authority_transition_receipt"
)

type authorityTransitionTerminalContract struct {
	RequiredTransition string
	BlockerKind        string
	SafetyInvariant    string
}

func runtimeTaskLooksDedicatedAuthorityTransitionCarrier(task WorkspaceTaskRecord) bool {
	return strings.TrimSpace(task.ProjectID) != "" && authorityTransitionTaskLooksDedicated(task)
}

func runtimeTaskLooksProjectRoleAssignAuthorityTransition(task WorkspaceTaskRecord) bool {
	if runtimeTaskLooksStrategicLeadRoleScopeRequest(task) ||
		runtimeTaskLooksDedicatedProjectRoleScopeAuthorityCarrier(task) ||
		runtimeTaskHasProjectRoleScopeAuthorityTransition(task) {
		return true
	}
	requirements := map[string]any{}
	_ = json.Unmarshal([]byte(strings.TrimSpace(task.TaskRequirementsJSON)), &requirements)
	if strings.EqualFold(taskSubmitRequirementString(requirements, "required_transition"), projectRoleScopeAuthorityTransitionTool) ||
		strings.EqualFold(taskSubmitRequirementString(requirements, "preferred_transition"), projectRoleScopeAuthorityTransitionTool) ||
		strings.EqualFold(taskSubmitRequirementString(requirements, "required_terminal_tool"), projectRoleScopeAuthorityTransitionTool) {
		return true
	}
	if strings.EqualFold(taskSubmitRequirementString(requirements, "authority_transition_kind"), "project_role_scope") {
		return true
	}
	if !runtimeTaskLooksDedicatedAuthorityTransitionCarrier(task) {
		return false
	}
	if runtimeTaskHasTag(task, "project-role-scope") || runtimeTaskHasTag(task, "role-scope") || runtimeTaskHasTag(task, "scope-grant") {
		return true
	}
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		strings.Join(task.Tags, " "),
		task.TaskRequirementsJSON,
	}, "\n"))
	return containsAnySignal(text, []string{
		"task-role-scope",
		"role-scope",
		"role/scope",
		"role scope",
		"scope grant",
		"scope-grant",
		"project_role_assign",
	})
}

func authorityTransitionTerminalContractForTask(task WorkspaceTaskRecord) authorityTransitionTerminalContract {
	if runtimeTaskLooksProjectRoleAssignAuthorityTransition(task) {
		return authorityTransitionTerminalContract{
			RequiredTransition: projectRoleScopeAuthorityTransitionTool,
			BlockerKind:        "no_fresh_claimable_scope_grant_path",
			SafetyInvariant:    "no_completion_without_project_role_assign_receipt",
		}
	}
	required := authorityTransitionRequiredTransitionFromTask(task)
	if required == "" {
		required = authorityTransitionGenericRequiredTransition
	}
	return authorityTransitionTerminalContract{
		RequiredTransition: required,
		BlockerKind:        authorityTransitionGenericTerminalBlockerKind,
		SafetyInvariant:    authorityTransitionGenericSafetyInvariant,
	}
}

func authorityTransitionRequiredTransitionFromTask(task WorkspaceTaskRecord) string {
	var requirements map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(task.TaskRequirementsJSON)), &requirements); err == nil {
		for _, key := range []string{"required_transition", "preferred_transition", "required_terminal_tool"} {
			if value := sanitizeAuthorityTransitionName(taskSubmitRequirementString(requirements, key)); value != "" {
				return value
			}
		}
	}
	return ""
}

func sanitizeAuthorityTransitionName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 96 {
		return ""
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			continue
		}
		return ""
	}
	return value
}

func runtimeTaskHasProjectRoleScopeAuthorityTransition(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(task.TaskID)), "task-role-scope-") {
		return false
	}
	if !runtimeTaskHasTag(task, "project-role-scope") {
		return false
	}
	return projectRoleScopeAuthorityTransitionRequirementsRaw(task.TaskRequirementsJSON)
}

func runtimeTaskLooksDedicatedProjectRoleScopeAuthorityCarrier(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(task.TaskID)), "task-role-scope-") {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.TaskKind), "COORDINATION") {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	return lane == "coordination" || runtimeProjectLaneIsStrategy(lane)
}

func projectRoleScopeAuthorityTransitionRequirementsRaw(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" {
		return false
	}
	var requirements map[string]any
	if err := json.Unmarshal([]byte(raw), &requirements); err != nil {
		return false
	}
	return projectRoleScopeAuthorityTransitionRequirements(requirements)
}

func projectRoleScopeAuthorityTransitionRequirements(requirements map[string]any) bool {
	if len(requirements) == 0 {
		return false
	}
	if strings.EqualFold(taskSubmitRequirementString(requirements, "schema"), projectRoleScopeAuthorityTransitionSchema) {
		return true
	}
	if strings.EqualFold(taskSubmitRequirementString(requirements, "project_role_scope_authority_transition"), projectRoleScopeAuthorityTransitionSchema) {
		return true
	}
	if strings.EqualFold(taskSubmitRequirementString(requirements, "task_requirement_kind"), projectRoleScopeAuthorityTransitionKind) {
		return true
	}
	if strings.EqualFold(taskSubmitRequirementString(requirements, "authority_transition_kind"), "project_role_scope") &&
		strings.EqualFold(taskSubmitRequirementString(requirements, "required_transition"), projectRoleScopeAuthorityTransitionTool) {
		return true
	}
	if strings.EqualFold(taskSubmitRequirementString(requirements, "authority_transition_kind"), "project_role_scope") &&
		strings.EqualFold(taskSubmitRequirementString(requirements, "preferred_transition"), projectRoleScopeAuthorityTransitionTool) {
		return true
	}
	return false
}

func taskSubmitRequirementsWithProjectRoleScopeAuthorityTransition(requirements map[string]any, taskID, taskKind, projectID, projectLane string, tags []string) map[string]any {
	if !taskSubmitLooksProjectRoleScopeAuthorityTransition(taskID, taskKind, projectID, projectLane, tags) {
		return requirements
	}
	if requirements == nil {
		requirements = map[string]any{}
	}
	requirements["schema"] = projectRoleScopeAuthorityTransitionSchema
	requirements["project_role_scope_authority_transition"] = projectRoleScopeAuthorityTransitionSchema
	requirements["task_requirement_kind"] = projectRoleScopeAuthorityTransitionKind
	requirements["authority_transition_kind"] = "project_role_scope"
	if strings.TrimSpace(taskSubmitRequirementString(requirements, "required_transition")) == "" {
		requirements["required_transition"] = projectRoleScopeAuthorityTransitionTool
	}
	return requirements
}

func taskSubmitLooksProjectRoleScopeAuthorityTransition(taskID, taskKind, projectID, projectLane string, tags []string) bool {
	if strings.TrimSpace(projectID) == "" {
		return false
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(taskID)), "task-role-scope-") {
		return false
	}
	if !taskSubmitTagsContain(tags, "project-role-scope") {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(taskKind), "COORDINATION") {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(projectLane))
	return lane == "coordination" || runtimeProjectLaneIsStrategy(lane)
}

func taskSubmitTagsContain(tags []string, want string) bool {
	want = strings.ToLower(strings.TrimSpace(want))
	if want == "" {
		return false
	}
	for _, tag := range tags {
		if strings.ToLower(strings.TrimSpace(tag)) == want {
			return true
		}
	}
	return false
}
