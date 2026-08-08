package main

import (
	"context"
	"fmt"
	"strings"
)

// projectImplementationFanoutDirective returns a deterministic next-action
// advisory for the strategic lead after IMPLEMENTATION opens but no concrete
// implementation/code task exists yet. This is the coordinator-side companion
// to G-SUB-1: a lead may correctly plan, document, and open the phase, then
// yield continue without the state transition that actually exposes work to
// peers: task_submit.
func (r *Runtime) projectImplementationFanoutDirective(ctx context.Context, task WorkspaceTaskRecord) string {
	if r == nil || r.client == nil {
		return ""
	}
	coordination, ok := r.projectImplementationFanoutCoordination(ctx, task)
	if !ok {
		return ""
	}
	if !projectCoordinationHasActiveStrategicLead(coordination, r.cfg.AgentID) {
		return ""
	}
	if !runtimeProjectPhaseAllowsImplementationWork(coordination.Profile.CurrentPhase) {
		return ""
	}
	if !projectImplementationFanoutRepoReady(coordination.Profile) {
		return ""
	}
	if projectImplementationFanoutConcreteTaskCount(coordination, task.TaskID) > 0 {
		return ""
	}
	projectID := firstNonEmpty(coordination.Profile.ProjectID, coordination.Project.ProjectID, task.ProjectID, "<project>")
	phase := firstNonEmpty(coordination.Profile.CurrentPhase, coordination.GateStatus.CurrentPhase, "IMPLEMENTATION")
	repoStatus := firstNonEmpty(coordination.Profile.RepoStatus, "UNKNOWN")
	return fmt.Sprintf("IMPLEMENTATION FANOUT REQUIRED: project %s is in phase %s with repo_status=%s, but there are no open concrete implementation/code tasks for peers to claim. Your next action must be task_submit for bounded product/code deliverables with project_id=%s, project_lane=implementation, acceptance-criteria mapping, and repository-relative write_scope_hints. Do not keep reading/writing docs, polling patch queue, or posting status until at least one real implementation task exists.", projectID, phase, repoStatus, projectID)
}

func (r *Runtime) projectImplementationFanoutCoordination(ctx context.Context, task WorkspaceTaskRecord) (ProjectCoordinationRecord, bool) {
	workspaceID := strings.TrimSpace(r.cfg.WorkspaceID)
	projectID := strings.TrimSpace(task.ProjectID)
	if workspaceID == "" {
		return ProjectCoordinationRecord{}, false
	}
	if projectID != "" {
		coordination, err := r.client.GetProjectCoordination(ctx, workspaceID, projectID)
		if err != nil {
			return ProjectCoordinationRecord{}, false
		}
		return coordination, true
	}
	projects, err := r.client.ListProjects(ctx, workspaceID)
	if err != nil {
		return ProjectCoordinationRecord{}, false
	}
	for _, project := range projects {
		if !projectImplementationFanoutProjectActive(project) {
			continue
		}
		coordination, err := r.client.GetProjectCoordination(ctx, workspaceID, project.ProjectID)
		if err != nil {
			continue
		}
		if !projectCoordinationHasActiveStrategicLead(coordination, r.cfg.AgentID) {
			continue
		}
		if !runtimeProjectPhaseAllowsImplementationWork(coordination.Profile.CurrentPhase) {
			continue
		}
		return coordination, true
	}
	return ProjectCoordinationRecord{}, false
}

func projectImplementationFanoutProjectActive(project ProjectRecord) bool {
	switch strings.ToUpper(strings.TrimSpace(project.Status)) {
	case "", "ACTIVE", "OPEN", "RUNNING":
		return true
	default:
		return false
	}
}

func projectCoordinationHasActiveStrategicLead(coordination ProjectCoordinationRecord, agentID string) bool {
	if coordination.StrategicLead == nil {
		return false
	}
	if strings.TrimSpace(coordination.StrategicLead.AgentID) != strings.TrimSpace(agentID) {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(coordination.StrategicLead.Status), "ACTIVE")
}

func projectImplementationFanoutRepoReady(profile ProjectProfileRecord) bool {
	if !profile.RepoRequired {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(profile.RepoStatus), "READY")
}

func projectImplementationFanoutConcreteTaskCount(coordination ProjectCoordinationRecord, activeTaskID string) int {
	activeTaskID = strings.TrimSpace(activeTaskID)
	count := 0
	for _, task := range coordination.Tasks {
		if activeTaskID != "" && strings.TrimSpace(task.TaskID) == activeTaskID {
			continue
		}
		if taskSubmitTaskIsTerminal(task) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(task.TaskKind), "EXECUTION") {
			continue
		}
		if !runtimeProjectLaneRequiresImplementationGate(task.ProjectLane) {
			continue
		}
		count++
	}
	return count
}

// traceNeedsProjectImplementationFanoutDirective keeps the coordination lookup
// off the hot path unless this cycle looked like fanout-adjacent work and did
// not already create the required task receipt.
func traceNeedsProjectImplementationFanoutDirective(trace *TaskRunTrace) bool {
	if trace == nil {
		return false
	}
	sawFanoutSurface := false
	for _, toolName := range trace.SuccessfulToolCalls {
		switch strings.ToLower(strings.TrimSpace(toolName)) {
		case "task_submit":
			return false
		case "project_phase_transition",
			"project_bootstrap",
			"project_coordination_get",
			"workspace_doc_get",
			"workspace_doc_put",
			"project_patch_queue_list",
			"agent_task_hydrate":
			sawFanoutSurface = true
		}
	}
	return sawFanoutSurface
}
