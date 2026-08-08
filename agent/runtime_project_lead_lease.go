package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"
)

const runtimeProjectLeadRenewBefore = 15 * time.Minute

func (r *Runtime) ensureStrategicLeadLeaseForTask(ctx context.Context, task WorkspaceTaskRecord) error {
	if r == nil || r.client == nil {
		return nil
	}
	projectID := strings.TrimSpace(task.ProjectID)
	if projectID == "" {
		return nil
	}
	// R50 blocker-1(A): the empty-frontier idle-reflection is exactly where the lead decides
	// convergence/DONE, but it carries lane="qa" so runtimeTaskCanStewardProjectLead is false and the
	// lead would never renew its lease while reflecting -> the lease lapses and DONE-authority is lost.
	// Let the INCUMBENT lead (only) renew its lease while running its own idle-reflection. A non-lead's
	// idle-reflection must NOT reach the claim path below (that would self-promote it onto a lapsed
	// lease - an authority leak), so it is filtered after we learn who the active lead is.
	stewardTask := runtimeTaskCanStewardProjectLead(task)
	idleReflectionSteward := !stewardTask && isIdleReflectionTask(task)
	if !stewardTask && !idleReflectionSteward {
		return nil
	}

	coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, projectID)
	if err != nil {
		return fmt.Errorf("get project coordination for strategic lead lease: %w", err)
	}

	now := time.Now().UTC()
	lead := coordination.StrategicLead
	if idleReflectionSteward {
		// Only the incumbent lead renews its lease via an idle-reflection; never promote a non-lead here.
		if lead == nil || strings.TrimSpace(lead.AgentID) != r.cfg.AgentID {
			return nil
		}
	}
	if lead != nil {
		leadAgentID := strings.TrimSpace(lead.AgentID)
		if leadAgentID != "" && leadAgentID != r.cfg.AgentID && projectLeadLeaseActiveUntil(*lead, now) {
			return nil
		}
		if leadAgentID == r.cfg.AgentID && !projectLeadLeaseNeedsRefresh(*lead, now) {
			return nil
		}
	}

	role, err := r.client.ClaimProjectLead(ctx, ProjectLeadClaimInput{
		WorkspaceID:  r.cfg.WorkspaceID,
		ProjectID:    projectID,
		ActorID:      r.cfg.AgentID,
		AgentID:      r.cfg.AgentID,
		LeaseSeconds: defaultProjectLeadLeaseSeconds,
		Summary:      "Runtime renewed strategic lead lease for active project strategy work.",
	})
	if err != nil {
		return fmt.Errorf("claim project strategic lead lease: %w", err)
	}
	log.Printf("[project] strategic lead lease active project=%s role=%s expires_at=%s", projectID, role.RoleID, role.LeaseExpiresAt)
	r.invalidateBootstrap()
	return nil
}

func runtimeTaskCanStewardProjectLead(task WorkspaceTaskRecord) bool {
	if runtimeTaskLooksStrategicLeadRoleScopeRequest(task) {
		return true
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if lane != "" {
		return runtimeProjectLaneIsStrategy(lane)
	}
	return runtimeTaskLooksAutonomousProjectRoot(task)
}

func runtimeTaskLooksStrategicLeadRoleScopeRequest(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" || !strings.HasPrefix(strings.TrimSpace(task.TaskID), "task-role-scope-") {
		return false
	}
	if !runtimeTaskHasTag(task, "project-role-scope") {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if lane != "coordination" && !runtimeProjectLaneIsStrategy(lane) {
		return false
	}
	return runtimeTaskHasProjectRoleScopeAuthorityTransition(task)
}

func runtimeTaskHasTag(task WorkspaceTaskRecord, tag string) bool {
	tag = strings.ToLower(strings.TrimSpace(tag))
	for _, existing := range task.Tags {
		if strings.ToLower(strings.TrimSpace(existing)) == tag {
			return true
		}
	}
	return false
}

func runtimeProjectLaneIsStrategy(lane string) bool {
	switch strings.ToLower(strings.TrimSpace(lane)) {
	case "strategy", "strategic", "planning", "plan", "spec", "specification", "requirements", "design", "framing":
		return true
	default:
		return false
	}
}

// activeProjectLeadID resolves the project's strategic lead and whether its lease is currently active.
// The third return is false when coordination could not be determined (RPC error / no client), so the
// caller can fall through rather than deadlock. Used by FF3 to gate the convergence reflection to the
// lead (only the lead can attest acceptance_coverage and transition DONE).
func (r *Runtime) activeProjectLeadID(ctx context.Context, projectID string, now time.Time) (leadID string, leadActive bool, ok bool) {
	if r == nil || r.client == nil || strings.TrimSpace(projectID) == "" {
		return "", false, false
	}
	coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, projectID)
	if err != nil {
		return "", false, false
	}
	lead := coordination.StrategicLead
	if lead == nil {
		return "", false, true
	}
	return strings.TrimSpace(lead.AgentID), projectLeadLeaseActiveUntil(*lead, now), true
}

func runtimeTaskLooksAutonomousProjectRoot(task WorkspaceTaskRecord) bool {
	kind := strings.ToUpper(strings.TrimSpace(task.TaskKind))
	templateName := strings.ToLower(strings.TrimSpace(task.TaskTemplate))
	if kind != "COORDINATION" && templateName != "integration" {
		return false
	}
	text := strings.ToLower(strings.Join([]string{
		task.Title,
		task.Description,
		strings.Join(task.Tags, " "),
	}, " "))
	for _, needle := range []string{
		"autonomous coordination",
		"strategic agent",
		"create any needed subtasks",
		"coordinate builders",
		"task decomposition",
	} {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func projectLeadLeaseActiveUntil(role ProjectRoleRecord, now time.Time) bool {
	if role.Status != "" && !strings.EqualFold(strings.TrimSpace(role.Status), "ACTIVE") {
		return false
	}
	expiresAt := strings.TrimSpace(role.LeaseExpiresAt)
	if expiresAt == "" {
		return false
	}
	parsed, err := time.Parse(time.RFC3339Nano, expiresAt)
	if err != nil {
		return false
	}
	return parsed.After(now)
}

func projectLeadLeaseNeedsRefresh(role ProjectRoleRecord, now time.Time) bool {
	if !projectLeadLeaseActiveUntil(role, now) {
		return true
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(role.LeaseExpiresAt))
	if err != nil {
		return true
	}
	return !parsed.After(now.Add(runtimeProjectLeadRenewBefore))
}
