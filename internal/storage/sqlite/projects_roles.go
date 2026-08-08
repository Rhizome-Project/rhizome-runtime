package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

const (
	ProjectRoleStrategicLead = "STRATEGIC_LEAD"
	ProjectRolePlanner       = "PLANNER"
	ProjectRoleImplementer   = "IMPLEMENTER"
	ProjectRoleReviewer      = "REVIEWER"
	ProjectRoleIntegrator    = "INTEGRATOR"
	ProjectRoleObserver      = "OBSERVER"

	ProjectRoleStatusActive   = "ACTIVE"
	ProjectRoleStatusReleased = "RELEASED"
	ProjectRoleStatusExpired  = "EXPIRED"
)

var (
	ErrProjectLeadRequired           = errors.New("active project strategic lead required")
	ErrProjectLeadConflict           = errors.New("active project strategic lead already held")
	ErrProjectLeadMismatch           = errors.New("project strategic lead held by different agent")
	ErrProjectRoleNotFound           = errors.New("project role not found")
	ErrProjectRoleWriteScopeConflict = errors.New("project implementation role write scope conflicts with live owner")
	ErrProjectRoleWriteScopeRequired = errors.New("project implementation role requires write scope")
)

type ProjectRoleRecord struct {
	RoleID         string `json:"role_id"`
	WorkspaceID    string `json:"workspace_id"`
	ProjectID      string `json:"project_id"`
	AgentID        string `json:"agent_id"`
	RoleType       string `json:"role_type"`
	Status         string `json:"status"`
	WriteScopeJSON string `json:"write_scope_json,omitempty"`
	LeaseToken     string `json:"lease_token,omitempty"`
	LeaseExpiresAt string `json:"lease_expires_at,omitempty"`
	Summary        string `json:"summary,omitempty"`
	ClaimedAt      string `json:"claimed_at,omitempty"`
	ReleasedAt     string `json:"released_at,omitempty"`
	UpdatedBy      string `json:"updated_by,omitempty"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

type ProjectLeadClaimInput struct {
	WorkspaceID  string
	ProjectID    string
	AgentID      string
	ActorID      string
	ActorType    string
	LeaseSeconds int
	LeaseToken   string
	Summary      string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectLeadRenewInput struct {
	WorkspaceID  string
	ProjectID    string
	RoleID       string
	ActorID      string
	ActorType    string
	LeaseSeconds int
	LeaseToken   string
	Summary      string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectLeadReleaseInput struct {
	WorkspaceID string
	ProjectID   string
	RoleID      string
	ActorID     string
	ActorType   string
	LeaseToken  string
	Summary     string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectLeadTransferInput struct {
	WorkspaceID  string
	ProjectID    string
	RoleID       string
	ToAgentID    string
	ActorID      string
	ActorType    string
	LeaseSeconds int
	LeaseToken   string
	Summary      string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectRoleAssignInput struct {
	WorkspaceID           string
	ProjectID             string
	AgentID               string
	RoleType              string
	WriteScopeJSON        string
	ActorID               string
	ActorType             string
	Summary               string
	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

func normalizeProjectRoleType(value string) (string, error) {
	roleType := strings.ToUpper(strings.TrimSpace(value))
	roleType = strings.ReplaceAll(roleType, "-", "_")
	roleType = strings.ReplaceAll(roleType, " ", "_")
	if roleType == "LEAD" || roleType == "STRATEGIC" {
		roleType = ProjectRoleStrategicLead
	}
	switch roleType {
	case ProjectRoleStrategicLead, ProjectRolePlanner, ProjectRoleImplementer, ProjectRoleReviewer, ProjectRoleIntegrator, ProjectRoleObserver:
		return roleType, nil
	default:
		return "", fmt.Errorf("invalid project role_type: %s", value)
	}
}

func normalizeProjectRoleStatus(value string) string {
	status := strings.ToUpper(strings.TrimSpace(value))
	switch status {
	case ProjectRoleStatusActive, ProjectRoleStatusReleased, ProjectRoleStatusExpired:
		return status
	default:
		return ProjectRoleStatusActive
	}
}

func normalizeProjectRoleWriteScope(roleType, raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "{}"
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return "", fmt.Errorf("invalid write_scope_json: %w", err)
	}
	paths, err := validateWriteScopeJSONPathset(raw)
	if err != nil {
		return "", err
	}
	if roleType == ProjectRoleImplementer && (projectWriteScopeEmpty(decoded) || len(paths) == 0) {
		return "", ErrProjectRoleWriteScopeRequired
	}
	return raw, nil
}

func projectWriteScopeEmpty(decoded any) bool {
	switch typed := decoded.(type) {
	case map[string]any:
		return len(typed) == 0
	case []any:
		return len(typed) == 0
	case nil:
		return true
	default:
		return false
	}
}

func projectLeadLeaseUntil(now time.Time, seconds int) string {
	if seconds <= 0 {
		seconds = 15 * 60
	}
	if seconds > 24*60*60 {
		seconds = 24 * 60 * 60
	}
	return now.Add(time.Duration(seconds) * time.Second).UTC().Format(time.RFC3339Nano)
}

func projectRoleLeaseActiveAt(role ProjectRoleRecord, referenceAt string) bool {
	if role.Status != ProjectRoleStatusActive {
		return false
	}
	if role.RoleType != ProjectRoleStrategicLead || strings.TrimSpace(role.LeaseExpiresAt) == "" {
		return true
	}
	referenceTS, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(referenceAt))
	if err != nil {
		return false
	}
	leaseTS, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(role.LeaseExpiresAt))
	if err != nil {
		return false
	}
	return leaseTS.After(referenceTS)
}

func (s *Store) ClaimProjectStrategicLeadWithEvent(ctx context.Context, input ProjectLeadClaimInput) (ProjectRoleRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	agentID := strings.TrimSpace(input.AgentID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || agentID == "" {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, and agent_id are required")
	}
	if actorID == "" {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}

	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	leaseToken := firstNonEmpty(strings.TrimSpace(input.LeaseToken), nextID("projleadlease"))
	leaseExpiresAt := projectLeadLeaseUntil(nowTime, input.LeaseSeconds)
	return s.mutateProjectRoleWithEvent(ctx, workspaceID, projectID, actorID, firstNonEmpty(strings.TrimSpace(input.ActorType), "human"), firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.lead.claim"), input.PromptContextEnvelope, func(tx *sql.Tx) (ProjectRoleRecord, string, map[string]any, error) {
		if err := s.ensureProjectRoleScopeTx(ctx, tx, workspaceID, projectID, agentID); err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		if err := s.expireProjectStrategicLeadTx(ctx, tx, workspaceID, projectID, now); err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		active, ok, err := s.getActiveProjectStrategicLeadTx(ctx, tx, workspaceID, projectID, now)
		if err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		if ok && active.AgentID != agentID {
			return ProjectRoleRecord{}, "", nil, fmt.Errorf("%w: %s", ErrProjectLeadConflict, active.AgentID)
		}
		if ok {
			if _, err := tx.ExecContext(ctx, `
UPDATE project_agent_roles
   SET lease_token = ?, lease_expires_at = ?, summary = ?, updated_by = ?, updated_at = ?
 WHERE role_id = ?`,
				leaseToken, leaseExpiresAt, strings.TrimSpace(input.Summary), actorID, now, active.RoleID); err != nil {
				return ProjectRoleRecord{}, "", nil, fmt.Errorf("renew project lead claim: %w", err)
			}
			role, err := s.getProjectRoleTx(ctx, tx, workspaceID, projectID, active.RoleID)
			return role, "project.lead.claimed", projectRoleEventPayload(role, "lead.claim", actorID), err
		}
		role := ProjectRoleRecord{
			RoleID:         nextID("projrole"),
			WorkspaceID:    workspaceID,
			ProjectID:      projectID,
			AgentID:        agentID,
			RoleType:       ProjectRoleStrategicLead,
			Status:         ProjectRoleStatusActive,
			WriteScopeJSON: "{}",
			LeaseToken:     leaseToken,
			LeaseExpiresAt: leaseExpiresAt,
			Summary:        strings.TrimSpace(input.Summary),
			ClaimedAt:      now,
			UpdatedBy:      actorID,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		if err := insertProjectRoleTx(ctx, tx, role); err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		return role, "project.lead.claimed", projectRoleEventPayload(role, "lead.claim", actorID), nil
	})
}

func (s *Store) RenewProjectStrategicLeadWithEvent(ctx context.Context, input ProjectLeadRenewInput) (ProjectRoleRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	roleID := strings.TrimSpace(input.RoleID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || roleID == "" || actorID == "" {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, role_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	leaseExpiresAt := projectLeadLeaseUntil(nowTime, input.LeaseSeconds)
	return s.mutateProjectRoleWithEvent(ctx, workspaceID, projectID, actorID, firstNonEmpty(strings.TrimSpace(input.ActorType), "human"), firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.lead.renew"), input.PromptContextEnvelope, func(tx *sql.Tx) (ProjectRoleRecord, string, map[string]any, error) {
		role, err := s.getProjectRoleTx(ctx, tx, workspaceID, projectID, roleID)
		if err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		if role.RoleType != ProjectRoleStrategicLead || !projectRoleLeaseActiveAt(role, now) {
			return ProjectRoleRecord{}, "", nil, ErrProjectLeadRequired
		}
		if err := requireProjectLeadActor(role, actorID, input.ActorType); err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		if token := strings.TrimSpace(input.LeaseToken); role.LeaseToken != "" && token != role.LeaseToken {
			return ProjectRoleRecord{}, "", nil, ErrProjectLeadMismatch
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE project_agent_roles
   SET lease_expires_at = ?, summary = ?, updated_by = ?, updated_at = ?
 WHERE role_id = ?`,
			leaseExpiresAt, firstNonEmpty(strings.TrimSpace(input.Summary), role.Summary), actorID, now, roleID); err != nil {
			return ProjectRoleRecord{}, "", nil, fmt.Errorf("renew project lead: %w", err)
		}
		role, err = s.getProjectRoleTx(ctx, tx, workspaceID, projectID, roleID)
		return role, "project.lead.renewed", projectRoleEventPayload(role, "lead.renew", actorID), err
	})
}

func (s *Store) ReleaseProjectStrategicLeadWithEvent(ctx context.Context, input ProjectLeadReleaseInput) (ProjectRoleRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	roleID := strings.TrimSpace(input.RoleID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || roleID == "" || actorID == "" {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, role_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return s.mutateProjectRoleWithEvent(ctx, workspaceID, projectID, actorID, firstNonEmpty(strings.TrimSpace(input.ActorType), "human"), firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.lead.release"), input.PromptContextEnvelope, func(tx *sql.Tx) (ProjectRoleRecord, string, map[string]any, error) {
		role, err := s.getProjectRoleTx(ctx, tx, workspaceID, projectID, roleID)
		if err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		if role.RoleType != ProjectRoleStrategicLead {
			return ProjectRoleRecord{}, "", nil, ErrProjectRoleNotFound
		}
		if err := requireProjectLeadActor(role, actorID, input.ActorType); err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		if token := strings.TrimSpace(input.LeaseToken); role.LeaseToken != "" && token != role.LeaseToken {
			return ProjectRoleRecord{}, "", nil, ErrProjectLeadMismatch
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE project_agent_roles
   SET status = ?, released_at = ?, summary = ?, updated_by = ?, updated_at = ?
 WHERE role_id = ?`,
			ProjectRoleStatusReleased, now, firstNonEmpty(strings.TrimSpace(input.Summary), role.Summary), actorID, now, roleID); err != nil {
			return ProjectRoleRecord{}, "", nil, fmt.Errorf("release project lead: %w", err)
		}
		role, err = s.getProjectRoleTx(ctx, tx, workspaceID, projectID, roleID)
		return role, "project.lead.released", projectRoleEventPayload(role, "lead.release", actorID), err
	})
}

func (s *Store) TransferProjectStrategicLeadWithEvent(ctx context.Context, input ProjectLeadTransferInput) (ProjectRoleRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	roleID := strings.TrimSpace(input.RoleID)
	toAgentID := strings.TrimSpace(input.ToAgentID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || roleID == "" || toAgentID == "" || actorID == "" {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, role_id, to_agent_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	leaseToken := firstNonEmpty(strings.TrimSpace(input.LeaseToken), nextID("projleadlease"))
	leaseExpiresAt := projectLeadLeaseUntil(nowTime, input.LeaseSeconds)
	return s.mutateProjectRoleWithEvent(ctx, workspaceID, projectID, actorID, firstNonEmpty(strings.TrimSpace(input.ActorType), "human"), firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.lead.transfer"), input.PromptContextEnvelope, func(tx *sql.Tx) (ProjectRoleRecord, string, map[string]any, error) {
		role, err := s.getProjectRoleTx(ctx, tx, workspaceID, projectID, roleID)
		if err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		if role.RoleType != ProjectRoleStrategicLead || !projectRoleLeaseActiveAt(role, now) {
			return ProjectRoleRecord{}, "", nil, ErrProjectLeadRequired
		}
		if err := requireProjectLeadActor(role, actorID, input.ActorType); err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		if err := s.ensureProjectRoleScopeTx(ctx, tx, workspaceID, projectID, toAgentID); err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE project_agent_roles
   SET status = ?, released_at = ?, summary = ?, updated_by = ?, updated_at = ?
 WHERE role_id = ?`,
			ProjectRoleStatusReleased, now, firstNonEmpty(strings.TrimSpace(input.Summary), role.Summary), actorID, now, roleID); err != nil {
			return ProjectRoleRecord{}, "", nil, fmt.Errorf("release transferred project lead: %w", err)
		}
		next := ProjectRoleRecord{
			RoleID:         nextID("projrole"),
			WorkspaceID:    workspaceID,
			ProjectID:      projectID,
			AgentID:        toAgentID,
			RoleType:       ProjectRoleStrategicLead,
			Status:         ProjectRoleStatusActive,
			WriteScopeJSON: "{}",
			LeaseToken:     leaseToken,
			LeaseExpiresAt: leaseExpiresAt,
			Summary:        strings.TrimSpace(input.Summary),
			ClaimedAt:      now,
			UpdatedBy:      actorID,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		if err := insertProjectRoleTx(ctx, tx, next); err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		return next, "project.lead.transferred", projectRoleEventPayload(next, "lead.transfer", actorID), nil
	})
}

func (s *Store) AssignProjectRoleWithEvent(ctx context.Context, input ProjectRoleAssignInput) (ProjectRoleRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	agentID := strings.TrimSpace(input.AgentID)
	actorID := strings.TrimSpace(input.ActorID)
	roleType, err := normalizeProjectRoleType(input.RoleType)
	if err != nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, err
	}
	if roleType == ProjectRoleStrategicLead {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, errors.New("use project.lead.* methods for strategic lead role")
	}
	writeScopeJSON, err := normalizeProjectRoleWriteScope(roleType, input.WriteScopeJSON)
	if err != nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, err
	}
	if workspaceID == "" || projectID == "" || agentID == "" || actorID == "" {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, agent_id, and actor_id are required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return s.mutateProjectRoleWithEvent(ctx, workspaceID, projectID, actorID, firstNonEmpty(strings.TrimSpace(input.ActorType), "human"), firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.role.assign"), input.PromptContextEnvelope, func(tx *sql.Tx) (ProjectRoleRecord, string, map[string]any, error) {
		if err := s.ensureProjectRoleScopeTx(ctx, tx, workspaceID, projectID, agentID); err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		if strings.EqualFold(strings.TrimSpace(input.ActorType), "agent") {
			lead, err := s.activeProjectStrategicLeadTx(ctx, tx, workspaceID, projectID, now)
			if err != nil {
				return ProjectRoleRecord{}, "", nil, err
			}
			if err := requireProjectLeadActor(lead, actorID, input.ActorType); err != nil {
				return ProjectRoleRecord{}, "", nil, err
			}
		}
		role := ProjectRoleRecord{
			RoleID:         nextID("projrole"),
			WorkspaceID:    workspaceID,
			ProjectID:      projectID,
			AgentID:        agentID,
			RoleType:       roleType,
			Status:         ProjectRoleStatusActive,
			WriteScopeJSON: writeScopeJSON,
			Summary:        strings.TrimSpace(input.Summary),
			ClaimedAt:      now,
			UpdatedBy:      actorID,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		if err := s.ensureProjectRoleImplementerScopeAvailableTx(ctx, tx, role); err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		if err := insertProjectRoleTx(ctx, tx, role); err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		supersededRoleIDs, err := s.releaseSupersededActiveProjectRolesTx(ctx, tx, role, actorID, now)
		if err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		rebindPayload, err := s.rebindSingleActiveProjectImplementationClaimToRoleTx(ctx, tx, role, actorID, now)
		if err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		resolvedRoleScopeTaskIDs, err := s.resolveProjectRoleScopeAuthorityTasksForRoleAssignTx(ctx, tx, role, rebindPayload, actorID, now)
		if err != nil {
			return ProjectRoleRecord{}, "", nil, err
		}
		payload := projectRoleEventPayload(role, "role.assign", actorID)
		if len(supersededRoleIDs) > 0 {
			payload["superseded_role_ids"] = supersededRoleIDs
		}
		if rebindPayload != nil {
			payload["active_claim_rebind"] = rebindPayload
		}
		if len(resolvedRoleScopeTaskIDs) > 0 {
			payload["role_scope_authority_transition_lifecycle"] = "resolved_no_longer_blocking"
			payload["resolved_project_role_scope_authority_task_ids"] = resolvedRoleScopeTaskIDs
		}
		return role, "project.role.assigned", payload, nil
	})
}

func (s *Store) ListProjectRoles(ctx context.Context, workspaceID, projectID string, includeInactive bool) ([]ProjectRoleRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	if workspaceID == "" || projectID == "" {
		return nil, errors.New("workspace_id and project_id are required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	query := `
SELECT role_id, workspace_id, project_id, agent_id, role_type, status, write_scope_json, lease_token,
       lease_expires_at, summary, claimed_at, released_at, updated_by, created_at, updated_at
  FROM project_agent_roles
 WHERE workspace_id = ? AND project_id = ?`
	args := []any{workspaceID, projectID}
	if !includeInactive {
		query += ` AND status = 'ACTIVE' AND (role_type <> 'STRATEGIC_LEAD' OR lease_expires_at = '' OR lease_expires_at > ?)`
		args = append(args, now)
	}
	query += ` ORDER BY CASE role_type WHEN 'STRATEGIC_LEAD' THEN 0 WHEN 'PLANNER' THEN 1 WHEN 'IMPLEMENTER' THEN 2 WHEN 'REVIEWER' THEN 3 ELSE 4 END, created_at ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list project roles: %w", err)
	}
	defer rows.Close()
	var roles []ProjectRoleRecord
	for rows.Next() {
		role, err := scanProjectRole(rows)
		if err != nil {
			return nil, err
		}
		roles = append(roles, role)
	}
	return roles, rows.Err()
}

func (s *Store) GetActiveProjectStrategicLead(ctx context.Context, workspaceID, projectID string) (ProjectRoleRecord, bool, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	role, ok, err := s.getActiveProjectStrategicLeadExact(ctx, workspaceID, projectID, now)
	if err != nil || ok {
		return role, ok, err
	}
	canonicalProjectID, err := s.resolveProjectIDWithQuerier(ctx, s.writeDB, workspaceID, projectID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectRoleRecord{}, false, nil
		}
		return ProjectRoleRecord{}, false, err
	}
	if canonicalProjectID == projectID {
		return ProjectRoleRecord{}, false, nil
	}
	return s.getActiveProjectStrategicLeadExact(ctx, workspaceID, canonicalProjectID, now)
}

func (s *Store) getActiveProjectStrategicLeadExact(ctx context.Context, workspaceID, projectID, now string) (ProjectRoleRecord, bool, error) {
	row := s.writeDB.QueryRowContext(ctx, `
SELECT role_id, workspace_id, project_id, agent_id, role_type, status, write_scope_json, lease_token,
       lease_expires_at, summary, claimed_at, released_at, updated_by, created_at, updated_at
  FROM project_agent_roles
 WHERE workspace_id = ? AND project_id = ? AND role_type = ? AND status = ? AND lease_expires_at > ?
 ORDER BY lease_expires_at DESC, updated_at DESC
 LIMIT 1`,
		workspaceID, projectID, ProjectRoleStrategicLead, ProjectRoleStatusActive, now)
	role, err := scanProjectRole(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectRoleRecord{}, false, nil
		}
		return ProjectRoleRecord{}, false, err
	}
	return role, true, nil
}

func (s *Store) activeProjectStrategicLeadTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, at string) (ProjectRoleRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT role_id, workspace_id, project_id, agent_id, role_type, status, write_scope_json, lease_token,
       lease_expires_at, summary, claimed_at, released_at, updated_by, created_at, updated_at
  FROM project_agent_roles
 WHERE workspace_id = ? AND project_id = ? AND role_type = ? AND status = ? AND lease_expires_at > ?
 ORDER BY lease_expires_at DESC, updated_at DESC
 LIMIT 1`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), ProjectRoleStrategicLead, ProjectRoleStatusActive, strings.TrimSpace(at))
	role, err := scanProjectRole(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectRoleRecord{}, ErrProjectLeadRequired
		}
		return ProjectRoleRecord{}, err
	}
	return role, nil
}

func (s *Store) mutateProjectRoleWithEvent(ctx context.Context, workspaceID, projectID, actorID, actorType, surface string, envelope map[string]any, mutate func(tx *sql.Tx) (ProjectRoleRecord, string, map[string]any, error)) (ProjectRoleRecord, RuntimeEventRecord, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project role tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var role ProjectRoleRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		mutated, eventType, payload, err := mutate(tx)
		if err != nil {
			return err
		}
		role = mutated
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project role mutation"); err != nil {
			return err
		}
		payload, err = attachProjectPromptContextEnvelope(payload, envelope, surface, map[string]string{
			"workspace_id":   workspaceID,
			"project_id":     projectID,
			"actor_id":       actorID,
			"agent_id":       role.AgentID,
			"role_id":        role.RoleID,
			"principal_type": actorType,
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   eventType,
			EntityType:  "project_role",
			EntityID:    role.RoleID,
			ActorType:   actorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectRoleRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project role tx: %w", err)
	}
	return role, event, nil
}

func (s *Store) ensureProjectRoleScopeTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, agentID string) error {
	if err := s.ensureProjectProfileTx(ctx, tx, workspaceID, projectID, agentID, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, agentID); err != nil {
		return err
	}
	return nil
}

func (s *Store) expireProjectStrategicLeadTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, now string) error {
	if _, err := tx.ExecContext(ctx, `
UPDATE project_agent_roles
   SET status = ?, updated_at = ?
 WHERE workspace_id = ? AND project_id = ? AND role_type = ? AND status = ? AND lease_expires_at <> '' AND lease_expires_at <= ?`,
		ProjectRoleStatusExpired, now, workspaceID, projectID, ProjectRoleStrategicLead, ProjectRoleStatusActive, now); err != nil {
		return fmt.Errorf("expire project strategic lead: %w", err)
	}
	return nil
}

func (s *Store) getActiveProjectStrategicLeadTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, now string) (ProjectRoleRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT role_id, workspace_id, project_id, agent_id, role_type, status, write_scope_json, lease_token,
       lease_expires_at, summary, claimed_at, released_at, updated_by, created_at, updated_at
  FROM project_agent_roles
 WHERE workspace_id = ? AND project_id = ? AND role_type = ? AND status = ? AND lease_expires_at > ?
 ORDER BY lease_expires_at DESC, updated_at DESC
 LIMIT 1`,
		workspaceID, projectID, ProjectRoleStrategicLead, ProjectRoleStatusActive, now)
	role, err := scanProjectRole(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectRoleRecord{}, false, nil
		}
		return ProjectRoleRecord{}, false, err
	}
	return role, true, nil
}

func (s *Store) getProjectRoleTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, roleID string) (ProjectRoleRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT role_id, workspace_id, project_id, agent_id, role_type, status, write_scope_json, lease_token,
       lease_expires_at, summary, claimed_at, released_at, updated_by, created_at, updated_at
  FROM project_agent_roles
 WHERE workspace_id = ? AND project_id = ? AND role_id = ?`,
		workspaceID, projectID, roleID)
	role, err := scanProjectRole(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectRoleRecord{}, ErrProjectRoleNotFound
		}
		return ProjectRoleRecord{}, err
	}
	return role, nil
}

type projectRoleScanner interface {
	Scan(dest ...any) error
}

func scanProjectRole(row projectRoleScanner) (ProjectRoleRecord, error) {
	var role ProjectRoleRecord
	if err := row.Scan(
		&role.RoleID,
		&role.WorkspaceID,
		&role.ProjectID,
		&role.AgentID,
		&role.RoleType,
		&role.Status,
		&role.WriteScopeJSON,
		&role.LeaseToken,
		&role.LeaseExpiresAt,
		&role.Summary,
		&role.ClaimedAt,
		&role.ReleasedAt,
		&role.UpdatedBy,
		&role.CreatedAt,
		&role.UpdatedAt,
	); err != nil {
		return ProjectRoleRecord{}, err
	}
	role.RoleType, _ = normalizeProjectRoleType(role.RoleType)
	role.Status = normalizeProjectRoleStatus(role.Status)
	return role, nil
}

func insertProjectRoleTx(ctx context.Context, tx *sql.Tx, role ProjectRoleRecord) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO project_agent_roles (
  role_id, workspace_id, project_id, agent_id, role_type, status, write_scope_json, lease_token,
  lease_expires_at, summary, claimed_at, released_at, updated_by, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		role.RoleID, role.WorkspaceID, role.ProjectID, role.AgentID, role.RoleType, role.Status, role.WriteScopeJSON, role.LeaseToken,
		role.LeaseExpiresAt, role.Summary, role.ClaimedAt, role.ReleasedAt, role.UpdatedBy, role.CreatedAt, role.UpdatedAt); err != nil {
		return fmt.Errorf("insert project role: %w", err)
	}
	return nil
}

func (s *Store) releaseSupersededActiveProjectRolesTx(ctx context.Context, tx *sql.Tx, role ProjectRoleRecord, actorID, now string) ([]string, error) {
	roleType := strings.TrimSpace(role.RoleType)
	if roleType == "" || roleType == ProjectRoleStrategicLead {
		return nil, nil
	}
	rows, err := tx.QueryContext(ctx, `
SELECT role_id
  FROM project_agent_roles
 WHERE workspace_id = ?
   AND project_id = ?
   AND agent_id = ?
   AND role_type = ?
   AND status = ?
   AND role_id <> ?
 ORDER BY created_at ASC, role_id ASC`,
		strings.TrimSpace(role.WorkspaceID),
		strings.TrimSpace(role.ProjectID),
		strings.TrimSpace(role.AgentID),
		roleType,
		ProjectRoleStatusActive,
		strings.TrimSpace(role.RoleID))
	if err != nil {
		return nil, fmt.Errorf("list superseded project roles: %w", err)
	}
	defer rows.Close()
	var roleIDs []string
	for rows.Next() {
		var roleID string
		if err := rows.Scan(&roleID); err != nil {
			return nil, fmt.Errorf("scan superseded project role: %w", err)
		}
		if roleID = strings.TrimSpace(roleID); roleID != "" {
			roleIDs = append(roleIDs, roleID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate superseded project roles: %w", err)
	}
	if len(roleIDs) == 0 {
		return nil, nil
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE project_agent_roles
   SET status = ?,
       released_at = ?,
       summary = CASE WHEN TRIM(summary) = '' THEN ? ELSE summary END,
       updated_by = ?,
       updated_at = ?
 WHERE workspace_id = ?
   AND project_id = ?
   AND agent_id = ?
   AND role_type = ?
   AND status = ?
   AND role_id <> ?`,
		ProjectRoleStatusReleased,
		strings.TrimSpace(now),
		"Superseded by project role "+strings.TrimSpace(role.RoleID),
		strings.TrimSpace(actorID),
		strings.TrimSpace(now),
		strings.TrimSpace(role.WorkspaceID),
		strings.TrimSpace(role.ProjectID),
		strings.TrimSpace(role.AgentID),
		roleType,
		ProjectRoleStatusActive,
		strings.TrimSpace(role.RoleID)); err != nil {
		return nil, fmt.Errorf("release superseded project roles: %w", err)
	}
	return roleIDs, nil
}

type projectRoleClaimRebindCandidate struct {
	TaskID         string
	ClaimStatus    string
	Summary        string
	ProjectRoleID  string
	RepoID         string
	CheckoutID     string
	BranchID       string
	WriteScopeJSON string
	ProjectLane    string
}

func (s *Store) ensureProjectRoleImplementerScopeAvailableTx(ctx context.Context, tx *sql.Tx, role ProjectRoleRecord) error {
	if strings.TrimSpace(role.RoleType) != ProjectRoleImplementer {
		return nil
	}
	rolePaths := writeScopePaths(role.WriteScopeJSON)
	if len(rolePaths) == 0 {
		return nil
	}
	rows, err := tx.QueryContext(ctx, `
SELECT tc.task_id, tc.agent_id, COALESCE(tc.repo_id, ''), COALESCE(tc.branch_id, ''),
       COALESCE(tc.write_scope_json, ''), COALESCE(t.project_lane, ''),
       COALESCE(pb.branch_id, ''), COALESCE(pb.repo_id, ''), COALESCE(pb.agent_id, ''),
       COALESCE(pb.active_task_id, ''), COALESCE(pb.active_claim_id, ''),
       COALESCE(pb.head_sha, ''), COALESCE(pb.review_doc_key, ''),
       COALESCE(pb.status, ''), COALESCE(pb.write_scope_json, '')
  FROM task_claims tc
  JOIN workspace_tasks wt ON wt.workspace_id = tc.workspace_id AND wt.task_id = tc.task_id
  JOIN tasks t ON t.task_id = tc.task_id
  LEFT JOIN project_branch_registry pb
    ON pb.workspace_id = tc.workspace_id
   AND pb.repo_id = tc.repo_id
   AND pb.branch_id = tc.branch_id
 WHERE tc.workspace_id = ?
   AND t.project_id = ?
   AND tc.agent_id <> ?
   AND tc.claim_status = ?
   AND tc.write_scope_json <> ''`,
		strings.TrimSpace(role.WorkspaceID),
		strings.TrimSpace(role.ProjectID),
		strings.TrimSpace(role.AgentID),
		model.TaskClaimStatusClaimed)
	if err != nil {
		return fmt.Errorf("scan live implementation claim scopes for project role assignment: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var taskID, agentID, repoID, branchID, rawScope, projectLane string
		var branch ProjectBranchRecord
		if err := rows.Scan(
			&taskID,
			&agentID,
			&repoID,
			&branchID,
			&rawScope,
			&projectLane,
			&branch.BranchID,
			&branch.RepoID,
			&branch.AgentID,
			&branch.ActiveTaskID,
			&branch.ActiveClaimID,
			&branch.HeadSHA,
			&branch.ReviewDocKey,
			&branch.Status,
			&branch.WriteScopeJSON,
		); err != nil {
			return fmt.Errorf("scan live implementation claim scope for project role assignment: %w", err)
		}
		if !projectLaneRequiresImplementationGate(projectLane) {
			continue
		}
		effectiveScope := taskClaimEffectiveWriteScopeJSONFromLiveBranch(rawScope, taskID, agentID, branchID, repoID, branch)
		if writeScopesOverlap(rolePaths, writeScopePaths(effectiveScope)) {
			return fmt.Errorf("%w: write_scope_json overlaps active claim task_id=%s agent_id=%s branch_id=%s; request the active owner to commit/publish, release, or abandon the lane before assigning a competing IMPLEMENTER role",
				ErrProjectRoleWriteScopeConflict,
				strings.TrimSpace(taskID),
				strings.TrimSpace(agentID),
				strings.TrimSpace(branchID))
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate live implementation claim scopes for project role assignment: %w", err)
	}
	rows, err = tx.QueryContext(ctx, `
SELECT pb.branch_id, pb.repo_id, pb.agent_id, pb.active_task_id, pb.active_claim_id, pb.head_sha, pb.review_doc_key, pb.status, pb.write_scope_json,
       COALESCE(t.task_id, ''), COALESCE(t.project_lane, ''), COALESCE(t.requires_project_gate, 0)
  FROM project_branch_registry pb
  LEFT JOIN tasks t ON t.task_id = pb.active_task_id
 WHERE pb.workspace_id = ?
   AND pb.project_id = ?
   AND pb.agent_id <> ?
   AND pb.status IN ('RESERVED', 'ACTIVE', 'BLOCKED', 'READY_FOR_REVIEW')
   AND pb.write_scope_json <> ''`,
		strings.TrimSpace(role.WorkspaceID),
		strings.TrimSpace(role.ProjectID),
		strings.TrimSpace(role.AgentID))
	if err != nil {
		return fmt.Errorf("scan live implementation branch scopes for project role assignment: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var branch ProjectBranchRecord
		var joinedTaskID, projectLane string
		var requiresProjectGate int
		if err := rows.Scan(
			&branch.BranchID,
			&branch.RepoID,
			&branch.AgentID,
			&branch.ActiveTaskID,
			&branch.ActiveClaimID,
			&branch.HeadSHA,
			&branch.ReviewDocKey,
			&branch.Status,
			&branch.WriteScopeJSON,
			&joinedTaskID,
			&projectLane,
			&requiresProjectGate,
		); err != nil {
			return fmt.Errorf("scan live implementation branch scope for project role assignment: %w", err)
		}
		if !projectBranchOwnsWriteScope(branch) {
			continue
		}
		if taskClaimReservedNonImplementationBranchDoesNotOwnWriteScope(branch.Status, branch.ActiveTaskID, joinedTaskID, projectLane, sqliteIntToBool(requiresProjectGate), branch.HeadSHA, branch.ReviewDocKey) {
			continue
		}
		if writeScopesOverlap(rolePaths, writeScopePaths(branch.WriteScopeJSON)) {
			return fmt.Errorf("%w: write_scope_json overlaps live branch_id=%s agent_id=%s active_task_id=%s; request the active owner to commit/publish, release, or abandon the lane before assigning a competing IMPLEMENTER role",
				ErrProjectRoleWriteScopeConflict,
				strings.TrimSpace(branch.BranchID),
				strings.TrimSpace(branch.AgentID),
				strings.TrimSpace(branch.ActiveTaskID))
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate live implementation branch scopes for project role assignment: %w", err)
	}
	return nil
}

func (s *Store) rebindSingleActiveProjectImplementationClaimToRoleTx(ctx context.Context, tx *sql.Tx, role ProjectRoleRecord, actorID, now string) (map[string]any, error) {
	if strings.TrimSpace(role.RoleType) != ProjectRoleImplementer || len(writeScopePaths(role.WriteScopeJSON)) == 0 {
		return nil, nil
	}
	candidates, err := s.activeProjectImplementationClaimCandidatesForRoleTx(ctx, tx, role)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	if len(candidates) > 1 {
		return map[string]any{
			"state":      "skipped_ambiguous_active_claims",
			"agent_id":   role.AgentID,
			"project_id": role.ProjectID,
			"count":      len(candidates),
			"guidance":   "Multiple active implementation claims exist for this agent/project; the strategic lead must release or retarget the stale claims explicitly before role-scope rebind.",
		}, nil
	}
	candidate := candidates[0]
	blockTransition := projectRoleRebindBlockedClaimTransition(candidate, role)
	admission := taskClaimProjectAdmissionRecord{
		ProjectID:      role.ProjectID,
		ProjectRoleID:  role.RoleID,
		RepoID:         candidate.RepoID,
		CheckoutID:     candidate.CheckoutID,
		BranchID:       candidate.BranchID,
		WriteScopeJSON: role.WriteScopeJSON,
	}
	if admission.RepoID == "" || admission.CheckoutID == "" || admission.BranchID == "" {
		return map[string]any{
			"state":      "skipped_missing_project_admission",
			"task_id":    candidate.TaskID,
			"agent_id":   role.AgentID,
			"project_id": role.ProjectID,
			"guidance":   "Active claim does not have repo/checkout/branch admission to rebind; the agent should reclaim after the role correction.",
		}, nil
	}
	branch, err := getProjectBranchByIDTx(ctx, tx, admission.BranchID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return map[string]any{
				"state":      "skipped_missing_branch",
				"task_id":    candidate.TaskID,
				"branch_id":  admission.BranchID,
				"agent_id":   role.AgentID,
				"project_id": role.ProjectID,
			}, nil
		}
		return nil, err
	}
	if strings.TrimSpace(branch.WorkspaceID) != strings.TrimSpace(role.WorkspaceID) ||
		strings.TrimSpace(branch.ProjectID) != strings.TrimSpace(role.ProjectID) ||
		strings.TrimSpace(branch.RepoID) != strings.TrimSpace(admission.RepoID) ||
		strings.TrimSpace(branch.AgentID) != strings.TrimSpace(role.AgentID) {
		return map[string]any{
			"state":      "skipped_branch_scope_mismatch",
			"task_id":    candidate.TaskID,
			"branch_id":  admission.BranchID,
			"agent_id":   role.AgentID,
			"project_id": role.ProjectID,
		}, nil
	}
	if !projectRoleClaimRebindBranchStatusMutable(branch.Status) {
		return map[string]any{
			"state":         "skipped_branch_status",
			"task_id":       candidate.TaskID,
			"branch_id":     admission.BranchID,
			"branch_status": branch.Status,
			"agent_id":      role.AgentID,
			"project_id":    role.ProjectID,
			"guidance":      "Review-ready or terminal branch evidence is immutable for role-scope rebind; create a revision/follow-up task instead.",
		}, nil
	}
	if branch.ActiveTaskID != "" && strings.TrimSpace(branch.ActiveTaskID) != strings.TrimSpace(candidate.TaskID) {
		return map[string]any{
			"state":          "skipped_branch_active_task_mismatch",
			"task_id":        candidate.TaskID,
			"branch_id":      admission.BranchID,
			"active_task_id": branch.ActiveTaskID,
			"agent_id":       role.AgentID,
			"project_id":     role.ProjectID,
		}, nil
	}
	if branch.ActiveClaimID != "" && strings.TrimSpace(branch.ActiveClaimID) != strings.TrimSpace(candidate.TaskID) {
		return map[string]any{
			"state":           "skipped_branch_active_claim_mismatch",
			"task_id":         candidate.TaskID,
			"branch_id":       admission.BranchID,
			"active_claim_id": branch.ActiveClaimID,
			"agent_id":        role.AgentID,
			"project_id":      role.ProjectID,
		}, nil
	}
	overlapAdvisories, err := s.projectRoleRebindWriteScopeOverlapAdvisoriesTx(ctx, tx, role.WorkspaceID, candidate.TaskID, admission)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE task_claims
   SET project_role_id = ?,
       claim_status = ?,
       summary = CASE WHEN ? = '1' THEN ? ELSE summary END,
       write_scope_json = ?,
       updated_at = ?
  WHERE workspace_id = ?
   AND task_id = ?
   AND agent_id = ?
   AND claim_status IN (?, ?)`,
		strings.TrimSpace(role.RoleID),
		blockTransition.NextClaimStatus,
		boolText(blockTransition.ResumeOwnerLane),
		blockTransition.ResumeSummary,
		strings.TrimSpace(role.WriteScopeJSON),
		strings.TrimSpace(now),
		strings.TrimSpace(role.WorkspaceID),
		strings.TrimSpace(candidate.TaskID),
		strings.TrimSpace(role.AgentID),
		model.TaskClaimStatusClaimed,
		model.TaskClaimStatusBlocked); err != nil {
		return nil, fmt.Errorf("rebind active task claim to project role: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE project_branch_registry
   SET write_scope_json = ?,
       updated_by = ?,
       updated_at = ?
 WHERE workspace_id = ?
   AND branch_id = ?
   AND agent_id = ?
   AND status IN (?, ?, ?)`,
		strings.TrimSpace(role.WriteScopeJSON),
		strings.TrimSpace(actorID),
		strings.TrimSpace(now),
		strings.TrimSpace(role.WorkspaceID),
		strings.TrimSpace(candidate.BranchID),
		strings.TrimSpace(role.AgentID),
		ProjectBranchStatusReserved,
		ProjectBranchStatusActive,
		ProjectBranchStatusBlocked); err != nil {
		return nil, fmt.Errorf("rebind active branch scope to project role: %w", err)
	}
	payload := map[string]any{
		"state":                     "updated",
		"task_id":                   candidate.TaskID,
		"branch_id":                 candidate.BranchID,
		"agent_id":                  role.AgentID,
		"project_id":                role.ProjectID,
		"previous_project_role_id":  candidate.ProjectRoleID,
		"project_role_id":           role.RoleID,
		"previous_write_scope_json": candidate.WriteScopeJSON,
		"write_scope_json":          role.WriteScopeJSON,
		"previous_claim_status":     candidate.ClaimStatus,
		"claim_status":              blockTransition.NextClaimStatus,
		"block_type":                blockTransition.BlockType,
		"block_resolution_state":    blockTransition.ResolutionState,
		"blocked_claim_policy":      blockTransition.Policy,
	}
	if blockTransition.ResumeOwnerLane {
		payload["preferred_transition"] = "request_resume"
		payload["next_lane_interaction"] = "owner_reclaim"
	}
	if blockTransition.ResolutionState == "side_effect_boundary_resolved" {
		resolvedTasks, err := s.resolveSideEffectClassificationTasksForRoleRebindTx(ctx, tx, role, candidate, actorID, now)
		if err != nil {
			return nil, err
		}
		if len(resolvedTasks) > 0 {
			payload["side_effect_classification_lifecycle"] = "resolved_no_longer_blocking"
			payload["resolved_side_effect_classification_task_ids"] = resolvedTasks
		}
		resolvedSuccessors, err := s.resolveSideEffectRecoverySuccessorTasksForRoleRebindTx(ctx, tx, role, candidate, actorID, now)
		if err != nil {
			return nil, err
		}
		if len(resolvedSuccessors) > 0 {
			payload["side_effect_recovery_successor_lifecycle"] = "cancelled_no_longer_blocking"
			payload["resolved_side_effect_recovery_successor_task_ids"] = resolvedSuccessors
		}
	}
	if len(overlapAdvisories) > 0 {
		payload["write_scope_overlap_advisory"] = true
		payload["overlap_advisories"] = overlapAdvisories
		payload["guidance"] = "Corrected role scope overlaps sibling live claim or branch evidence. The target claim and mutable branch were rebound because this was an explicit strategic-lead correction; do not retry role-scope correction for the same scope. Continue with commit/review-ready evidence and resolve real file conflicts through patch queue or integration."
	}
	return payload, nil
}

func (s *Store) resolveSideEffectClassificationTasksForRoleRebindTx(ctx context.Context, tx *sql.Tx, role ProjectRoleRecord, candidate projectRoleClaimRebindCandidate, actorID, now string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT t.task_id, t.status, COALESCE(t.task_requirements_json, '{}')
  FROM tasks t
  JOIN workspace_tasks wt ON wt.task_id = t.task_id
 WHERE wt.workspace_id = ?
   AND t.project_id = ?
   AND t.status NOT IN (?, ?, ?)
   AND COALESCE(t.task_requirements_json, '') LIKE '%side_effect_classification%'`,
		strings.TrimSpace(role.WorkspaceID),
		strings.TrimSpace(role.ProjectID),
		model.TaskStatusResolved,
		model.TaskStatusFailed,
		model.TaskStatusCancelled)
	if err != nil {
		return nil, fmt.Errorf("query side-effect classification tasks for role rebind: %w", err)
	}
	defer rows.Close()
	type candidateTask struct {
		taskID       string
		requirements string
	}
	var matches []candidateTask
	for rows.Next() {
		var taskID, status, requirements string
		if err := rows.Scan(&taskID, &status, &requirements); err != nil {
			return nil, fmt.Errorf("scan side-effect classification task for role rebind: %w", err)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(requirements)), &payload); err != nil {
			continue
		}
		if !projectRoleRebindMatchesSideEffectClassificationTask(payload, role, candidate) {
			continue
		}
		matches = append(matches, candidateTask{taskID: strings.TrimSpace(taskID), requirements: requirements})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate side-effect classification tasks for role rebind: %w", err)
	}
	if len(matches) == 0 {
		return nil, nil
	}
	reason := "ABPC boundary expansion applied; side-effect classification is no longer blocking the owner lane."
	resolvedTaskIDs := make([]string, 0, len(matches))
	for _, match := range matches {
		if match.taskID == "" {
			continue
		}
		if _, err := s.closeTaskTx(ctx, tx, TaskCloseInput{
			WorkspaceID: strings.TrimSpace(role.WorkspaceID),
			TaskID:      match.taskID,
			ActorID:     strings.TrimSpace(actorID),
			Resolution:  model.TaskStatusResolved,
			Reason:      reason,
		}, strings.TrimSpace(now)); err != nil {
			return nil, fmt.Errorf("resolve side-effect classification task %s: %w", match.taskID, err)
		}
		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  "side_effect_classification_task_resolved",
			EntityType: "task",
			EntityID:   match.taskID,
			ActorID:    strings.TrimSpace(actorID),
			PayloadJSON: mustJSON(map[string]any{
				"task_id":              match.taskID,
				"project_id":           role.ProjectID,
				"agent_id":             role.AgentID,
				"active_task_id":       candidate.TaskID,
				"branch_id":            candidate.BranchID,
				"project_role_id":      role.RoleID,
				"resolution_state":     "side_effect_boundary_resolved",
				"lifecycle":            "resolved_no_longer_blocking",
				"reason":               reason,
				"task_requirements":    match.requirements,
				"operational_boundary": role.WriteScopeJSON,
			}),
		}); err != nil {
			return nil, err
		}
		resolvedTaskIDs = append(resolvedTaskIDs, match.taskID)
	}
	return resolvedTaskIDs, nil
}

func (s *Store) resolveSideEffectRecoverySuccessorTasksForRoleRebindTx(ctx context.Context, tx *sql.Tx, role ProjectRoleRecord, candidate projectRoleClaimRebindCandidate, actorID, now string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT t.task_id, t.status, COALESCE(t.task_requirements_json, '{}')
  FROM tasks t
  JOIN workspace_tasks wt ON wt.task_id = t.task_id
 WHERE wt.workspace_id = ?
   AND t.project_id = ?
   AND t.status NOT IN (?, ?, ?)
   AND (
        COALESCE(t.task_requirements_json, '') LIKE '%artifact_bound_side_effect_resolution_followup.v1%'
     OR COALESCE(t.task_requirements_json, '') LIKE '%abpc_recovery_action%'
     OR COALESCE(t.task_requirements_json, '') LIKE '%side_effect_verification%'
   )`,
		strings.TrimSpace(role.WorkspaceID),
		strings.TrimSpace(role.ProjectID),
		model.TaskStatusResolved,
		model.TaskStatusFailed,
		model.TaskStatusCancelled)
	if err != nil {
		return nil, fmt.Errorf("query side-effect recovery successors for role rebind: %w", err)
	}
	defer rows.Close()
	type candidateTask struct {
		taskID       string
		requirements string
	}
	var matches []candidateTask
	for rows.Next() {
		var taskID, status, requirements string
		if err := rows.Scan(&taskID, &status, &requirements); err != nil {
			return nil, fmt.Errorf("scan side-effect recovery successor for role rebind: %w", err)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(requirements)), &payload); err != nil {
			continue
		}
		if !projectRoleRebindMatchesSideEffectRecoverySuccessorTask(payload, role, candidate) {
			continue
		}
		matches = append(matches, candidateTask{taskID: strings.TrimSpace(taskID), requirements: requirements})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate side-effect recovery successors for role rebind: %w", err)
	}
	if len(matches) == 0 {
		return nil, nil
	}
	reason := "ABPC boundary expansion applied; side-effect recovery successor is superseded and no longer blocking the owner lane."
	resolvedTaskIDs := make([]string, 0, len(matches))
	for _, match := range matches {
		if match.taskID == "" {
			continue
		}
		if _, err := s.closeTaskTx(ctx, tx, TaskCloseInput{
			WorkspaceID: strings.TrimSpace(role.WorkspaceID),
			TaskID:      match.taskID,
			ActorID:     strings.TrimSpace(actorID),
			Resolution:  model.TaskStatusCancelled,
			Reason:      reason,
		}, strings.TrimSpace(now)); err != nil {
			return nil, fmt.Errorf("resolve side-effect recovery successor task %s: %w", match.taskID, err)
		}
		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  "side_effect_recovery_successor_task_resolved",
			EntityType: "task",
			EntityID:   match.taskID,
			ActorID:    strings.TrimSpace(actorID),
			PayloadJSON: mustJSON(map[string]any{
				"task_id":              match.taskID,
				"project_id":           role.ProjectID,
				"agent_id":             role.AgentID,
				"active_task_id":       candidate.TaskID,
				"branch_id":            candidate.BranchID,
				"project_role_id":      role.RoleID,
				"resolution_state":     "side_effect_boundary_resolved",
				"lifecycle":            "cancelled_no_longer_blocking",
				"reason":               reason,
				"task_requirements":    match.requirements,
				"operational_boundary": role.WriteScopeJSON,
			}),
		}); err != nil {
			return nil, err
		}
		resolvedTaskIDs = append(resolvedTaskIDs, match.taskID)
	}
	return resolvedTaskIDs, nil
}

func (s *Store) resolveProjectRoleScopeAuthorityTasksForRoleAssignTx(ctx context.Context, tx *sql.Tx, role ProjectRoleRecord, rebindPayload map[string]any, actorID, now string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT t.task_id, COALESCE(t.task_requirements_json, '{}')
  FROM tasks t
  JOIN workspace_tasks wt ON wt.task_id = t.task_id
 WHERE wt.workspace_id = ?
   AND t.project_id = ?
   AND t.task_kind = ?
   AND t.status NOT IN (?, ?, ?)
   AND COALESCE(t.task_requirements_json, '') LIKE '%project_role_scope_authority_transition.v1%'`,
		strings.TrimSpace(role.WorkspaceID),
		strings.TrimSpace(role.ProjectID),
		model.TaskKindCoordination,
		model.TaskStatusResolved,
		model.TaskStatusFailed,
		model.TaskStatusCancelled)
	if err != nil {
		return nil, fmt.Errorf("query project role/scope authority tasks for role assign: %w", err)
	}
	defer rows.Close()

	type candidateTask struct {
		taskID       string
		requirements string
	}
	var matches []candidateTask
	for rows.Next() {
		var taskID, requirements string
		if err := rows.Scan(&taskID, &requirements); err != nil {
			return nil, fmt.Errorf("scan project role/scope authority task for role assign: %w", err)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(normalizeTaskRequirementsJSON(requirements)), &payload); err != nil {
			continue
		}
		if !projectRoleScopeAuthorityTaskSatisfiedByRoleAssign(payload, role, rebindPayload) {
			continue
		}
		matches = append(matches, candidateTask{taskID: strings.TrimSpace(taskID), requirements: requirements})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate project role/scope authority tasks for role assign: %w", err)
	}
	if len(matches) == 0 {
		return nil, nil
	}

	reason := "project_role_assign applied; role/scope authority transition is satisfied and no longer blocking the owner lane."
	resolvedTaskIDs := make([]string, 0, len(matches))
	for _, match := range matches {
		if match.taskID == "" {
			continue
		}
		if _, err := s.closeTaskTx(ctx, tx, TaskCloseInput{
			WorkspaceID: strings.TrimSpace(role.WorkspaceID),
			TaskID:      match.taskID,
			ActorID:     strings.TrimSpace(actorID),
			Resolution:  model.TaskStatusResolved,
			Reason:      reason,
		}, strings.TrimSpace(now)); err != nil {
			return nil, fmt.Errorf("resolve project role/scope authority task %s: %w", match.taskID, err)
		}
		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  "project_role_scope_authority_task_resolved",
			EntityType: "task",
			EntityID:   match.taskID,
			ActorID:    strings.TrimSpace(actorID),
			PayloadJSON: mustJSON(map[string]any{
				"task_id":           match.taskID,
				"project_id":        role.ProjectID,
				"agent_id":          role.AgentID,
				"role_id":           role.RoleID,
				"role_type":         role.RoleType,
				"write_scope_json":  role.WriteScopeJSON,
				"resolution_state":  "project_role_assign_applied",
				"lifecycle":         "resolved_no_longer_blocking",
				"reason":            reason,
				"task_requirements": match.requirements,
			}),
		}); err != nil {
			return nil, err
		}
		resolvedTaskIDs = append(resolvedTaskIDs, match.taskID)
	}
	return resolvedTaskIDs, nil
}

func projectRoleScopeAuthorityTaskSatisfiedByRoleAssign(payload map[string]any, role ProjectRoleRecord, rebindPayload map[string]any) bool {
	if strings.TrimSpace(projectRoleRebindPayloadString(payload, "schema")) != "project_role_scope_authority_transition.v1" {
		return false
	}
	if transition := strings.TrimSpace(projectRoleRebindPayloadString(payload, "required_transition")); transition != "" && transition != "project_role_assign" {
		return false
	}
	if transition := strings.TrimSpace(projectRoleRebindPayloadString(payload, "preferred_transition")); transition != "" && transition != "project_role_assign" {
		return false
	}
	if projectID := strings.TrimSpace(projectRoleRebindPayloadString(payload, "project_id")); projectID != "" && projectID != strings.TrimSpace(role.ProjectID) {
		return false
	}
	targetAgentID := strings.TrimSpace(projectRoleRebindPayloadString(payload, "target_agent_id"))
	if targetAgentID == "" || targetAgentID != strings.TrimSpace(role.AgentID) {
		return false
	}
	if roleType := strings.ToUpper(strings.TrimSpace(projectRoleRebindPayloadString(payload, "role_type"))); roleType != "" && roleType != strings.ToUpper(strings.TrimSpace(role.RoleType)) {
		return false
	}
	if !projectRoleScopeAuthorityTaskWriteScopeSatisfied(payload, role) {
		return false
	}
	return projectRoleScopeAuthorityTaskActiveBindingSatisfied(payload, rebindPayload)
}

func projectRoleScopeAuthorityTaskWriteScopeSatisfied(payload map[string]any, role ProjectRoleRecord) bool {
	requestedScope := strings.TrimSpace(projectRoleRebindPayloadString(payload, "write_scope_json"))
	if requestedScope == "" {
		return true
	}
	requestedPaths := writeScopePaths(requestedScope)
	if len(requestedPaths) == 0 {
		return true
	}
	return writeScopePathsCoverScopeJSON(requestedScope, role.WriteScopeJSON)
}

func projectRoleScopeAuthorityTaskActiveBindingSatisfied(payload map[string]any, rebindPayload map[string]any) bool {
	activeTaskID := strings.TrimSpace(projectRoleRebindPayloadString(payload, "active_task_id"))
	branchID := strings.TrimSpace(projectRoleRebindPayloadString(payload, "branch_id"))
	if activeTaskID == "" && branchID == "" {
		return true
	}
	if len(rebindPayload) == 0 {
		return false
	}
	state := strings.ToLower(strings.TrimSpace(projectRoleRebindPayloadString(rebindPayload, "state")))
	if state != "" && state != "updated" && state != "already_applied" {
		return false
	}
	if activeTaskID != "" && strings.TrimSpace(projectRoleRebindPayloadString(rebindPayload, "task_id")) != activeTaskID {
		return false
	}
	if branchID != "" && strings.TrimSpace(projectRoleRebindPayloadString(rebindPayload, "branch_id")) != branchID {
		return false
	}
	return true
}

func projectRoleRebindMatchesSideEffectClassificationTask(payload map[string]any, role ProjectRoleRecord, candidate projectRoleClaimRebindCandidate) bool {
	if strings.TrimSpace(projectRoleRebindPayloadString(payload, "abpc_task_class")) != "side_effect_classification" {
		return false
	}
	ownerAgentID := strings.TrimSpace(projectRoleRebindPayloadString(payload, "owner_agent_id"))
	if ownerAgentID != "" && ownerAgentID != strings.TrimSpace(role.AgentID) {
		return false
	}
	activeTaskID := strings.TrimSpace(projectRoleRebindPayloadString(payload, "active_task_id"))
	branchID := strings.TrimSpace(projectRoleRebindPayloadString(payload, "branch_id"))
	if activeTaskID == "" && branchID == "" {
		return false
	}
	if activeTaskID != "" && activeTaskID != strings.TrimSpace(candidate.TaskID) {
		return false
	}
	if branchID != "" && branchID != strings.TrimSpace(candidate.BranchID) {
		return false
	}
	dirtyPaths := projectRoleRebindPayloadStringSlice(payload, "dirty_paths")
	if len(dirtyPaths) > 0 && !projectRoleRebindDirtyPathsCoveredByRole(dirtyPaths, role) {
		return false
	}
	return true
}

func projectRoleRebindMatchesSideEffectRecoverySuccessorTask(payload map[string]any, role ProjectRoleRecord, candidate projectRoleClaimRebindCandidate) bool {
	schema := strings.TrimSpace(projectRoleRebindPayloadString(payload, "schema"))
	admissionKind := strings.TrimSpace(projectRoleRebindPayloadString(payload, "admission_kind"))
	abpcClass := strings.TrimSpace(projectRoleRebindPayloadString(payload, "abpc_task_class"))
	if schema != "artifact_bound_side_effect_resolution_followup.v1" && admissionKind != "abpc_recovery_action" {
		return false
	}
	if abpcClass == "side_effect_classification" {
		return false
	}
	ownerAgentID := strings.TrimSpace(projectRoleRebindPayloadString(payload, "owner_agent_id"))
	if ownerAgentID != "" && ownerAgentID != strings.TrimSpace(role.AgentID) {
		return false
	}
	activeTaskID := strings.TrimSpace(projectRoleRebindPayloadString(payload, "active_task_id"))
	branchID := strings.TrimSpace(projectRoleRebindPayloadString(payload, "branch_id"))
	if activeTaskID == "" && branchID == "" {
		return false
	}
	if activeTaskID != "" && activeTaskID != strings.TrimSpace(candidate.TaskID) {
		return false
	}
	if branchID != "" && branchID != strings.TrimSpace(candidate.BranchID) {
		return false
	}
	dirtyPaths := projectRoleRebindPayloadStringSlice(payload, "path_bucket")
	if len(dirtyPaths) == 0 {
		dirtyPaths = projectRoleRebindPayloadStringSlice(payload, "dirty_paths")
	}
	if len(dirtyPaths) == 0 {
		dirtyPaths = projectRoleRebindPayloadStringSlice(payload, "write_scope_hints")
	}
	if len(dirtyPaths) == 0 || !projectRoleRebindDirtyPathsCoveredByRole(dirtyPaths, role) {
		return false
	}
	return true
}

func projectRoleRebindDirtyPathsCoveredByRole(dirtyPaths []string, role ProjectRoleRecord) bool {
	rolePaths := writeScopePaths(role.WriteScopeJSON)
	if len(dirtyPaths) == 0 || len(rolePaths) == 0 {
		return false
	}
	for _, dirtyPath := range dirtyPaths {
		dirtyPath = strings.TrimSpace(dirtyPath)
		if dirtyPath == "" {
			continue
		}
		if !writeScopesOverlap([]string{dirtyPath}, rolePaths) {
			return false
		}
	}
	return true
}

func projectRoleRebindPayloadString(payload map[string]any, key string) string {
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func projectRoleRebindPayloadStringSlice(payload map[string]any, key string) []string {
	value, ok := payload[key]
	if !ok || value == nil {
		return nil
	}
	out := []string{}
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	switch typed := value.(type) {
	case []string:
		for _, item := range typed {
			add(item)
		}
	case []any:
		for _, item := range typed {
			switch itemTyped := item.(type) {
			case string:
				add(itemTyped)
			default:
				add(fmt.Sprint(itemTyped))
			}
		}
	case string:
		add(typed)
	}
	return out
}

type projectRoleRebindBlockedClaimState struct {
	NextClaimStatus string
	BlockType       string
	ResolutionState string
	Policy          string
	ResumeOwnerLane bool
	ResumeSummary   string
}

func projectRoleRebindBlockedClaimTransition(candidate projectRoleClaimRebindCandidate, role ProjectRoleRecord) projectRoleRebindBlockedClaimState {
	status := strings.ToUpper(strings.TrimSpace(candidate.ClaimStatus))
	if status == "" {
		status = model.TaskClaimStatusClaimed
	}
	state := projectRoleRebindBlockedClaimState{
		NextClaimStatus: status,
		BlockType:       "not_blocked",
		ResolutionState: "not_applicable",
		Policy:          "preserve_current_lane_state",
	}
	if status != model.TaskClaimStatusBlocked {
		return state
	}
	blockType := projectRoleRebindBlockedClaimType(candidate.Summary)
	state.BlockType = blockType
	switch blockType {
	case "blocked_scope_conflict":
		state.NextClaimStatus = model.TaskClaimStatusClaimed
		state.ResolutionState = "blocked_resolved_repair"
		state.Policy = "resume_owner_lane_only"
		state.ResumeOwnerLane = true
		state.ResumeSummary = "Role/scope repair resolved a blocked operational-boundary conflict; resume the owner lane."
	case "blocked_side_effect_classification":
		if projectRoleRebindSummaryResolvesSideEffectBoundary(role.Summary) {
			state.NextClaimStatus = model.TaskClaimStatusClaimed
			state.ResolutionState = "side_effect_boundary_resolved"
			state.Policy = "resume_owner_lane_after_boundary_transition"
			state.ResumeOwnerLane = true
			state.ResumeSummary = "ABPC boundary expansion resolved side-effect classification; resume the owner lane."
		} else {
			state.ResolutionState = "pending_side_effect_resolution"
			state.Policy = "route_to_side_effect_resolution"
		}
	case "blocked_dependency":
		state.ResolutionState = "dependency_unresolved"
		state.Policy = "preserve_lane_until_dependency_resolution"
	default:
		state.ResolutionState = "blocked_unclassified"
		state.Policy = "preserve_lane_until_classified_resolution"
	}
	return state
}

func projectRoleRebindSummaryResolvesSideEffectBoundary(summary string) bool {
	lower := strings.ToLower(strings.TrimSpace(summary))
	if lower == "" {
		return false
	}
	sideEffect := strings.Contains(lower, "side-effect") || strings.Contains(lower, "side effect") || strings.Contains(lower, "side_effect")
	if !sideEffect {
		return false
	}
	return strings.Contains(lower, "expand_boundary") ||
		strings.Contains(lower, "boundary expansion") ||
		strings.Contains(lower, "boundary_transition") ||
		strings.Contains(lower, "abpc side-effect resolution")
}

func projectRoleRebindBlockedClaimType(summary string) string {
	lower := strings.ToLower(strings.TrimSpace(summary))
	if lower == "" {
		return "blocked_unclassified"
	}
	if strings.Contains(lower, "side_effect") ||
		strings.Contains(lower, "side effect") ||
		strings.Contains(lower, "side-effect") ||
		strings.Contains(lower, "pending classification") ||
		strings.Contains(lower, "classification") {
		return "blocked_side_effect_classification"
	}
	if strings.Contains(lower, "write_scope") ||
		strings.Contains(lower, "write scope") ||
		strings.Contains(lower, "role/scope") ||
		strings.Contains(lower, "scope conflict") ||
		strings.Contains(lower, "scope repair") ||
		strings.Contains(lower, "claim scope") ||
		strings.Contains(lower, "claimed write scope") ||
		strings.Contains(lower, "project claim admission") ||
		strings.Contains(lower, "role correction") ||
		strings.Contains(lower, "role mismatch") ||
		strings.Contains(lower, "active project role") {
		return "blocked_scope_conflict"
	}
	if strings.Contains(lower, "dependency") ||
		strings.Contains(lower, "waiting for") ||
		strings.Contains(lower, "awaiting") ||
		strings.Contains(lower, "blocked on") ||
		strings.Contains(lower, "upstream") ||
		strings.Contains(lower, "approval") ||
		strings.Contains(lower, "review") ||
		strings.Contains(lower, "verification") {
		return "blocked_dependency"
	}
	return "blocked_unclassified"
}

func boolText(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func (s *Store) projectRoleRebindWriteScopeOverlapAdvisoriesTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID string, admission taskClaimProjectAdmissionRecord) ([]map[string]any, error) {
	claimPaths := writeScopePaths(admission.WriteScopeJSON)
	if len(claimPaths) == 0 {
		return nil, nil
	}
	var advisories []map[string]any
	rows, err := tx.QueryContext(ctx, `
SELECT task_id, branch_id, write_scope_json
  FROM task_claims
 WHERE workspace_id = ?
   AND repo_id = ?
   AND task_id <> ?
   AND claim_status = ?
   AND write_scope_json <> ''`,
		strings.TrimSpace(workspaceID), admission.RepoID, strings.TrimSpace(taskID), model.TaskClaimStatusClaimed)
	if err != nil {
		return nil, fmt.Errorf("scan live task claim write scopes for role rebind advisory: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var otherTaskID, otherBranchID, rawScope string
		if err := rows.Scan(&otherTaskID, &otherBranchID, &rawScope); err != nil {
			return nil, fmt.Errorf("scan live task claim write scope for role rebind advisory: %w", err)
		}
		if admission.BranchID != "" && strings.TrimSpace(otherBranchID) == admission.BranchID {
			continue
		}
		if writeScopesOverlap(claimPaths, writeScopePaths(rawScope)) {
			advisories = append(advisories, map[string]any{
				"kind":             "active_task_claim",
				"task_id":          strings.TrimSpace(otherTaskID),
				"branch_id":        strings.TrimSpace(otherBranchID),
				"write_scope_json": strings.TrimSpace(rawScope),
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live task claim write scopes for role rebind advisory: %w", err)
	}
	rows, err = tx.QueryContext(ctx, `
SELECT branch_id, active_task_id, write_scope_json
  FROM project_branch_registry
 WHERE workspace_id = ?
   AND repo_id = ?
   AND branch_id <> ?
   AND status IN ('RESERVED', 'ACTIVE', 'BLOCKED', 'READY_FOR_REVIEW')
   AND write_scope_json <> ''`,
		strings.TrimSpace(workspaceID), admission.RepoID, admission.BranchID)
	if err != nil {
		return nil, fmt.Errorf("scan live project branch write scopes for role rebind advisory: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var otherBranchID, otherTaskID, rawScope string
		if err := rows.Scan(&otherBranchID, &otherTaskID, &rawScope); err != nil {
			return nil, fmt.Errorf("scan live project branch write scope for role rebind advisory: %w", err)
		}
		if strings.TrimSpace(otherTaskID) == strings.TrimSpace(taskID) {
			continue
		}
		if writeScopesOverlap(claimPaths, writeScopePaths(rawScope)) {
			advisories = append(advisories, map[string]any{
				"kind":             "live_project_branch",
				"task_id":          strings.TrimSpace(otherTaskID),
				"branch_id":        strings.TrimSpace(otherBranchID),
				"write_scope_json": strings.TrimSpace(rawScope),
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live project branch write scopes for role rebind advisory: %w", err)
	}
	return advisories, nil
}

func (s *Store) activeProjectImplementationClaimCandidatesForRoleTx(ctx context.Context, tx *sql.Tx, role ProjectRoleRecord) ([]projectRoleClaimRebindCandidate, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT tc.task_id, tc.claim_status, COALESCE(tc.summary, ''), COALESCE(tc.project_role_id, ''), COALESCE(tc.repo_id, ''),
       COALESCE(tc.checkout_id, ''), COALESCE(tc.branch_id, ''), COALESCE(tc.write_scope_json, ''),
       COALESCE(t.project_lane, '')
  FROM task_claims tc
  JOIN tasks t ON t.task_id = tc.task_id
 WHERE tc.workspace_id = ?
   AND tc.agent_id = ?
   AND t.project_id = ?
   AND tc.claim_status IN (?, ?)
 ORDER BY tc.updated_at DESC, tc.task_id ASC`,
		strings.TrimSpace(role.WorkspaceID),
		strings.TrimSpace(role.AgentID),
		strings.TrimSpace(role.ProjectID),
		model.TaskClaimStatusClaimed,
		model.TaskClaimStatusBlocked)
	if err != nil {
		return nil, fmt.Errorf("list active project implementation claims for role rebind: %w", err)
	}
	defer rows.Close()
	var candidates []projectRoleClaimRebindCandidate
	for rows.Next() {
		var candidate projectRoleClaimRebindCandidate
		if err := rows.Scan(
			&candidate.TaskID,
			&candidate.ClaimStatus,
			&candidate.Summary,
			&candidate.ProjectRoleID,
			&candidate.RepoID,
			&candidate.CheckoutID,
			&candidate.BranchID,
			&candidate.WriteScopeJSON,
			&candidate.ProjectLane,
		); err != nil {
			return nil, fmt.Errorf("scan active project implementation claim for role rebind: %w", err)
		}
		if projectLaneRequiresImplementationGate(candidate.ProjectLane) {
			candidates = append(candidates, candidate)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active project implementation claims for role rebind: %w", err)
	}
	return candidates, nil
}

func projectRoleClaimRebindBranchStatusMutable(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case ProjectBranchStatusReserved, ProjectBranchStatusActive, ProjectBranchStatusBlocked:
		return true
	default:
		return false
	}
}

func requireProjectLeadActor(role ProjectRoleRecord, actorID, actorType string) error {
	if strings.EqualFold(strings.TrimSpace(actorType), "agent") && strings.TrimSpace(role.AgentID) != strings.TrimSpace(actorID) {
		return ErrProjectLeadMismatch
	}
	return nil
}

func projectRoleEventPayload(role ProjectRoleRecord, operation, actorID string) map[string]any {
	return map[string]any{
		"workspace_id":       role.WorkspaceID,
		"project_id":         role.ProjectID,
		"role_id":            role.RoleID,
		"agent_id":           role.AgentID,
		"actor_id":           strings.TrimSpace(actorID),
		"entity_type":        "project_role",
		"entity_id":          role.RoleID,
		"summary":            "Project role mutation: " + role.RoleType + " " + role.Status,
		"mutation_operation": operation,
		"role":               role,
	}
}
