package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	ProjectPhaseIntake         = "INTAKE"
	ProjectPhaseSpec           = "SPEC"
	ProjectPhasePlanning       = "PLANNING"
	ProjectPhaseImplementation = "IMPLEMENTATION"
	ProjectPhaseReview         = "REVIEW"
	ProjectPhaseIntegration    = "INTEGRATION"
	ProjectPhaseValidation     = "VALIDATION"
	ProjectPhaseDone           = "DONE"

	ProjectRepoStatusNotRequired = "NOT_REQUIRED"
	ProjectRepoStatusMissing     = "MISSING"
	ProjectRepoStatusReady       = "READY"
	ProjectRepoStatusBlocked     = "BLOCKED"
	ProjectRepoStatusUnknown     = "UNKNOWN"

	ProjectGateStateUnknown   = "UNKNOWN"
	ProjectGateStateBlocked   = "BLOCKED"
	ProjectGateStateSatisfied = "SATISFIED"
	ProjectGateStateWaived    = "WAIVED"
)

type ProjectProfileRecord struct {
	WorkspaceID             string `json:"workspace_id"`
	ProjectID               string `json:"project_id"`
	Goal                    string `json:"goal"`
	CurrentPhase            string `json:"current_phase"`
	DesignDocID             string `json:"design_doc_id,omitempty"`
	ImplementationPlanDocID string `json:"implementation_plan_doc_id,omitempty"`
	RepoRequired            bool   `json:"repo_required"`
	RepoStatus              string `json:"repo_status"`
	RepoURL                 string `json:"repo_url,omitempty"`
	RepoDefaultBranch       string `json:"repo_default_branch,omitempty"`
	UpdatedBy               string `json:"updated_by,omitempty"`
	CreatedAt               string `json:"created_at"`
	UpdatedAt               string `json:"updated_at"`
}

type ProjectProfileUpdateInput struct {
	WorkspaceID             string
	ProjectID               string
	Goal                    *string
	DesignDocID             *string
	ImplementationPlanDocID *string
	RepoRequired            *bool
	RepoStatus              *string
	RepoURL                 *string
	RepoDefaultBranch       *string
	ActorID                 string
	ActorType               string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectPhaseHistoryRecord struct {
	HistoryID      string `json:"history_id"`
	WorkspaceID    string `json:"workspace_id"`
	ProjectID      string `json:"project_id"`
	FromPhase      string `json:"from_phase"`
	ToPhase        string `json:"to_phase"`
	TransitionedBy string `json:"transitioned_by"`
	Reason         string `json:"reason,omitempty"`
	CreatedAt      string `json:"created_at"`
}

type ProjectPhaseTransitionInput struct {
	WorkspaceID      string
	ProjectID        string
	ToPhase          string
	Reason           string
	ActorID          string
	ActorType        string
	CoordinationMode string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectGateRecord struct {
	GateKey     string `json:"gate_key"`
	State       string `json:"state"`
	Required    bool   `json:"required"`
	Summary     string `json:"summary,omitempty"`
	EvidenceRef string `json:"evidence_ref,omitempty"`
	UpdatedBy   string `json:"updated_by,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
	Source      string `json:"source"`
}

type ProjectGateStatusRecord struct {
	WorkspaceID         string              `json:"workspace_id"`
	ProjectID           string              `json:"project_id"`
	CurrentPhase        string              `json:"current_phase"`
	OverallState        string              `json:"overall_state"`
	ImplementationReady bool                `json:"implementation_ready"`
	Gates               []ProjectGateRecord `json:"gates"`
	UpdatedAt           string              `json:"updated_at"`
}

type ProjectGateStatusUpdateInput struct {
	WorkspaceID string
	ProjectID   string
	GateKey     string
	State       string
	Summary     string
	EvidenceRef string
	ActorID     string
	ActorType   string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectCoordinationRecord struct {
	SnapshotAt          string                        `json:"snapshot_at"`
	CoordinationVersion string                        `json:"coordination_version"`
	LatestEventID       string                        `json:"latest_event_id,omitempty"`
	SourceEventIDs      map[string]string             `json:"source_event_ids,omitempty"`
	Project             ProjectRecord                 `json:"project"`
	Profile             ProjectProfileRecord          `json:"profile"`
	GateStatus          ProjectGateStatusRecord       `json:"gate_status"`
	StrategicLead       *ProjectRoleRecord            `json:"strategic_lead,omitempty"`
	Roles               []ProjectRoleRecord           `json:"roles,omitempty"`
	Repositories        []ProjectRepositoryRecord     `json:"repositories,omitempty"`
	Checkouts           []ProjectCheckoutRecord       `json:"checkouts,omitempty"`
	Branches            []ProjectBranchRecord         `json:"branches,omitempty"`
	PatchQueueItems     []ProjectPatchQueueItemRecord `json:"patch_queue_items,omitempty"`
	ServiceRuns         []ServiceRunRecord            `json:"service_runs,omitempty"`
	Tasks               []WorkspaceTaskRecord         `json:"tasks,omitempty"`
	OpenTaskCount       int                           `json:"open_task_count"`
	TaskCountsByLane    map[string]int                `json:"task_counts_by_lane,omitempty"`
	TaskCountsByStatus  map[string]int                `json:"task_counts_by_status,omitempty"`
}

var projectPhaseOrder = map[string]int{
	ProjectPhaseIntake:         0,
	ProjectPhaseSpec:           1,
	ProjectPhasePlanning:       2,
	ProjectPhaseImplementation: 3,
	ProjectPhaseReview:         4,
	ProjectPhaseIntegration:    5,
	ProjectPhaseValidation:     6,
	ProjectPhaseDone:           7,
}

func normalizeProjectPhase(value string) (string, error) {
	phase := strings.ToUpper(strings.TrimSpace(value))
	phase = strings.ReplaceAll(phase, "-", "_")
	phase = strings.ReplaceAll(phase, " ", "_")
	if _, ok := projectPhaseOrder[phase]; !ok {
		return "", fmt.Errorf("invalid project phase: %s", value)
	}
	return phase, nil
}

func normalizeProjectRepoStatus(value string, repoRequired bool) (string, error) {
	status := strings.ToUpper(strings.TrimSpace(value))
	if status == "" {
		if repoRequired {
			return ProjectRepoStatusMissing, nil
		}
		return ProjectRepoStatusNotRequired, nil
	}
	switch status {
	case ProjectRepoStatusNotRequired, ProjectRepoStatusMissing, ProjectRepoStatusReady, ProjectRepoStatusBlocked, ProjectRepoStatusUnknown:
		if repoRequired && status == ProjectRepoStatusNotRequired {
			return ProjectRepoStatusMissing, nil
		}
		return status, nil
	default:
		return "", fmt.Errorf("invalid project repo_status: %s", value)
	}
}

func normalizeProjectGateState(value string) (string, error) {
	state := strings.ToUpper(strings.TrimSpace(value))
	if state == "" {
		state = ProjectGateStateUnknown
	}
	switch state {
	case ProjectGateStateUnknown, ProjectGateStateBlocked, ProjectGateStateSatisfied, ProjectGateStateWaived:
		return state, nil
	default:
		return "", fmt.Errorf("invalid project gate state: %s", value)
	}
}

func normalizeProjectGateKey(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func projectGateKeyKnown(value string) bool {
	switch normalizeProjectGateKey(value) {
	case "design_doc_ready",
		"implementation_plan_ready",
		"repo_ready_or_not_required",
		"repo_materialization_allowed",
		"strategic_lead_active",
		"implementation_phase_open",
		"reviewer_available_or_scarcity_open",
		"integration_evidence_ready",
		"validation_evidence_ready":
		return true
	default:
		return false
	}
}

func (s *Store) ensureProjectProfileTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, updatedBy, now string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	if workspaceID == "" || projectID == "" {
		return errors.New("workspace_id and project_id are required")
	}
	project, err := s.getProjectTx(ctx, tx, workspaceID, projectID)
	if err != nil {
		return err
	}
	projectID = project.ProjectID
	_, err = tx.ExecContext(ctx, `
INSERT OR IGNORE INTO project_profiles (
  workspace_id, project_id, goal, current_phase, repo_required, repo_status, updated_by, created_at, updated_at
)
SELECT workspace_id, project_id, description, 'INTAKE', 0, 'NOT_REQUIRED', ?, ?, ?
FROM projects
WHERE workspace_id = ? AND project_id = ?`,
		strings.TrimSpace(updatedBy), now, now, workspaceID, projectID)
	if err != nil {
		return fmt.Errorf("ensure project profile: %w", err)
	}
	return nil
}

func (s *Store) GetProjectProfile(ctx context.Context, workspaceID, projectID string) (ProjectProfileRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	if workspaceID == "" || projectID == "" {
		return ProjectProfileRecord{}, errors.New("workspace_id and project_id are required")
	}
	project, projectErr := s.GetProject(ctx, workspaceID, projectID)
	if projectErr != nil {
		return ProjectProfileRecord{}, projectErr
	}
	projectID = project.ProjectID
	profile, err := s.getProjectProfileRead(ctx, workspaceID, projectID)
	if err == nil {
		return profile, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ProjectProfileRecord{}, err
	}
	return defaultProjectProfileFromProject(*project), nil
}

func (s *Store) getProjectProfileRead(ctx context.Context, workspaceID, projectID string) (ProjectProfileRecord, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT workspace_id, project_id, goal, current_phase, design_doc_id, implementation_plan_doc_id,
       repo_required, repo_status, repo_url, repo_default_branch, updated_by, created_at, updated_at
  FROM project_profiles
 WHERE workspace_id = ? AND project_id = ?`,
		workspaceID, projectID)
	return scanProjectProfile(row)
}

func (s *Store) getProjectProfileTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string) (ProjectProfileRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT workspace_id, project_id, goal, current_phase, design_doc_id, implementation_plan_doc_id,
       repo_required, repo_status, repo_url, repo_default_branch, updated_by, created_at, updated_at
  FROM project_profiles
 WHERE workspace_id = ? AND project_id = ?`,
		workspaceID, projectID)
	return scanProjectProfile(row)
}

type projectProfileScanner interface {
	Scan(dest ...any) error
}

func scanProjectProfile(row projectProfileScanner) (ProjectProfileRecord, error) {
	var profile ProjectProfileRecord
	var repoRequired int
	if err := row.Scan(
		&profile.WorkspaceID,
		&profile.ProjectID,
		&profile.Goal,
		&profile.CurrentPhase,
		&profile.DesignDocID,
		&profile.ImplementationPlanDocID,
		&repoRequired,
		&profile.RepoStatus,
		&profile.RepoURL,
		&profile.RepoDefaultBranch,
		&profile.UpdatedBy,
		&profile.CreatedAt,
		&profile.UpdatedAt,
	); err != nil {
		return ProjectProfileRecord{}, err
	}
	profile.RepoRequired = sqliteIntToBool(repoRequired)
	return profile, nil
}

func defaultProjectProfileFromProject(project ProjectRecord) ProjectProfileRecord {
	return ProjectProfileRecord{
		WorkspaceID:  project.WorkspaceID,
		ProjectID:    project.ProjectID,
		Goal:         project.Description,
		CurrentPhase: ProjectPhaseIntake,
		RepoRequired: false,
		RepoStatus:   ProjectRepoStatusNotRequired,
		UpdatedBy:    project.CreatedBy,
		CreatedAt:    project.CreatedAt,
		UpdatedAt:    project.UpdatedAt,
	}
}

func (s *Store) UpsertProjectProfileWithEvent(ctx context.Context, input ProjectProfileUpdateInput) (ProjectProfileRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" {
		return ProjectProfileRecord{}, RuntimeEventRecord{}, errors.New("workspace_id and project_id are required")
	}
	if actorID == "" {
		return ProjectProfileRecord{}, RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	if !projectProfileInputHasChanges(input) {
		return ProjectProfileRecord{}, RuntimeEventRecord{}, errors.New("nothing to update")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectProfileRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectProfileRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectProfileRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project profile tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var profile ProjectProfileRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureProjectProfileTx(ctx, tx, workspaceID, projectID, actorID, now); err != nil {
			return err
		}
		current, err := s.getProjectProfileTx(ctx, tx, workspaceID, projectID)
		if err != nil {
			return fmt.Errorf("get project profile before update: %w", err)
		}
		repoRequired := current.RepoRequired
		if input.RepoRequired != nil {
			repoRequired = *input.RepoRequired
		}
		repoStatus := current.RepoStatus
		if input.RepoStatus != nil {
			repoStatus = strings.TrimSpace(*input.RepoStatus)
		}
		repoStatus, err = normalizeProjectRepoStatus(repoStatus, repoRequired)
		if err != nil {
			return err
		}
		if err := s.updateProjectProfileTx(ctx, tx, input, repoRequired, repoStatus, now); err != nil {
			return err
		}
		profile, err = s.getProjectProfileTx(ctx, tx, workspaceID, projectID)
		if err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project profile update"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(map[string]any{
			"workspace_id":                    workspaceID,
			"project_id":                      projectID,
			"actor_id":                        actorID,
			"entity_type":                     "project",
			"entity_id":                       projectID,
			"summary":                         "Project profile updated: " + projectID,
			"mutation_operation":              "profile.update",
			"current_phase":                   profile.CurrentPhase,
			"goal_changed":                    input.Goal != nil,
			"design_doc_changed":              input.DesignDocID != nil,
			"implementation_plan_doc_changed": input.ImplementationPlanDocID != nil,
			"repo_changed":                    input.RepoRequired != nil || input.RepoStatus != nil || input.RepoURL != nil || input.RepoDefaultBranch != nil,
			"profile":                         profile,
		}, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.profile.update"), map[string]string{
			"workspace_id":   workspaceID,
			"project_id":     projectID,
			"actor_id":       actorID,
			"principal_type": firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.profile.updated",
			EntityType:  "project",
			EntityID:    projectID,
			ActorType:   firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectProfileRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectProfileRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project profile update: %w", err)
	}
	return profile, event, nil
}

func projectProfileInputHasChanges(input ProjectProfileUpdateInput) bool {
	return input.Goal != nil ||
		input.DesignDocID != nil ||
		input.ImplementationPlanDocID != nil ||
		input.RepoRequired != nil ||
		input.RepoStatus != nil ||
		input.RepoURL != nil ||
		input.RepoDefaultBranch != nil
}

func (s *Store) updateProjectProfileTx(ctx context.Context, tx *sql.Tx, input ProjectProfileUpdateInput, repoRequired bool, repoStatus, now string) error {
	sets := []string{}
	args := []any{}
	if input.Goal != nil {
		sets = append(sets, "goal = ?")
		args = append(args, strings.TrimSpace(*input.Goal))
	}
	if input.DesignDocID != nil {
		sets = append(sets, "design_doc_id = ?")
		args = append(args, strings.TrimSpace(*input.DesignDocID))
	}
	if input.ImplementationPlanDocID != nil {
		sets = append(sets, "implementation_plan_doc_id = ?")
		args = append(args, strings.TrimSpace(*input.ImplementationPlanDocID))
	}
	if input.RepoRequired != nil {
		sets = append(sets, "repo_required = ?")
		args = append(args, boolToSQLiteInt(repoRequired))
	}
	if input.RepoStatus != nil || input.RepoRequired != nil {
		sets = append(sets, "repo_status = ?")
		args = append(args, repoStatus)
	}
	if input.RepoURL != nil {
		sets = append(sets, "repo_url = ?")
		args = append(args, strings.TrimSpace(*input.RepoURL))
	}
	if input.RepoDefaultBranch != nil {
		sets = append(sets, "repo_default_branch = ?")
		args = append(args, strings.TrimSpace(*input.RepoDefaultBranch))
	}
	sets = append(sets, "updated_by = ?", "updated_at = ?")
	args = append(args, strings.TrimSpace(input.ActorID), now, strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(input.ProjectID))
	query := "UPDATE project_profiles SET " + strings.Join(sets, ", ") + " WHERE workspace_id = ? AND project_id = ?"
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update project profile: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("project profile not found: %s", strings.TrimSpace(input.ProjectID))
	}
	return nil
}

func (s *Store) TransitionProjectPhaseWithEvent(ctx context.Context, input ProjectPhaseTransitionInput) (ProjectProfileRecord, ProjectPhaseHistoryRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	actorID := strings.TrimSpace(input.ActorID)
	toPhase, err := normalizeProjectPhase(input.ToPhase)
	if err != nil {
		return ProjectProfileRecord{}, ProjectPhaseHistoryRecord{}, RuntimeEventRecord{}, err
	}
	if workspaceID == "" || projectID == "" {
		return ProjectProfileRecord{}, ProjectPhaseHistoryRecord{}, RuntimeEventRecord{}, errors.New("workspace_id and project_id are required")
	}
	if actorID == "" {
		return ProjectProfileRecord{}, ProjectPhaseHistoryRecord{}, RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectProfileRecord{}, ProjectPhaseHistoryRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectProfileRecord{}, ProjectPhaseHistoryRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectProfileRecord{}, ProjectPhaseHistoryRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project phase tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var profile ProjectProfileRecord
	var history ProjectPhaseHistoryRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureProjectProfileTx(ctx, tx, workspaceID, projectID, actorID, now); err != nil {
			return err
		}
		current, err := s.getProjectProfileTx(ctx, tx, workspaceID, projectID)
		if err != nil {
			return err
		}
		fromPhase, err := normalizeProjectPhase(current.CurrentPhase)
		if err != nil {
			return err
		}
		if fromPhase == toPhase {
			return fmt.Errorf("project already in phase %s", toPhase)
		}
		if err := s.requireProjectStrategicLeadForPhaseTransitionTx(ctx, tx, workspaceID, projectID, actorID, input.ActorType, now, toPhase); err != nil && !coordinationModeTrustFirst(input.CoordinationMode) {
			return err
		}
		// W2.1 (P06): phase -> DONE is a terminal write and was the DUAL HOLE behind R44 - the DONE
		// receipt the empty-frontier reflection gate points lanes at was itself ungated. DONE now
		// requires the project completion contract (no open non-reflection task) REGARDLESS of
		// coordination mode: trust-first bypasses ONLY the strategic-lead AUTHORITY gate above, never
		// completion. Non-DONE transitions are never gated by this contract.
		if toPhase == ProjectPhaseDone {
			adm, err := s.evaluateProjectPhaseTerminalAdmissionTx(ctx, tx, workspaceID, projectID, actorID, input.CoordinationMode)
			if err != nil {
				return err
			}
			if adm.Decision == TerminalReject {
				return adm.Err
			}
		}
		history = ProjectPhaseHistoryRecord{
			HistoryID:      nextID("projphase"),
			WorkspaceID:    workspaceID,
			ProjectID:      projectID,
			FromPhase:      fromPhase,
			ToPhase:        toPhase,
			TransitionedBy: actorID,
			Reason:         strings.TrimSpace(input.Reason),
			CreatedAt:      now,
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO project_phase_history (
  history_id, workspace_id, project_id, from_phase, to_phase, transitioned_by, reason, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			history.HistoryID, workspaceID, projectID, fromPhase, toPhase, actorID, history.Reason, now); err != nil {
			return fmt.Errorf("insert project phase history: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE project_profiles
   SET current_phase = ?, updated_by = ?, updated_at = ?
 WHERE workspace_id = ? AND project_id = ?`,
			toPhase, actorID, now, workspaceID, projectID); err != nil {
			return fmt.Errorf("update project phase: %w", err)
		}
		profile, err = s.getProjectProfileTx(ctx, tx, workspaceID, projectID)
		if err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project phase transition"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(map[string]any{
			"workspace_id":       workspaceID,
			"project_id":         projectID,
			"actor_id":           actorID,
			"entity_type":        "project",
			"entity_id":          projectID,
			"summary":            fmt.Sprintf("Project phase transitioned: %s -> %s", fromPhase, toPhase),
			"mutation_operation": "phase.transition",
			"from_phase":         fromPhase,
			"to_phase":           toPhase,
			"reason":             history.Reason,
			"profile":            profile,
			"history":            history,
		}, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.phase.transition"), map[string]string{
			"workspace_id":   workspaceID,
			"project_id":     projectID,
			"actor_id":       actorID,
			"principal_type": firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.phase.transitioned",
			EntityType:  "project",
			EntityID:    projectID,
			ActorType:   firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectProfileRecord{}, ProjectPhaseHistoryRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectProfileRecord{}, ProjectPhaseHistoryRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project phase transition: %w", err)
	}
	return profile, history, event, nil
}

func (s *Store) GetProjectGateStatus(ctx context.Context, workspaceID, projectID string) (ProjectGateStatusRecord, error) {
	profile, err := s.GetProjectProfile(ctx, workspaceID, projectID)
	if err != nil {
		return ProjectGateStatusRecord{}, err
	}
	repositories, err := s.ListProjectRepositories(ctx, ProjectRepositoryListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
	})
	if err != nil {
		return ProjectGateStatusRecord{}, err
	}
	profile = reconcileProjectProfileWithRepositories(profile, repositories)
	rows, err := s.db.QueryContext(ctx, `
SELECT gate_key, state, summary, evidence_ref, updated_by, updated_at
  FROM project_gate_status
 WHERE workspace_id = ? AND project_id = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID))
	if err != nil {
		return ProjectGateStatusRecord{}, fmt.Errorf("list project gates: %w", err)
	}
	defer rows.Close()
	overrides := map[string]ProjectGateRecord{}
	for rows.Next() {
		var gate ProjectGateRecord
		if err := rows.Scan(&gate.GateKey, &gate.State, &gate.Summary, &gate.EvidenceRef, &gate.UpdatedBy, &gate.UpdatedAt); err != nil {
			return ProjectGateStatusRecord{}, fmt.Errorf("scan project gate: %w", err)
		}
		gate.GateKey = normalizeProjectGateKey(gate.GateKey)
		gate.Source = "manual"
		overrides[gate.GateKey] = gate
	}
	if err := rows.Err(); err != nil {
		return ProjectGateStatusRecord{}, err
	}
	_, leadActive, err := s.GetActiveProjectStrategicLead(ctx, workspaceID, projectID)
	if err != nil {
		return ProjectGateStatusRecord{}, err
	}
	return buildProjectGateStatus(profile, overrides, leadActive), nil
}

func buildProjectGateStatus(profile ProjectProfileRecord, overrides map[string]ProjectGateRecord, strategicLeadActive bool) ProjectGateStatusRecord {
	now := profile.UpdatedAt
	gates := []ProjectGateRecord{
		projectGateFromProfile("design_doc_ready", profile.DesignDocID != "", true, "Design document is required before implementation work", profile.DesignDocID, now),
		projectGateFromProfile("implementation_plan_ready", profile.ImplementationPlanDocID != "", true, "Implementation plan is required before implementation work", profile.ImplementationPlanDocID, now),
		projectGateFromProfile("repo_ready_or_not_required", !profile.RepoRequired || profile.RepoStatus == ProjectRepoStatusReady || profile.RepoStatus == ProjectRepoStatusNotRequired, true, "Repository must be ready or explicitly not required", profile.RepoURL, now),
		projectGateFromProfile("repo_materialization_allowed", !profile.RepoRequired || profile.RepoURL != "" || profile.RepoStatus == ProjectRepoStatusReady, true, "Repository materialization must be explicitly possible before code work", profile.RepoURL, now),
		projectGateFromProfile("strategic_lead_active", strategicLeadActive, true, "Active strategic lead lease is required before implementation work", "", now),
		projectGateFromProfile("implementation_phase_open", projectPhaseAllowsImplementationWork(profile.CurrentPhase), true, "Project phase must be IMPLEMENTATION or later active delivery phase before implementation work", profile.CurrentPhase, now),
		{GateKey: "reviewer_available_or_scarcity_open", State: ProjectGateStateUnknown, Required: false, Summary: "Reviewer availability is established in later role waves", UpdatedAt: now, Source: "derived"},
		{GateKey: "integration_evidence_ready", State: ProjectGateStateUnknown, Required: false, Summary: "Integration evidence is collected after implementation", UpdatedAt: now, Source: "derived"},
		{GateKey: "validation_evidence_ready", State: ProjectGateStateUnknown, Required: false, Summary: "Validation evidence is collected after integration", UpdatedAt: now, Source: "derived"},
	}
	for i := range gates {
		if override, ok := overrides[gates[i].GateKey]; ok {
			override.Required = gates[i].Required
			override.Source = "manual"
			gates[i] = override
		}
	}
	implementationReady := true
	overall := ProjectGateStateSatisfied
	for _, gate := range gates {
		if !gate.Required {
			if overall == ProjectGateStateSatisfied && gate.State == ProjectGateStateUnknown {
				overall = "PARTIAL"
			}
			continue
		}
		if gate.State != ProjectGateStateSatisfied && gate.State != ProjectGateStateWaived {
			if gate.GateKey == "design_doc_ready" ||
				gate.GateKey == "implementation_plan_ready" ||
				gate.GateKey == "repo_ready_or_not_required" ||
				gate.GateKey == "repo_materialization_allowed" ||
				gate.GateKey == "strategic_lead_active" ||
				gate.GateKey == "implementation_phase_open" {
				implementationReady = false
			}
			overall = ProjectGateStateBlocked
		}
	}
	return ProjectGateStatusRecord{
		WorkspaceID:         profile.WorkspaceID,
		ProjectID:           profile.ProjectID,
		CurrentPhase:        profile.CurrentPhase,
		OverallState:        overall,
		ImplementationReady: implementationReady,
		Gates:               gates,
		UpdatedAt:           now,
	}
}

func projectGateFromProfile(key string, satisfied, required bool, summary, evidenceRef, updatedAt string) ProjectGateRecord {
	state := ProjectGateStateBlocked
	if satisfied {
		state = ProjectGateStateSatisfied
	}
	return ProjectGateRecord{
		GateKey:     key,
		State:       state,
		Required:    required,
		Summary:     summary,
		EvidenceRef: strings.TrimSpace(evidenceRef),
		UpdatedAt:   updatedAt,
		Source:      "derived",
	}
}

func reconcileProjectProfileWithRepositories(profile ProjectProfileRecord, repositories []ProjectRepositoryRecord) ProjectProfileRecord {
	for _, repo := range repositories {
		if !repo.IsCanonical || strings.EqualFold(strings.TrimSpace(repo.RepoStatus), ProjectRepositoryStatusArchived) {
			continue
		}
		profile.RepoRequired = true
		profile.RepoStatus = projectProfileRepoStatusFromRepositoryStatus(repo.RepoStatus)
		profile.RepoURL = strings.TrimSpace(repo.RemoteURL)
		profile.RepoDefaultBranch = firstNonEmpty(strings.TrimSpace(repo.DefaultBranch), strings.TrimSpace(profile.RepoDefaultBranch))
		if profile.UpdatedAt == "" || strings.TrimSpace(repo.UpdatedAt) > strings.TrimSpace(profile.UpdatedAt) {
			profile.UpdatedAt = strings.TrimSpace(repo.UpdatedAt)
			profile.UpdatedBy = strings.TrimSpace(repo.UpdatedBy)
		}
		return profile
	}
	return profile
}

func (s *Store) UpsertProjectGateStatusWithEvent(ctx context.Context, input ProjectGateStatusUpdateInput) (ProjectGateStatusRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	actorID := strings.TrimSpace(input.ActorID)
	gateKey := normalizeProjectGateKey(input.GateKey)
	state, err := normalizeProjectGateState(input.State)
	if err != nil {
		return ProjectGateStatusRecord{}, RuntimeEventRecord{}, err
	}
	if workspaceID == "" || projectID == "" || gateKey == "" {
		return ProjectGateStatusRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, and gate_key are required")
	}
	if !projectGateKeyKnown(gateKey) {
		return ProjectGateStatusRecord{}, RuntimeEventRecord{}, fmt.Errorf("unknown project gate key: %s", gateKey)
	}
	if actorID == "" {
		return ProjectGateStatusRecord{}, RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectGateStatusRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectGateStatusRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectGateStatusRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project gate tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureProjectProfileTx(ctx, tx, workspaceID, projectID, actorID, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO project_gate_status (
  workspace_id, project_id, gate_key, state, summary, evidence_ref, updated_by, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(workspace_id, project_id, gate_key) DO UPDATE SET
  state = excluded.state,
  summary = excluded.summary,
  evidence_ref = excluded.evidence_ref,
  updated_by = excluded.updated_by,
  updated_at = excluded.updated_at`,
			workspaceID, projectID, gateKey, state, strings.TrimSpace(input.Summary), strings.TrimSpace(input.EvidenceRef), actorID, now); err != nil {
			return fmt.Errorf("upsert project gate: %w", err)
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project gate update"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(map[string]any{
			"workspace_id":       workspaceID,
			"project_id":         projectID,
			"actor_id":           actorID,
			"entity_type":        "project_gate",
			"entity_id":          gateKey,
			"summary":            "Project gate updated: " + gateKey + " -> " + state,
			"mutation_operation": "gate.update",
			"gate_key":           gateKey,
			"state":              state,
			"evidence_ref":       strings.TrimSpace(input.EvidenceRef),
		}, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.gate.update"), map[string]string{
			"workspace_id":   workspaceID,
			"project_id":     projectID,
			"actor_id":       actorID,
			"principal_type": firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.gate.updated",
			EntityType:  "project_gate",
			EntityID:    gateKey,
			ActorType:   firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectGateStatusRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectGateStatusRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project gate update: %w", err)
	}
	status, err := s.GetProjectGateStatus(ctx, workspaceID, projectID)
	if err != nil {
		return ProjectGateStatusRecord{}, RuntimeEventRecord{}, err
	}
	return status, event, nil
}

type sqlReadQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *Store) GetProjectCoordination(ctx context.Context, workspaceID, projectID string) (ProjectCoordinationRecord, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ProjectCoordinationRecord{}, fmt.Errorf("begin project coordination read tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	coordination, err := s.getProjectCoordinationTx(ctx, tx, workspaceID, projectID)
	if err != nil {
		return ProjectCoordinationRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return ProjectCoordinationRecord{}, fmt.Errorf("commit project coordination read tx: %w", err)
	}
	return coordination, nil
}

func (s *Store) getProjectCoordinationTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string) (ProjectCoordinationRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	project, err := s.getProjectTx(ctx, tx, workspaceID, projectID)
	if err != nil {
		return ProjectCoordinationRecord{}, err
	}
	projectID = project.ProjectID
	profile, err := s.getProjectProfileTx(ctx, tx, workspaceID, projectID)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return ProjectCoordinationRecord{}, err
		}
		profile = defaultProjectProfileFromProject(project)
	}
	snapshotTime := time.Now().UTC()
	snapshotAt := snapshotTime.Format(time.RFC3339Nano)
	roles, err := listProjectRolesForCoordinationTx(ctx, tx, workspaceID, projectID, false, snapshotAt)
	if err != nil {
		return ProjectCoordinationRecord{}, err
	}
	var strategicLead *ProjectRoleRecord
	for i := range roles {
		if roles[i].RoleType == ProjectRoleStrategicLead {
			role := roles[i]
			strategicLead = &role
			break
		}
	}
	repositories, err := listProjectRepositoriesForCoordinationTx(ctx, tx, ProjectRepositoryListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
	})
	if err != nil {
		return ProjectCoordinationRecord{}, err
	}
	profile = reconcileProjectProfileWithRepositories(profile, repositories)
	gates, err := s.getProjectGateStatusTx(ctx, tx, workspaceID, projectID, profile, snapshotAt)
	if err != nil {
		return ProjectCoordinationRecord{}, err
	}
	checkouts, err := listProjectCheckoutsForCoordinationTx(ctx, tx, ProjectCheckoutListFilter{
		WorkspaceID:        workspaceID,
		ProjectID:          projectID,
		StaleAfterSeconds:  3600,
		ReferenceTimestamp: snapshotAt,
	})
	if err != nil {
		return ProjectCoordinationRecord{}, err
	}
	branches, err := listProjectBranchesForCoordinationTx(ctx, tx, ProjectBranchListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
	})
	if err != nil {
		return ProjectCoordinationRecord{}, err
	}
	patchQueueItems, err := listProjectPatchQueueItemsForCoordinationTx(ctx, tx, ProjectPatchQueueListFilter{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
	})
	if err != nil {
		return ProjectCoordinationRecord{}, err
	}
	serviceRuns, err := listServiceRunsForCoordinationTx(ctx, tx, workspaceID, projectID)
	if err != nil {
		return ProjectCoordinationRecord{}, err
	}
	tasks, err := s.listWorkspaceTasksForProjectTx(ctx, tx, workspaceID, projectID)
	if err != nil {
		return ProjectCoordinationRecord{}, err
	}
	laneCounts, statusCounts, openCount := projectTaskCountsFromRecords(tasks)
	sourceEventIDs, latestEventID, coordinationVersion := s.projectCoordinationSourceEventsTx(ctx, tx, workspaceID, projectID, profile.UpdatedAt)
	if checkoutVersion := projectCheckoutCoordinationVersion(checkouts); checkoutVersion != "" {
		coordinationVersion = firstNonEmpty(strings.TrimSpace(coordinationVersion), strings.TrimSpace(profile.UpdatedAt), snapshotAt) + "|checkouts:" + checkoutVersion
	}
	if branchVersion := projectBranchCoordinationVersion(branches); branchVersion != "" {
		coordinationVersion = firstNonEmpty(strings.TrimSpace(coordinationVersion), strings.TrimSpace(profile.UpdatedAt), snapshotAt) + "|branches:" + branchVersion
	}
	if patchQueueVersion := projectPatchQueueCoordinationVersion(patchQueueItems); patchQueueVersion != "" {
		coordinationVersion = firstNonEmpty(strings.TrimSpace(coordinationVersion), strings.TrimSpace(profile.UpdatedAt), snapshotAt) + "|patch_queue:" + patchQueueVersion
	}
	if serviceRunVersion := projectServiceRunCoordinationVersion(serviceRuns); serviceRunVersion != "" {
		coordinationVersion = firstNonEmpty(strings.TrimSpace(coordinationVersion), strings.TrimSpace(profile.UpdatedAt), snapshotAt) + "|service_runs:" + serviceRunVersion
	}
	if taskVersion := projectTaskCoordinationVersion(tasks); taskVersion != "" {
		coordinationVersion = firstNonEmpty(strings.TrimSpace(coordinationVersion), strings.TrimSpace(profile.UpdatedAt), snapshotAt) + "|tasks:" + taskVersion
	}
	return ProjectCoordinationRecord{
		SnapshotAt:          snapshotAt,
		CoordinationVersion: coordinationVersion,
		LatestEventID:       latestEventID,
		SourceEventIDs:      sourceEventIDs,
		Project:             project,
		Profile:             profile,
		GateStatus:          gates,
		StrategicLead:       strategicLead,
		Roles:               roles,
		Repositories:        repositories,
		Checkouts:           checkouts,
		Branches:            branches,
		PatchQueueItems:     patchQueueItems,
		ServiceRuns:         serviceRuns,
		Tasks:               tasks,
		OpenTaskCount:       openCount,
		TaskCountsByLane:    laneCounts,
		TaskCountsByStatus:  statusCounts,
	}, nil
}

func (s *Store) projectCoordinationSourceEvents(ctx context.Context, workspaceID, projectID, fallbackVersion string) (map[string]string, string, string) {
	return s.projectCoordinationSourceEventsWithQuerier(ctx, s.db, workspaceID, projectID, fallbackVersion)
}

func (s *Store) projectCoordinationSourceEventsTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, fallbackVersion string) (map[string]string, string, string) {
	return s.projectCoordinationSourceEventsWithQuerier(ctx, tx, workspaceID, projectID, fallbackVersion)
}

func (s *Store) projectCoordinationSourceEventsWithQuerier(ctx context.Context, q sqlReadQuerier, workspaceID, projectID, fallbackVersion string) (map[string]string, string, string) {
	rows, err := q.QueryContext(ctx, `
SELECT event_type, event_id, created_at, COALESCE(ingest_seq, 0)
  FROM runtime_events
 WHERE workspace_id = ?
   AND event_type LIKE 'project.%'
   AND (
        (entity_type = 'project' AND entity_id = ?)
        OR (json_valid(payload_json) AND json_extract(payload_json, '$.project_id') = ?)
   )
 ORDER BY ingest_seq DESC, created_at DESC, event_id DESC
 LIMIT 100`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), strings.TrimSpace(projectID))
	if err != nil {
		return nil, "", strings.TrimSpace(fallbackVersion)
	}
	defer rows.Close()
	sourceEventIDs := map[string]string{}
	latestEventID := ""
	coordinationVersion := strings.TrimSpace(fallbackVersion)
	for rows.Next() {
		var eventType, eventID, createdAt string
		var ingestSeq int64
		if err := rows.Scan(&eventType, &eventID, &createdAt, &ingestSeq); err != nil {
			return sourceEventIDs, latestEventID, coordinationVersion
		}
		if latestEventID == "" {
			latestEventID = eventID
			coordinationVersion = fmt.Sprintf("%d:%s:%s", ingestSeq, strings.TrimSpace(createdAt), strings.TrimSpace(eventID))
		}
		if _, ok := sourceEventIDs[eventType]; !ok {
			sourceEventIDs[eventType] = eventID
		}
	}
	if len(sourceEventIDs) == 0 {
		sourceEventIDs = nil
	}
	return sourceEventIDs, latestEventID, coordinationVersion
}

func (s *Store) getProjectGateStatusTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string, profile ProjectProfileRecord, now string) (ProjectGateStatusRecord, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT gate_key, state, summary, evidence_ref, updated_by, updated_at
  FROM project_gate_status
 WHERE workspace_id = ? AND project_id = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID))
	if err != nil {
		return ProjectGateStatusRecord{}, fmt.Errorf("list project gates: %w", err)
	}
	defer rows.Close()
	overrides := map[string]ProjectGateRecord{}
	for rows.Next() {
		var gate ProjectGateRecord
		if err := rows.Scan(&gate.GateKey, &gate.State, &gate.Summary, &gate.EvidenceRef, &gate.UpdatedBy, &gate.UpdatedAt); err != nil {
			return ProjectGateStatusRecord{}, fmt.Errorf("scan project gate: %w", err)
		}
		gate.GateKey = normalizeProjectGateKey(gate.GateKey)
		gate.Source = "manual"
		overrides[gate.GateKey] = gate
	}
	if err := rows.Err(); err != nil {
		return ProjectGateStatusRecord{}, err
	}
	_, leadActive, err := s.getActiveProjectStrategicLeadTx(ctx, tx, workspaceID, projectID, now)
	if err != nil {
		return ProjectGateStatusRecord{}, err
	}
	return buildProjectGateStatus(profile, overrides, leadActive), nil
}

func listProjectRolesForCoordinationTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string, includeInactive bool, now string) ([]ProjectRoleRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	if workspaceID == "" || projectID == "" {
		return nil, errors.New("workspace_id and project_id are required")
	}
	if strings.TrimSpace(now) == "" {
		now = time.Now().UTC().Format(time.RFC3339Nano)
	}
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
	rows, err := tx.QueryContext(ctx, query, args...)
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

func listProjectRepositoriesForCoordinationTx(ctx context.Context, tx *sql.Tx, filter ProjectRepositoryListFilter) ([]ProjectRepositoryRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	projectID := strings.TrimSpace(filter.ProjectID)
	if workspaceID == "" || projectID == "" {
		return nil, errors.New("workspace_id and project_id are required")
	}
	query := `
SELECT repo_id, workspace_id, project_id, remote_url, remote_kind, owner, name, default_branch,
       integration_branch, credential_vault_entry_id, repo_status, is_canonical, created_by_agent_id,
       updated_by, created_at, updated_at
  FROM project_repositories
 WHERE workspace_id = ? AND project_id = ?`
	args := []any{workspaceID, projectID}
	if !filter.IncludeArchived {
		query += ` AND repo_status <> 'ARCHIVED'`
	}
	query += ` ORDER BY is_canonical DESC, updated_at DESC, repo_id ASC`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list project repositories: %w", err)
	}
	defer rows.Close()
	var repos []ProjectRepositoryRecord
	for rows.Next() {
		repo, err := scanProjectRepository(rows)
		if err != nil {
			return nil, err
		}
		repos = append(repos, repo)
	}
	return repos, rows.Err()
}

func listProjectCheckoutsForCoordinationTx(ctx context.Context, tx *sql.Tx, filter ProjectCheckoutListFilter) ([]ProjectCheckoutRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	projectID := strings.TrimSpace(filter.ProjectID)
	if workspaceID == "" || projectID == "" {
		return nil, errors.New("workspace_id and project_id are required")
	}
	query := `
SELECT checkout_id, workspace_id, project_id, repo_id, machine_id, machine_label, owner_user_id, agent_id,
       local_path, checkout_kind, branch_name, base_branch, head_sha, base_sha, dirty_state,
       active_task_id, active_claim_id, status, last_seen_at, updated_by, created_at, updated_at
  FROM project_checkout_registry
 WHERE workspace_id = ? AND project_id = ?`
	args := []any{workspaceID, projectID}
	if repoID := strings.TrimSpace(filter.RepoID); repoID != "" {
		query += ` AND repo_id = ?`
		args = append(args, repoID)
	}
	if agentID := strings.TrimSpace(filter.AgentID); agentID != "" {
		query += ` AND agent_id = ?`
		args = append(args, agentID)
	}
	if !filter.IncludeInactive {
		query += ` AND status IN ('ACTIVE', 'BLOCKED', 'STALE')`
	}
	query += ` ORDER BY updated_at DESC, checkout_id ASC`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list project checkouts: %w", err)
	}
	defer rows.Close()
	referenceAt := parseOptionalRFC3339(filter.ReferenceTimestamp, time.Now().UTC())
	staleAfter := time.Duration(filter.StaleAfterSeconds) * time.Second
	if staleAfter <= 0 {
		staleAfter = time.Hour
	}
	var checkouts []ProjectCheckoutRecord
	for rows.Next() {
		checkout, err := scanProjectCheckout(rows)
		if err != nil {
			return nil, err
		}
		checkout.DerivedStatus = deriveProjectCheckoutStatus(checkout, referenceAt, staleAfter)
		checkouts = append(checkouts, checkout)
	}
	return checkouts, rows.Err()
}

func listProjectBranchesForCoordinationTx(ctx context.Context, tx *sql.Tx, filter ProjectBranchListFilter) ([]ProjectBranchRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	projectID := strings.TrimSpace(filter.ProjectID)
	if workspaceID == "" || projectID == "" {
		return nil, errors.New("workspace_id and project_id are required")
	}
	query := `
SELECT branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id, active_task_id,
       active_claim_id, branch_name, branch_kind, base_branch, head_sha, base_sha,
       write_scope_json, review_doc_key, status, updated_by, created_at, updated_at
  FROM project_branch_registry
 WHERE workspace_id = ? AND project_id = ?`
	args := []any{workspaceID, projectID}
	if repoID := strings.TrimSpace(filter.RepoID); repoID != "" {
		query += ` AND repo_id = ?`
		args = append(args, repoID)
	}
	if agentID := strings.TrimSpace(filter.AgentID); agentID != "" {
		query += ` AND agent_id = ?`
		args = append(args, agentID)
	}
	if activeTaskID := strings.TrimSpace(filter.ActiveTaskID); activeTaskID != "" {
		query += ` AND active_task_id = ?`
		args = append(args, activeTaskID)
	}
	if !filter.IncludeInactive {
		query += ` AND status IN ('RESERVED', 'ACTIVE', 'BLOCKED', 'READY_FOR_REVIEW')`
	}
	query += ` ORDER BY updated_at DESC, branch_id ASC`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list project branches: %w", err)
	}
	defer rows.Close()
	var branches []ProjectBranchRecord
	for rows.Next() {
		branch, err := scanProjectBranch(rows)
		if err != nil {
			return nil, err
		}
		branches = append(branches, branch)
	}
	return branches, rows.Err()
}

func listProjectPatchQueueItemsForCoordinationTx(ctx context.Context, tx *sql.Tx, filter ProjectPatchQueueListFilter) ([]ProjectPatchQueueItemRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	projectID := strings.TrimSpace(filter.ProjectID)
	if workspaceID == "" || projectID == "" {
		return nil, errors.New("workspace_id and project_id are required")
	}
	query := `
SELECT ` + projectPatchQueueItemSelectColumns + `
  FROM project_patch_queue_items
 WHERE workspace_id = ? AND project_id = ?`
	args := []any{workspaceID, projectID}
	if repoID := strings.TrimSpace(filter.RepoID); repoID != "" {
		query += ` AND repo_id = ?`
		args = append(args, repoID)
	}
	if branchID := strings.TrimSpace(filter.BranchID); branchID != "" {
		query += ` AND branch_id = ?`
		args = append(args, branchID)
	}
	if state := strings.ToUpper(strings.TrimSpace(filter.State)); state != "" {
		query += ` AND state = ?`
		args = append(args, state)
	}
	query += ` ORDER BY updated_at DESC, queue_id ASC, item_id ASC`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list project patch queue items: %w", err)
	}
	defer rows.Close()
	var items []ProjectPatchQueueItemRecord
	for rows.Next() {
		item, err := scanProjectPatchQueueItem(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return annotateProjectPatchQueueReviewTaskReceiptsTx(ctx, tx, items)
}

func projectCheckoutCoordinationVersion(checkouts []ProjectCheckoutRecord) string {
	if len(checkouts) == 0 {
		return ""
	}
	parts := make([]string, 0, len(checkouts))
	for _, checkout := range checkouts {
		parts = append(parts, strings.Join([]string{
			strings.TrimSpace(checkout.CheckoutID),
			strings.TrimSpace(checkout.RepoID),
			strings.TrimSpace(checkout.AgentID),
			strings.TrimSpace(checkout.Status),
			strings.TrimSpace(checkout.DerivedStatus),
			strings.TrimSpace(checkout.LastSeenAt),
			strings.TrimSpace(checkout.UpdatedAt),
		}, ":"))
	}
	return strings.Join(parts, ";")
}

func projectBranchCoordinationVersion(branches []ProjectBranchRecord) string {
	if len(branches) == 0 {
		return ""
	}
	parts := make([]string, 0, len(branches))
	for _, branch := range branches {
		parts = append(parts, strings.Join([]string{
			strings.TrimSpace(branch.BranchID),
			strings.TrimSpace(branch.RepoID),
			strings.TrimSpace(branch.CheckoutID),
			strings.TrimSpace(branch.AgentID),
			strings.TrimSpace(branch.ActiveTaskID),
			strings.TrimSpace(branch.ActiveClaimID),
			strings.TrimSpace(branch.BranchName),
			strings.TrimSpace(branch.Status),
			strings.TrimSpace(branch.UpdatedAt),
		}, ":"))
	}
	return strings.Join(parts, ";")
}

func projectPatchQueueCoordinationVersion(items []ProjectPatchQueueItemRecord) string {
	if len(items) == 0 {
		return ""
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, strings.Join([]string{
			strings.TrimSpace(item.QueueID),
			strings.TrimSpace(item.ItemID),
			strings.TrimSpace(item.RepoID),
			strings.TrimSpace(item.BranchID),
			strings.TrimSpace(item.State),
			strings.TrimSpace(item.HeadSHA),
			strings.TrimSpace(item.UpdatedAt),
		}, ":"))
	}
	return strings.Join(parts, ";")
}

func projectServiceRunCoordinationVersion(runs []ServiceRunRecord) string {
	if len(runs) == 0 {
		return ""
	}
	parts := make([]string, 0, len(runs))
	for _, run := range runs {
		parts = append(parts, strings.Join([]string{
			strings.TrimSpace(run.RunID),
			strings.TrimSpace(run.CandidateID),
			strings.TrimSpace(run.ProjectID),
			strings.TrimSpace(run.Status),
			strings.TrimSpace(run.PublicURL),
			strings.TrimSpace(run.UpdatedAt),
			strings.TrimSpace(run.CompletedAt),
		}, ":"))
	}
	return strings.Join(parts, ";")
}

func projectTaskCoordinationVersion(tasks []WorkspaceTaskRecord) string {
	if len(tasks) == 0 {
		return ""
	}
	parts := make([]string, 0, len(tasks))
	for _, task := range tasks {
		claimAgentID := ""
		if task.ClaimAgentID != nil {
			claimAgentID = strings.TrimSpace(*task.ClaimAgentID)
		}
		claimStatus := ""
		if task.ClaimStatus != nil {
			claimStatus = strings.TrimSpace(*task.ClaimStatus)
		}
		claimUpdatedAt := ""
		if task.ClaimUpdatedAt != nil {
			claimUpdatedAt = strings.TrimSpace(*task.ClaimUpdatedAt)
		}
		parts = append(parts, strings.Join([]string{
			strings.TrimSpace(task.TaskID),
			strings.TrimSpace(task.Title),
			strings.TrimSpace(task.Description),
			strings.TrimSpace(task.Priority),
			strings.TrimSpace(task.Status),
			strings.TrimSpace(task.ProjectLane),
			strings.Join(normalizeStringSlice(task.Tags), ","),
			fmt.Sprintf("%t", task.RequiresProjectGate),
			strings.TrimSpace(task.CloseReason),
			claimAgentID,
			claimStatus,
			claimUpdatedAt,
			strings.TrimSpace(task.LinkedAt),
			strings.TrimSpace(task.UpdatedAt),
		}, ":"))
	}
	return strings.Join(parts, ";")
}

func (s *Store) requireProjectStrategicLeadForPhaseTransitionTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, actorID, actorType, now, toPhase string) error {
	toPhase = strings.TrimSpace(toPhase)
	if toPhase == "" || toPhase == ProjectPhaseIntake {
		return nil
	}
	if err := s.expireProjectStrategicLeadTx(ctx, tx, workspaceID, projectID, now); err != nil {
		return err
	}
	lead, ok, err := s.getActiveProjectStrategicLeadTx(ctx, tx, workspaceID, projectID, now)
	if err != nil {
		return err
	}
	if !ok {
		return ErrProjectLeadRequired
	}
	return requireProjectLeadActor(lead, actorID, actorType)
}

func projectTaskCountsFromRecords(tasks []WorkspaceTaskRecord) (map[string]int, map[string]int, int) {
	laneCounts := map[string]int{}
	statusCounts := map[string]int{}
	openCount := 0
	for _, task := range tasks {
		lane := strings.TrimSpace(task.ProjectLane)
		if lane == "" {
			lane = "unlaned"
		}
		status := strings.TrimSpace(task.Status)
		if status == "" {
			status = taskStatusPending
		}
		laneCounts[lane]++
		statusCounts[status]++
		if !isTerminalTaskStatus(status) {
			openCount++
		}
	}
	return laneCounts, statusCounts, openCount
}

func projectLaneRequiresImplementationGate(projectLane string) bool {
	switch strings.ToLower(strings.TrimSpace(projectLane)) {
	case "implementation", "implement", "coding", "code", "frontend", "front-end", "ui", "backend", "back-end", "api", "fullstack", "full-stack":
		return true
	default:
		return false
	}
}

func projectTaskRequiresImplementationGate(task WorkspaceTaskRecord) bool {
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if projectLaneBypassesImplementationGate(lane) {
		return false
	}
	if agentWorkTaskBypassesImplementationGateByStructuredContract(task) {
		return false
	}
	return task.RequiresProjectGate || projectLaneRequiresImplementationGate(task.ProjectLane)
}

func projectLaneBypassesImplementationGate(projectLane string) bool {
	switch strings.ToLower(strings.TrimSpace(projectLane)) {
	case "strategy", "strategic", "planning", "plan", "spec", "specification", "requirements", "design", "framing", "review", "qa", "test", "testing", "verification", "validation", "acceptance", "integration", "integrator", "integrate", "synthesis", "summary", "summarization", "documentation", "docs", "handoff", "report", "final":
		return true
	default:
		return false
	}
}

func projectPhaseAllowsImplementationWork(phase string) bool {
	switch strings.ToUpper(strings.TrimSpace(phase)) {
	case ProjectPhaseImplementation, ProjectPhaseReview, ProjectPhaseIntegration, ProjectPhaseValidation:
		return true
	default:
		return false
	}
}

func projectGateSatisfied(state string) bool {
	state = strings.ToUpper(strings.TrimSpace(state))
	return state == ProjectGateStateSatisfied || state == ProjectGateStateWaived
}
