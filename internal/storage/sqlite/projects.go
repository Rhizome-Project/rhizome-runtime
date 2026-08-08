package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ── Project types ────────────────────────────────────────────────────

type ProjectRecord struct {
	ProjectID   string `json:"project_id"`
	WorkspaceID string `json:"workspace_id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Status      string `json:"status"`
	CreatedBy   string `json:"created_by"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
	TaskCount   int    `json:"task_count"`
}

type ProjectCreateInput struct {
	ProjectID   string
	WorkspaceID string
	Title       string
	Description string
	CreatedBy   string
	ActorID     string
	ActorType   string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

// ── Project CRUD ─────────────────────────────────────────────────────

func (s *Store) CreateProject(ctx context.Context, input ProjectCreateInput) error {
	projectID := strings.TrimSpace(input.ProjectID)
	if projectID == "" {
		return errors.New("project_id is required")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	title := strings.TrimSpace(input.Title)
	if title == "" {
		return errors.New("title is required")
	}
	createdBy := strings.TrimSpace(input.CreatedBy)
	if createdBy == "" {
		return errors.New("created_by is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin project create tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.createProjectTx(ctx, tx, input, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit project create: %w", err)
	}
	return nil
}

func (s *Store) CreateProjectWithEvent(ctx context.Context, input ProjectCreateInput) (ProjectRecord, RuntimeEventRecord, error) {
	projectID := strings.TrimSpace(input.ProjectID)
	if projectID == "" {
		return ProjectRecord{}, RuntimeEventRecord{}, errors.New("project_id is required")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return ProjectRecord{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	title := strings.TrimSpace(input.Title)
	if title == "" {
		return ProjectRecord{}, RuntimeEventRecord{}, errors.New("title is required")
	}
	createdBy := strings.TrimSpace(input.CreatedBy)
	if createdBy == "" {
		return ProjectRecord{}, RuntimeEventRecord{}, errors.New("created_by is required")
	}
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		actorID = createdBy
	}
	if actorID != createdBy {
		return ProjectRecord{}, RuntimeEventRecord{}, errors.New("actor_id must match created_by")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project create tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.createProjectTx(ctx, tx, input, now); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project create"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(map[string]any{
			"workspace_id":       workspaceID,
			"project_id":         projectID,
			"actor_id":           actorID,
			"created_by":         createdBy,
			"title":              title,
			"description":        strings.TrimSpace(input.Description),
			"status":             "ACTIVE",
			"entity_type":        "project",
			"entity_id":          projectID,
			"summary":            "Project created: " + title,
			"mutation_operation": "create",
		}, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.create"), map[string]string{
			"workspace_id":   workspaceID,
			"project_id":     projectID,
			"actor_id":       actorID,
			"created_by":     createdBy,
			"principal_type": firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.created",
			EntityType:  "project",
			EntityID:    projectID,
			ActorType:   firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project create: %w", err)
	}
	project, err := s.GetProject(ctx, workspaceID, projectID)
	if err != nil {
		return ProjectRecord{}, RuntimeEventRecord{}, err
	}
	return *project, event, nil
}

func (s *Store) createProjectTx(ctx context.Context, tx *sql.Tx, input ProjectCreateInput, now string) error {
	projectID := strings.TrimSpace(input.ProjectID)
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	title := strings.TrimSpace(input.Title)
	createdBy := strings.TrimSpace(input.CreatedBy)
	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx,
		`INSERT INTO projects (project_id, workspace_id, title, description, status, created_by, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 'ACTIVE', ?, ?, ?)`,
		projectID, workspaceID, title, strings.TrimSpace(input.Description), createdBy, now, now,
	)
	if err != nil {
		return fmt.Errorf("create project: %w", err)
	}
	if err := s.ensureProjectProfileTx(ctx, tx, workspaceID, projectID, createdBy, now); err != nil {
		return err
	}
	return nil
}

func (s *Store) ListProjects(ctx context.Context, workspaceID string) ([]ProjectRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT p.project_id, p.workspace_id, p.title, p.description, p.status, p.created_by, p.created_at, p.updated_at,
		        COALESCE((SELECT COUNT(*)
		          FROM workspace_tasks wt
		          JOIN tasks t ON t.task_id = wt.task_id
		         WHERE wt.workspace_id = p.workspace_id AND t.project_id = p.project_id), 0) as task_count
		 FROM projects p
		 WHERE p.workspace_id = ?
		 ORDER BY p.created_at DESC`,
		workspaceID,
	)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()

	var projects []ProjectRecord
	for rows.Next() {
		var p ProjectRecord
		if err := rows.Scan(&p.ProjectID, &p.WorkspaceID, &p.Title, &p.Description, &p.Status, &p.CreatedBy, &p.CreatedAt, &p.UpdatedAt, &p.TaskCount); err != nil {
			return nil, fmt.Errorf("scan project: %w", err)
		}
		projects = append(projects, p)
	}
	return projects, rows.Err()
}

func (s *Store) GetProject(ctx context.Context, workspaceID, projectID string) (*ProjectRecord, error) {
	projectID = strings.TrimSpace(projectID)
	workspaceID = strings.TrimSpace(workspaceID)
	if projectID == "" {
		return nil, errors.New("project_id is required")
	}
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	canonicalProjectID, err := s.resolveProjectIDWithQuerier(ctx, s.db, workspaceID, projectID)
	if err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT p.project_id, p.workspace_id, p.title, p.description, p.status, p.created_by, p.created_at, p.updated_at,
		        COALESCE((SELECT COUNT(*)
		          FROM workspace_tasks wt
		          JOIN tasks t ON t.task_id = wt.task_id
		         WHERE wt.workspace_id = p.workspace_id AND t.project_id = p.project_id), 0) as task_count
		 FROM projects p
		 WHERE p.project_id = ? AND p.workspace_id = ?`,
		canonicalProjectID,
		workspaceID,
	)
	var p ProjectRecord
	if err := row.Scan(&p.ProjectID, &p.WorkspaceID, &p.Title, &p.Description, &p.Status, &p.CreatedBy, &p.CreatedAt, &p.UpdatedAt, &p.TaskCount); err != nil {
		return nil, fmt.Errorf("get project: %w", err)
	}
	return &p, nil
}

func (s *Store) ResolveProjectID(ctx context.Context, workspaceID, projectID string) (string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	if workspaceID == "" || projectID == "" {
		return "", errors.New("workspace_id and project_id are required")
	}
	return s.resolveProjectIDWithQuerier(ctx, s.db, workspaceID, projectID)
}

func (s *Store) resolveProjectIDWithQuerier(ctx context.Context, q sqlReadQuerier, workspaceID, projectID string) (string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	if workspaceID == "" || projectID == "" {
		return "", errors.New("workspace_id and project_id are required")
	}
	var canonical string
	err := q.QueryRowContext(ctx,
		`SELECT project_id FROM projects WHERE workspace_id = ? AND project_id = ?`,
		workspaceID, projectID,
	).Scan(&canonical)
	if err == nil {
		return canonical, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("resolve project_id: %w", err)
	}
	rows, err := q.QueryContext(ctx,
		`SELECT project_id
		   FROM projects
		  WHERE workspace_id = ? AND lower(project_id) = lower(?)
		  ORDER BY project_id ASC
		  LIMIT 2`,
		workspaceID, projectID,
	)
	if err != nil {
		return "", fmt.Errorf("resolve project_id case alias: %w", err)
	}
	defer rows.Close()
	var matches []string
	for rows.Next() {
		var match string
		if err := rows.Scan(&match); err != nil {
			return "", fmt.Errorf("scan project_id case alias: %w", err)
		}
		matches = append(matches, strings.TrimSpace(match))
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("resolve project_id case alias: %w", err)
	}
	switch len(matches) {
	case 0:
		return "", fmt.Errorf("get project: %w", sql.ErrNoRows)
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf("project_id %q is ambiguous in workspace %s", projectID, workspaceID)
	}
}

func (s *Store) listWorkspaceProjects(ctx context.Context, workspaceID string) ([]ProjectRecord, error) {
	return s.ListProjects(ctx, workspaceID)
}

type ProjectUpdateInput struct {
	WorkspaceID string
	ProjectID   string
	Title       string
	Description *string // nil = no change, pointer allows setting to ""
	Status      string
	ActorID     string
	ActorType   string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectDeleteInput struct {
	WorkspaceID string
	ProjectID   string
	ActorID     string
	ActorType   string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

func (s *Store) UpdateProject(ctx context.Context, input ProjectUpdateInput) error {
	projectID := strings.TrimSpace(input.ProjectID)
	if projectID == "" {
		return errors.New("project_id is required")
	}

	sets := []string{}
	args := []any{}

	if t := strings.TrimSpace(input.Title); t != "" {
		sets = append(sets, "title = ?")
		args = append(args, t)
	}
	if input.Description != nil {
		sets = append(sets, "description = ?")
		args = append(args, *input.Description)
	}
	if st := strings.TrimSpace(input.Status); st != "" {
		sets = append(sets, "status = ?")
		args = append(args, st)
	}
	if len(sets) == 0 {
		return errors.New("nothing to update")
	}

	sets = append(sets, "updated_at = ?")
	args = append(args, time.Now().UTC().Format(time.RFC3339Nano))
	args = append(args, projectID)

	workspaceID := strings.TrimSpace(input.WorkspaceID)
	args = append(args, workspaceID)

	query := "UPDATE projects SET " + strings.Join(sets, ", ") + " WHERE project_id = ? AND workspace_id = ?"
	res, err := s.writeDB.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update project: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("project not found: %s", projectID)
	}
	return nil
}

func (s *Store) UpdateProjectWithEvent(ctx context.Context, input ProjectUpdateInput) (ProjectRecord, RuntimeEventRecord, error) {
	projectID := strings.TrimSpace(input.ProjectID)
	if projectID == "" {
		return ProjectRecord{}, RuntimeEventRecord{}, errors.New("project_id is required")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return ProjectRecord{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		return ProjectRecord{}, RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project update tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var project ProjectRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.updateProjectTx(ctx, tx, input, now); err != nil {
			return err
		}
		var err error
		project, err = s.getProjectTx(ctx, tx, workspaceID, projectID)
		if err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project update"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(map[string]any{
			"workspace_id":        workspaceID,
			"project_id":          projectID,
			"actor_id":            actorID,
			"created_by":          project.CreatedBy,
			"title":               project.Title,
			"description":         project.Description,
			"status":              project.Status,
			"description_changed": input.Description != nil,
			"status_changed":      strings.TrimSpace(input.Status) != "",
			"title_changed":       strings.TrimSpace(input.Title) != "",
			"entity_type":         "project",
			"entity_id":           projectID,
			"summary":             "Project updated: " + project.Title,
			"mutation_operation":  "update",
			"task_count_after":    project.TaskCount,
		}, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.update"), map[string]string{
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
			EventType:   "project.updated",
			EntityType:  "project",
			EntityID:    projectID,
			ActorType:   firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project update: %w", err)
	}
	return project, event, nil
}

func (s *Store) updateProjectTx(ctx context.Context, tx *sql.Tx, input ProjectUpdateInput, now string) error {
	projectID := strings.TrimSpace(input.ProjectID)
	if projectID == "" {
		return errors.New("project_id is required")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}

	sets := []string{}
	args := []any{}

	if t := strings.TrimSpace(input.Title); t != "" {
		sets = append(sets, "title = ?")
		args = append(args, t)
	}
	if input.Description != nil {
		sets = append(sets, "description = ?")
		args = append(args, *input.Description)
	}
	if st := strings.TrimSpace(input.Status); st != "" {
		sets = append(sets, "status = ?")
		args = append(args, st)
	}
	if len(sets) == 0 {
		return errors.New("nothing to update")
	}

	sets = append(sets, "updated_at = ?")
	args = append(args, now)
	args = append(args, projectID, workspaceID)

	query := "UPDATE projects SET " + strings.Join(sets, ", ") + " WHERE project_id = ? AND workspace_id = ?"
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update project: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("project not found: %s", projectID)
	}
	return nil
}

func (s *Store) DeleteProject(ctx context.Context, workspaceID, projectID string) error {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return errors.New("project_id is required")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin project delete tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := s.getProjectTx(ctx, tx, workspaceID, projectID); err != nil {
		return err
	}
	if _, err := unlinkProjectTasksTx(ctx, tx, workspaceID, projectID); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM projects WHERE project_id = ? AND workspace_id = ?`, projectID, workspaceID)
	if err != nil {
		return fmt.Errorf("delete project: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("project not found: %s", projectID)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit project delete: %w", err)
	}
	return nil
}

func (s *Store) DeleteProjectWithEvent(ctx context.Context, input ProjectDeleteInput) (RuntimeEventRecord, error) {
	projectID := strings.TrimSpace(input.ProjectID)
	if projectID == "" {
		return RuntimeEventRecord{}, errors.New("project_id is required")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	actorID := strings.TrimSpace(input.ActorID)
	if actorID == "" {
		return RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	if input.PromptContextEnvelope == nil {
		return RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin project delete tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		project, err := s.getProjectTx(ctx, tx, workspaceID, projectID)
		if err != nil {
			return err
		}
		unlinkRes, err := unlinkProjectTasksTx(ctx, tx, workspaceID, projectID)
		if err != nil {
			return err
		}
		unlinkedTasks, _ := unlinkRes.RowsAffected()
		res, err := tx.ExecContext(ctx, `DELETE FROM projects WHERE project_id = ? AND workspace_id = ?`, projectID, workspaceID)
		if err != nil {
			return fmt.Errorf("delete project: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return fmt.Errorf("project not found: %s", projectID)
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project delete"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(map[string]any{
			"workspace_id":        workspaceID,
			"project_id":          projectID,
			"actor_id":            actorID,
			"created_by":          project.CreatedBy,
			"title":               project.Title,
			"description":         project.Description,
			"status":              project.Status,
			"entity_type":         "project",
			"entity_id":           projectID,
			"summary":             "Project deleted: " + project.Title,
			"mutation_operation":  "delete",
			"task_count_before":   project.TaskCount,
			"unlinked_task_count": unlinkedTasks,
		}, input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.delete"), map[string]string{
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
			EventType:   "project.deleted",
			EntityType:  "project",
			EntityID:    projectID,
			ActorType:   firstNonEmpty(strings.TrimSpace(input.ActorType), "human"),
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit project delete: %w", err)
	}
	return event, nil
}

func (s *Store) getProjectTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string) (ProjectRecord, error) {
	projectID = strings.TrimSpace(projectID)
	workspaceID = strings.TrimSpace(workspaceID)
	if projectID == "" {
		return ProjectRecord{}, errors.New("project_id is required")
	}
	if workspaceID == "" {
		return ProjectRecord{}, errors.New("workspace_id is required")
	}
	canonicalProjectID, err := s.resolveProjectIDWithQuerier(ctx, tx, workspaceID, projectID)
	if err != nil {
		return ProjectRecord{}, err
	}
	row := tx.QueryRowContext(ctx,
		`SELECT p.project_id, p.workspace_id, p.title, p.description, p.status, p.created_by, p.created_at, p.updated_at,
		        COALESCE((SELECT COUNT(*)
		          FROM workspace_tasks wt
		          JOIN tasks t ON t.task_id = wt.task_id
		         WHERE wt.workspace_id = p.workspace_id AND t.project_id = p.project_id), 0) as task_count
		 FROM projects p
		 WHERE p.project_id = ? AND p.workspace_id = ?`,
		canonicalProjectID,
		workspaceID,
	)
	var p ProjectRecord
	if err := row.Scan(&p.ProjectID, &p.WorkspaceID, &p.Title, &p.Description, &p.Status, &p.CreatedBy, &p.CreatedAt, &p.UpdatedAt, &p.TaskCount); err != nil {
		return ProjectRecord{}, fmt.Errorf("get project: %w", err)
	}
	return p, nil
}

func unlinkProjectTasksTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string) (sql.Result, error) {
	res, err := tx.ExecContext(ctx,
		`UPDATE tasks
		    SET project_id = ''
		  WHERE project_id = ?
		    AND EXISTS (
		      SELECT 1
		        FROM workspace_tasks wt
		       WHERE wt.task_id = tasks.task_id
		         AND wt.workspace_id = ?
		    )`,
		projectID,
		workspaceID,
	)
	if err != nil {
		return nil, fmt.Errorf("unlink tasks before project delete: %w", err)
	}
	return res, nil
}

func attachProjectPromptContextEnvelope(payload map[string]any, envelope map[string]any, expectedSurface string, fields map[string]string) (map[string]any, error) {
	if envelope == nil {
		return payload, nil
	}
	enriched, err := enrichPromptContextEnvelope(envelope, fields)
	if err != nil {
		return nil, err
	}
	out, err := AttachProjectPromptContextEnvelope(payload, enriched)
	if err != nil {
		return nil, err
	}
	required := map[string]string{
		"surface":      strings.TrimSpace(expectedSurface),
		"origin":       expectedPromptContextOriginForSurface(expectedSurface),
		"workspace_id": strings.TrimSpace(fields["workspace_id"]),
		"project_id":   strings.TrimSpace(fields["project_id"]),
		"actor_id":     strings.TrimSpace(fields["actor_id"]),
	}
	for key, value := range fields {
		required[key] = strings.TrimSpace(value)
	}
	if err := validatePromptContextEnvelopeRequiredBindings("project.payload_json", out, required); err != nil {
		return nil, err
	}
	return out, nil
}
