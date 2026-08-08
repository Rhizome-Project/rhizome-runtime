package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

var hydrationTaskIDPattern = regexp.MustCompile(`(?i)(^|[^a-z0-9_])(task-[a-z0-9][a-z0-9_-]*)`)

type TaskHydrationFilter struct {
	TaskID           string
	WorkspaceID      string
	DocKeys          []string
	IncludeAllDocs   bool
	UpdatesLimit     int
	ArtifactLimit    int
	RelatedTaskLimit int
}

type TaskHydrationBundle struct {
	GeneratedAt   string                          `json:"generated_at"`
	Workspace     *WorkspaceRecord                `json:"workspace,omitempty"`
	WorkspaceTask *WorkspaceTaskRecord            `json:"workspace_task,omitempty"`
	Task          TaskStatus                      `json:"task"`
	Docs          []WorkspaceDocRecord            `json:"docs"`
	TaskLinks     []WorkspaceTaskLinkRecord       `json:"task_links"`
	RelatedTasks  []TaskStatus                    `json:"related_tasks"`
	Artifacts     []WorkspaceArtifactRecord       `json:"artifacts"`
	Updates       []AgentUpdateRecord             `json:"updates"`
	SideEffects   []model.AgentUpdateSideEffectV1 `json:"side_effects,omitempty"`
}

func (s *Store) GetTaskHydrationBundle(ctx context.Context, filter TaskHydrationFilter) (TaskHydrationBundle, error) {
	taskID := strings.TrimSpace(filter.TaskID)
	if taskID == "" {
		return TaskHydrationBundle{}, errors.New("task_id is required")
	}

	task, err := s.GetTaskStatus(ctx, "", taskID)
	if err != nil {
		return TaskHydrationBundle{}, err
	}

	workspaceID, err := s.resolveTaskWorkspace(ctx, strings.TrimSpace(filter.WorkspaceID), taskID)
	if err != nil && !errors.Is(err, ErrWorkspaceTaskAbsent) {
		return TaskHydrationBundle{}, err
	}

	bundle := TaskHydrationBundle{
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		Task:         task,
		Docs:         []WorkspaceDocRecord{},
		TaskLinks:    []WorkspaceTaskLinkRecord{},
		RelatedTasks: []TaskStatus{},
		Artifacts:    []WorkspaceArtifactRecord{},
		Updates:      []AgentUpdateRecord{},
		SideEffects:  []model.AgentUpdateSideEffectV1{},
	}
	if workspaceID == "" {
		return bundle, nil
	}

	workspace, err := s.GetWorkspace(ctx, workspaceID)
	if err != nil {
		return TaskHydrationBundle{}, err
	}
	bundle.Workspace = &workspace

	workspaceTask, err := s.getWorkspaceTaskRecord(ctx, workspaceID, taskID)
	if err != nil {
		return TaskHydrationBundle{}, err
	}
	bundle.WorkspaceTask = &workspaceTask

	docs, err := s.listHydrationDocs(ctx, workspaceID, taskID, &workspaceTask, filter.DocKeys, filter.IncludeAllDocs)
	if err != nil {
		return TaskHydrationBundle{}, err
	}
	bundle.Docs = docs

	links, err := s.ListWorkspaceTaskLinks(ctx, WorkspaceTaskLinkFilter{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Limit:       100,
	})
	if err != nil {
		return TaskHydrationBundle{}, err
	}
	bundle.TaskLinks = links

	related, err := s.listHydrationRelatedTasks(ctx, links, taskID, filter.RelatedTaskLimit)
	if err != nil {
		return TaskHydrationBundle{}, err
	}
	bundle.RelatedTasks = related
	if len(related) > 0 {
		relatedDocs, err := s.listHydrationRelatedTaskDocs(ctx, workspaceID, workspaceTask.ProjectID, related)
		if err != nil {
			return TaskHydrationBundle{}, err
		}
		bundle.Docs = mergeHydrationDocs(bundle.Docs, relatedDocs)
	}

	artifacts, err := s.ListWorkspaceArtifacts(ctx, WorkspaceArtifactFilter{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Limit:       filter.ArtifactLimit,
	})
	if err != nil {
		return TaskHydrationBundle{}, err
	}
	bundle.Artifacts = artifacts

	updates, err := s.listTaskRelatedUpdates(ctx, workspaceID, taskID, workspaceTask.ProjectID, filter.UpdatesLimit)
	if err != nil {
		return TaskHydrationBundle{}, err
	}
	bundle.Updates = updates
	sideEffects, err := collectHydrationSideEffects(updates)
	if err != nil {
		return TaskHydrationBundle{}, err
	}
	bundle.SideEffects = sideEffects

	return bundle, nil
}

func collectHydrationSideEffects(updates []AgentUpdateRecord) ([]model.AgentUpdateSideEffectV1, error) {
	out := []model.AgentUpdateSideEffectV1{}
	seen := map[string]struct{}{}
	for _, update := range updates {
		effects, err := model.ParseAgentUpdateSideEffectsV1(update.PayloadJSON)
		if err != nil {
			return nil, fmt.Errorf("parse side effects for agent update %s: %w", strings.TrimSpace(update.UpdateID), err)
		}
		for _, effect := range effects {
			ref := strings.TrimSpace(effect.SideEffectRef)
			if ref == "" {
				continue
			}
			if _, ok := seen[ref]; ok {
				continue
			}
			seen[ref] = struct{}{}
			if sideEffectHydrationStillPending(effect) {
				out = append(out, effect)
			}
		}
	}
	return out, nil
}

func sideEffectHydrationStillPending(effect model.AgentUpdateSideEffectV1) bool {
	status := strings.TrimSpace(effect.IntegrationStatus)
	decision := strings.TrimSpace(effect.Decision)
	return status == "pending_classification" || decision == "" || decision == "none_pending"
}

func (s *Store) resolveTaskWorkspace(ctx context.Context, requestedWorkspaceID, taskID string) (string, error) {
	if requestedWorkspaceID != "" {
		var count int
		if err := s.db.QueryRowContext(
			ctx,
			`SELECT COUNT(1) FROM workspace_tasks WHERE workspace_id = ? AND task_id = ?`,
			requestedWorkspaceID,
			taskID,
		).Scan(&count); err != nil {
			return "", fmt.Errorf("check requested task workspace: %w", err)
		}
		if count == 0 {
			return "", ErrWorkspaceTaskAbsent
		}
		return requestedWorkspaceID, nil
	}

	var workspaceID string
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT wt.workspace_id
		 FROM workspace_tasks wt
		 JOIN workspaces w ON w.workspace_id = wt.workspace_id
		 WHERE wt.task_id = ?
		 ORDER BY
		   CASE w.status
		     WHEN 'ACTIVE' THEN 0
		     WHEN 'PAUSED' THEN 1
		     WHEN 'ARCHIVED' THEN 2
		     ELSE 3
		   END,
		   w.updated_at DESC,
		   wt.created_at DESC,
		   wt.workspace_id
		 LIMIT 1`,
		taskID,
	).Scan(&workspaceID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("resolve task workspace: %w", err)
	}
	return workspaceID, nil
}

func (s *Store) getWorkspaceTaskRecord(ctx context.Context, workspaceID, taskID string) (WorkspaceTaskRecord, error) {
	taskID = strings.TrimSpace(taskID)
	if resolved, rerr := s.resolveTaskIDWithQuerier(ctx, s.db, workspaceID, taskID); rerr == nil {
		taskID = resolved
	}
	var row WorkspaceTaskRecord
	var claimAgentID, claimStatus, claimSummary, claimUpdatedAt sql.NullString
	var claimProjectRoleID, claimRepoID, claimCheckoutID, claimBranchID, claimWriteScopeJSON sql.NullString
	var requiresProjectGate int
	var tagsJSON, taskRequirementsJSON, writeScopeHintsJSON string
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT wt.task_id, t.title, t.description, t.owner_user_id, t.priority, t.status, t.task_kind, t.task_template,
		        COALESCE(t.task_class, ''), COALESCE(t.task_class_source, ''), COALESCE(t.task_class_updated_at, ''),
		        COALESCE(t.project_id, ''), COALESCE(t.project_lane, ''), COALESCE(t.requires_project_gate, 0),
		        COALESCE(t.task_requirements_json, '{}'), COALESCE(t.write_scope_hints_json, '[]'),
		        t.close_reason, wt.linked_by, wt.created_at, COALESCE(t.tags_json, '[]'), t.updated_at,
		        tc.agent_id, tc.claim_status, tc.summary, tc.updated_at,
		        tc.project_role_id, tc.repo_id, tc.checkout_id, tc.branch_id, tc.write_scope_json
		 FROM workspace_tasks wt
		 JOIN tasks t ON t.task_id = wt.task_id
		 LEFT JOIN task_claims tc ON tc.task_id = wt.task_id AND tc.workspace_id = wt.workspace_id
		 WHERE wt.workspace_id = ? AND wt.task_id = ?`,
		workspaceID,
		taskID,
	).Scan(
		&row.TaskID,
		&row.Title,
		&row.Description,
		&row.OwnerUserID,
		&row.Priority,
		&row.Status,
		&row.TaskKind,
		&row.TaskTemplate,
		&row.TaskClass,
		&row.TaskClassSource,
		&row.TaskClassUpdatedAt,
		&row.ProjectID,
		&row.ProjectLane,
		&requiresProjectGate,
		&taskRequirementsJSON,
		&writeScopeHintsJSON,
		&row.CloseReason,
		&row.LinkedBy,
		&row.LinkedAt,
		&tagsJSON,
		&row.UpdatedAt,
		&claimAgentID,
		&claimStatus,
		&claimSummary,
		&claimUpdatedAt,
		&claimProjectRoleID,
		&claimRepoID,
		&claimCheckoutID,
		&claimBranchID,
		&claimWriteScopeJSON,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WorkspaceTaskRecord{}, ErrWorkspaceTaskAbsent
		}
		return WorkspaceTaskRecord{}, fmt.Errorf("query workspace task record: %w", err)
	}
	row.ClaimAgentID = nullStringPtr(claimAgentID)
	row.ClaimStatus = nullStringPtr(claimStatus)
	row.ClaimSummary = nullStringPtr(claimSummary)
	row.ClaimUpdatedAt = nullStringPtr(claimUpdatedAt)
	row.ClaimProjectRoleID = nullStringPtr(claimProjectRoleID)
	row.ClaimRepoID = nullStringPtr(claimRepoID)
	row.ClaimCheckoutID = nullStringPtr(claimCheckoutID)
	row.ClaimBranchID = nullStringPtr(claimBranchID)
	row.ClaimWriteScopeJSON = nullStringPtr(claimWriteScopeJSON)
	row.RequiresProjectGate = sqliteIntToBool(requiresProjectGate)
	row.TaskRequirementsJSON = normalizeTaskRequirementsJSON(taskRequirementsJSON)
	row.WriteScopeHints = parseTaskWriteScopeHintsJSON(writeScopeHintsJSON)
	row.Tags = parseTaskTagsJSON(tagsJSON)
	return row, nil
}

func (s *Store) listHydrationDocs(ctx context.Context, workspaceID, taskID string, workspaceTask *WorkspaceTaskRecord, docKeys []string, includeAll bool) ([]WorkspaceDocRecord, error) {
	docKeys = uniqueTrimmedSourceDocKeys(append(docKeys, hydrationSourceDocKeys(workspaceTask)...))
	if includeAll || len(docKeys) == 0 {
		docs, err := s.listWorkspaceDocs(ctx, workspaceID)
		if err != nil {
			return nil, err
		}
		return filterHydrationDocs(taskID, workspaceTask, docs), nil
	}

	return s.listHydrationDocsByKeys(ctx, workspaceID, docKeys)
}

func (s *Store) listHydrationDocsByKeys(ctx context.Context, workspaceID string, docKeys []string) ([]WorkspaceDocRecord, error) {
	keys := normalizeCapabilities(docKeys)
	out := make([]WorkspaceDocRecord, 0, len(keys))
	for _, docKey := range keys {
		record, err := s.GetWorkspaceDoc(ctx, workspaceID, docKey)
		if err != nil {
			if strings.Contains(err.Error(), "workspace doc not found") {
				continue
			}
			return nil, err
		}
		out = append(out, record)
	}
	return out, nil
}

func (s *Store) listHydrationRelatedTaskDocs(ctx context.Context, workspaceID, projectID string, related []TaskStatus) ([]WorkspaceDocRecord, error) {
	keys := make([]string, 0, len(related)*len(hydrationTaskEvidenceDocKeys("x")))
	for _, task := range related {
		if projectID != "" && !sameHydrationProjectID(task.ProjectID, projectID) {
			continue
		}
		if !hydrationRelatedTaskCarriesEvidence(task) {
			continue
		}
		keys = append(keys, hydrationTaskEvidenceDocKeys(task.TaskID)...)
	}
	return s.listHydrationDocsByKeys(ctx, workspaceID, keys)
}

func hydrationTaskDocKeys(taskID string) []string {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil
	}
	base := "task." + taskID
	return append([]string{base}, hydrationTaskEvidenceDocKeys(taskID)...)
}

func hydrationTaskEvidenceDocKeys(taskID string) []string {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil
	}
	base := "task." + taskID
	return []string{
		base + ".result",
		base + ".evidence_gap",
		base + ".artifact_reality_check",
		base + ".candidate_provenance",
		base + ".provenance_review_status",
		base + ".review_feedback",
		base + ".review_ready_status",
		base + ".implementation_evidence",
		base + ".verification",
		base + ".validation_evidence",
		base + ".visual_acceptance",
		base + ".visual_acceptance_attempt",
		base + ".visual_acceptance_partial",
	}
}

func hydrationProjectPlanningDocKeys(projectID string) []string {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return nil
	}
	base := "project." + sanitizeProjectDocKeySegment(projectID)
	return []string{
		base + ".source_refs",
		base + ".source_requirements_trace",
		base + ".product_contract",
		base + ".acceptance_criteria",
		base + ".plan_review",
		base + ".reflection_board",
		base + ".quality_reflection",
	}
}

func hydrationRelatedTaskCarriesEvidence(task TaskStatus) bool {
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	switch lane {
	case "review", "validation", "verification", "test", "testing", "qa", "quality", "quality-review", "ux", "ux-review", "usability", "accessibility", "a11y", "smoke", "browser-smoke":
		return true
	}
	text := strings.ToLower(strings.Join([]string{task.TaskID, task.Title, task.Description, task.TaskKind, task.TaskTemplate}, " "))
	for _, marker := range []string{"patchq-validation", "patchq-review", "patch queue review", "validate", "validation", "review", "quality", "ux", "usability", "accessibility", "a11y", "smoke"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func mergeHydrationDocs(base []WorkspaceDocRecord, extra []WorkspaceDocRecord) []WorkspaceDocRecord {
	if len(extra) == 0 {
		return base
	}
	out := append([]WorkspaceDocRecord(nil), base...)
	seen := make(map[string]struct{}, len(out)+len(extra))
	for _, doc := range out {
		if key := strings.TrimSpace(doc.DocKey); key != "" {
			seen[key] = struct{}{}
		}
	}
	for _, doc := range extra {
		key := strings.TrimSpace(doc.DocKey)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, doc)
	}
	return out
}

func filterHydrationDocs(taskID string, workspaceTask *WorkspaceTaskRecord, docs []WorkspaceDocRecord) []WorkspaceDocRecord {
	taskID = strings.TrimSpace(taskID)
	claimAgentID := hydrationClaimAgentID(workspaceTask)
	sourceDocKeys := map[string]struct{}{}
	for _, key := range hydrationSourceDocKeys(workspaceTask) {
		sourceDocKeys[strings.TrimSpace(key)] = struct{}{}
	}
	allowedGeneric := map[string]struct{}{
		"current_context": {},
		"decisions":       {},
		"open_questions":  {},
		"handoff":         {},
		"tooling":         {},
		"autonomy_policy": {},
		"charter":         {},
	}

	out := make([]WorkspaceDocRecord, 0, len(docs))
	for _, doc := range docs {
		docKey := strings.TrimSpace(doc.DocKey)
		if docKey == "" {
			continue
		}
		if _, ok := allowedGeneric[docKey]; ok {
			out = append(out, doc)
			continue
		}
		if _, ok := sourceDocKeys[docKey]; ok {
			out = append(out, doc)
			continue
		}
		if isHydrationTaskDoc(docKey, taskID) {
			out = append(out, doc)
			continue
		}
		if workspaceTask != nil && isHydrationProjectDoc(docKey, workspaceTask.ProjectID) {
			out = append(out, doc)
			continue
		}
		if agentID, suffix, ok := parseHydrationAgentDocKey(docKey); ok {
			if claimAgentID == "" || claimAgentID != agentID {
				continue
			}
			docTaskID := hydrationDocTaskID(doc.Content)
			if docTaskID != "" && docTaskID != taskID {
				continue
			}
			if suffix == "claimed_work" && strings.Contains(strings.ToLower(doc.Content), "active_claimed_work: none") {
				continue
			}
			out = append(out, doc)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].DocKey < out[j].DocKey })
	return out
}

func hydrationSourceDocKeys(workspaceTask *WorkspaceTaskRecord) []string {
	if workspaceTask == nil {
		return nil
	}
	return sourceDocKeysFromTaskRequirementsJSON(workspaceTask.TaskRequirementsJSON)
}

func isHydrationProjectDoc(docKey, projectID string) bool {
	docProjectID, suffix, ok := parseHydrationProjectDocKey(docKey)
	if !ok || strings.TrimSpace(projectID) == "" || !sameHydrationProjectID(docProjectID, projectID) {
		return false
	}
	switch suffix {
	case "source_refs", "source_requirements_trace", "product_contract", "acceptance_criteria", "plan_review", "planning_review", "strategy_review", "reflection_board", "quality_reflection":
		return true
	default:
		return false
	}
}

func parseHydrationProjectDocKey(docKey string) (projectID, suffix string, ok bool) {
	parts := strings.Split(strings.TrimSpace(docKey), ".")
	if len(parts) < 3 || !strings.EqualFold(parts[0], "project") {
		return "", "", false
	}
	projectID = strings.TrimSpace(parts[1])
	suffix = strings.TrimSpace(strings.Join(parts[2:], "."))
	if projectID == "" || suffix == "" {
		return "", "", false
	}
	return projectID, suffix, true
}

func hydrationClaimAgentID(workspaceTask *WorkspaceTaskRecord) string {
	if workspaceTask == nil || workspaceTask.ClaimAgentID == nil || workspaceTask.ClaimStatus == nil {
		return ""
	}
	switch strings.TrimSpace(*workspaceTask.ClaimStatus) {
	case "CLAIMED", "BLOCKED":
		return strings.TrimSpace(*workspaceTask.ClaimAgentID)
	default:
		return ""
	}
}

func isHydrationTaskDoc(docKey, taskID string) bool {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return false
	}
	prefix := "task." + taskID
	return docKey == prefix || strings.HasPrefix(docKey, prefix+".")
}

func parseHydrationAgentDocKey(docKey string) (agentID, suffix string, ok bool) {
	if !strings.HasPrefix(docKey, "agent.") {
		return "", "", false
	}
	rest := strings.TrimPrefix(docKey, "agent.")
	idx := strings.Index(rest, ".")
	if idx <= 0 || idx >= len(rest)-1 {
		return "", "", false
	}
	agentID = strings.TrimSpace(rest[:idx])
	suffix = strings.TrimSpace(rest[idx+1:])
	switch suffix {
	case "current_context", "claimed_work":
		return agentID, suffix, agentID != ""
	default:
		return "", "", false
	}
}

func hydrationDocTaskID(content string) string {
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		trimmed = strings.TrimPrefix(trimmed, "- ")
		switch {
		case strings.HasPrefix(trimmed, "task_id:"):
			return strings.TrimSpace(strings.TrimPrefix(trimmed, "task_id:"))
		case strings.HasPrefix(trimmed, "task_id="):
			return strings.TrimSpace(strings.TrimPrefix(trimmed, "task_id="))
		}
	}
	return ""
}

func (s *Store) listHydrationRelatedTasks(ctx context.Context, links []WorkspaceTaskLinkRecord, taskID string, limit int) ([]TaskStatus, error) {
	if limit <= 0 {
		limit = 10
	}
	relatedIDs := make([]string, 0, len(links))
	seen := map[string]struct{}{}
	for _, link := range links {
		other := link.FromTaskID
		if other == taskID {
			other = link.ToTaskID
		}
		if other == "" || other == taskID {
			continue
		}
		if _, ok := seen[other]; ok {
			continue
		}
		seen[other] = struct{}{}
		relatedIDs = append(relatedIDs, other)
		if len(relatedIDs) >= limit {
			break
		}
	}

	out := make([]TaskStatus, 0, len(relatedIDs))
	for _, relatedID := range relatedIDs {
		status, err := s.GetTaskStatus(ctx, "", relatedID)
		if err != nil {
			return nil, fmt.Errorf("get related task status %s: %w", relatedID, err)
		}
		out = append(out, status)
	}
	return out, nil
}

func (s *Store) listTaskRelatedUpdates(ctx context.Context, workspaceID, taskID, projectID string, limit int) ([]AgentUpdateRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	needle := "%" + taskID + "%"
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT u.update_id, u.agent_id, a.display_name, u.update_type, u.summary, u.payload_json, u.requires_human, u.created_at
		 FROM agent_updates u
		 JOIN agents a ON a.agent_id = u.agent_id AND a.workspace_id = u.workspace_id
		 WHERE u.workspace_id = ?
		   AND (u.summary LIKE ? OR u.payload_json LIKE ?)
		 ORDER BY u.created_at DESC, u.update_id DESC
		 LIMIT ?`,
		workspaceID,
		needle,
		needle,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query task-related updates: %w", err)
	}
	defer rows.Close()

	out := []AgentUpdateRecord{}
	for rows.Next() {
		var row AgentUpdateRecord
		var requiresHuman int
		if err := rows.Scan(
			&row.UpdateID,
			&row.AgentID,
			&row.AgentName,
			&row.UpdateType,
			&row.Summary,
			&row.PayloadJSON,
			&requiresHuman,
			&row.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan task-related update: %w", err)
		}
		row.RequiresHuman = requiresHuman != 0
		foreign, err := s.agentUpdateReferencesForeignProjectTask(ctx, taskID, projectID, row)
		if err != nil {
			return nil, err
		}
		if foreign {
			continue
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate task-related updates: %w", err)
	}
	return out, nil
}

func (s *Store) agentUpdateReferencesForeignProjectTask(ctx context.Context, currentTaskID, currentProjectID string, update AgentUpdateRecord) (bool, error) {
	currentProjectID = strings.TrimSpace(currentProjectID)
	if currentProjectID == "" {
		return false, nil
	}
	taskIDs := extractHydrationTaskIDs(update.Summary + "\n" + update.PayloadJSON)
	for _, taskID := range taskIDs {
		taskID = strings.TrimSpace(taskID)
		if taskID == "" || taskID == currentTaskID {
			continue
		}
		var projectID string
		err := s.db.QueryRowContext(ctx, `SELECT COALESCE(project_id, '') FROM tasks WHERE task_id = ?`, taskID).Scan(&projectID)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("load hydration update task project %s: %w", taskID, err)
		}
		projectID = strings.TrimSpace(projectID)
		if projectID != "" && !sameHydrationProjectID(projectID, currentProjectID) {
			return true, nil
		}
	}
	return false, nil
}

func extractHydrationTaskIDs(text string) []string {
	matches := hydrationTaskIDPattern.FindAllStringSubmatch(text, -1)
	out := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		taskID := strings.TrimSpace(match[2])
		if taskID == "" {
			continue
		}
		key := strings.ToLower(taskID)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, taskID)
	}
	return out
}

func sameHydrationProjectID(left, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return false
	}
	return strings.EqualFold(left, right) || sanitizeProjectDocKeySegment(left) == sanitizeProjectDocKeySegment(right)
}

func sanitizeProjectDocKeySegment(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_'
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}
