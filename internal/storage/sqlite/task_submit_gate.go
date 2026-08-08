package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var ErrTaskSubmitPatchQueueGate = errors.New("task submit patch queue gate")

type TaskSubmitPatchQueueGateError struct {
	Gate               string
	ExistingTaskID     string
	ExistingTaskStatus string
	EvidenceDocKey     string
	RequiredTransition string
	QueueID            string
	ItemID             string
	BranchID           string
	HeadSHA            string
}

func (e *TaskSubmitPatchQueueGateError) Error() string {
	if e == nil {
		return ErrTaskSubmitPatchQueueGate.Error()
	}
	parts := []string{ErrTaskSubmitPatchQueueGate.Error()}
	if e.Gate != "" {
		parts = append(parts, "task_submit_gate="+e.Gate)
	}
	if e.ExistingTaskID != "" {
		parts = append(parts, "existing_task_id="+e.ExistingTaskID)
	}
	if e.ExistingTaskStatus != "" {
		parts = append(parts, "existing_status="+e.ExistingTaskStatus)
	}
	if e.EvidenceDocKey != "" {
		parts = append(parts, "existing_evidence_doc_key="+e.EvidenceDocKey)
	}
	if e.RequiredTransition != "" {
		parts = append(parts, "required_transition="+e.RequiredTransition)
	}
	if e.QueueID != "" {
		parts = append(parts, "queue_id="+e.QueueID)
	}
	if e.ItemID != "" {
		parts = append(parts, "item_id="+e.ItemID)
	}
	if e.BranchID != "" {
		parts = append(parts, "branch_id="+e.BranchID)
	}
	if e.HeadSHA != "" {
		parts = append(parts, "head_sha="+e.HeadSHA)
	}
	return strings.Join(parts, ": ")
}

func (e *TaskSubmitPatchQueueGateError) Unwrap() error {
	return ErrTaskSubmitPatchQueueGate
}

type taskSubmitStoragePatchQueueIdentity struct {
	ProjectID string
	QueueID   string
	ItemID    string
	BranchID  string
	HeadSHA   string
	Kind      string
}

type taskPatchQueueIdentityCandidate struct {
	TaskID string
	Status string
	taskSubmitStoragePatchQueueIdentity
}

var (
	taskSubmitStoragePatchQueueIDPattern     = regexp.MustCompile(`(?i)\bpatchq-[A-Za-z0-9_.-]+`)
	taskSubmitStoragePatchQueueItemPattern   = regexp.MustCompile(`(?i)\bpatchitem-[A-Za-z0-9_.-]+`)
	taskSubmitStoragePatchQueueBranchPattern = regexp.MustCompile(`(?i)\bprojbranch-[A-Za-z0-9_.-]+`)
	taskSubmitStoragePatchQueueHeadPattern   = regexp.MustCompile(`(?i)\b(?:expected[_ -]?head[_ -]?sha|head[_ -]?sha|expected[_ -]?head|head)\b\s*[:=]?\s*` + "`?" + `([0-9a-f]{7,40})` + "`?")
)

func (s *Store) enforceTaskSubmitPatchQueueGateTx(ctx context.Context, tx *sql.Tx, input TaskCreateInput) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	task := taskSubmitStorageWorkspaceTask(input)
	identity := taskSubmitStoragePatchQueueIdentityFromTask(task)
	projectID := firstNonEmpty(taskSubmitStorageTrimRef(input.ProjectID), identity.ProjectID)
	if workspaceID == "" || projectID == "" {
		return nil
	}
	identity.ProjectID = firstNonEmpty(identity.ProjectID, projectID)
	identity = taskSubmitStorageNormalizePatchQueueIdentity(identity)
	if !taskSubmitStoragePatchQueueIdentityActionable(identity) {
		return nil
	}
	requestedKind := firstNonEmpty(identity.Kind, taskSubmitStoragePatchQueueTaskKindFromTask(task))
	if requestedKind == "" {
		return nil
	}
	structuredEvidenceRefresh := agentWorkPatchQueueEvidenceRefreshTask(task)

	candidates, err := s.listTaskPatchQueueIdentityCandidatesTx(ctx, tx, workspaceID, projectID, requestedKind)
	if err != nil {
		return fmt.Errorf("query indexed patch queue task identities for submit gate: %w", err)
	}
	legacyCandidates, err := s.listLegacyTaskPatchQueueIdentityCandidatesTx(ctx, tx, workspaceID, projectID, requestedKind)
	if err != nil {
		return fmt.Errorf("query legacy patch queue task identities for submit gate: %w", err)
	}
	candidates = append(candidates, legacyCandidates...)
	canonicalRetry := taskSubmitStoragePatchQueueRequestIsCanonicalRetry(task)
	canonicalRetryMatchedTerminal := false
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate.TaskID) == strings.TrimSpace(input.TaskID) {
			continue
		}
		existingIdentity := candidate.taskSubmitStoragePatchQueueIdentity
		if !taskSubmitStoragePatchQueueIdentityActionable(existingIdentity) {
			continue
		}
		existingKind := existingIdentity.Kind
		if !taskSubmitStoragePatchQueueKindsOverlap(requestedKind, existingKind) {
			continue
		}
		if !taskSubmitStoragePatchQueueIdentityMatches(identity, existingIdentity) {
			continue
		}
		if canonicalRetry && isTerminalTaskStatus(candidate.Status) {
			canonicalRetryMatchedTerminal = true
			continue
		}
		if structuredEvidenceRefresh && isTerminalTaskStatus(candidate.Status) {
			continue
		}
		return &TaskSubmitPatchQueueGateError{
			Gate:               "patch_queue_identity_duplicate",
			ExistingTaskID:     strings.TrimSpace(candidate.TaskID),
			ExistingTaskStatus: strings.ToUpper(strings.TrimSpace(candidate.Status)),
			RequiredTransition: "project_patch_queue_followup",
			QueueID:            identity.QueueID,
			ItemID:             identity.ItemID,
			BranchID:           identity.BranchID,
			HeadSHA:            identity.HeadSHA,
		}
	}

	if canonicalRetryMatchedTerminal || !taskSubmitStoragePatchQueueKindNeedsVisualEvidenceReceipt(requestedKind) || !taskSubmitStorageVisualEvidenceDuplicateCandidate(task) {
		return nil
	}
	docKey, ok, err := s.taskSubmitStorageVisualEvidenceDocForIdentity(ctx, workspaceID, projectID, task, identity)
	if err != nil || !ok {
		return err
	}
	return &TaskSubmitPatchQueueGateError{
		Gate:               "patch_queue_visual_evidence_exists",
		EvidenceDocKey:     docKey,
		RequiredTransition: "project_patch_queue_followup",
		QueueID:            identity.QueueID,
		ItemID:             identity.ItemID,
		BranchID:           identity.BranchID,
		HeadSHA:            identity.HeadSHA,
	}
}

func (s *Store) upsertTaskPatchQueueIdentityTx(ctx context.Context, tx *sql.Tx, input TaskCreateInput, now string) error {
	if s == nil || tx == nil {
		return nil
	}
	task := taskSubmitStorageWorkspaceTask(input)
	identity := taskSubmitStoragePatchQueueIdentityFromTask(task)
	identity.ProjectID = firstNonEmpty(identity.ProjectID, taskSubmitStorageTrimRef(input.ProjectID))
	identity.Kind = firstNonEmpty(identity.Kind, taskSubmitStoragePatchQueueTaskKindFromTask(task))
	identity = taskSubmitStorageNormalizePatchQueueIdentity(identity)
	if strings.TrimSpace(input.WorkspaceID) == "" || strings.TrimSpace(input.TaskID) == "" || identity.ProjectID == "" || identity.Kind == "" || !taskSubmitStoragePatchQueueIdentityActionable(identity) {
		return nil
	}
	now = strings.TrimSpace(now)
	if now == "" {
		now = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO task_patch_queue_identities(
	task_id, workspace_id, project_id, queue_id, item_id, branch_id, head_sha, kind, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(task_id) DO UPDATE SET
	workspace_id = excluded.workspace_id,
	project_id = excluded.project_id,
	queue_id = excluded.queue_id,
	item_id = excluded.item_id,
	branch_id = excluded.branch_id,
	head_sha = excluded.head_sha,
	kind = excluded.kind,
	updated_at = excluded.updated_at`,
		strings.TrimSpace(input.TaskID),
		strings.TrimSpace(input.WorkspaceID),
		identity.ProjectID,
		identity.QueueID,
		identity.ItemID,
		identity.BranchID,
		identity.HeadSHA,
		identity.Kind,
		now,
		now,
	); err != nil {
		return fmt.Errorf("upsert task patch queue identity: %w", err)
	}
	return nil
}

func (s *Store) listTaskPatchQueueIdentityCandidatesTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, kind string) ([]taskPatchQueueIdentityCandidate, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	kind = strings.ToLower(strings.TrimSpace(kind))
	if workspaceID == "" || projectID == "" || kind == "" {
		return nil, nil
	}
	rows, err := tx.QueryContext(ctx, `
SELECT i.task_id,
       t.status,
       i.project_id,
       i.queue_id,
       i.item_id,
       i.branch_id,
       i.head_sha,
       i.kind
  FROM task_patch_queue_identities i
  JOIN tasks t ON t.task_id = i.task_id
  JOIN workspace_tasks wt ON wt.workspace_id = i.workspace_id AND wt.task_id = i.task_id
 WHERE i.workspace_id = ?
   AND i.project_id = ?
   AND i.kind = ?
 ORDER BY t.updated_at DESC, i.task_id ASC`,
		workspaceID,
		projectID,
		kind,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []taskPatchQueueIdentityCandidate
	for rows.Next() {
		var candidate taskPatchQueueIdentityCandidate
		if err := rows.Scan(
			&candidate.TaskID,
			&candidate.Status,
			&candidate.ProjectID,
			&candidate.QueueID,
			&candidate.ItemID,
			&candidate.BranchID,
			&candidate.HeadSHA,
			&candidate.Kind,
		); err != nil {
			return nil, fmt.Errorf("scan indexed patch queue task identity: %w", err)
		}
		candidate.taskSubmitStoragePatchQueueIdentity = taskSubmitStorageNormalizePatchQueueIdentity(candidate.taskSubmitStoragePatchQueueIdentity)
		out = append(out, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate indexed patch queue task identities: %w", err)
	}
	return out, nil
}

func (s *Store) listLegacyTaskPatchQueueIdentityCandidatesTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, kind string) ([]taskPatchQueueIdentityCandidate, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	kind = strings.ToLower(strings.TrimSpace(kind))
	if workspaceID == "" || projectID == "" || kind == "" {
		return nil, nil
	}
	rows, err := tx.QueryContext(ctx, `
SELECT t.task_id,
       t.status,
       COALESCE(t.title, ''),
       COALESCE(t.description, ''),
       COALESCE(t.owner_user_id, ''),
       COALESCE(t.priority, ''),
       COALESCE(t.task_kind, ''),
       COALESCE(t.task_template, ''),
       COALESCE(t.project_id, ''),
       COALESCE(t.project_lane, ''),
       COALESCE(t.requires_project_gate, 0),
       COALESCE(t.task_requirements_json, '{}'),
       COALESCE(t.tags_json, '[]'),
       COALESCE(t.write_scope_hints_json, '[]')
  FROM tasks t
  JOIN workspace_tasks wt ON wt.task_id = t.task_id AND wt.workspace_id = ?
 LEFT JOIN task_patch_queue_identities i ON i.workspace_id = wt.workspace_id AND i.task_id = t.task_id
 WHERE i.task_id IS NULL
   AND (COALESCE(t.project_id, '') = ? OR COALESCE(t.project_id, '') = '')
 ORDER BY t.updated_at DESC, t.task_id ASC`,
		workspaceID,
		projectID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []taskPatchQueueIdentityCandidate
	for rows.Next() {
		var task WorkspaceTaskRecord
		var requiresProjectGate int
		var tagsJSON, writeScopeHintsJSON string
		if err := rows.Scan(
			&task.TaskID,
			&task.Status,
			&task.Title,
			&task.Description,
			&task.OwnerUserID,
			&task.Priority,
			&task.TaskKind,
			&task.TaskTemplate,
			&task.ProjectID,
			&task.ProjectLane,
			&requiresProjectGate,
			&task.TaskRequirementsJSON,
			&tagsJSON,
			&writeScopeHintsJSON,
		); err != nil {
			return nil, fmt.Errorf("scan legacy patch queue task identity candidate: %w", err)
		}
		task.RequiresProjectGate = sqliteIntToBool(requiresProjectGate)
		task.Tags = parseTaskTagsJSON(tagsJSON)
		task.WriteScopeHints = parseTaskWriteScopeHintsJSON(writeScopeHintsJSON)
		task.TaskRequirementsJSON = normalizeTaskRequirementsJSON(task.TaskRequirementsJSON)
		identity := taskSubmitStoragePatchQueueIdentityFromTask(task)
		identity.ProjectID = firstNonEmpty(identity.ProjectID, projectID)
		identity.Kind = firstNonEmpty(identity.Kind, taskSubmitStoragePatchQueueTaskKindFromTask(task))
		identity = taskSubmitStorageNormalizePatchQueueIdentity(identity)
		if strings.TrimSpace(identity.ProjectID) != projectID {
			continue
		}
		if !taskSubmitStoragePatchQueueIdentityActionable(identity) || !taskSubmitStoragePatchQueueKindsOverlap(kind, identity.Kind) {
			continue
		}
		out = append(out, taskPatchQueueIdentityCandidate{
			TaskID:                              strings.TrimSpace(task.TaskID),
			Status:                              strings.TrimSpace(task.Status),
			taskSubmitStoragePatchQueueIdentity: identity,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy patch queue task identity candidates: %w", err)
	}
	return out, nil
}

func taskSubmitStoragePatchQueueProjectIDForCreateInput(input TaskCreateInput) string {
	projectID := taskSubmitStorageTrimRef(input.ProjectID)
	if projectID != "" {
		return projectID
	}
	identity := taskSubmitStoragePatchQueueIdentityFromTask(taskSubmitStorageWorkspaceTask(input))
	if !taskSubmitStoragePatchQueueIdentityActionable(identity) {
		return ""
	}
	return taskSubmitStorageTrimRef(identity.ProjectID)
}

func taskSubmitStorageWorkspaceTask(input TaskCreateInput) WorkspaceTaskRecord {
	return WorkspaceTaskRecord{
		TaskID:               strings.TrimSpace(input.TaskID),
		Title:                strings.TrimSpace(input.Title),
		Description:          strings.TrimSpace(input.Description),
		OwnerUserID:          strings.TrimSpace(input.OwnerUserID),
		Priority:             strings.TrimSpace(input.Priority),
		Status:               taskStatusPending,
		TaskKind:             strings.TrimSpace(input.TaskKind),
		TaskTemplate:         strings.TrimSpace(input.TaskTemplate),
		Tags:                 append([]string(nil), input.Tags...),
		ProjectID:            strings.TrimSpace(input.ProjectID),
		ProjectLane:          strings.TrimSpace(input.ProjectLane),
		RequiresProjectGate:  input.RequiresProjectGate,
		TaskRequirementsJSON: normalizeTaskRequirementsJSON(input.TaskRequirementsJSON),
		WriteScopeHints:      append([]string(nil), input.WriteScopeHints...),
	}
}

func taskSubmitStoragePatchQueueIdentityFromTask(task WorkspaceTaskRecord) taskSubmitStoragePatchQueueIdentity {
	requirements := taskSubmitStorageTaskRequirements(task.TaskRequirementsJSON)
	identity := taskSubmitStoragePatchQueueIdentity{
		ProjectID: firstNonEmpty(taskSubmitStorageRequirementString(requirements, "project_id"), taskSubmitStorageRequirementString(requirements, "candidate_project_id"), task.ProjectID),
		QueueID:   firstNonEmpty(taskSubmitStorageRequirementString(requirements, "queue_id"), taskSubmitStorageRequirementString(requirements, "patch_queue_id")),
		ItemID:    firstNonEmpty(taskSubmitStorageRequirementString(requirements, "item_id"), taskSubmitStorageRequirementString(requirements, "patch_item_id")),
		BranchID:  firstNonEmpty(taskSubmitStorageRequirementString(requirements, "branch_id"), taskSubmitStorageRequirementString(requirements, "target_branch_id"), taskSubmitStorageRequirementString(requirements, "candidate_branch_id")),
		HeadSHA:   firstNonEmpty(taskSubmitStorageRequirementString(requirements, "head_sha"), taskSubmitStorageRequirementString(requirements, "expected_head_sha"), taskSubmitStorageRequirementString(requirements, "candidate_head_sha"), taskSubmitStorageRequirementString(requirements, "target_head_sha")),
		Kind:      taskSubmitStorageRequirementString(requirements, "patch_queue_task_kind"),
	}
	if identity.BranchID == "" && task.ClaimBranchID != nil {
		identity.BranchID = *task.ClaimBranchID
	}

	text := strings.Join([]string{task.Title, task.Description, strings.Join(task.Tags, "\n")}, "\n")
	taskLike := strings.ToLower(strings.Join([]string{task.ProjectLane, text, task.TaskRequirementsJSON}, "\n"))
	if !taskSubmitStorageContainsAny(taskLike, []string{"patch_queue", "patch-queue", "patch queue", "patchq", "patchitem", "blocked integration candidate", "visual acceptance", "visual evidence", "browser evidence", "source-fidelity evidence", "rhizome_visual_acceptance_v1"}) {
		return taskSubmitStorageNormalizePatchQueueIdentity(identity)
	}
	texts := []string{task.Title, task.Description}
	identity.QueueID = firstNonEmpty(identity.QueueID, agentWorkTaskTextFieldValue(texts, "queue_id"), agentWorkTaskTextFieldValue(texts, "patch_queue_id"), taskSubmitStorageFirstRegexRef(taskSubmitStoragePatchQueueIDPattern, text))
	identity.ItemID = firstNonEmpty(identity.ItemID, agentWorkTaskTextFieldValue(texts, "item_id"), agentWorkTaskTextFieldValue(texts, "patch_item_id"), taskSubmitStorageFirstRegexRef(taskSubmitStoragePatchQueueItemPattern, text))
	identity.BranchID = firstNonEmpty(identity.BranchID, agentWorkTaskTextFieldValue(texts, "branch_id"), agentWorkTaskTextFieldValue(texts, "target_branch_id"), agentWorkTaskTextFieldValue(texts, "candidate_branch_id"), agentWorkTaskTextFieldValue(texts, "Branch ID"), taskSubmitStorageFirstRegexRef(taskSubmitStoragePatchQueueBranchPattern, text))
	identity.HeadSHA = firstNonEmpty(identity.HeadSHA, agentWorkTaskTextFieldValue(texts, "head_sha"), agentWorkTaskTextFieldValue(texts, "expected_head_sha"), agentWorkTaskTextFieldValue(texts, "candidate_head_sha"), agentWorkTaskTextFieldValue(texts, "head"), taskSubmitStorageFirstHeadRef(text))
	if identity.Kind == "" {
		identity.Kind = taskSubmitStoragePatchQueueTaskKindFromTask(task)
	}
	return taskSubmitStorageNormalizePatchQueueIdentity(identity)
}

func taskSubmitStorageTaskRequirements(raw string) map[string]any {
	raw = normalizeTaskRequirementsJSON(raw)
	if raw == "{}" {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil
	}
	return payload
}

func taskSubmitStorageRequirementString(requirements map[string]any, key string) string {
	if len(requirements) == 0 {
		return ""
	}
	raw, ok := requirements[strings.TrimSpace(key)]
	if !ok || raw == nil {
		return ""
	}
	switch value := raw.(type) {
	case string:
		return taskSubmitStorageTrimRef(value)
	case []string:
		if len(value) > 0 {
			return taskSubmitStorageTrimRef(value[0])
		}
	case []any:
		if len(value) > 0 {
			return taskSubmitStorageTrimRef(fmt.Sprint(value[0]))
		}
	default:
		return taskSubmitStorageTrimRef(fmt.Sprint(value))
	}
	return ""
}

func taskSubmitStorageTrimRef(value string) string {
	value = strings.TrimSpace(value)
	for {
		trimmed := strings.Trim(value, " \t\r\n`'\"")
		if trimmed == value {
			return value
		}
		value = trimmed
	}
}

func taskSubmitStorageFirstRegexRef(pattern *regexp.Regexp, text string) string {
	if pattern == nil {
		return ""
	}
	return taskSubmitStorageTrimRef(pattern.FindString(text))
}

func taskSubmitStorageFirstHeadRef(text string) string {
	match := taskSubmitStoragePatchQueueHeadPattern.FindStringSubmatch(text)
	if len(match) < 2 {
		return ""
	}
	return taskSubmitStorageTrimRef(match[1])
}

func taskSubmitStorageNormalizePatchQueueIdentity(identity taskSubmitStoragePatchQueueIdentity) taskSubmitStoragePatchQueueIdentity {
	identity.ProjectID = taskSubmitStorageTrimRef(identity.ProjectID)
	identity.QueueID = taskSubmitStorageTrimRef(identity.QueueID)
	identity.ItemID = taskSubmitStorageTrimRef(identity.ItemID)
	identity.BranchID = taskSubmitStorageTrimRef(identity.BranchID)
	identity.HeadSHA = strings.ToLower(taskSubmitStorageTrimRef(identity.HeadSHA))
	identity.Kind = strings.ToLower(strings.TrimSpace(identity.Kind))
	return identity
}

func taskSubmitStoragePatchQueueIdentityActionable(identity taskSubmitStoragePatchQueueIdentity) bool {
	identity = taskSubmitStorageNormalizePatchQueueIdentity(identity)
	if identity.QueueID != "" && identity.ItemID != "" {
		return true
	}
	if identity.BranchID != "" && identity.HeadSHA != "" {
		return true
	}
	if identity.ItemID != "" && identity.HeadSHA != "" {
		return true
	}
	return identity.ProjectID != "" && identity.HeadSHA != ""
}

func taskSubmitStoragePatchQueueTaskKindFromTask(task WorkspaceTaskRecord) string {
	text := strings.ToLower(strings.Join([]string{task.ProjectLane, task.Title, task.Description, strings.Join(task.Tags, "\n"), task.TaskRequirementsJSON}, "\n"))
	switch {
	case taskSubmitStorageContainsAny(text, []string{"revision", "revise", "repair", "unblock integration candidate"}):
		return "revision"
	case taskSubmitStorageContainsAny(text, []string{"review"}):
		return "review"
	case taskSubmitStorageContainsAny(text, []string{"validation", "validate", "visual", "browser", "source-fidelity", "evidence"}):
		return "validation"
	default:
		return ""
	}
}

func taskSubmitStoragePatchQueueKindsOverlap(left, right string) bool {
	left = strings.ToLower(strings.TrimSpace(left))
	right = strings.ToLower(strings.TrimSpace(right))
	if left == "" || right == "" {
		return false
	}
	if !taskSubmitStoragePatchQueueKindNeedsDuplicateGate(left) {
		return false
	}
	if left == right {
		return true
	}
	return false
}

func taskSubmitStoragePatchQueueKindNeedsDuplicateGate(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "validation", "review":
		return true
	default:
		return false
	}
}

func taskSubmitStoragePatchQueueKindNeedsVisualEvidenceReceipt(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "validation", "review":
		return true
	default:
		return false
	}
}

func taskSubmitStoragePatchQueueIdentityMatches(left, right taskSubmitStoragePatchQueueIdentity) bool {
	left = taskSubmitStorageNormalizePatchQueueIdentity(left)
	right = taskSubmitStorageNormalizePatchQueueIdentity(right)
	if left.QueueID != "" && right.QueueID != "" && !strings.EqualFold(left.QueueID, right.QueueID) {
		return false
	}
	if left.QueueID != "" && left.ItemID != "" && right.QueueID != "" && right.ItemID != "" &&
		strings.EqualFold(left.QueueID, right.QueueID) && strings.EqualFold(left.ItemID, right.ItemID) {
		return taskSubmitStorageHeadsCompatible(left.HeadSHA, right.HeadSHA)
	}
	if left.BranchID != "" && right.BranchID != "" && strings.EqualFold(left.BranchID, right.BranchID) {
		return taskSubmitStorageHeadsCompatible(left.HeadSHA, right.HeadSHA)
	}
	if left.ItemID != "" && right.ItemID != "" && strings.EqualFold(left.ItemID, right.ItemID) {
		return taskSubmitStorageHeadsCompatible(left.HeadSHA, right.HeadSHA)
	}
	if taskSubmitStorageHasPatchQueueLineage(left) && taskSubmitStorageHasPatchQueueLineage(right) {
		return false
	}
	if left.ProjectID != "" && right.ProjectID != "" && strings.EqualFold(left.ProjectID, right.ProjectID) &&
		left.HeadSHA != "" && right.HeadSHA != "" {
		return taskSubmitStorageFullHeadsEqual(left.HeadSHA, right.HeadSHA)
	}
	return false
}

func taskSubmitStorageHasPatchQueueLineage(identity taskSubmitStoragePatchQueueIdentity) bool {
	return identity.QueueID != "" || identity.ItemID != "" || identity.BranchID != ""
}

func taskSubmitStorageVisualEvidenceIdentityReferences(left, right taskSubmitStoragePatchQueueIdentity) bool {
	left = taskSubmitStorageNormalizePatchQueueIdentity(left)
	right = taskSubmitStorageNormalizePatchQueueIdentity(right)
	if taskSubmitStorageHasPatchQueueLineage(left) {
		return false
	}
	if left.ProjectID == "" || right.ProjectID == "" || !strings.EqualFold(left.ProjectID, right.ProjectID) {
		return false
	}
	if left.HeadSHA == "" || right.HeadSHA == "" {
		return false
	}
	return taskSubmitStorageHeadsCompatible(left.HeadSHA, right.HeadSHA)
}

func taskSubmitStorageHeadsCompatible(left, right string) bool {
	left = strings.ToLower(strings.TrimSpace(left))
	right = strings.ToLower(strings.TrimSpace(right))
	if left == "" || right == "" {
		return true
	}
	if left == right {
		return true
	}
	if len(left) >= 7 && strings.HasPrefix(right, left) {
		return true
	}
	return len(right) >= 7 && strings.HasPrefix(left, right)
}

func taskSubmitStorageFullHeadsEqual(left, right string) bool {
	left = strings.ToLower(strings.TrimSpace(left))
	right = strings.ToLower(strings.TrimSpace(right))
	return left == right && len(left) >= 40 && len(right) >= 40
}

func taskSubmitStoragePatchQueueRequestIsCanonicalRetry(task WorkspaceTaskRecord) bool {
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		strings.Join(task.Tags, "\n"),
		task.TaskRequirementsJSON,
	}, "\n"))
	if strings.Contains(text, "retry_of_terminal_followup_task") {
		return true
	}
	return strings.Contains(text, "patchq") && strings.Contains(text, "retry")
}

func taskSubmitStorageVisualEvidenceDuplicateCandidate(task WorkspaceTaskRecord) bool {
	if agentWorkPatchQueueEvidenceRefreshTask(task) {
		return true
	}
	text := strings.ToLower(strings.Join([]string{
		task.Title,
		task.Description,
		task.ProjectLane,
		strings.Join(task.Tags, " "),
		task.TaskRequirementsJSON,
	}, "\n"))
	return taskSubmitStorageContainsAny(text, []string{
		"rhizome_visual_acceptance_v1",
		"visual acceptance",
		"visual evidence",
		"browser evidence",
		"browser smoke",
		"browser-smoke",
		"source-fidelity evidence",
	})
}

func (s *Store) taskSubmitStorageVisualEvidenceDocForIdentity(ctx context.Context, workspaceID, projectID string, task WorkspaceTaskRecord, identity taskSubmitStoragePatchQueueIdentity) (string, bool, error) {
	coordination, err := s.GetProjectCoordination(ctx, workspaceID, projectID)
	if err != nil {
		return "", false, err
	}
	branches := make(map[string]ProjectBranchRecord, len(coordination.Branches))
	for _, branch := range coordination.Branches {
		if id := strings.TrimSpace(branch.BranchID); id != "" {
			branches[id] = branch
		}
	}
	type visualEvidenceCandidate struct {
		item   ProjectPatchQueueItemRecord
		branch ProjectBranchRecord
	}
	var candidates []visualEvidenceCandidate
	for _, item := range coordination.PatchQueueItems {
		branch := branches[strings.TrimSpace(item.BranchID)]
		itemIdentity := taskSubmitStoragePatchQueueIdentity{
			ProjectID: strings.TrimSpace(item.ProjectID),
			QueueID:   strings.TrimSpace(item.QueueID),
			ItemID:    strings.TrimSpace(item.ItemID),
			BranchID:  strings.TrimSpace(firstNonEmpty(item.BranchID, branch.BranchID)),
			HeadSHA:   strings.TrimSpace(firstNonEmpty(item.HeadSHA, branch.HeadSHA)),
		}
		if !taskSubmitStoragePatchQueueIdentityMatches(identity, itemIdentity) &&
			!taskSubmitStorageVisualEvidenceIdentityReferences(identity, itemIdentity) {
			continue
		}
		if !agentWorkPatchQueueTaskHeadCompatible(task, item, branch) {
			continue
		}
		candidates = append(candidates, visualEvidenceCandidate{item: item, branch: branch})
	}
	if len(candidates) > 1 && taskSubmitStorageHeadSHAIsShortPrefix(identity.HeadSHA) {
		fullHeads := make(map[string]ProjectPatchQueueItemRecord)
		for _, candidate := range candidates {
			head := strings.ToLower(strings.TrimSpace(firstNonEmpty(candidate.item.HeadSHA, candidate.branch.HeadSHA)))
			if !isCanonicalProjectGitObjectID(head) {
				continue
			}
			fullHeads[head] = candidate.item
		}
		if len(fullHeads) > 1 {
			for head, item := range fullHeads {
				return "", false, &TaskSubmitPatchQueueGateError{
					Gate:               "patch_queue_head_ambiguous",
					RequiredTransition: "project_patch_queue_followup",
					QueueID:            item.QueueID,
					ItemID:             item.ItemID,
					BranchID:           item.BranchID,
					HeadSHA:            head,
				}
			}
		}
	}
	for _, candidate := range candidates {
		docKey, ok, err := s.taskSubmitStorageVisualEvidenceDocForItem(ctx, workspaceID, candidate.item, candidate.branch)
		if err != nil || ok {
			return docKey, ok, err
		}
	}
	return "", false, nil
}

func taskSubmitStorageHeadSHAIsShortPrefix(headSHA string) bool {
	headSHA = strings.ToLower(strings.TrimSpace(headSHA))
	if len(headSHA) < 7 || len(headSHA) >= 40 {
		return false
	}
	for _, ch := range headSHA {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func (s *Store) taskSubmitStorageVisualEvidenceDocForItem(ctx context.Context, workspaceID string, item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) (string, bool, error) {
	summaries, err := s.ListWorkspaceDocs(ctx, workspaceID, false)
	if err != nil {
		return "", false, err
	}
	for _, summary := range summaries {
		if !agentWorkPatchQueueVisualEvidenceSummaryCouldMatch(summary, item, branch) {
			continue
		}
		doc, err := s.GetWorkspaceDoc(ctx, workspaceID, summary.DocKey)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return "", false, err
		}
		if doc.ArchivedAt != nil {
			continue
		}
		if agentWorkPatchQueueVisualEvidenceDocNamesItem(doc, item, branch) && agentWorkPatchQueueVisualEvidenceDocFreshForItem(doc, item) {
			return strings.TrimSpace(doc.DocKey), true, nil
		}
	}
	return "", false, nil
}

func taskSubmitStorageContainsAny(text string, signals []string) bool {
	for _, signal := range signals {
		if strings.Contains(text, strings.ToLower(strings.TrimSpace(signal))) {
			return true
		}
	}
	return false
}
