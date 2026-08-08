package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

const taskSubmitIntegratedAcceptanceCoverageGate = "integrated_acceptance_contract_satisfied"

type taskIntegratedAcceptanceCoverageReceipt struct {
	ProjectID          string
	RequestedCriteria  []string
	CoveredCriteria    []string
	SourceTaskIDs      []string
	IntegratedItemRefs []string
	QueueID            string
	ItemID             string
	BranchID           string
	HeadSHA            string
}

type taskIntegratedAcceptanceCoverageEvidence struct {
	CriteriaKeys map[string]struct{}
	SourceTaskID string
	ItemRef      string
	QueueID      string
	ItemID       string
	BranchID     string
	HeadSHA      string
}

func (s *Store) enforceTaskSubmitIntegratedAcceptanceCoverageGateTx(ctx context.Context, tx *sql.Tx, input TaskCreateInput) error {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	task := taskSubmitStorageWorkspaceTask(input)
	projectID := firstNonEmpty(taskSubmitStorageTrimRef(input.ProjectID), strings.TrimSpace(task.ProjectID))
	if workspaceID == "" || projectID == "" || !taskCompletionCoverageSubmitCandidate(task) {
		return nil
	}
	identity := taskSubmitStoragePatchQueueIdentityFromTask(task)
	identity.ProjectID = firstNonEmpty(identity.ProjectID, projectID)
	if taskSubmitStoragePatchQueueIdentityActionable(identity) {
		return nil
	}
	receipt, ok, err := s.taskIntegratedAcceptanceCoverageReceipt(ctx, tx, workspaceID, projectID, task)
	if err != nil || !ok {
		return err
	}
	return &TaskSubmitPatchQueueGateError{
		Gate:               taskSubmitIntegratedAcceptanceCoverageGate,
		ExistingTaskID:     firstString(receipt.SourceTaskIDs),
		ExistingTaskStatus: ProjectPatchQueueStateIntegrated,
		RequiredTransition: "open_uncovered_acceptance_delta",
		QueueID:            receipt.QueueID,
		ItemID:             receipt.ItemID,
		BranchID:           receipt.BranchID,
		HeadSHA:            receipt.HeadSHA,
	}
}

func taskCompletionCoverageSubmitCandidate(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.TaskKind), model.TaskKindExecution) {
		return false
	}
	if !projectLaneRequiresImplementationGate(task.ProjectLane) {
		return false
	}
	if len(taskCompletionAcceptanceCriteriaMapping(task)) == 0 {
		return false
	}
	return true
}

func taskCompletionCoverageTerminalizationCandidate(task WorkspaceTaskRecord) bool {
	if !taskCompletionCoverageSubmitCandidate(task) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.Status), model.TaskStatusPending) {
		return false
	}
	claimStatus := strings.TrimSpace(workspaceTaskPointerValue(task.ClaimStatus))
	switch {
	case claimStatus == "":
	case strings.EqualFold(claimStatus, model.TaskClaimStatusReleased):
	case strings.EqualFold(claimStatus, model.TaskClaimStatusClaimed):
	default:
		return false
	}
	// Claim admission for implementation work may allocate a branch/checkout before any
	// separate patch-queue identity exists. The safety boundary is the no-identity check
	// in agentWorkIntegratedAcceptanceCoverageTerminalReceipt plus full coverage of a
	// non-empty acceptance contract; requiring empty branch/checkout lets stale resume
	// siblings survive selection.
	return true
}

func (s *Store) taskIntegratedAcceptanceCoverageReceipt(ctx context.Context, q sqlReadQuerier, workspaceID, projectID string, task WorkspaceTaskRecord) (taskIntegratedAcceptanceCoverageReceipt, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	if q == nil || workspaceID == "" || projectID == "" {
		return taskIntegratedAcceptanceCoverageReceipt{}, false, nil
	}
	requestedCriteria := taskCompletionAcceptanceCriteriaMapping(task)
	if len(requestedCriteria) == 0 {
		return taskIntegratedAcceptanceCoverageReceipt{}, false, nil
	}
	evidence, err := s.taskIntegratedAcceptanceCoverageEvidence(ctx, q, workspaceID, projectID)
	if err != nil || len(evidence) == 0 {
		return taskIntegratedAcceptanceCoverageReceipt{}, false, err
	}

	receipt := taskIntegratedAcceptanceCoverageReceipt{
		ProjectID:         projectID,
		RequestedCriteria: requestedCriteria,
	}
	sourceSeen := map[string]struct{}{}
	itemSeen := map[string]struct{}{}
	for _, criterion := range requestedCriteria {
		matched, ok := taskCompletionCriterionCoveredByIntegratedEvidence(criterion, evidence)
		if !ok {
			return taskIntegratedAcceptanceCoverageReceipt{}, false, nil
		}
		receipt.CoveredCriteria = append(receipt.CoveredCriteria, criterion)
		if matched.SourceTaskID != "" {
			sourceSeen[matched.SourceTaskID] = struct{}{}
		}
		if matched.ItemRef != "" {
			itemSeen[matched.ItemRef] = struct{}{}
		}
		if receipt.QueueID == "" {
			receipt.QueueID = matched.QueueID
			receipt.ItemID = matched.ItemID
			receipt.BranchID = matched.BranchID
			receipt.HeadSHA = matched.HeadSHA
		}
	}
	receipt.SourceTaskIDs = sortedStringSetKeys(sourceSeen)
	receipt.IntegratedItemRefs = sortedStringSetKeys(itemSeen)
	return receipt, true, nil
}

func (s *Store) taskIntegratedAcceptanceCoverageEvidence(ctx context.Context, q sqlReadQuerier, workspaceID, projectID string) ([]taskIntegratedAcceptanceCoverageEvidence, error) {
	items, err := listIntegratedProjectPatchQueueItemsWithQuerier(ctx, q, workspaceID, projectID)
	if err != nil || len(items) == 0 {
		return nil, err
	}
	var sourceTaskIDs []string
	for _, item := range items {
		if taskID := strings.TrimSpace(item.TaskID); taskID != "" {
			sourceTaskIDs = append(sourceTaskIDs, taskID)
		}
	}
	sourceTasks, err := s.workspaceTaskRecordsByIDsWithQuerier(ctx, q, workspaceID, sourceTaskIDs)
	if err != nil {
		return nil, err
	}
	evidence := make([]taskIntegratedAcceptanceCoverageEvidence, 0, len(items))
	for _, item := range items {
		sourceTaskID := strings.TrimSpace(item.TaskID)
		if sourceTaskID == "" {
			continue
		}
		sourceTask, ok := sourceTasks[sourceTaskID]
		if !ok {
			continue
		}
		sourceCriteria := taskCompletionAcceptanceCriteriaMapping(sourceTask)
		criteriaKeys := make(map[string]struct{}, len(sourceCriteria))
		for _, criterion := range sourceCriteria {
			if key := taskCompletionCriterionKey(criterion); key != "" {
				criteriaKeys[key] = struct{}{}
			}
		}
		if len(criteriaKeys) == 0 {
			continue
		}
		entry := taskIntegratedAcceptanceCoverageEvidence{
			CriteriaKeys: criteriaKeys,
			SourceTaskID: sourceTaskID,
			ItemRef:      agentWorkPatchQueueItemPairKey(item.QueueID, item.ItemID),
			QueueID:      strings.TrimSpace(item.QueueID),
			ItemID:       strings.TrimSpace(item.ItemID),
			BranchID:     strings.TrimSpace(item.BranchID),
			HeadSHA:      strings.TrimSpace(item.HeadSHA),
		}
		evidence = append(evidence, entry)
	}
	return evidence, nil
}

func listIntegratedProjectPatchQueueItemsWithQuerier(ctx context.Context, q sqlReadQuerier, workspaceID, projectID string) ([]ProjectPatchQueueItemRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	if q == nil || workspaceID == "" || projectID == "" {
		return nil, nil
	}
	rows, err := q.QueryContext(ctx, `
SELECT `+projectPatchQueueItemSelectColumns+`
  FROM project_patch_queue_items
 WHERE workspace_id = ?
   AND project_id = ?
   AND state = ?
 ORDER BY updated_at DESC, queue_id ASC, item_id ASC`,
		workspaceID,
		projectID,
		ProjectPatchQueueStateIntegrated,
	)
	if err != nil {
		return nil, fmt.Errorf("list integrated project patch queue items for acceptance coverage: %w", err)
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
	return items, nil
}

func taskCompletionCriterionCoveredByIntegratedEvidence(criterion string, evidence []taskIntegratedAcceptanceCoverageEvidence) (taskIntegratedAcceptanceCoverageEvidence, bool) {
	key := taskCompletionCriterionKey(criterion)
	if key == "" {
		return taskIntegratedAcceptanceCoverageEvidence{}, false
	}
	for _, entry := range evidence {
		if _, ok := entry.CriteriaKeys[key]; ok {
			return entry, true
		}
	}
	return taskIntegratedAcceptanceCoverageEvidence{}, false
}

func taskCompletionAcceptanceCriteriaMapping(task WorkspaceTaskRecord) []string {
	payload := taskCompletionTaskRequirements(task.TaskRequirementsJSON)
	if len(payload) == 0 {
		return nil
	}
	return taskCompletionStringSlice(payload["acceptance_criteria_mapping"])
}

func taskCompletionTaskRequirements(raw string) map[string]any {
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

func taskCompletionStringSlice(value any) []string {
	var out []string
	var add func(any)
	add = func(value any) {
		switch typed := value.(type) {
		case []any:
			for _, item := range typed {
				add(item)
			}
		case []string:
			for _, item := range typed {
				add(item)
			}
		case string:
			out = append(out, typed)
		case map[string]any:
			if str := taskCompletionFirstStringField(typed, "criterion", "text", "title", "summary", "description", "name"); str != "" {
				out = append(out, str)
			}
		case map[string]string:
			for _, key := range []string{"criterion", "text", "title", "summary", "description", "name"} {
				if str := strings.TrimSpace(typed[key]); str != "" {
					out = append(out, str)
					return
				}
			}
		}
	}
	add(value)
	return uniqueTrimmedStrings(out)
}

func taskCompletionFirstStringField(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		if str := strings.TrimSpace(agentWorkStringValue(payload[key])); str != "" {
			return str
		}
	}
	return ""
}

func taskCompletionCriterionKey(criterion string) string {
	criterion = strings.ToLower(strings.TrimSpace(criterion))
	if criterion == "" {
		return ""
	}
	normalized := strings.NewReplacer(
		"`", " ",
		"'", " ",
		"\"", " ",
		"\r", " ",
		"\n", " ",
		"\t", " ",
		"(", " ",
		")", " ",
		"[", " ",
		"]", " ",
		"{", " ",
		"}", " ",
		",", " ",
		".", " ",
		":", " ",
		";", " ",
		"/", " ",
		"\\", " ",
		"-", " ",
		"_", " ",
		"|", " ",
	).Replace(criterion)
	return strings.Join(strings.Fields(normalized), " ")
}

func (s *Store) taskHasPatchQueueIdentityWithQuerier(ctx context.Context, q sqlReadQuerier, workspaceID, taskID string) (bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	if q == nil || workspaceID == "" || taskID == "" {
		return false, nil
	}
	var count int
	if err := q.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM task_patch_queue_identities
 WHERE workspace_id = ?
   AND task_id = ?`,
		workspaceID,
		taskID,
	).Scan(&count); err != nil {
		return false, fmt.Errorf("query task patch queue identity presence: %w", err)
	}
	return count > 0, nil
}

func sortedStringSetKeys(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for value := range set {
		if strings.TrimSpace(value) != "" {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}
