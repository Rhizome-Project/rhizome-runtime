package main

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"strings"
	"time"
)

const strategyBlockerHoldMaxAge = 10 * time.Minute

func (r *Runtime) shouldHoldRepeatedStrategyBlocker(ctx context.Context, task WorkspaceTaskRecord) (bool, error) {
	if r == nil || r.client == nil || !strategyBlockerTaskInScope(task) {
		return false, nil
	}
	r.mu.Lock()
	state := r.scratch
	r.mu.Unlock()
	if strings.TrimSpace(state.StrategyBlockerTaskID) != strings.TrimSpace(task.TaskID) ||
		strings.TrimSpace(state.StrategyBlockerProjectID) != strings.TrimSpace(task.ProjectID) ||
		strings.TrimSpace(state.StrategyBlockerResultFingerprint) == "" ||
		strings.TrimSpace(state.StrategyBlockerCoordFingerprint) == "" {
		return false, nil
	}
	if strategyBlockerHoldExpired(state.StrategyBlockerRecordedAt, time.Now().UTC()) {
		if err := r.clearStrategyBlockerScratch(ctx, task.TaskID); err != nil {
			return false, err
		}
		return false, nil
	}
	fingerprint, err := r.currentProjectCoordinationFingerprint(ctx, task.ProjectID, task.TaskID)
	if err != nil || fingerprint == "" {
		return false, err
	}
	if fingerprint != strings.TrimSpace(state.StrategyBlockerCoordFingerprint) {
		if err := r.clearStrategyBlockerScratch(ctx, task.TaskID); err != nil {
			return false, err
		}
		return false, nil
	}
	log.Printf("[runtime] holding repeated strategy blocker for task %s until project coordination changes", task.TaskID)
	return true, nil
}

func strategyBlockerHoldExpired(recordedAt string, now time.Time) bool {
	recordedAt = strings.TrimSpace(recordedAt)
	if recordedAt == "" {
		return true
	}
	parsed, err := time.Parse(time.RFC3339Nano, recordedAt)
	if err != nil {
		return true
	}
	return now.Sub(parsed) >= strategyBlockerHoldMaxAge
}

func (r *Runtime) recordStrategyBlockerFingerprint(ctx context.Context, task WorkspaceTaskRecord, result StructuredTaskResult) {
	if r == nil || r.client == nil || !strategyBlockerTaskInScope(task) || !strategyBlockerResultInScope(result) {
		return
	}
	coordFingerprint, err := r.currentProjectCoordinationFingerprint(ctx, task.ProjectID, task.TaskID)
	if err != nil || coordFingerprint == "" {
		if err != nil {
			log.Printf("[runtime] strategy blocker coordination fingerprint degraded for task %s: %v", task.TaskID, err)
		}
		return
	}
	resultFingerprint := strategyBlockerResultFingerprint(result)
	if resultFingerprint == "" {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.StrategyBlockerTaskID = strings.TrimSpace(task.TaskID)
		state.StrategyBlockerProjectID = strings.TrimSpace(task.ProjectID)
		state.StrategyBlockerResultFingerprint = resultFingerprint
		state.StrategyBlockerCoordFingerprint = coordFingerprint
		state.StrategyBlockerSummary = strings.TrimSpace(result.Summary)
		state.StrategyBlockerRecordedAt = now
	}); err != nil {
		log.Printf("[runtime] strategy blocker fingerprint persistence degraded for task %s: %v", task.TaskID, err)
	}
}

func (r *Runtime) clearStrategyBlockerScratch(ctx context.Context, taskID string) error {
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		if taskID != "" && strings.TrimSpace(state.StrategyBlockerTaskID) != strings.TrimSpace(taskID) {
			return
		}
		state.StrategyBlockerTaskID = ""
		state.StrategyBlockerProjectID = ""
		state.StrategyBlockerResultFingerprint = ""
		state.StrategyBlockerCoordFingerprint = ""
		state.StrategyBlockerSummary = ""
		state.StrategyBlockerRecordedAt = ""
	})
}

func (r *Runtime) currentProjectCoordinationFingerprint(ctx context.Context, projectID, excludeTaskID string) (string, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" || r == nil || r.client == nil {
		return "", nil
	}
	coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, projectID)
	if err != nil {
		return "", err
	}
	return projectCoordinationStateFingerprintForTask(coordination, excludeTaskID), nil
}

func strategyBlockerTaskInScope(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(task.ProjectLane), "strategy")
}

func strategyBlockerTriggerBypasses(trigger pendingWorkTrigger) bool {
	switch normalizeWorkTrigger(trigger.Trigger) {
	case "", "request_resume":
		return false
	default:
		return true
	}
}

func strategyBlockerResultInScope(result StructuredTaskResult) bool {
	if normalizeOutcome(result.Outcome) != "blocked" || result.RequiresHuman || len(result.BlockedOn) == 0 {
		return false
	}
	for _, blocker := range result.BlockedOn {
		if normalizeBlockedKind(blocker.Kind, blocker.Detail) != "dependency" {
			return false
		}
	}
	return true
}

func strategyBlockerResultFingerprint(result StructuredTaskResult) string {
	blockers := normalizedBlockedRefs(result.BlockedOn, result.Summary)
	sort.SliceStable(blockers, func(i, j int) bool {
		left := strings.Join([]string{blockers[i].Kind, blockers[i].Detail}, "|")
		right := strings.Join([]string{blockers[j].Kind, blockers[j].Detail}, "|")
		return left < right
	})
	payload := map[string]any{
		"outcome":     normalizeOutcome(result.Outcome),
		"summary":     canonicalStrategyBlockerText(result.Summary),
		"details":     canonicalStrategyBlockerText(result.Details),
		"next_action": canonicalStrategyBlockerText(result.NextAction),
		"blocked_on":  blockers,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return "sha256:" + sha256Hex(string(raw))
}

func projectCoordinationStateFingerprint(coordination ProjectCoordinationRecord) string {
	return projectCoordinationStateFingerprintForTask(coordination, "")
}

func projectCoordinationStateFingerprintForTask(coordination ProjectCoordinationRecord, excludeTaskID string) string {
	type branchFingerprint struct {
		BranchID       string `json:"branch_id"`
		AgentID        string `json:"agent_id,omitempty"`
		ActiveTaskID   string `json:"active_task_id,omitempty"`
		BranchName     string `json:"branch_name,omitempty"`
		HeadSHA        string `json:"head_sha,omitempty"`
		BaseSHA        string `json:"base_sha,omitempty"`
		ReviewDocKey   string `json:"review_doc_key,omitempty"`
		Status         string `json:"status,omitempty"`
		UpdatedAt      string `json:"updated_at,omitempty"`
		WriteScopeJSON string `json:"write_scope_json,omitempty"`
	}
	type patchQueueFingerprint struct {
		QueueID         string `json:"queue_id"`
		ItemID          string `json:"item_id"`
		BranchID        string `json:"branch_id"`
		State           string `json:"state"`
		HeadSHA         string `json:"head_sha,omitempty"`
		DecisionSummary string `json:"decision_summary,omitempty"`
		DecisionDocKey  string `json:"decision_doc_key,omitempty"`
		UpdatedAt       string `json:"updated_at,omitempty"`
	}
	type taskFingerprint struct {
		TaskID        string `json:"task_id"`
		Status        string `json:"status"`
		ProjectLane   string `json:"project_lane,omitempty"`
		ClaimAgentID  string `json:"claim_agent_id,omitempty"`
		ClaimStatus   string `json:"claim_status,omitempty"`
		ClaimBranchID string `json:"claim_branch_id,omitempty"`
		UpdatedAt     string `json:"updated_at,omitempty"`
	}

	branches := make([]branchFingerprint, 0, len(coordination.Branches))
	for _, branch := range coordination.Branches {
		branches = append(branches, branchFingerprint{
			BranchID:       strings.TrimSpace(branch.BranchID),
			AgentID:        strings.TrimSpace(branch.AgentID),
			ActiveTaskID:   strings.TrimSpace(branch.ActiveTaskID),
			BranchName:     strings.TrimSpace(branch.BranchName),
			HeadSHA:        strings.TrimSpace(branch.HeadSHA),
			BaseSHA:        strings.TrimSpace(branch.BaseSHA),
			ReviewDocKey:   strings.TrimSpace(branch.ReviewDocKey),
			Status:         strings.ToUpper(strings.TrimSpace(branch.Status)),
			UpdatedAt:      strings.TrimSpace(branch.UpdatedAt),
			WriteScopeJSON: strings.TrimSpace(branch.WriteScopeJSON),
		})
	}
	sort.SliceStable(branches, func(i, j int) bool { return branches[i].BranchID < branches[j].BranchID })

	items := make([]patchQueueFingerprint, 0, len(coordination.PatchQueueItems))
	for _, item := range coordination.PatchQueueItems {
		items = append(items, patchQueueFingerprint{
			QueueID:         strings.TrimSpace(item.QueueID),
			ItemID:          strings.TrimSpace(item.ItemID),
			BranchID:        strings.TrimSpace(item.BranchID),
			State:           strings.ToUpper(strings.TrimSpace(item.State)),
			HeadSHA:         strings.TrimSpace(item.HeadSHA),
			DecisionSummary: strings.TrimSpace(item.DecisionSummary),
			DecisionDocKey:  strings.TrimSpace(item.DecisionDocKey),
			UpdatedAt:       strings.TrimSpace(item.UpdatedAt),
		})
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].ItemID < items[j].ItemID })

	tasks := make([]taskFingerprint, 0, len(coordination.Tasks))
	for _, task := range coordination.Tasks {
		if strings.TrimSpace(excludeTaskID) != "" && strings.TrimSpace(task.TaskID) == strings.TrimSpace(excludeTaskID) {
			continue
		}
		tasks = append(tasks, taskFingerprint{
			TaskID:        strings.TrimSpace(task.TaskID),
			Status:        strings.ToUpper(strings.TrimSpace(task.Status)),
			ProjectLane:   strings.ToLower(strings.TrimSpace(task.ProjectLane)),
			ClaimAgentID:  taskPointerValue(task.ClaimAgentID),
			ClaimStatus:   strings.ToUpper(taskPointerValue(task.ClaimStatus)),
			ClaimBranchID: taskPointerValue(task.ClaimBranchID),
			UpdatedAt:     strings.TrimSpace(task.UpdatedAt),
		})
	}
	sort.SliceStable(tasks, func(i, j int) bool { return tasks[i].TaskID < tasks[j].TaskID })

	payload := map[string]any{
		"project_id":           strings.TrimSpace(coordination.Project.ProjectID),
		"project_status":       strings.ToUpper(strings.TrimSpace(coordination.Project.Status)),
		"profile_phase":        strings.ToUpper(strings.TrimSpace(coordination.Profile.CurrentPhase)),
		"profile_repo_status":  strings.ToUpper(strings.TrimSpace(coordination.Profile.RepoStatus)),
		"gate_phase":           strings.ToUpper(strings.TrimSpace(coordination.GateStatus.CurrentPhase)),
		"gate_overall":         strings.ToUpper(strings.TrimSpace(coordination.GateStatus.OverallState)),
		"implementation_ready": coordination.GateStatus.ImplementationReady,
		"branches":             branches,
		"patch_queue_items":    items,
		"tasks":                tasks,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return "sha256:" + sha256Hex(string(raw))
}

func canonicalStrategyBlockerText(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(value))), " ")
}
