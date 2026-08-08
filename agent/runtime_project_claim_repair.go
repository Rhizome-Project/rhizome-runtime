package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

const (
	projectClaimRepairRequestFreshness        = 10 * time.Minute
	projectClaimScopeBusyEvidenceSchema       = "project_claim_scope_busy_evidence.v1"
	projectClaimOwnerResumeRequestSchema      = "project_claim_owner_resume_request.v1"
	projectClaimOwnerResumeRequestKind        = "project_claim_owner_resume"
	projectClaimScopeBusyEvidenceKind         = "project_claim_scope_busy"
	projectClaimRepairLiberationCandidateKind = "stale_claim_liberation_narrow_candidate"
)

type projectClaimRepairConflict struct {
	ProjectID              string
	BlockedTaskID          string
	BlockedAgentID         string
	BlockedWriteScopeJSON  string
	RepoID                 string
	ConflictKind           string
	ConflictOwnerAgentID   string
	ConflictTaskID         string
	ConflictBranchID       string
	ConflictBranchStatus   string
	ConflictBranchHeadSHA  string
	ConflictReviewDocKey   string
	ConflictPatchItemID    string
	ConflictPatchState     string
	ConflictPatchDecision  string
	ConflictWriteScopeJSON string
	RepairKey              string
	RepairTaskID           string
	Reason                 string
}

func (r *Runtime) maybeEnqueueProjectClaimRepairFromWork(ctx context.Context, work AgentWorkNextResult) (bool, error) {
	if !agentWorkProjectClaimScopeBusy(work) {
		return false, nil
	}
	// R67 finalization-liveness freeze: claim-repair is discretionary coordination. When the project is
	// already convergeable (spec-complete, lead should be attesting), minting more claim-repair churn
	// only starves the lead's convergence turn. Suppress it - the claim conflict no longer matters once
	// no spec-rooted blocking work remains. Errs toward allow (only skips with a loaded, clear frontier).
	if coordination, ok := projectCoordinationFromWork(&work); ok {
		if runtimeProjectFinalizationConvergeable(coordination, strings.TrimSpace(projectClaimRepairBlockedTaskFromWork(work).ProjectID)) {
			return true, nil
		}
	}
	task := projectClaimRepairBlockedTaskFromWork(work)
	reason := firstNonEmpty(closedGateWorkSummary(work, closedGateWorkPacket(work)), "project claim scope busy")
	if strings.TrimSpace(task.TaskID) == "" && strings.TrimSpace(task.ProjectID) != "" {
		inferred, candidateCount, err := r.inferProjectClaimRepairBlockedTask(ctx, task, &work)
		if err != nil {
			return true, err
		}
		if strings.TrimSpace(inferred.TaskID) != "" {
			task = mergeProjectClaimRepairTaskDigest(task, inferred)
			reason = strings.TrimSpace(reason + " (malformed_work_next_packet_missing_anchor=true; blocked task inferred from single unambiguous project implementation candidate)")
		} else {
			return true, r.postMalformedProjectClaimScopeBusyUpdate(ctx, task, &work, candidateCount, reason)
		}
	}
	if strings.TrimSpace(task.TaskID) == "" || strings.TrimSpace(task.ProjectID) == "" {
		return true, r.postMalformedProjectClaimScopeBusyUpdate(ctx, task, &work, 0, reason)
	}
	return true, r.enqueueProjectClaimRepair(ctx, task, &work, reason)
}

func agentWorkProjectClaimScopeBusy(work AgentWorkNextResult) bool {
	if strings.EqualFold(strings.TrimSpace(work.Reason), "project_claim_scope_busy") {
		return true
	}
	if work.Packet == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(work.Packet.WorkType), "project_claim_scope_busy") ||
		strings.EqualFold(strings.TrimSpace(work.Packet.CoordinationState), "project_claim_scope_busy")
}

func projectClaimRepairBlockedTaskFromWork(work AgentWorkNextResult) WorkspaceTaskRecord {
	if work.Task != nil {
		return *work.Task
	}
	taskID := ""
	if work.Packet != nil {
		for _, candidate := range work.Packet.ContextHints.AnchorTaskIDs {
			if strings.TrimSpace(candidate) != "" {
				taskID = strings.TrimSpace(candidate)
				break
			}
		}
	}
	if taskID != "" {
		if coordination, ok := projectCoordinationFromWork(&work); ok {
			for _, candidate := range coordination.Tasks {
				if strings.TrimSpace(candidate.TaskID) == taskID {
					return candidate
				}
			}
		}
	}
	return WorkspaceTaskRecord{
		TaskID:              taskID,
		ProjectID:           firstNonEmpty(strings.TrimSpace(work.ProjectID), packetProjectID(work.Packet)),
		TaskKind:            firstNonEmpty(strings.TrimSpace(work.TaskKind), packetTaskKind(work.Packet)),
		ProjectLane:         firstNonEmpty(strings.TrimSpace(work.ProjectLane), packetProjectLane(work.Packet)),
		RequiresProjectGate: firstNonNilBool(work.RequiresProjectGate, packetRequiresProjectGate(work.Packet)),
	}
}

func mergeProjectClaimRepairTaskDigest(base, inferred WorkspaceTaskRecord) WorkspaceTaskRecord {
	if strings.TrimSpace(base.TaskID) == "" {
		base.TaskID = strings.TrimSpace(inferred.TaskID)
	}
	base.ProjectID = firstNonEmpty(strings.TrimSpace(base.ProjectID), strings.TrimSpace(inferred.ProjectID))
	base.TaskKind = firstNonEmpty(strings.TrimSpace(base.TaskKind), strings.TrimSpace(inferred.TaskKind))
	base.ProjectLane = firstNonEmpty(strings.TrimSpace(base.ProjectLane), strings.TrimSpace(inferred.ProjectLane))
	base.RequiresProjectGate = firstNonNilBool(base.RequiresProjectGate, inferred.RequiresProjectGate)
	return base
}

func (r *Runtime) inferProjectClaimRepairBlockedTask(ctx context.Context, task WorkspaceTaskRecord, work *AgentWorkNextResult) (WorkspaceTaskRecord, int, error) {
	projectID := strings.TrimSpace(task.ProjectID)
	if projectID == "" {
		return WorkspaceTaskRecord{}, 0, nil
	}
	coordination, ok := projectCoordinationFromWork(work)
	if !ok {
		if r == nil || r.client == nil {
			return WorkspaceTaskRecord{}, 0, nil
		}
		var err error
		coordination, err = r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, projectID)
		if err != nil {
			return WorkspaceTaskRecord{}, 0, fmt.Errorf("project claim repair fallback coordination: %w", err)
		}
	}
	return inferProjectClaimRepairBlockedTaskFromCoordination(coordination, strings.TrimSpace(r.cfg.AgentID), projectID)
}

func inferProjectClaimRepairBlockedTaskFromCoordination(coordination ProjectCoordinationRecord, agentID, projectID string) (WorkspaceTaskRecord, int, error) {
	agentID = strings.TrimSpace(agentID)
	projectID = firstNonEmpty(strings.TrimSpace(projectID), strings.TrimSpace(coordination.Project.ProjectID), strings.TrimSpace(coordination.Profile.ProjectID))
	roleScopeJSON, _, _ := selectProjectClaimWriteScope(coordination, agentID)
	rolePaths := projectRoleAssignWriteScopePaths(roleScopeJSON)
	type candidate struct {
		task  WorkspaceTaskRecord
		score int
	}
	candidates := []candidate{}
	for _, task := range coordination.Tasks {
		if !projectClaimRepairInferenceTaskEligible(task, projectID) {
			continue
		}
		score := 0
		if strings.TrimSpace(pointerValue(task.ClaimAgentID)) == agentID {
			score += 4
		}
		if len(rolePaths) > 0 && runtimeProjectWriteScopesOverlap(rolePaths, projectRoleAssignWriteScopePaths(pointerValue(task.ClaimWriteScopeJSON))) {
			score += 2
		}
		candidates = append(candidates, candidate{task: task, score: score})
	}
	if len(candidates) == 0 {
		return WorkspaceTaskRecord{}, 0, nil
	}
	bestScore := candidates[0].score
	for _, candidate := range candidates[1:] {
		if candidate.score > bestScore {
			bestScore = candidate.score
		}
	}
	best := []WorkspaceTaskRecord{}
	for _, candidate := range candidates {
		if candidate.score == bestScore {
			best = append(best, candidate.task)
		}
	}
	if len(best) != 1 {
		return WorkspaceTaskRecord{}, len(candidates), nil
	}
	return best[0], len(candidates), nil
}

func projectClaimRepairInferenceTaskEligible(task WorkspaceTaskRecord, projectID string) bool {
	if strings.TrimSpace(task.TaskID) == "" || taskSubmitTaskIsTerminal(task) {
		return false
	}
	if strings.TrimSpace(task.ProjectID) != strings.TrimSpace(projectID) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.TaskKind), "EXECUTION") {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.ProjectLane), "implementation") {
		return false
	}
	if task.RequiresProjectGate == nil || !*task.RequiresProjectGate {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(task.Status), "PENDING") {
		return false
	}
	switch taskClaimStatus(task) {
	case "", "RELEASED", "DECLINED":
		return true
	default:
		return false
	}
}

func packetProjectID(packet *AgentWorkPacket) string {
	if packet == nil {
		return ""
	}
	return strings.TrimSpace(packet.ProjectID)
}

func packetTaskKind(packet *AgentWorkPacket) string {
	if packet == nil {
		return ""
	}
	return strings.TrimSpace(packet.TaskKind)
}

func packetProjectLane(packet *AgentWorkPacket) string {
	if packet == nil {
		return ""
	}
	return strings.TrimSpace(packet.ProjectLane)
}

func packetRequiresProjectGate(packet *AgentWorkPacket) *bool {
	if packet == nil {
		return nil
	}
	return packet.RequiresProjectGate
}

func firstNonNilBool(values ...*bool) *bool {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func (r *Runtime) enqueueProjectClaimRepair(ctx context.Context, task WorkspaceTaskRecord, work *AgentWorkNextResult, reason string) error {
	if r == nil || r.client == nil {
		return nil
	}
	conflict, coordination, err := r.buildProjectClaimRepairConflict(ctx, task, work, reason)
	if err != nil {
		return err
	}
	if strings.TrimSpace(conflict.ProjectID) == "" || strings.TrimSpace(conflict.BlockedTaskID) == "" {
		return nil
	}
	if r.tryQueueSameAgentProjectClaimRepair(ctx, conflict) {
		return nil
	}
	if r.tryRequestConflictOwnerProjectClaimRepair(ctx, conflict, work) {
		return nil
	}
	if r.projectClaimRepairRecentlySent(conflict.RepairKey, time.Now().UTC()) {
		return nil
	}
	leadID := projectCoordinationStrategicLeadAgentID(coordination)
	if strings.TrimSpace(leadID) == "" {
		return r.postProjectClaimRepairUpdate(ctx, "issue", "Project claim repair requires an active strategic lead before the blocked lane can move.", conflict, "", true, "")
	}

	repairTaskID := strings.TrimSpace(conflict.RepairTaskID)
	taskCreated := false
	taskReused := false
	if existing, ok, lookupErr := r.findOpenProjectClaimRepairTask(ctx, conflict); lookupErr != nil {
		log.Printf("[project-claim-repair] open repair lookup failed for %s: %v", strings.TrimSpace(conflict.BlockedTaskID), lookupErr)
	} else if ok {
		repairTaskID = strings.TrimSpace(existing.TaskID)
		conflict.RepairTaskID = repairTaskID
		taskReused = true
	}
	requiresGate := false
	if !taskReused {
		submitResult, submitErr := r.client.SubmitTask(ctx, TaskSubmitInput{
			WorkspaceID:         r.cfg.WorkspaceID,
			TaskID:              repairTaskID,
			OwnerUserID:         firstNonEmpty(r.cfg.OwnerUserID, r.cfg.AgentID),
			Priority:            "high",
			Title:               "Repair project claim scope conflict",
			Description:         renderProjectClaimRepairTaskDescription(conflict),
			TaskKind:            "COORDINATION",
			TaskTemplate:        "generic",
			ProjectID:           conflict.ProjectID,
			ProjectLane:         "strategy",
			RequiresProjectGate: &requiresGate,
			Tags:                []string{"project-claim-repair", "strategic-lead", "blocker-unblock"},
			LinkedBy:            r.cfg.AgentID,
		})
		taskCreated = submitErr == nil
		if submitErr != nil {
			if !isDuplicateTaskSubmitError(submitErr) {
				return fmt.Errorf("create project claim repair task: %w", submitErr)
			}
			taskReused = true
		}
		if strings.TrimSpace(submitResult.TaskID) != "" {
			repairTaskID = strings.TrimSpace(submitResult.TaskID)
			conflict.RepairTaskID = repairTaskID
		}
	}

	requestID := ""
	warning := ""
	if strings.TrimSpace(leadID) == strings.TrimSpace(r.cfg.AgentID) {
		if err := r.setPendingWorkTrigger(ctx, "runtime_switch_task", repairTaskID, ""); err != nil {
			warning = "local strategic-lead repair wake failed: " + err.Error()
		}
	} else if taskCreated {
		created, requestErr := r.requestStrategicLeadProjectClaimRepair(ctx, leadID, repairTaskID, conflict)
		if requestErr != nil {
			warning = "repair task exists, but direct strategic lead wake request failed: " + requestErr.Error()
		} else {
			requestID = created.RequestID
		}
	} else if taskReused {
		warning = "repair task already open; strategic lead wake request was not re-sent to avoid duplicate repair nudges"
	}
	if err := r.recordProjectClaimRepairScratch(ctx, conflict.RepairKey, repairTaskID, requestID); err != nil {
		return err
	}
	summary := fmt.Sprintf("Queued project claim repair task %s for strategic lead %s after write-scope conflict blocked %s.", repairTaskID, leadID, conflict.BlockedTaskID)
	if taskReused {
		summary = fmt.Sprintf("Reused project claim repair task %s for strategic lead %s after write-scope conflict blocked %s.", repairTaskID, leadID, conflict.BlockedTaskID)
	}
	return r.postProjectClaimRepairUpdate(ctx, "coordination", summary, conflict, requestID, false, warning)
}

func (r *Runtime) buildProjectClaimRepairConflict(ctx context.Context, task WorkspaceTaskRecord, work *AgentWorkNextResult, reason string) (projectClaimRepairConflict, ProjectCoordinationRecord, error) {
	projectID := strings.TrimSpace(task.ProjectID)
	coordination, ok := projectCoordinationFromWork(work)
	if !ok {
		if projectID == "" {
			return projectClaimRepairConflict{}, ProjectCoordinationRecord{}, nil
		}
		var err error
		coordination, err = r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, projectID)
		if err != nil {
			return projectClaimRepairConflict{}, ProjectCoordinationRecord{}, fmt.Errorf("project claim repair coordination: %w", err)
		}
	}
	if projectID == "" {
		projectID = firstNonEmpty(strings.TrimSpace(coordination.Project.ProjectID), strings.TrimSpace(coordination.Profile.ProjectID))
	}
	blockedTaskID := strings.TrimSpace(task.TaskID)
	if blockedTaskID == "" && work != nil && work.Packet != nil {
		for _, candidate := range work.Packet.ContextHints.AnchorTaskIDs {
			if strings.TrimSpace(candidate) != "" {
				blockedTaskID = strings.TrimSpace(candidate)
				break
			}
		}
	}
	blockedWriteScopeJSON, _, _ := selectProjectClaimWriteScope(coordination, r.cfg.AgentID)
	if len(projectRoleAssignWriteScopePaths(blockedWriteScopeJSON)) == 0 {
		blockedWriteScopeJSON = firstNonEmpty(strings.TrimSpace(pointerValue(task.ClaimWriteScopeJSON)), r.inferProjectTaskWriteScopeJSON(ctx, task, work))
	}
	repo, _ := selectProjectClaimRepository(coordination)
	conflict := projectClaimRepairConflict{
		ProjectID:             projectID,
		BlockedTaskID:         blockedTaskID,
		BlockedAgentID:        strings.TrimSpace(r.cfg.AgentID),
		BlockedWriteScopeJSON: strings.TrimSpace(blockedWriteScopeJSON),
		RepoID:                strings.TrimSpace(repo.RepoID),
		Reason:                strings.TrimSpace(reason),
	}
	conflict = attachProjectClaimRepairConflictOwner(conflict, coordination, work)
	conflict = attachProjectClaimRepairPatchQueue(conflict, coordination)
	conflict.RepairKey = projectClaimRepairKey(r.cfg.WorkspaceID, conflict)
	conflict.RepairTaskID = "task-project-claim-repair-" + shortRefHash(conflict.RepairKey)
	return conflict, coordination, nil
}

func (r *Runtime) findOpenProjectClaimRepairTask(ctx context.Context, conflict projectClaimRepairConflict) (WorkspaceTaskRecord, bool, error) {
	if r == nil || r.client == nil {
		return WorkspaceTaskRecord{}, false, nil
	}
	tasks, err := r.client.ListTasks(ctx, r.cfg.WorkspaceID)
	if err != nil {
		return WorkspaceTaskRecord{}, false, err
	}
	projectID := strings.TrimSpace(conflict.ProjectID)
	blockedTaskID := strings.TrimSpace(conflict.BlockedTaskID)
	for _, task := range tasks {
		if !runtimeProjectClaimRepairTask(task) || taskSubmitTaskIsTerminal(task) {
			continue
		}
		if projectID != "" && !strings.EqualFold(strings.TrimSpace(task.ProjectID), projectID) {
			continue
		}
		if strings.TrimSpace(task.TaskID) == strings.TrimSpace(conflict.RepairTaskID) {
			return task, true, nil
		}
		if projectClaimRepairTaskMatchesRootConflict(task, conflict) {
			return task, true, nil
		}
		if blockedTaskID != "" && strings.TrimSpace(runtimeTaskTextFieldValue([]string{task.Title, task.Description}, "blocked_task_id")) == blockedTaskID {
			return task, true, nil
		}
	}
	return WorkspaceTaskRecord{}, false, nil
}

func projectClaimRepairTaskMatchesRootConflict(task WorkspaceTaskRecord, conflict projectClaimRepairConflict) bool {
	fields := []string{task.Title, task.Description}
	conflictBranchID := strings.TrimSpace(conflict.ConflictBranchID)
	conflictTaskID := strings.TrimSpace(conflict.ConflictTaskID)
	if conflictBranchID == "" && conflictTaskID == "" {
		return false
	}
	conflictRepoID := strings.TrimSpace(conflict.RepoID)
	if conflictRepoID != "" {
		existingRepoID := strings.TrimSpace(runtimeTaskTextFieldValue(fields, "repo_id"))
		if !strings.EqualFold(existingRepoID, conflictRepoID) {
			return false
		}
	}
	existingBranchID := strings.TrimSpace(runtimeTaskTextFieldValue(fields, "conflict_branch_id"))
	existingTaskID := strings.TrimSpace(runtimeTaskTextFieldValue(fields, "conflict_task_id"))
	if conflictBranchID != "" && existingBranchID != conflictBranchID {
		return false
	}
	if conflictBranchID == "" && conflictTaskID != "" && existingTaskID != conflictTaskID {
		return false
	}
	return true
}

func runtimeProjectClaimRepairTask(task WorkspaceTaskRecord) bool {
	if strings.HasPrefix(strings.TrimSpace(task.TaskID), "task-project-claim-repair-") {
		return true
	}
	if runtimeTaskLooksStrategicLeadRoleScopeRequest(task) {
		return true
	}
	for _, tag := range task.Tags {
		if strings.EqualFold(strings.TrimSpace(tag), "project-claim-repair") {
			return true
		}
	}
	return false
}

func attachProjectClaimRepairConflictOwner(conflict projectClaimRepairConflict, coordination ProjectCoordinationRecord, work *AgentWorkNextResult) projectClaimRepairConflict {
	blockedPaths := projectRoleAssignWriteScopePaths(conflict.BlockedWriteScopeJSON)
	if len(blockedPaths) == 0 {
		return conflict
	}
	if hinted := attachProjectClaimRepairHintedBranch(conflict, coordination, blockedPaths, projectClaimRepairConflictBranchHints(work)); strings.TrimSpace(hinted.ConflictBranchID) != "" {
		return hinted
	}
	if hinted := attachProjectClaimRepairAdmissionReasonConflict(conflict, coordination, blockedPaths); strings.TrimSpace(hinted.ConflictTaskID) != "" || strings.TrimSpace(hinted.ConflictBranchID) != "" {
		return hinted
	}
	for _, task := range coordination.Tasks {
		if strings.TrimSpace(task.TaskID) == conflict.BlockedTaskID {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(pointerValue(task.ClaimStatus)), "CLAIMED") {
			continue
		}
		if conflict.RepoID != "" && strings.TrimSpace(pointerValue(task.ClaimRepoID)) != conflict.RepoID {
			continue
		}
		scopeJSON := strings.TrimSpace(pointerValue(task.ClaimWriteScopeJSON))
		if !runtimeProjectWriteScopesOverlap(blockedPaths, projectRoleAssignWriteScopePaths(scopeJSON)) {
			continue
		}
		conflict.ConflictKind = "active_claim"
		conflict.ConflictOwnerAgentID = strings.TrimSpace(pointerValue(task.ClaimAgentID))
		conflict.ConflictTaskID = strings.TrimSpace(task.TaskID)
		conflict.ConflictBranchID = strings.TrimSpace(pointerValue(task.ClaimBranchID))
		conflict.ConflictWriteScopeJSON = scopeJSON
		return conflict
	}
	for _, branch := range coordination.Branches {
		if conflict.RepoID != "" && strings.TrimSpace(branch.RepoID) != conflict.RepoID {
			continue
		}
		if projectClaimRepairBranchIsSameAgentEmptyStale(conflict, branch) {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
		case "RESERVED", "ACTIVE", "BLOCKED", "READY_FOR_REVIEW":
		default:
			continue
		}
		if !runtimeProjectBranchOwnsWriteScope(branch) {
			continue
		}
		scopeJSON := strings.TrimSpace(branch.WriteScopeJSON)
		if !runtimeProjectWriteScopesOverlap(blockedPaths, projectRoleAssignWriteScopePaths(scopeJSON)) {
			continue
		}
		conflict.ConflictKind = "live_branch"
		conflict.ConflictOwnerAgentID = strings.TrimSpace(branch.AgentID)
		conflict.ConflictTaskID = strings.TrimSpace(branch.ActiveTaskID)
		conflict.ConflictBranchID = strings.TrimSpace(branch.BranchID)
		conflict.ConflictBranchStatus = strings.TrimSpace(branch.Status)
		conflict.ConflictBranchHeadSHA = strings.TrimSpace(branch.HeadSHA)
		conflict.ConflictReviewDocKey = strings.TrimSpace(branch.ReviewDocKey)
		conflict.ConflictWriteScopeJSON = scopeJSON
		return conflict
	}
	conflict.ConflictKind = "unknown_overlap"
	return conflict
}

type projectClaimRepairAdmissionConflictHint struct {
	Kind     string
	TaskID   string
	BranchID string
}

func attachProjectClaimRepairAdmissionReasonConflict(conflict projectClaimRepairConflict, coordination ProjectCoordinationRecord, blockedPaths []string) projectClaimRepairConflict {
	for _, hint := range projectClaimRepairAdmissionReasonConflictHints(conflict.Reason) {
		switch hint.Kind {
		case "active_claim":
			if attached := attachProjectClaimRepairHintedClaimTask(conflict, coordination, blockedPaths, hint.TaskID, hint.BranchID); strings.TrimSpace(attached.ConflictTaskID) != "" {
				return attached
			}
			if attached := attachProjectClaimRepairHintedBranchForTask(conflict, coordination, blockedPaths, hint.BranchID, hint.TaskID); strings.TrimSpace(attached.ConflictBranchID) != "" {
				return attached
			}
		case "live_branch":
			if attached := attachProjectClaimRepairHintedBranchForTask(conflict, coordination, blockedPaths, hint.BranchID, hint.TaskID); strings.TrimSpace(attached.ConflictBranchID) != "" {
				return attached
			}
			if attached := attachProjectClaimRepairHintedClaimTask(conflict, coordination, blockedPaths, hint.TaskID, hint.BranchID); strings.TrimSpace(attached.ConflictTaskID) != "" {
				return attached
			}
		}
	}
	return conflict
}

func projectClaimRepairAdmissionReasonConflictHints(reason string) []projectClaimRepairAdmissionConflictHint {
	reason = strings.TrimSpace(reason)
	lower := strings.ToLower(reason)
	if !strings.Contains(lower, "task claim project admission invalid") || !strings.Contains(lower, "write_scope_json overlaps") {
		return nil
	}
	kind := ""
	switch {
	case strings.Contains(lower, "overlaps active claim"):
		kind = "active_claim"
	case strings.Contains(lower, "overlaps live branch_id"):
		kind = "live_branch"
	default:
		return nil
	}
	var hints []projectClaimRepairAdmissionConflictHint
	seen := map[string]bool{}
	current := projectClaimRepairAdmissionConflictHint{Kind: kind}
	for _, field := range strings.Fields(reason) {
		switch {
		case strings.HasPrefix(field, "task_id="):
			current.TaskID = cleanProjectClaimRepairAdmissionHintValue(strings.TrimPrefix(field, "task_id="))
		case strings.HasPrefix(field, "active_task_id="):
			current.TaskID = cleanProjectClaimRepairAdmissionHintValue(strings.TrimPrefix(field, "active_task_id="))
		case strings.HasPrefix(field, "branch_id="):
			current.BranchID = cleanProjectClaimRepairAdmissionHintValue(strings.TrimPrefix(field, "branch_id="))
		}
		if current.TaskID == "" || current.BranchID == "" {
			continue
		}
		key := current.Kind + "\x00" + current.TaskID + "\x00" + current.BranchID
		if !seen[key] {
			hints = append(hints, current)
			seen[key] = true
		}
		current = projectClaimRepairAdmissionConflictHint{Kind: kind}
	}
	if current.TaskID != "" || current.BranchID != "" {
		key := current.Kind + "\x00" + current.TaskID + "\x00" + current.BranchID
		if !seen[key] {
			hints = append(hints, current)
		}
	}
	return hints
}

func cleanProjectClaimRepairAdmissionHintValue(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "`'\"")
	value = strings.TrimLeft(value, "([{")
	value = strings.TrimRight(value, ".,;:)]}")
	return strings.TrimSpace(value)
}

func attachProjectClaimRepairHintedClaimTask(conflict projectClaimRepairConflict, coordination ProjectCoordinationRecord, blockedPaths []string, taskID, branchID string) projectClaimRepairConflict {
	taskID = strings.TrimSpace(taskID)
	branchID = strings.TrimSpace(branchID)
	if taskID == "" {
		return conflict
	}
	for _, task := range coordination.Tasks {
		if strings.TrimSpace(task.TaskID) != taskID {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(pointerValue(task.ClaimStatus)), "CLAIMED") {
			continue
		}
		if conflict.RepoID != "" && strings.TrimSpace(pointerValue(task.ClaimRepoID)) != conflict.RepoID {
			continue
		}
		claimBranchID := strings.TrimSpace(pointerValue(task.ClaimBranchID))
		if branchID != "" && claimBranchID != "" && claimBranchID != branchID {
			continue
		}
		scopeJSON := strings.TrimSpace(pointerValue(task.ClaimWriteScopeJSON))
		if !runtimeProjectWriteScopesOverlap(blockedPaths, projectRoleAssignWriteScopePaths(scopeJSON)) {
			continue
		}
		conflict.ConflictKind = "active_claim"
		conflict.ConflictOwnerAgentID = strings.TrimSpace(pointerValue(task.ClaimAgentID))
		conflict.ConflictTaskID = strings.TrimSpace(task.TaskID)
		conflict.ConflictBranchID = firstNonEmpty(claimBranchID, branchID)
		conflict.ConflictWriteScopeJSON = scopeJSON
		return conflict
	}
	return conflict
}

func attachProjectClaimRepairHintedBranchForTask(conflict projectClaimRepairConflict, coordination ProjectCoordinationRecord, blockedPaths []string, branchID, taskID string) projectClaimRepairConflict {
	branchID = strings.TrimSpace(branchID)
	taskID = strings.TrimSpace(taskID)
	if branchID == "" {
		return conflict
	}
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.BranchID) != branchID {
			continue
		}
		if taskID != "" && strings.TrimSpace(branch.ActiveTaskID) != taskID {
			continue
		}
		if conflict.RepoID != "" && strings.TrimSpace(branch.RepoID) != conflict.RepoID {
			continue
		}
		if projectClaimRepairBranchIsSameAgentEmptyStale(conflict, branch) {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
		case "RESERVED", "ACTIVE", "BLOCKED", "READY_FOR_REVIEW":
		default:
			continue
		}
		if !runtimeProjectBranchOwnsWriteScope(branch) {
			continue
		}
		scopeJSON := strings.TrimSpace(branch.WriteScopeJSON)
		if !runtimeProjectWriteScopesOverlap(blockedPaths, projectRoleAssignWriteScopePaths(scopeJSON)) {
			continue
		}
		conflict.ConflictKind = "live_branch"
		conflict.ConflictOwnerAgentID = strings.TrimSpace(branch.AgentID)
		conflict.ConflictTaskID = strings.TrimSpace(branch.ActiveTaskID)
		conflict.ConflictBranchID = strings.TrimSpace(branch.BranchID)
		conflict.ConflictBranchStatus = strings.TrimSpace(branch.Status)
		conflict.ConflictBranchHeadSHA = strings.TrimSpace(branch.HeadSHA)
		conflict.ConflictReviewDocKey = strings.TrimSpace(branch.ReviewDocKey)
		conflict.ConflictWriteScopeJSON = scopeJSON
		return conflict
	}
	return conflict
}

func projectClaimRepairConflictBranchHints(work *AgentWorkNextResult) []string {
	if work == nil || work.Packet == nil {
		return nil
	}
	return append([]string(nil), work.Packet.ContextHints.AnchorBranchIDs...)
}

func attachProjectClaimRepairHintedBranch(conflict projectClaimRepairConflict, coordination ProjectCoordinationRecord, blockedPaths []string, branchIDs []string) projectClaimRepairConflict {
	for _, branchID := range branchIDs {
		branchID = strings.TrimSpace(branchID)
		if branchID == "" {
			continue
		}
		for _, branch := range coordination.Branches {
			if strings.TrimSpace(branch.BranchID) != branchID {
				continue
			}
			if conflict.RepoID != "" && strings.TrimSpace(branch.RepoID) != conflict.RepoID {
				continue
			}
			switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
			case "RESERVED", "ACTIVE", "BLOCKED", "READY_FOR_REVIEW":
			default:
				continue
			}
			if !runtimeProjectBranchOwnsWriteScope(branch) {
				continue
			}
			scopeJSON := strings.TrimSpace(branch.WriteScopeJSON)
			if !runtimeProjectWriteScopesOverlap(blockedPaths, projectRoleAssignWriteScopePaths(scopeJSON)) {
				continue
			}
			conflict.ConflictKind = "live_branch"
			conflict.ConflictOwnerAgentID = strings.TrimSpace(branch.AgentID)
			conflict.ConflictTaskID = strings.TrimSpace(branch.ActiveTaskID)
			conflict.ConflictBranchID = strings.TrimSpace(branch.BranchID)
			conflict.ConflictBranchStatus = strings.TrimSpace(branch.Status)
			conflict.ConflictBranchHeadSHA = strings.TrimSpace(branch.HeadSHA)
			conflict.ConflictReviewDocKey = strings.TrimSpace(branch.ReviewDocKey)
			conflict.ConflictWriteScopeJSON = scopeJSON
			return conflict
		}
	}
	return conflict
}

func attachProjectClaimRepairPatchQueue(conflict projectClaimRepairConflict, coordination ProjectCoordinationRecord) projectClaimRepairConflict {
	branchID := strings.TrimSpace(conflict.ConflictBranchID)
	if branchID == "" {
		return conflict
	}
	var best *ProjectPatchQueueItemRecord
	bestRank := -1
	for i := range coordination.PatchQueueItems {
		item := coordination.PatchQueueItems[i]
		if strings.TrimSpace(item.BranchID) != branchID {
			continue
		}
		rank := projectClaimRepairPatchQueueStateRank(item.State)
		if best == nil || rank > bestRank || (rank == bestRank && strings.TrimSpace(item.UpdatedAt) > strings.TrimSpace(best.UpdatedAt)) {
			itemCopy := item
			best = &itemCopy
			bestRank = rank
		}
	}
	if best == nil {
		return conflict
	}
	conflict.ConflictPatchItemID = strings.TrimSpace(best.ItemID)
	conflict.ConflictPatchState = strings.TrimSpace(best.State)
	conflict.ConflictPatchDecision = strings.TrimSpace(best.DecisionSummary)
	if strings.TrimSpace(conflict.ConflictBranchHeadSHA) == "" {
		conflict.ConflictBranchHeadSHA = strings.TrimSpace(best.HeadSHA)
	}
	if strings.TrimSpace(conflict.ConflictReviewDocKey) == "" {
		conflict.ConflictReviewDocKey = strings.TrimSpace(best.ReviewDocKey)
	}
	return conflict
}

func projectClaimRepairPatchQueueStateRank(state string) int {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "ACCEPTED":
		return 50
	case "CLAIMED", "PROPOSED":
		return 40
	case "BLOCKED":
		return 30
	case "REJECTED":
		return 20
	default:
		return 10
	}
}

func projectClaimRepairBranchIsSameAgentEmptyStale(conflict projectClaimRepairConflict, branch ProjectBranchRecord) bool {
	if strings.TrimSpace(branch.AgentID) != strings.TrimSpace(conflict.BlockedAgentID) {
		return false
	}
	return strings.TrimSpace(branch.ActiveTaskID) == "" && strings.TrimSpace(branch.ActiveClaimID) == ""
}

func runtimeProjectWriteScopesOverlap(left, right []string) bool {
	for _, l := range left {
		l = runtimeProjectNormalizeWriteScopePath(l)
		if l == "" {
			continue
		}
		for _, r := range right {
			r = runtimeProjectNormalizeWriteScopePath(r)
			if r == "" {
				continue
			}
			if runtimeProjectWriteScopePathOverlaps(l, r) {
				return true
			}
		}
	}
	return false
}

func runtimeProjectNormalizeWriteScopePath(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, `\`, `/`))
	value = strings.TrimSuffix(value, "/**")
	value = strings.TrimSuffix(value, "/*")
	value = strings.Trim(value, "/")
	if value == "." {
		return ""
	}
	return strings.ToLower(value)
}

func runtimeProjectWriteScopePathOverlaps(a, b string) bool {
	a = runtimeProjectNormalizeWriteScopePath(a)
	b = runtimeProjectNormalizeWriteScopePath(b)
	if a == "" || b == "" {
		return true
	}
	if a == "*" || a == "**" || b == "*" || b == "**" {
		return true
	}
	aHasWildcard := strings.Contains(a, "*")
	bHasWildcard := strings.Contains(b, "*")
	if aHasWildcard && bHasWildcard {
		return runtimeProjectWriteScopeStaticPrefixesOverlap(runtimeProjectWriteScopeWildcardStaticPrefix(a), runtimeProjectWriteScopeWildcardStaticPrefix(b))
	}
	if aHasWildcard {
		return runtimeProjectWriteScopeWildcardMayOverlapPath(a, b)
	}
	if bHasWildcard {
		return runtimeProjectWriteScopeWildcardMayOverlapPath(b, a)
	}
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func runtimeProjectWriteScopeWildcardMayOverlapPath(pattern, pathValue string) bool {
	pattern = runtimeProjectNormalizeWriteScopePath(pattern)
	pathValue = runtimeProjectNormalizeWriteScopePath(pathValue)
	if pattern == "" || pathValue == "" {
		return true
	}
	if pattern == "*" || pattern == "**" || pathValue == "*" || pathValue == "**" {
		return true
	}
	staticPrefix := runtimeProjectWriteScopeWildcardStaticPrefix(pattern)
	if staticPrefix == "" {
		return true
	}
	return runtimeProjectWriteScopeStaticPrefixesOverlap(staticPrefix, pathValue)
}

func runtimeProjectWriteScopeWildcardStaticPrefix(value string) string {
	value = runtimeProjectNormalizeWriteScopePath(value)
	idx := strings.Index(value, "*")
	if idx < 0 {
		return value
	}
	prefix := value[:idx]
	prefix = strings.TrimSuffix(prefix, "/")
	return strings.Trim(prefix, "/")
}

func runtimeProjectWriteScopeStaticPrefixesOverlap(a, b string) bool {
	a = strings.Trim(strings.TrimSpace(a), "/")
	b = strings.Trim(strings.TrimSpace(b), "/")
	if a == "" || b == "" {
		return true
	}
	if a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/") {
		return true
	}
	if runtimeProjectSameWriteScopeParentDir(a, b) && (strings.HasPrefix(a, b) || strings.HasPrefix(b, a)) {
		return true
	}
	return false
}

func runtimeProjectSameWriteScopeParentDir(a, b string) bool {
	return runtimeProjectWriteScopeParentDir(a) == runtimeProjectWriteScopeParentDir(b)
}

func runtimeProjectWriteScopeParentDir(value string) string {
	value = runtimeProjectNormalizeWriteScopePath(value)
	if value == "" {
		return ""
	}
	if idx := strings.LastIndex(value, "/"); idx >= 0 {
		return value[:idx]
	}
	return ""
}

func projectClaimRepairKey(workspaceID string, conflict projectClaimRepairConflict) string {
	if projectClaimRepairHasRootConflictIdentity(conflict) {
		identityKind := projectClaimRepairIdentityKind(conflict)
		conflictTaskID := strings.TrimSpace(conflict.ConflictTaskID)
		if strings.TrimSpace(conflict.ConflictBranchID) != "" {
			conflictTaskID = ""
		}
		scopeKey := normalizedProjectClaimRepairScopeKey(conflict.ConflictWriteScopeJSON)
		if scopeKey == "" {
			scopeKey = normalizedProjectClaimRepairScopeKey(conflict.BlockedWriteScopeJSON)
		}
		return strings.Join([]string{
			"project_claim_repair_root_v3",
			strings.TrimSpace(workspaceID),
			strings.TrimSpace(conflict.ProjectID),
			strings.TrimSpace(conflict.RepoID),
			identityKind,
			strings.TrimSpace(conflict.ConflictOwnerAgentID),
			strings.TrimSpace(conflict.ConflictBranchID),
			conflictTaskID,
			scopeKey,
		}, "\x00")
	}
	if blockedTaskID := strings.TrimSpace(conflict.BlockedTaskID); blockedTaskID != "" {
		scopeKey := normalizedProjectClaimRepairScopeKey(conflict.BlockedWriteScopeJSON)
		if scopeKey == "" {
			scopeKey = normalizedProjectClaimRepairScopeKey(conflict.ConflictWriteScopeJSON)
		}
		return strings.Join([]string{
			"project_claim_repair_task_v1",
			strings.TrimSpace(workspaceID),
			strings.TrimSpace(conflict.ProjectID),
			strings.TrimSpace(conflict.RepoID),
			blockedTaskID,
			scopeKey,
		}, "\x00")
	}
	identityKind := projectClaimRepairIdentityKind(conflict)
	conflictTaskID := strings.TrimSpace(conflict.ConflictTaskID)
	blockedTaskID := ""
	blockedAgentID := ""
	if strings.TrimSpace(conflict.ConflictBranchID) != "" {
		conflictTaskID = ""
	} else if conflictTaskID == "" {
		blockedTaskID = strings.TrimSpace(conflict.BlockedTaskID)
		if identityKind != "unknown_overlap" {
			blockedAgentID = strings.TrimSpace(conflict.BlockedAgentID)
		}
	}
	scopeKey := normalizedProjectClaimRepairScopeKey(conflict.ConflictWriteScopeJSON)
	if scopeKey == "" {
		scopeKey = normalizedProjectClaimRepairScopeKey(conflict.BlockedWriteScopeJSON)
	}
	parts := []string{
		"project_claim_repair_root_v2",
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(conflict.ProjectID),
		strings.TrimSpace(conflict.RepoID),
		identityKind,
		strings.TrimSpace(conflict.ConflictOwnerAgentID),
		strings.TrimSpace(conflict.ConflictBranchID),
		conflictTaskID,
		blockedTaskID,
		blockedAgentID,
		scopeKey,
	}
	return strings.Join(parts, "\x00")
}

func projectClaimRepairHasRootConflictIdentity(conflict projectClaimRepairConflict) bool {
	if strings.TrimSpace(conflict.ConflictBranchID) != "" || strings.TrimSpace(conflict.ConflictTaskID) != "" {
		return true
	}
	return strings.TrimSpace(conflict.ConflictOwnerAgentID) != "" &&
		normalizedProjectClaimRepairScopeKey(conflict.ConflictWriteScopeJSON) != ""
}

func projectClaimRepairIdentityKind(conflict projectClaimRepairConflict) string {
	if strings.TrimSpace(conflict.ConflictBranchID) != "" {
		return "live_branch"
	}
	if strings.TrimSpace(conflict.ConflictTaskID) != "" {
		return "active_claim"
	}
	return firstNonEmpty(strings.TrimSpace(conflict.ConflictKind), "unknown_overlap")
}

func normalizedProjectClaimRepairScopeKey(raw string) string {
	paths := projectRoleAssignWriteScopePaths(raw)
	sort.Strings(paths)
	return strings.Join(paths, ",")
}

func (r *Runtime) projectClaimRepairRecentlySent(key string, now time.Time) bool {
	key = strings.TrimSpace(key)
	if key == "" || r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if strings.TrimSpace(r.scratch.ProjectClaimRepairKey) != key {
		return false
	}
	at, ok := parseRFC3339Nano(strings.TrimSpace(r.scratch.ProjectClaimRepairAt))
	return ok && at != nil && now.Sub(*at) < projectClaimRepairRequestFreshness
}

func (r *Runtime) recordProjectClaimRepairScratch(ctx context.Context, key, taskID, requestID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.ProjectClaimRepairKey = strings.TrimSpace(key)
		state.ProjectClaimRepairTaskID = strings.TrimSpace(taskID)
		state.ProjectClaimRepairRequestID = strings.TrimSpace(requestID)
		state.ProjectClaimRepairAt = now
		state.LastWakeTrigger = "project_claim_admission"
		state.LastWakeReason = "project_claim_repair"
		state.LastWakeSummary = "Queued project claim repair task " + strings.TrimSpace(taskID)
		state.LastWakeTaskID = strings.TrimSpace(taskID)
		state.LastWakeSessionID = ""
		state.LastWakeAt = now
		state.LastSummary = state.LastWakeSummary
	})
}

func (r *Runtime) postMalformedProjectClaimScopeBusyUpdate(ctx context.Context, task WorkspaceTaskRecord, work *AgentWorkNextResult, candidateCount int, reason string) error {
	if r == nil || r.client == nil {
		return nil
	}
	projectID := strings.TrimSpace(task.ProjectID)
	if projectID == "" && work != nil {
		projectID = firstNonEmpty(strings.TrimSpace(work.ProjectID), packetProjectID(work.Packet))
	}
	key := projectMalformedClaimScopeBusyKey(r.cfg.WorkspaceID, r.cfg.AgentID, projectID, reason)
	now := time.Now().UTC()
	if r.projectClaimRepairRecentlySent(key, now) {
		return nil
	}
	summary := fmt.Sprintf("project_claim_scope_busy packet missing anchor_task_ids; cannot create unambiguous repair task for project %s.", firstNonEmpty(projectID, "unknown"))
	if candidateCount == 1 {
		summary = fmt.Sprintf("project_claim_scope_busy packet missing anchor_task_ids; one fallback candidate was unavailable for project %s.", firstNonEmpty(projectID, "unknown"))
	} else if candidateCount > 1 {
		summary = fmt.Sprintf("project_claim_scope_busy packet missing anchor_task_ids; %d candidate implementation tasks found, refusing to guess.", candidateCount)
	}
	payload := map[string]any{
		"project_claim_repair":                      true,
		"malformed_work_next_packet_missing_anchor": true,
		"repair_key":              key,
		"project_id":              projectID,
		"blocked_agent_id":        strings.TrimSpace(r.cfg.AgentID),
		"candidate_count":         candidateCount,
		"observed_reason":         strings.TrimSpace(reason),
		"do_not_wait_passively":   true,
		"recommended_next_action": "fix agent.work.next project_claim_scope_busy packet to include context_hints.anchor_task_ids, or create a strategic repair task with explicit blocked_task_id",
		"generic_idle_reflection_suppressed_for_packet": true,
	}
	if work != nil && work.Packet != nil {
		payload["packet_work_type"] = strings.TrimSpace(work.Packet.WorkType)
		payload["packet_coordination_state"] = strings.TrimSpace(work.Packet.CoordinationState)
		payload["packet_preferred_transition"] = strings.TrimSpace(work.Packet.PreferredTransition)
		payload["packet_why_now"] = strings.TrimSpace(work.Packet.WhyNow)
	}
	raw, _ := json.Marshal(payload)
	if err := r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID:   r.cfg.WorkspaceID,
		AgentID:       r.cfg.AgentID,
		UpdateType:    "issue",
		Summary:       summary,
		PayloadJSON:   string(raw),
		RequiresHuman: false,
	}); err != nil {
		return err
	}
	return r.recordProjectClaimRepairNoticeScratch(ctx, key, summary)
}

func projectMalformedClaimScopeBusyKey(workspaceID, agentID, projectID, reason string) string {
	parts := []string{
		"project_claim_repair_malformed_packet_v1",
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(agentID),
		strings.TrimSpace(projectID),
		strings.TrimSpace(reason),
	}
	return strings.Join(parts, "\x00")
}

func (r *Runtime) recordProjectClaimRepairNoticeScratch(ctx context.Context, key, summary string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.ProjectClaimRepairKey = strings.TrimSpace(key)
		state.ProjectClaimRepairTaskID = ""
		state.ProjectClaimRepairRequestID = ""
		state.ProjectClaimRepairAt = now
		state.LastWakeTrigger = "project_claim_admission"
		state.LastWakeReason = "project_claim_repair_malformed_packet"
		state.LastWakeSummary = strings.TrimSpace(summary)
		state.LastWakeTaskID = ""
		state.LastWakeSessionID = ""
		state.LastWakeAt = now
		state.LastSummary = strings.TrimSpace(summary)
	})
}

func (r *Runtime) tryQueueSameAgentProjectClaimRepair(ctx context.Context, conflict projectClaimRepairConflict) bool {
	if strings.TrimSpace(conflict.ConflictOwnerAgentID) != strings.TrimSpace(r.cfg.AgentID) || strings.TrimSpace(conflict.ConflictTaskID) == "" {
		return false
	}
	if !r.localProjectClaimRepairTargetVisible(ctx, conflict.ConflictTaskID) {
		blockedTaskID := strings.TrimSpace(conflict.BlockedTaskID)
		if blockedTaskID == "" || !r.localProjectClaimRepairTargetVisible(ctx, blockedTaskID) {
			return false
		}
		summary := fmt.Sprintf("Project claim conflict for %s is owned by this same agent, but prior owner task %s is no longer active; resuming the blocked product lane instead of spinning repair coordination.", conflict.BlockedTaskID, conflict.ConflictTaskID)
		if err := r.postProjectClaimRepairUpdate(ctx, "coordination", summary, conflict, "", false, "terminal same-agent owner redirected to blocked product task"); err != nil {
			log.Printf("[project-claim-repair] same-agent blocked-task update failed: %v", err)
		}
		if err := r.setPendingWorkTrigger(ctx, "request_resume", blockedTaskID, ""); err != nil {
			log.Printf("[project-claim-repair] same-agent blocked-task resume failed: %v", err)
			return false
		}
		return true
	}
	summary := fmt.Sprintf("Project claim conflict for %s is owned by this same agent on %s; resuming local owner lane before asking strategic lead to replan.", conflict.BlockedTaskID, conflict.ConflictTaskID)
	if err := r.postProjectClaimRepairUpdate(ctx, "coordination", summary, conflict, "", false, ""); err != nil {
		log.Printf("[project-claim-repair] same-agent update failed: %v", err)
	}
	if err := r.setPendingWorkTrigger(ctx, "request_resume", conflict.ConflictTaskID, ""); err != nil {
		log.Printf("[project-claim-repair] same-agent resume failed: %v", err)
		return false
	}
	return true
}

func (r *Runtime) tryRequestConflictOwnerProjectClaimRepair(ctx context.Context, conflict projectClaimRepairConflict, work *AgentWorkNextResult) bool {
	ownerID := strings.TrimSpace(conflict.ConflictOwnerAgentID)
	activeTaskID := strings.TrimSpace(conflict.ConflictTaskID)
	if ownerID == "" || ownerID == strings.TrimSpace(r.cfg.AgentID) || activeTaskID == "" {
		return false
	}
	if !projectClaimRepairWorkRequestsConflictOwner(work, ownerID) {
		return false
	}
	key := strings.TrimSpace(conflict.RepairKey) + "\x00owner_resume\x00" + ownerID + "\x00" + activeTaskID
	now := time.Now().UTC()
	if r.projectClaimRepairRecentlySent(key, now) {
		return true
	}
	created, err := r.requestConflictOwnerProjectClaimResume(ctx, ownerID, conflict)
	if err != nil {
		log.Printf("[project-claim-repair] conflict-owner resume request failed: %v", err)
		return false
	}
	summary := fmt.Sprintf("Requested active project lane owner %s to publish, release, or abandon %s before strategic reassignment for blocked task %s.", ownerID, activeTaskID, conflict.BlockedTaskID)
	if err := r.recordProjectClaimOwnerResumeScratch(ctx, key, activeTaskID, created.RequestID, summary); err != nil {
		log.Printf("[project-claim-repair] owner resume scratch failed: %v", err)
	}
	if err := r.postProjectClaimRepairUpdate(ctx, "coordination", summary, conflict, created.RequestID, false, "known active owner was asked to resume before strategic repair task creation"); err != nil {
		log.Printf("[project-claim-repair] owner resume update failed: %v", err)
	}
	return true
}

func projectClaimRepairWorkRequestsConflictOwner(work *AgentWorkNextResult, ownerID string) bool {
	ownerID = strings.TrimSpace(ownerID)
	if work == nil || work.Packet == nil || ownerID == "" {
		return false
	}
	packet := work.Packet
	if strings.EqualFold(strings.TrimSpace(packet.PreferredTransition), "delegate_to_branch_owner") {
		return true
	}
	if strings.TrimSpace(packet.HandoffToAgentID) == ownerID {
		return true
	}
	if packet.Handoff != nil && strings.TrimSpace(packet.Handoff.ToAgentID) == ownerID {
		return true
	}
	return packet.Gate != nil && strings.TrimSpace(packet.Gate.NeededFrom) == ownerID
}

func (r *Runtime) requestConflictOwnerProjectClaimResume(ctx context.Context, ownerID string, conflict projectClaimRepairConflict) (AgentRequestCreateResult, error) {
	payload := map[string]any{
		"schema":                    projectClaimOwnerResumeRequestSchema,
		"request_kind":              projectClaimOwnerResumeRequestKind,
		"evidence_kind":             "project_claim_owner_resume_request",
		"task_id":                   strings.TrimSpace(conflict.ConflictTaskID),
		"active_task_id":            strings.TrimSpace(conflict.ConflictTaskID),
		"blocked_task_id":           strings.TrimSpace(conflict.BlockedTaskID),
		"project_id":                strings.TrimSpace(conflict.ProjectID),
		"repo_id":                   strings.TrimSpace(conflict.RepoID),
		"conflict_branch_id":        strings.TrimSpace(conflict.ConflictBranchID),
		"conflict_write_scope_json": strings.TrimSpace(conflict.ConflictWriteScopeJSON),
		"prompt": fmt.Sprintf("Your active project implementation lane %s owns write scope that is blocking %s. Resume that owned task and either commit/publish review-ready evidence, release/block with current evidence, or abandon the stale branch/claim. Do not create a sibling provenance task for this same lane.",
			strings.TrimSpace(conflict.ConflictTaskID),
			strings.TrimSpace(conflict.BlockedTaskID)),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return AgentRequestCreateResult{}, err
	}
	return r.client.RequestAgent(ctx, AgentRequestInput{
		WorkspaceID: r.cfg.WorkspaceID,
		FromAgentID: r.cfg.AgentID,
		ToAgentID:   strings.TrimSpace(ownerID),
		Method:      "model.ask",
		PayloadJSON: string(raw),
		TimeoutSec:  60,
	})
}

func (r *Runtime) recordProjectClaimOwnerResumeScratch(ctx context.Context, key, taskID, requestID, summary string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return r.updateScratch(ctx, func(state *RuntimeScratchState) {
		state.ProjectClaimRepairKey = strings.TrimSpace(key)
		state.ProjectClaimRepairTaskID = ""
		state.ProjectClaimRepairRequestID = strings.TrimSpace(requestID)
		state.ProjectClaimRepairAt = now
		state.LastWakeTrigger = "project_claim_admission"
		state.LastWakeReason = "project_claim_owner_resume"
		state.LastWakeSummary = strings.TrimSpace(summary)
		state.LastWakeTaskID = strings.TrimSpace(taskID)
		state.LastWakeSessionID = ""
		state.LastWakeAt = now
		state.LastSummary = strings.TrimSpace(summary)
	})
}

func (r *Runtime) localProjectClaimRepairTargetVisible(ctx context.Context, taskID string) bool {
	if r == nil || r.client == nil || strings.TrimSpace(taskID) == "" {
		return false
	}
	bundle, err := r.client.HydrateTask(ctx, TaskHydrationInput{
		WorkspaceID:      r.cfg.WorkspaceID,
		TaskID:           strings.TrimSpace(taskID),
		UpdatesLimit:     1,
		ArtifactLimit:    1,
		RelatedTaskLimit: 1,
	})
	if err != nil || bundle.WorkspaceTask == nil || strings.TrimSpace(bundle.WorkspaceTask.TaskID) == "" {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(bundle.WorkspaceTask.Status)) {
	case "DONE", "COMPLETED", "RESOLVED", "CLOSED", "CANCELLED", "CANCELED", "FAILED":
		return false
	default:
		return true
	}
}

func (r *Runtime) requestStrategicLeadProjectClaimRepair(ctx context.Context, leadID, repairTaskID string, conflict projectClaimRepairConflict) (AgentRequestCreateResult, error) {
	payload := map[string]any{
		"request_kind":   "delegate_task",
		"task_id":        repairTaskID,
		"repair_task_id": repairTaskID,
		"prompt":         fmt.Sprintf("Claim project claim repair task %s. It records a write-scope conflict blocking %s; diagnose fresh project/claim/branch evidence, then update the role/task matrix or sequence dependencies with durable project docs/tools. Completion requires executable follow-through: the blocked task must become claimable, a product/patch follow-up task must be materialized, or the gate must remain as a typed blocker; project_role_assign/denial alone is not a completion receipt.", repairTaskID, conflict.BlockedTaskID),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return AgentRequestCreateResult{}, err
	}
	return r.client.RequestAgent(ctx, AgentRequestInput{
		WorkspaceID: r.cfg.WorkspaceID,
		FromAgentID: r.cfg.AgentID,
		ToAgentID:   strings.TrimSpace(leadID),
		Method:      "model.ask",
		PayloadJSON: string(raw),
		TimeoutSec:  60,
	})
}

func renderProjectClaimRepairTaskDescription(conflict projectClaimRepairConflict) string {
	var b strings.Builder
	b.WriteString("A project implementation lane is blocked by an overlapping write scope. Claim this task as the active strategic lead and repair the project coordination state instead of waiting.\n\n")
	b.WriteString("Conflict:\n")
	b.WriteString("- repair_identity_scope: root_conflict\n")
	b.WriteString("- repair_convergence_id: project-claim-repair-" + shortRefHash(conflict.RepairKey) + "\n")
	b.WriteString("- blocked_task_id: " + conflict.BlockedTaskID + "\n")
	b.WriteString("- blocked_agent_id: " + conflict.BlockedAgentID + "\n")
	b.WriteString("- blocked_write_scope_json: " + firstNonEmpty(conflict.BlockedWriteScopeJSON, "{}") + "\n")
	b.WriteString("- repo_id: " + conflict.RepoID + "\n")
	b.WriteString("- conflict_kind: " + conflict.ConflictKind + "\n")
	b.WriteString("- conflict_owner_agent_id: " + conflict.ConflictOwnerAgentID + "\n")
	b.WriteString("- conflict_task_id: " + conflict.ConflictTaskID + "\n")
	b.WriteString("- conflict_branch_id: " + conflict.ConflictBranchID + "\n")
	b.WriteString("- conflict_branch_status: " + conflict.ConflictBranchStatus + "\n")
	b.WriteString("- conflict_branch_head_sha: " + conflict.ConflictBranchHeadSHA + "\n")
	b.WriteString("- conflict_review_doc_key: " + conflict.ConflictReviewDocKey + "\n")
	b.WriteString("- conflict_patch_item_id: " + conflict.ConflictPatchItemID + "\n")
	b.WriteString("- conflict_patch_state: " + conflict.ConflictPatchState + "\n")
	b.WriteString("- conflict_patch_decision: " + conflict.ConflictPatchDecision + "\n")
	b.WriteString("- conflict_write_scope_json: " + firstNonEmpty(conflict.ConflictWriteScopeJSON, "{}") + "\n")
	if strings.TrimSpace(conflict.Reason) != "" {
		b.WriteString("- observed_reason: " + oneLine(conflict.Reason) + "\n")
	}
	b.WriteString("\nAdvisory repair options:\n")
	b.WriteString("- split_scope: narrow one role/task so scaffold/config and UI/code lanes no longer overlap.\n")
	b.WriteString("- rebind_role: use project_role_assign to make the durable role matrix match the intended task ownership.\n")
	b.WriteString("- sequence_dependency: make the blocked lane depend on the current owner lane if the overlap is an intentional upstream scaffold.\n")
	b.WriteString("- integrate_ready_branch: if the conflicting branch is READY_FOR_REVIEW, review/integrate it before reopening downstream work.\n")
	b.WriteString("- accepted_upstream_first: if conflict_patch_state is ACCEPTED, sequence or integrate that upstream branch before assigning overlapping downstream implementation.\n")
	b.WriteString("- release_stale_claim_after_freshness_check: only release after checking active session/branch evidence is stale.\n")
	b.WriteString("- strategic_override_after_evidence_check: only broaden/override scope after documenting why parallel overlap is intentional and how integration will stay coherent.\n")
	b.WriteString("\nCompletion contract:\n")
	b.WriteString("- Do not complete this repair from project_role_assign or denial evidence alone.\n")
	b.WriteString("- Complete only after materializing executable follow-through: the blocked_task_id is claimable by an eligible free agent, or a concrete product/patch follow-up task exists, or this gate remains BLOCKED with a typed durable blocker explaining why no eligible claimant can exist.\n")
	b.WriteString("- If a role/scope mutation still leaves the blocked product task unclaimable, keep this repair open or blocked; do not create another coordination split as a substitute.\n")
	return strings.TrimSpace(b.String())
}

func (r *Runtime) postProjectClaimRepairUpdate(ctx context.Context, updateType, summary string, conflict projectClaimRepairConflict, requestID string, requiresHuman bool, warning string) error {
	payload := map[string]any{
		"schema":                    projectClaimScopeBusyEvidenceSchema,
		"evidence_kind":             projectClaimScopeBusyEvidenceKind,
		"project_claim_repair":      true,
		"repair_key":                conflict.RepairKey,
		"repair_identity_scope":     "root_conflict",
		"repair_convergence_id":     "project-claim-repair-" + shortRefHash(conflict.RepairKey),
		"coverage_state":            "repair_episode_open",
		"repair_task_id":            conflict.RepairTaskID,
		"request_id":                strings.TrimSpace(requestID),
		"liberation_candidate_kind": projectClaimRepairLiberationCandidateKind,
		"server_verified_predicates": []string{
			"agent_work_next.project_claim_scope_busy",
			"project_coordination.blocked_task",
			"project_coordination.conflict_owner_or_branch",
			"write_scope_overlap",
		},
		"project_id":                conflict.ProjectID,
		"blocked_task_id":           conflict.BlockedTaskID,
		"blocked_agent_id":          conflict.BlockedAgentID,
		"blocked_write_scope_json":  conflict.BlockedWriteScopeJSON,
		"repo_id":                   conflict.RepoID,
		"conflict_kind":             conflict.ConflictKind,
		"conflict_owner_agent_id":   conflict.ConflictOwnerAgentID,
		"conflict_task_id":          conflict.ConflictTaskID,
		"conflict_branch_id":        conflict.ConflictBranchID,
		"conflict_branch_status":    conflict.ConflictBranchStatus,
		"conflict_branch_head_sha":  conflict.ConflictBranchHeadSHA,
		"conflict_review_doc_key":   conflict.ConflictReviewDocKey,
		"conflict_patch_item_id":    conflict.ConflictPatchItemID,
		"conflict_patch_state":      conflict.ConflictPatchState,
		"conflict_patch_decision":   conflict.ConflictPatchDecision,
		"conflict_write_scope_json": conflict.ConflictWriteScopeJSON,
		"advisory_only_repair":      true,
		"recommended_next_owner":    "strategic_lead",
		"recommended_next_action":   "claim repair task and update project role/task matrix",
		"do_not_wait_passively":     true,
	}
	if strings.TrimSpace(requestID) != "" {
		payload["release_request_kind"] = projectClaimOwnerResumeRequestKind
		payload["release_request_id"] = strings.TrimSpace(requestID)
		payload["release_request_schema"] = projectClaimOwnerResumeRequestSchema
	}
	if strings.TrimSpace(warning) != "" {
		payload["warning"] = strings.TrimSpace(warning)
	}
	raw, _ := json.Marshal(payload)
	return r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID:   r.cfg.WorkspaceID,
		AgentID:       r.cfg.AgentID,
		UpdateType:    firstNonEmpty(strings.TrimSpace(updateType), "coordination"),
		Summary:       summary,
		PayloadJSON:   string(raw),
		RequiresHuman: requiresHuman,
	})
}
