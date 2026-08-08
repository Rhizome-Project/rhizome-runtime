package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
	"unicode"
)

const defaultProjectRoleRequestWriteScope = `{}`

type projectClaimAdmission struct {
	ProjectRoleID  string
	ProjectID      string
	RepoID         string
	CheckoutID     string
	BranchID       string
	BranchName     string
	BaseBranch     string
	WriteScopeJSON string
}

type projectClaimRoleMissingError struct {
	ProjectID      string
	TaskID         string
	AgentID        string
	LeadAgentID    string
	RoleType       string
	WriteScopeJSON string
}

type projectClaimGateClosedError struct {
	ProjectID string
	TaskID    string
	Summary   string
}

func (e *projectClaimGateClosedError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("project claim admission blocked by project implementation gate for task %s on project %s: %s",
		firstNonEmpty(strings.TrimSpace(e.TaskID), "unknown"),
		firstNonEmpty(strings.TrimSpace(e.ProjectID), "unknown"),
		firstNonEmpty(strings.TrimSpace(e.Summary), "project gate closed"))
}

func (e *projectClaimRoleMissingError) Error() string {
	if e == nil {
		return ""
	}
	agentID := firstNonEmpty(strings.TrimSpace(e.AgentID), "unknown")
	projectID := firstNonEmpty(strings.TrimSpace(e.ProjectID), "unknown")
	return fmt.Sprintf("project claim admission requires an active project role with write_scope_json for agent %s on project %s", agentID, projectID)
}

func (r *Runtime) claimTaskWithAdmission(ctx context.Context, task WorkspaceTaskRecord, summary string, work *AgentWorkNextResult, captured ...*projectClaimAdmission) error {
	input := TaskClaimInput{
		WorkspaceID:      r.cfg.WorkspaceID,
		AgentID:          r.cfg.AgentID,
		TaskID:           task.TaskID,
		CoordinationMode: coordinationModeLabel(r.cfg),
		Summary:          summary,
	}
	if work != nil && work.Packet != nil && work.Packet.Frontier != nil {
		frontier := work.Packet.Frontier
		if strings.TrimSpace(frontier.SelectedTaskID) == strings.TrimSpace(task.TaskID) {
			input.SelectedFromFrontier = true
			input.FrontierGenerationID = strings.TrimSpace(frontier.GenerationID)
			input.SelfFitSummary = strings.TrimSpace(frontier.SelfFitSummary)
			if input.SelfFitSummary != "" {
				input.Summary = firstNonEmpty(summary, input.SelfFitSummary)
			}
		}
	}
	admission, err := r.ensureProjectClaimAdmission(ctx, task, work)
	if err != nil {
		return err
	}
	admission.apply(&input)
	if err := r.client.ClaimTask(ctx, input); err != nil {
		if cleanupErr := r.cleanupFailedProjectClaimAdmission(ctx, task, admission); cleanupErr != nil {
			return fmt.Errorf("%w; failed to abandon provisional project branch %s: %v", err, strings.TrimSpace(admission.BranchID), cleanupErr)
		}
		return err
	}
	if admission.hasBranchBinding() {
		if err := r.postProjectClaimAdmittedUpdate(ctx, task, admission); err != nil && ctx.Err() == nil {
			log.Printf("[project-claim-admission] claim admitted update failed for task %s: %v", strings.TrimSpace(task.TaskID), err)
		}
	}
	r.clearProjectClaimHoldForTask(task)
	if len(captured) > 0 && captured[0] != nil {
		*captured[0] = admission
	}
	return nil
}

func (a projectClaimAdmission) apply(input *TaskClaimInput) {
	if input == nil {
		return
	}
	input.ProjectRoleID = strings.TrimSpace(a.ProjectRoleID)
	input.RepoID = strings.TrimSpace(a.RepoID)
	input.CheckoutID = strings.TrimSpace(a.CheckoutID)
	input.BranchID = strings.TrimSpace(a.BranchID)
	input.WriteScopeJSON = strings.TrimSpace(a.WriteScopeJSON)
}

func (a projectClaimAdmission) applyTask(task *WorkspaceTaskRecord) {
	if task == nil {
		return
	}
	if strings.TrimSpace(a.ProjectRoleID) != "" {
		task.ClaimProjectRoleID = stringPtr(strings.TrimSpace(a.ProjectRoleID))
	}
	if strings.TrimSpace(a.RepoID) != "" {
		task.ClaimRepoID = stringPtr(strings.TrimSpace(a.RepoID))
	}
	if strings.TrimSpace(a.CheckoutID) != "" {
		task.ClaimCheckoutID = stringPtr(strings.TrimSpace(a.CheckoutID))
	}
	if strings.TrimSpace(a.BranchID) != "" {
		task.ClaimBranchID = stringPtr(strings.TrimSpace(a.BranchID))
	}
	if strings.TrimSpace(a.WriteScopeJSON) != "" {
		task.ClaimWriteScopeJSON = stringPtr(strings.TrimSpace(a.WriteScopeJSON))
	}
}

func (a projectClaimAdmission) hasBranchBinding() bool {
	return strings.TrimSpace(a.ProjectID) != "" &&
		strings.TrimSpace(a.RepoID) != "" &&
		strings.TrimSpace(a.CheckoutID) != "" &&
		strings.TrimSpace(a.BranchID) != "" &&
		strings.TrimSpace(a.WriteScopeJSON) != ""
}

func (r *Runtime) postProjectClaimAdmittedUpdate(ctx context.Context, task WorkspaceTaskRecord, admission projectClaimAdmission) error {
	if r == nil || r.client == nil || strings.TrimSpace(task.TaskID) == "" {
		return nil
	}
	payload := map[string]any{
		"delegation_state":  "claim_admitted",
		"branch_bound":      strings.TrimSpace(admission.BranchID) != "",
		"task_id":           strings.TrimSpace(task.TaskID),
		"project_id":        strings.TrimSpace(firstNonEmpty(admission.ProjectID, task.ProjectID)),
		"agent_id":          strings.TrimSpace(r.cfg.AgentID),
		"project_role_id":   strings.TrimSpace(admission.ProjectRoleID),
		"repo_id":           strings.TrimSpace(admission.RepoID),
		"checkout_id":       strings.TrimSpace(admission.CheckoutID),
		"branch_id":         strings.TrimSpace(admission.BranchID),
		"branch_name":       strings.TrimSpace(admission.BranchName),
		"write_scope_json":  strings.TrimSpace(admission.WriteScopeJSON),
		"completion_state":  "not_completed",
		"evidence_boundary": "claim_admitted_and_branch_bound_only",
	}
	raw, _ := json.Marshal(payload)
	return r.client.PostUpdate(ctx, UpdatePostInput{
		WorkspaceID: r.cfg.WorkspaceID,
		AgentID:     r.cfg.AgentID,
		UpdateType:  "coordination",
		Summary:     fmt.Sprintf("Project claim admitted for task %s; branch_bound=%t. Product evidence still required.", strings.TrimSpace(task.TaskID), strings.TrimSpace(admission.BranchID) != ""),
		PayloadJSON: string(raw),
	})
}

func (r *Runtime) ensureProjectClaimAdmission(ctx context.Context, task WorkspaceTaskRecord, work *AgentWorkNextResult) (projectClaimAdmission, error) {
	if r == nil || r.client == nil || !runtimeTaskShouldPrepareProjectClaimAdmission(task) {
		return projectClaimAdmission{}, nil
	}
	coordination, ok := projectCoordinationFromWork(work)
	if !ok {
		var err error
		coordination, err = r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, task.ProjectID)
		if err != nil {
			return projectClaimAdmission{}, fmt.Errorf("project claim admission coordination: %w", err)
		}
	}
	attachProjectCoordinationToWork(work, coordination)
	if summary, blocked := runtimeProjectImplementationGateClosed(task, coordination); blocked {
		return projectClaimAdmission{}, &projectClaimGateClosedError{
			ProjectID: strings.TrimSpace(task.ProjectID),
			TaskID:    strings.TrimSpace(task.TaskID),
			Summary:   summary,
		}
	}
	if !runtimeProjectClaimAdmissionRequired(task, coordination) {
		return projectClaimAdmission{}, nil
	}
	trustFirst := runtimeTrustFirst(r.cfg)
	requiredRoleType := runtimeProjectClaimRequiredRoleType(task.ProjectLane)
	if requiredRoleType != "" && requiredRoleType != "IMPLEMENTER" {
		if role, ok := selectProjectClaimRole(coordination, r.cfg.AgentID, requiredRoleType); ok {
			return projectClaimAdmission{
				ProjectRoleID: strings.TrimSpace(role.RoleID),
				ProjectID:     task.ProjectID,
			}, nil
		}
		if trustFirst {
			return projectClaimAdmission{
				ProjectID: task.ProjectID,
			}, nil
		}
		return projectClaimAdmission{}, &projectClaimRoleMissingError{
			ProjectID:      strings.TrimSpace(task.ProjectID),
			TaskID:         strings.TrimSpace(task.TaskID),
			AgentID:        strings.TrimSpace(r.cfg.AgentID),
			LeadAgentID:    projectCoordinationStrategicLeadAgentID(coordination),
			RoleType:       requiredRoleType,
			WriteScopeJSON: defaultProjectRoleRequestWriteScope,
		}
	}
	repo, ok := selectProjectClaimRepository(coordination)
	if !ok {
		if trustFirst {
			return projectClaimAdmission{}, nil
		}
		return projectClaimAdmission{}, fmt.Errorf("project claim admission requires READY repository for project %s", strings.TrimSpace(task.ProjectID))
	}
	taskWriteScopeJSON := ""
	if trustFirst {
		taskWriteScopeJSON = r.inferProjectTaskWriteScopeJSON(ctx, task, work)
	}
	role, roleOK := selectProjectClaimRole(coordination, r.cfg.AgentID, firstNonEmpty(requiredRoleType, "IMPLEMENTER"))
	writeScopeJSON := ""
	roleID := ""
	roleWriteScopeJSON := ""
	activeClaimScopeOK := false
	if roleOK {
		writeScopeJSON = strings.TrimSpace(role.WriteScopeJSON)
		roleID = strings.TrimSpace(role.RoleID)
		roleWriteScopeJSON = writeScopeJSON
	}
	ok = roleOK
	if trustFirst {
		if activeClaimWriteScopeJSON, activeClaimRoleID, activeOK := selectProjectActiveClaimWriteScope(coordination, r.cfg.AgentID, task.TaskID); activeOK {
			writeScopeJSON = activeClaimWriteScopeJSON
			roleID = firstNonEmpty(activeClaimRoleID, roleID)
			ok = true
			activeClaimScopeOK = true
		}
	}
	if trustFirst && taskWriteScopeJSON != "" {
		if activeClaimScopeOK {
			// Keep the lane's already-applied operational boundary authoritative over
			// original task hints when a resumable owner claim exists.
		} else if roleOK && projectClaimAdmissionShouldPreferRoleScopeForTrustFirstTask(task, taskWriteScopeJSON, role) {
			writeScopeJSON = roleWriteScopeJSON
			roleID = strings.TrimSpace(role.RoleID)
		} else {
			writeScopeJSON = taskWriteScopeJSON
			roleID = ""
		}
		ok = true
	}
	if !trustFirst && (!ok || projectWriteScopeJSONIsBroad(writeScopeJSON)) {
		taskWriteScopeJSON = r.inferProjectTaskWriteScopeJSON(ctx, task, work)
	}
	if !ok {
		if trustFirst && taskWriteScopeJSON != "" {
			writeScopeJSON = taskWriteScopeJSON
			roleID = ""
			ok = true
		}
		if !ok {
			return projectClaimAdmission{}, &projectClaimRoleMissingError{
				ProjectID:      strings.TrimSpace(task.ProjectID),
				TaskID:         strings.TrimSpace(task.TaskID),
				AgentID:        strings.TrimSpace(r.cfg.AgentID),
				LeadAgentID:    projectCoordinationStrategicLeadAgentID(coordination),
				RoleType:       firstNonEmpty(requiredRoleType, "IMPLEMENTER"),
				WriteScopeJSON: firstNonEmpty(taskWriteScopeJSON, defaultProjectRoleRequestWriteScope),
			}
		}
	}
	if taskWriteScopeJSON != "" && projectWriteScopeJSONIsBroad(writeScopeJSON) && !activeClaimScopeOK {
		writeScopeJSON = taskWriteScopeJSON
	}
	branchName := projectClaimBranchName(r.cfg.AgentID, task.ProjectID, task.TaskID)
	baseBranch := firstNonEmpty(repo.DefaultBranch, coordination.Profile.RepoDefaultBranch, "main")
	var hintedBranch ProjectBranchRecord
	var sourceRevisionBranch ProjectBranchRecord
	var repairBranch ProjectBranchRecord
	if branch, ok := selectProjectClaimExistingBranch(coordination.Branches, task, repo.RepoID, r.cfg.AgentID); ok {
		if projectClaimAdmissionNeedsFreshSuccessorBranch(task, branch) {
			sourceRevisionBranch = branch
			branchName = projectClaimSuccessorBranchName(r.cfg.AgentID, task.ProjectID, task.TaskID, branch)
		} else if projectClaimAdmissionShouldForkRevisionBranch(coordination.PatchQueueItems, branch, task) {
			sourceRevisionBranch = branch
			if priorBranchName := strings.TrimSpace(branch.BranchName); priorBranchName != "" {
				baseBranch = priorBranchName
			}
		} else {
			hintedBranch = branch
			branchName = strings.TrimSpace(branch.BranchName)
			if sourceBranch, ok := selectProjectClaimRevisionSourceBranchExcluding(coordination.PatchQueueItems, coordination.Branches, task, repo.RepoID, hintedBranch.BranchID); ok {
				sourceRevisionBranch = sourceBranch
				if priorBranchName := strings.TrimSpace(sourceBranch.BranchName); priorBranchName != "" {
					baseBranch = priorBranchName
				}
			}
		}
	} else if branch, ok := selectProjectClaimRevisionSourceBranch(coordination.PatchQueueItems, coordination.Branches, task, repo.RepoID); ok {
		sourceRevisionBranch = branch
		if priorBranchName := strings.TrimSpace(branch.BranchName); priorBranchName != "" {
			baseBranch = priorBranchName
		}
	}
	if strings.TrimSpace(hintedBranch.BranchID) == "" && strings.TrimSpace(sourceRevisionBranch.BranchID) == "" {
		if branch, ok := selectProjectClaimReadyPredecessorByName(coordination.Branches, task, repo.RepoID, r.cfg.AgentID, branchName); ok {
			sourceRevisionBranch = branch
			branchName = projectClaimSuccessorBranchName(r.cfg.AgentID, task.ProjectID, task.TaskID, branch)
		}
	}
	if trustFirst && !activeClaimScopeOK && strings.TrimSpace(sourceRevisionBranch.AgentID) == strings.TrimSpace(r.cfg.AgentID) {
		if sourceScopeJSON := strings.TrimSpace(sourceRevisionBranch.WriteScopeJSON); sourceScopeJSON != "" && json.Valid([]byte(sourceScopeJSON)) && !projectRoleAssignWriteScopeEmpty(sourceScopeJSON) {
			if taskWriteScopeJSON == "" || projectClaimAdmissionScopeOverrideAnchored(taskWriteScopeJSON, sourceScopeJSON) {
				writeScopeJSON = sourceScopeJSON
				ok = true
			}
		}
	}
	if trustFirst && !activeClaimScopeOK {
		if branchScopeJSON := strings.TrimSpace(hintedBranch.WriteScopeJSON); branchScopeJSON != "" && json.Valid([]byte(branchScopeJSON)) && !projectRoleAssignWriteScopeEmpty(branchScopeJSON) {
			if taskWriteScopeJSON == "" || projectClaimAdmissionScopeOverrideAnchored(taskWriteScopeJSON, branchScopeJSON) {
				writeScopeJSON = branchScopeJSON
			}
		}
	}
	if branch, ok := selectProjectClaimRepairableBranchByName(coordination.Branches, task, repo.RepoID, r.cfg.AgentID, branchName); ok {
		repairBranch = branch
	}
	if err := preflightProjectClaimWriteScopeAvailable(task, repo, coordination, writeScopeJSON, hintedBranch, sourceRevisionBranch, repairBranch); err != nil {
		return projectClaimAdmission{}, err
	}
	checkout, err := r.ensureProjectClaimCheckout(ctx, task, repo, branchName, baseBranch, coordination, hintedBranch)
	if err != nil {
		return projectClaimAdmission{}, err
	}
	branch, err := r.ensureProjectClaimBranch(ctx, task, repo, checkout, branchName, baseBranch, writeScopeJSON, coordination, hintedBranch)
	if err != nil {
		return projectClaimAdmission{}, err
	}
	return projectClaimAdmission{
		ProjectRoleID:  roleID,
		ProjectID:      task.ProjectID,
		RepoID:         repo.RepoID,
		CheckoutID:     checkout.CheckoutID,
		BranchID:       branch.BranchID,
		BranchName:     branchName,
		BaseBranch:     baseBranch,
		WriteScopeJSON: writeScopeJSON,
	}, nil
}

func preflightProjectClaimWriteScopeAvailable(task WorkspaceTaskRecord, repo ProjectRepositoryRecord, coordination ProjectCoordinationRecord, writeScopeJSON string, hintedBranch ProjectBranchRecord, sourceRevisionBranch ProjectBranchRecord, repairBranch ProjectBranchRecord) error {
	taskID := strings.TrimSpace(task.TaskID)
	repoID := strings.TrimSpace(repo.RepoID)
	claimPaths := projectRoleAssignWriteScopePaths(writeScopeJSON)
	if taskID == "" || repoID == "" || len(claimPaths) == 0 {
		return nil
	}
	hintedBranchID := strings.TrimSpace(hintedBranch.BranchID)
	sourceRevisionBranchID := strings.TrimSpace(sourceRevisionBranch.BranchID)
	sourceRevisionOwnerID := strings.TrimSpace(sourceRevisionBranch.AgentID)
	repairBranchID := strings.TrimSpace(repairBranch.BranchID)
	for _, other := range coordination.Tasks {
		otherTaskID := strings.TrimSpace(other.TaskID)
		if otherTaskID == "" || otherTaskID == taskID {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(pointerValue(other.ClaimStatus)), "CLAIMED") {
			continue
		}
		if strings.TrimSpace(pointerValue(other.ClaimRepoID)) != repoID {
			continue
		}
		otherBranchID := strings.TrimSpace(pointerValue(other.ClaimBranchID))
		if hintedBranchID != "" && otherBranchID == hintedBranchID {
			continue
		}
		if sourceRevisionBranchID != "" && otherBranchID == sourceRevisionBranchID && sourceRevisionOwnerID != "" {
			claimAgentID := strings.TrimSpace(pointerValue(other.ClaimAgentID))
			if claimAgentID == sourceRevisionOwnerID {
				continue
			}
		}
		otherWriteScopeJSON := projectClaimAdmissionEffectiveClaimWriteScopeJSON(other, coordination.Branches, repoID)
		if runtimeProjectWriteScopesOverlap(claimPaths, projectRoleAssignWriteScopePaths(otherWriteScopeJSON)) {
			return fmt.Errorf("task claim project admission invalid: write_scope_json overlaps active claim task_id=%s branch_id=%s", otherTaskID, otherBranchID)
		}
	}
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.RepoID) != repoID {
			continue
		}
		branchID := strings.TrimSpace(branch.BranchID)
		if hintedBranchID != "" && branchID == hintedBranchID {
			continue
		}
		if sourceRevisionBranchID != "" && branchID == sourceRevisionBranchID && sourceRevisionOwnerID != "" && strings.TrimSpace(branch.AgentID) == sourceRevisionOwnerID {
			continue
		}
		if repairBranchID != "" && branchID == repairBranchID {
			continue
		}
		if projectClaimAdmissionAllowsOlderPatchQueueRevisionPredecessorBranch(task, repoID, coordination.PatchQueueItems, branch, sourceRevisionBranch) {
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
		if runtimeReservedNonImplementationBranchDoesNotOwnWriteScope(branch, coordination.Tasks) {
			continue
		}
		if runtimeProjectBranchWriteScopeReleasedByIntegratedTerminalRefs(branch, coordination.Tasks, coordination.PatchQueueItems) {
			continue
		}
		if runtimeProjectBranchWriteScopeReleasedByTerminalPatchQueueDecision(branch, coordination.PatchQueueItems) {
			continue
		}
		if strings.TrimSpace(branch.ActiveTaskID) == taskID {
			continue
		}
		if runtimeProjectWriteScopesOverlap(claimPaths, projectRoleAssignWriteScopePaths(branch.WriteScopeJSON)) {
			return fmt.Errorf("task claim project admission invalid: write_scope_json overlaps live branch_id=%s active_task_id=%s", branchID, strings.TrimSpace(branch.ActiveTaskID))
		}
	}
	return nil
}

func runtimeReservedNonImplementationBranchDoesNotOwnWriteScope(branch ProjectBranchRecord, tasks []WorkspaceTaskRecord) bool {
	if !strings.EqualFold(strings.TrimSpace(branch.Status), "RESERVED") {
		return false
	}
	if strings.TrimSpace(branch.HeadSHA) != "" || strings.TrimSpace(branch.ReviewDocKey) != "" {
		return false
	}
	activeTaskID := strings.TrimSpace(branch.ActiveTaskID)
	if activeTaskID == "" {
		return false
	}
	for _, task := range tasks {
		if strings.TrimSpace(task.TaskID) != activeTaskID {
			continue
		}
		return !runtimeProjectTaskRequiresImplementationGate(task)
	}
	return false
}

func runtimeProjectBranchWriteScopeReleasedByTerminalPatchQueueDecision(branch ProjectBranchRecord, items []ProjectPatchQueueItemRecord) bool {
	branchID := strings.TrimSpace(branch.BranchID)
	headSHA := strings.TrimSpace(branch.HeadSHA)
	if branchID == "" || headSHA == "" {
		return false
	}
	if strings.TrimSpace(branch.ActiveTaskID) != "" || strings.TrimSpace(branch.ActiveClaimID) != "" {
		return false
	}
	return runtimeProjectPatchQueueItemsReleaseBranchWriteScope(branchID, headSHA, items)
}

func runtimeProjectBranchWriteScopeReleasedByIntegratedTerminalRefs(branch ProjectBranchRecord, tasks []WorkspaceTaskRecord, items []ProjectPatchQueueItemRecord) bool {
	activeTaskID := strings.TrimSpace(branch.ActiveTaskID)
	activeClaimID := strings.TrimSpace(branch.ActiveClaimID)
	if activeTaskID == "" && activeClaimID == "" {
		return false
	}
	if activeTaskID != "" && !runtimeProjectTaskStatusTerminal(runtimeProjectTaskStatus(tasks, activeTaskID)) {
		return false
	}
	if activeClaimID != "" && !runtimeProjectTaskClaimStatusTerminal(runtimeProjectTaskClaimStatus(tasks, activeClaimID)) {
		return false
	}
	branchID := strings.TrimSpace(branch.BranchID)
	headSHA := strings.TrimSpace(branch.HeadSHA)
	if branchID == "" || headSHA == "" {
		return false
	}
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) == branchID &&
			strings.TrimSpace(item.HeadSHA) == headSHA &&
			strings.EqualFold(strings.TrimSpace(item.State), "INTEGRATED") {
			return true
		}
	}
	return false
}

func runtimeProjectTaskStatus(tasks []WorkspaceTaskRecord, taskID string) string {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return ""
	}
	for _, task := range tasks {
		if strings.TrimSpace(task.TaskID) == taskID {
			return strings.TrimSpace(task.Status)
		}
	}
	return ""
}

func runtimeProjectTaskClaimStatus(tasks []WorkspaceTaskRecord, taskID string) string {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return ""
	}
	for _, task := range tasks {
		if strings.TrimSpace(task.TaskID) == taskID {
			return strings.TrimSpace(pointerValue(task.ClaimStatus))
		}
	}
	return ""
}

func runtimeProjectTaskStatusTerminal(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "RESOLVED", "FAILED", "CANCELLED", "CANCELED":
		return true
	default:
		return false
	}
}

func runtimeProjectTaskClaimStatusTerminal(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "COMPLETED", "FAILED", "CANCELLED", "CANCELED":
		return true
	default:
		return false
	}
}

func runtimeProjectPatchQueueItemsReleaseBranchWriteScope(branchID, headSHA string, items []ProjectPatchQueueItemRecord) bool {
	branchID = strings.TrimSpace(branchID)
	headSHA = strings.TrimSpace(headSHA)
	if branchID == "" || headSHA == "" {
		return false
	}
	hasTerminalDecisionForHead := false
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) != branchID {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case "PROPOSED", "CLAIMED":
			return false
		case "ACCEPTED":
			if strings.TrimSpace(item.HeadSHA) == headSHA {
				return false
			}
		case "BLOCKED", "REJECTED", "CANCELED", "CANCELLED", "INTEGRATED":
			if strings.TrimSpace(item.HeadSHA) == headSHA {
				hasTerminalDecisionForHead = true
			}
		}
	}
	return hasTerminalDecisionForHead
}

func projectClaimAdmissionAllowsOlderPatchQueueRevisionPredecessorBranch(task WorkspaceTaskRecord, repoID string, items []ProjectPatchQueueItemRecord, branch ProjectBranchRecord, sourceBranch ProjectBranchRecord) bool {
	repoID = strings.TrimSpace(repoID)
	branchID := strings.TrimSpace(branch.BranchID)
	headSHA := strings.TrimSpace(branch.HeadSHA)
	sourceBranchID := strings.TrimSpace(sourceBranch.BranchID)
	sourceOwnerID := strings.TrimSpace(sourceBranch.AgentID)
	if repoID == "" || branchID == "" || headSHA == "" || sourceBranchID == "" || sourceOwnerID == "" {
		return false
	}
	if branchID == sourceBranchID || strings.TrimSpace(branch.AgentID) != sourceOwnerID {
		return false
	}
	if strings.TrimSpace(branch.ActiveTaskID) != "" || strings.TrimSpace(branch.ActiveClaimID) != "" {
		return false
	}
	if !projectClaimAdmissionLooksLikePatchQueueRevisionTask(task) {
		return false
	}

	var source ProjectPatchQueueItemRecord
	sourceOK := false
	var predecessor ProjectPatchQueueItemRecord
	predecessorOK := false
	for _, item := range items {
		if strings.TrimSpace(item.RepoID) != repoID {
			continue
		}
		if strings.TrimSpace(item.BranchID) == branchID && strings.TrimSpace(item.HeadSHA) == headSHA && projectClaimAdmissionRevisionItemState(item.State) {
			predecessor = item
			predecessorOK = true
			continue
		}
		if projectClaimAdmissionRevisionItemMatchesTask(task, item, repoID) {
			if !sourceOK || projectClaimAdmissionPatchQueueItemDecidedAfter(item, source) {
				source = item
				sourceOK = true
			}
		}
	}
	if !sourceOK || !predecessorOK || strings.TrimSpace(source.BranchID) == branchID {
		return false
	}
	return projectClaimAdmissionPatchQueueItemDecidedAfter(source, predecessor)
}

func projectClaimAdmissionRevisionItemState(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "BLOCKED", "REJECTED":
		return true
	default:
		return false
	}
}

func projectClaimAdmissionPatchQueueItemDecidedAfter(candidate, stale ProjectPatchQueueItemRecord) bool {
	candidateAt := strings.TrimSpace(firstNonEmpty(candidate.DecidedAt, candidate.UpdatedAt, candidate.CreatedAt))
	staleAt := strings.TrimSpace(firstNonEmpty(stale.DecidedAt, stale.UpdatedAt, stale.CreatedAt))
	if candidateAt == "" || staleAt == "" {
		return false
	}
	candidateTime, candidateErr := time.Parse(time.RFC3339Nano, candidateAt)
	staleTime, staleErr := time.Parse(time.RFC3339Nano, staleAt)
	if candidateErr == nil && staleErr == nil {
		return candidateTime.After(staleTime)
	}
	return candidateAt > staleAt
}

func runtimeProjectBranchOwnsWriteScope(branch ProjectBranchRecord) bool {
	switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
	case "RESERVED", "ACTIVE", "BLOCKED":
		return strings.TrimSpace(branch.ActiveTaskID) != "" || strings.TrimSpace(branch.ActiveClaimID) != ""
	case "READY_FOR_REVIEW":
		return strings.TrimSpace(branch.HeadSHA) != "" && strings.TrimSpace(branch.ReviewDocKey) != ""
	default:
		return false
	}
}

func projectClaimAdmissionEffectiveClaimWriteScopeJSON(other WorkspaceTaskRecord, branches []ProjectBranchRecord, repoID string) string {
	rawScopeJSON := strings.TrimSpace(pointerValue(other.ClaimWriteScopeJSON))
	branchID := strings.TrimSpace(pointerValue(other.ClaimBranchID))
	repoID = strings.TrimSpace(repoID)
	if branchID == "" || repoID == "" {
		return rawScopeJSON
	}
	for _, branch := range branches {
		if !projectClaimAdmissionBranchMatchesActiveClaim(branch, other, repoID, branchID) {
			continue
		}
		scopeJSON := strings.TrimSpace(branch.WriteScopeJSON)
		if len(projectRoleAssignWriteScopePaths(scopeJSON)) == 0 {
			return rawScopeJSON
		}
		if !runtimeProjectWriteScopePathsSameSet(projectRoleAssignWriteScopePaths(scopeJSON), projectRoleAssignWriteScopePaths(rawScopeJSON)) {
			return rawScopeJSON
		}
		return scopeJSON
	}
	return rawScopeJSON
}

func runtimeProjectWriteScopePathsSameSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]int, len(left))
	for _, path := range left {
		path = normalizeRuntimeProjectWriteScopePath(path)
		if path == "" {
			continue
		}
		seen[path]++
	}
	for _, path := range right {
		path = normalizeRuntimeProjectWriteScopePath(path)
		if path == "" {
			continue
		}
		seen[path]--
	}
	for _, count := range seen {
		if count != 0 {
			return false
		}
	}
	return true
}

func normalizeRuntimeProjectWriteScopePath(path string) string {
	normalized := normalizeProjectWriteScopePaths([]string{path})
	if len(normalized) == 0 {
		return ""
	}
	return normalized[0]
}

func projectClaimAdmissionBranchMatchesActiveClaim(branch ProjectBranchRecord, other WorkspaceTaskRecord, repoID, branchID string) bool {
	if strings.TrimSpace(branch.BranchID) != branchID || strings.TrimSpace(branch.RepoID) != repoID {
		return false
	}
	if !runtimeProjectBranchOwnsWriteScope(branch) {
		return false
	}
	claimAgentID := strings.TrimSpace(pointerValue(other.ClaimAgentID))
	if branchAgentID := strings.TrimSpace(branch.AgentID); claimAgentID != "" && branchAgentID != "" && branchAgentID != claimAgentID {
		return false
	}
	taskID := strings.TrimSpace(other.TaskID)
	activeTaskID := strings.TrimSpace(branch.ActiveTaskID)
	activeClaimID := strings.TrimSpace(branch.ActiveClaimID)
	return activeTaskID == taskID || activeClaimID == taskID
}

func selectProjectClaimRepairableBranchByName(branches []ProjectBranchRecord, task WorkspaceTaskRecord, repoID, agentID, branchName string) (ProjectBranchRecord, bool) {
	taskID := strings.TrimSpace(task.TaskID)
	repoID = strings.TrimSpace(repoID)
	agentID = strings.TrimSpace(agentID)
	branchName = strings.TrimSpace(branchName)
	legacyBranchName := projectClaimLegacyBranchName(agentID, task.ProjectID, task.TaskID)
	if taskID == "" || repoID == "" || agentID == "" || branchName == "" {
		return ProjectBranchRecord{}, false
	}
	for _, branch := range branches {
		if strings.TrimSpace(branch.RepoID) != repoID || strings.TrimSpace(branch.AgentID) != agentID {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
		case "RESERVED", "ACTIVE", "BLOCKED":
		default:
			continue
		}
		storedBranchName := strings.TrimSpace(branch.BranchName)
		if storedBranchName != branchName && storedBranchName != legacyBranchName {
			continue
		}
		if activeTaskID := strings.TrimSpace(branch.ActiveTaskID); activeTaskID != "" && activeTaskID != taskID {
			continue
		}
		if activeClaimID := strings.TrimSpace(branch.ActiveClaimID); activeClaimID != "" && activeClaimID != taskID {
			continue
		}
		if strings.TrimSpace(branch.BranchID) == "" {
			continue
		}
		return branch, true
	}
	return ProjectBranchRecord{}, false
}

func (r *Runtime) cleanupFailedProjectClaimAdmission(ctx context.Context, task WorkspaceTaskRecord, admission projectClaimAdmission) error {
	if r == nil || r.client == nil || (strings.TrimSpace(admission.BranchID) == "" && strings.TrimSpace(admission.CheckoutID) == "") {
		return nil
	}
	projectID := firstNonEmpty(strings.TrimSpace(admission.ProjectID), strings.TrimSpace(task.ProjectID))
	if projectID == "" {
		return nil
	}
	coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, projectID)
	if err != nil {
		return err
	}
	var firstErr error
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.BranchID) != strings.TrimSpace(admission.BranchID) {
			continue
		}
		if strings.TrimSpace(branch.ActiveTaskID) != "" || strings.TrimSpace(branch.ActiveClaimID) != "" {
			break
		}
		if !strings.EqualFold(strings.TrimSpace(branch.Status), "RESERVED") {
			break
		}
		if _, err := r.client.RegisterProjectBranch(ctx, ProjectBranchRegisterInput{
			WorkspaceID:    r.cfg.WorkspaceID,
			ProjectID:      projectID,
			ActorID:        r.cfg.AgentID,
			BranchID:       branch.BranchID,
			RepoID:         firstNonEmpty(strings.TrimSpace(admission.RepoID), strings.TrimSpace(branch.RepoID)),
			CheckoutID:     firstNonEmpty(strings.TrimSpace(admission.CheckoutID), strings.TrimSpace(branch.CheckoutID)),
			AgentID:        r.cfg.AgentID,
			BranchName:     firstNonEmpty(strings.TrimSpace(admission.BranchName), strings.TrimSpace(branch.BranchName)),
			BranchKind:     firstNonEmpty(strings.TrimSpace(branch.BranchKind), "feature"),
			BaseBranch:     firstNonEmpty(strings.TrimSpace(admission.BaseBranch), strings.TrimSpace(branch.BaseBranch)),
			WriteScopeJSON: firstNonEmpty(strings.TrimSpace(admission.WriteScopeJSON), strings.TrimSpace(branch.WriteScopeJSON)),
			Status:         "ABANDONED",
		}); err != nil && firstErr == nil {
			firstErr = err
		}
		break
	}
	for _, checkout := range coordination.Checkouts {
		if strings.TrimSpace(checkout.CheckoutID) != strings.TrimSpace(admission.CheckoutID) {
			continue
		}
		if strings.TrimSpace(checkout.ActiveTaskID) != "" || strings.TrimSpace(checkout.ActiveClaimID) != "" {
			break
		}
		if !strings.EqualFold(strings.TrimSpace(checkout.Status), "ACTIVE") {
			break
		}
		localPath := strings.TrimSpace(checkout.LocalPath)
		if localPath == "" {
			break
		}
		if _, err := r.client.RegisterProjectCheckout(ctx, ProjectCheckoutRegisterInput{
			WorkspaceID:  r.cfg.WorkspaceID,
			ProjectID:    projectID,
			ActorID:      r.cfg.AgentID,
			CheckoutID:   checkout.CheckoutID,
			RepoID:       firstNonEmpty(strings.TrimSpace(admission.RepoID), strings.TrimSpace(checkout.RepoID)),
			MachineID:    firstNonEmpty(strings.TrimSpace(checkout.MachineID), runtimeMachineID()),
			MachineLabel: firstNonEmpty(strings.TrimSpace(checkout.MachineLabel), runtimeMachineID()),
			OwnerUserID:  firstNonEmpty(strings.TrimSpace(checkout.OwnerUserID), r.cfg.OwnerUserID),
			AgentID:      r.cfg.AgentID,
			LocalPath:    localPath,
			CheckoutKind: firstNonEmpty(strings.TrimSpace(checkout.CheckoutKind), "clone"),
			BranchName:   firstNonEmpty(strings.TrimSpace(admission.BranchName), strings.TrimSpace(checkout.BranchName)),
			BaseBranch:   firstNonEmpty(strings.TrimSpace(admission.BaseBranch), strings.TrimSpace(checkout.BaseBranch)),
			HeadSHA:      strings.TrimSpace(checkout.HeadSHA),
			BaseSHA:      strings.TrimSpace(checkout.BaseSHA),
			DirtyState:   firstNonEmpty(strings.TrimSpace(checkout.DirtyState), "unknown"),
			Status:       "ABANDONED",
		}); err != nil && firstErr == nil {
			firstErr = err
		}
		break
	}
	return firstErr
}

func (r *Runtime) ensureProjectClaimCheckout(ctx context.Context, task WorkspaceTaskRecord, repo ProjectRepositoryRecord, branchName, baseBranch string, coordination ProjectCoordinationRecord, hintedBranch ProjectBranchRecord) (ProjectCheckoutRecord, error) {
	machineID := runtimeMachineID()
	localPath := projectCheckoutMaterializeDefaultPath(r.cfg.Workdir, task.ProjectID, repo)
	if err := ensureProjectCheckoutMaterializePath(r.cfg.Workdir, localPath); err != nil {
		return ProjectCheckoutRecord{}, fmt.Errorf("project claim admission checkout path: %w", err)
	}
	if checkoutID := strings.TrimSpace(hintedBranch.CheckoutID); checkoutID != "" {
		matchedHint := false
		for _, checkout := range coordination.Checkouts {
			if strings.TrimSpace(checkout.CheckoutID) != checkoutID {
				continue
			}
			matchedHint = true
			if strings.TrimSpace(checkout.RepoID) != strings.TrimSpace(repo.RepoID) ||
				strings.TrimSpace(checkout.AgentID) != strings.TrimSpace(r.cfg.AgentID) {
				return ProjectCheckoutRecord{}, fmt.Errorf("project claim admission referenced branch %s checkout_id %s belongs to repo %s agent %s, not repo %s agent %s",
					strings.TrimSpace(hintedBranch.BranchID), checkoutID, strings.TrimSpace(checkout.RepoID), strings.TrimSpace(checkout.AgentID), strings.TrimSpace(repo.RepoID), strings.TrimSpace(r.cfg.AgentID))
			}
			if !strings.EqualFold(strings.TrimSpace(checkout.Status), "ACTIVE") {
				if recovered, ok, err := r.recoverProjectClaimCheckout(ctx, task, repo, checkout, branchName, baseBranch, machineID, true); err != nil {
					return ProjectCheckoutRecord{}, err
				} else if ok {
					return recovered, nil
				}
				localPath = projectCheckoutMaterializeBranchPath(r.cfg.Workdir, task.ProjectID, repo, branchName, strings.TrimSpace(hintedBranch.HeadSHA))
				if err := ensureProjectCheckoutMaterializePath(r.cfg.Workdir, localPath); err != nil {
					return ProjectCheckoutRecord{}, fmt.Errorf("project claim admission recovery checkout path: %w", err)
				}
				break
			}
			checkoutPath := strings.TrimSpace(checkout.LocalPath)
			if checkoutPath == "" {
				break
			}
			if err := validateProjectCheckoutWorkdir(ctx, checkoutPath, repo); err != nil {
				return ProjectCheckoutRecord{}, fmt.Errorf("project claim admission hinted checkout %s invalid: %w", checkoutID, err)
			}
			allowDirtyExisting := projectClaimAdmissionCanReuseDirtyCheckout(checkout, task, branchName, coordination.Branches)
			if _, _, err := materializeGitCheckout(ctx, checkoutPath, repo, baseBranch, allowDirtyExisting); err != nil {
				return ProjectCheckoutRecord{}, fmt.Errorf("project claim admission hinted checkout refresh: %w", err)
			}
			if err := checkoutProjectClaimBranch(ctx, checkoutPath, branchName, baseBranch); err != nil {
				return ProjectCheckoutRecord{}, err
			}
			return checkout, nil
		}
		if !matchedHint && strings.TrimSpace(hintedBranch.BranchID) == "" {
			return ProjectCheckoutRecord{}, fmt.Errorf("project claim admission referenced checkout_id %s was not present in project coordination for agent %s", checkoutID, strings.TrimSpace(r.cfg.AgentID))
		}
		if !matchedHint {
			localPath = projectCheckoutMaterializeBranchPath(r.cfg.Workdir, task.ProjectID, repo, branchName, strings.TrimSpace(hintedBranch.HeadSHA))
			if err := ensureProjectCheckoutMaterializePath(r.cfg.Workdir, localPath); err != nil {
				return ProjectCheckoutRecord{}, fmt.Errorf("project claim admission recovery checkout path: %w", err)
			}
		}
	}
	for _, checkout := range coordination.Checkouts {
		if strings.TrimSpace(checkout.RepoID) != strings.TrimSpace(repo.RepoID) {
			continue
		}
		if strings.TrimSpace(checkout.AgentID) != strings.TrimSpace(r.cfg.AgentID) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(checkout.Status), "ACTIVE") {
			continue
		}
		if !projectClaimAdmissionCanReuseCheckoutForBranch(checkout, task, branchName) {
			continue
		}
		checkoutPath := strings.TrimSpace(checkout.LocalPath)
		if checkoutPath == "" {
			continue
		}
		if err := validateProjectCheckoutWorkdir(ctx, checkoutPath, repo); err == nil {
			allowDirtyExisting := projectClaimAdmissionCanReuseDirtyCheckout(checkout, task, branchName, coordination.Branches)
			if _, _, err := materializeGitCheckout(ctx, checkoutPath, repo, baseBranch, allowDirtyExisting); err != nil {
				return ProjectCheckoutRecord{}, fmt.Errorf("project claim admission existing checkout refresh: %w", err)
			}
			if err := checkoutProjectClaimBranch(ctx, checkoutPath, branchName, baseBranch); err != nil {
				return ProjectCheckoutRecord{}, err
			}
			return checkout, nil
		}
	}
	for _, checkout := range coordination.Checkouts {
		if recovered, ok, err := r.recoverProjectClaimCheckout(ctx, task, repo, checkout, branchName, baseBranch, machineID, false); err != nil {
			return ProjectCheckoutRecord{}, err
		} else if ok {
			return recovered, nil
		}
	}
	if redirectedPath, _, _, redirected := projectCheckoutMaterializeRedirectDirtyForeignDefault(ctx, r.cfg.Workdir, task.ProjectID, repo, localPath, branchName, strings.TrimSpace(hintedBranch.HeadSHA)); redirected {
		localPath = redirectedPath
		if err := ensureProjectCheckoutMaterializePath(r.cfg.Workdir, localPath); err != nil {
			return ProjectCheckoutRecord{}, fmt.Errorf("project claim admission redirected checkout path: %w", err)
		}
	}
	if _, _, err := materializeGitCheckout(ctx, localPath, repo, baseBranch, false); err != nil {
		return ProjectCheckoutRecord{}, fmt.Errorf("project claim admission git checkout: %w", err)
	}
	if err := checkoutProjectClaimBranch(ctx, localPath, branchName, baseBranch); err != nil {
		return ProjectCheckoutRecord{}, err
	}
	dirtyState := "UNKNOWN"
	if dirty, err := gitWorktreeDirty(ctx, localPath); err == nil {
		if dirty {
			dirtyState = "DIRTY"
		} else {
			dirtyState = "CLEAN"
		}
	}
	checkout, err := r.client.RegisterProjectCheckout(ctx, ProjectCheckoutRegisterInput{
		WorkspaceID:  r.cfg.WorkspaceID,
		ProjectID:    task.ProjectID,
		ActorID:      r.cfg.AgentID,
		RepoID:       repo.RepoID,
		MachineID:    machineID,
		MachineLabel: machineID,
		OwnerUserID:  r.cfg.OwnerUserID,
		AgentID:      r.cfg.AgentID,
		LocalPath:    localPath,
		CheckoutKind: "clone",
		BranchName:   branchName,
		BaseBranch:   baseBranch,
		DirtyState:   dirtyState,
		Status:       "ACTIVE",
	})
	if err != nil {
		return ProjectCheckoutRecord{}, fmt.Errorf("project claim admission checkout register: %w", err)
	}
	return checkout, nil
}

func (r *Runtime) recoverProjectClaimCheckout(ctx context.Context, task WorkspaceTaskRecord, repo ProjectRepositoryRecord, checkout ProjectCheckoutRecord, branchName, baseBranch, machineID string, hinted bool) (ProjectCheckoutRecord, bool, error) {
	if r == nil || r.client == nil || !projectClaimAdmissionCanRecoverCheckout(checkout, task, repo, r.cfg.AgentID, branchName, hinted) {
		return ProjectCheckoutRecord{}, false, nil
	}
	checkoutPath := strings.TrimSpace(checkout.LocalPath)
	if checkoutPath == "" {
		return ProjectCheckoutRecord{}, false, nil
	}
	if err := ensureProjectCheckoutMaterializePath(r.cfg.Workdir, checkoutPath); err != nil {
		return ProjectCheckoutRecord{}, false, nil
	}
	if err := validateProjectCheckoutWorkdir(ctx, checkoutPath, repo); err != nil {
		return ProjectCheckoutRecord{}, false, nil
	}
	dirty, dirtyErr := gitWorktreeDirty(ctx, checkoutPath)
	if dirtyErr != nil {
		return ProjectCheckoutRecord{}, false, fmt.Errorf("project claim admission recovery dirty check: %w", dirtyErr)
	}
	currentBranch := ""
	if dirty {
		currentBranch, _ = currentGitBranch(ctx, checkoutPath)
		if strings.TrimSpace(currentBranch) != strings.TrimSpace(branchName) {
			return ProjectCheckoutRecord{}, false, nil
		}
	}
	if _, _, err := materializeGitCheckout(ctx, checkoutPath, repo, baseBranch, dirty); err != nil {
		return ProjectCheckoutRecord{}, false, fmt.Errorf("project claim admission recovered checkout refresh: %w", err)
	}
	if !(dirty && strings.TrimSpace(currentBranch) == strings.TrimSpace(branchName)) {
		if err := checkoutProjectClaimBranch(ctx, checkoutPath, branchName, baseBranch); err != nil {
			return ProjectCheckoutRecord{}, false, err
		}
	}
	dirtyState := "CLEAN"
	if stillDirty, err := gitWorktreeDirty(ctx, checkoutPath); err == nil && stillDirty {
		dirtyState = "DIRTY"
	} else if err != nil {
		dirtyState = firstNonEmpty(strings.TrimSpace(checkout.DirtyState), "UNKNOWN")
	}
	recovered, err := r.client.RegisterProjectCheckout(ctx, ProjectCheckoutRegisterInput{
		WorkspaceID:  r.cfg.WorkspaceID,
		ProjectID:    task.ProjectID,
		ActorID:      r.cfg.AgentID,
		CheckoutID:   strings.TrimSpace(checkout.CheckoutID),
		RepoID:       repo.RepoID,
		MachineID:    firstNonEmpty(strings.TrimSpace(checkout.MachineID), strings.TrimSpace(machineID)),
		MachineLabel: firstNonEmpty(strings.TrimSpace(checkout.MachineLabel), strings.TrimSpace(machineID)),
		OwnerUserID:  firstNonEmpty(strings.TrimSpace(checkout.OwnerUserID), r.cfg.OwnerUserID),
		AgentID:      r.cfg.AgentID,
		LocalPath:    checkoutPath,
		CheckoutKind: firstNonEmpty(strings.TrimSpace(checkout.CheckoutKind), "clone"),
		BranchName:   firstNonEmpty(strings.TrimSpace(branchName), strings.TrimSpace(checkout.BranchName)),
		BaseBranch:   firstNonEmpty(strings.TrimSpace(baseBranch), strings.TrimSpace(checkout.BaseBranch)),
		HeadSHA:      strings.TrimSpace(checkout.HeadSHA),
		BaseSHA:      strings.TrimSpace(checkout.BaseSHA),
		DirtyState:   dirtyState,
		Status:       "ACTIVE",
	})
	if err != nil {
		return ProjectCheckoutRecord{}, false, fmt.Errorf("project claim admission recovered checkout register: %w", err)
	}
	return recovered, true, nil
}

func projectClaimAdmissionCanRecoverCheckout(checkout ProjectCheckoutRecord, task WorkspaceTaskRecord, repo ProjectRepositoryRecord, agentID, branchName string, hinted bool) bool {
	if strings.TrimSpace(checkout.CheckoutID) == "" ||
		strings.TrimSpace(checkout.RepoID) != strings.TrimSpace(repo.RepoID) ||
		strings.TrimSpace(checkout.AgentID) != strings.TrimSpace(agentID) ||
		strings.EqualFold(strings.TrimSpace(checkout.Status), "ACTIVE") {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(checkout.CheckoutKind)) {
	case "review", "validation", "qa", "integration":
		return false
	}
	if hinted {
		return true
	}
	taskID := strings.TrimSpace(task.TaskID)
	if taskID != "" && (strings.TrimSpace(checkout.ActiveTaskID) == taskID || strings.TrimSpace(checkout.ActiveClaimID) == taskID) {
		return true
	}
	storedBranchName := strings.TrimSpace(checkout.BranchName)
	legacyBranchName := projectClaimLegacyBranchName(checkout.AgentID, task.ProjectID, task.TaskID)
	return storedBranchName != "" && (storedBranchName == strings.TrimSpace(branchName) || storedBranchName == legacyBranchName)
}

func (r *Runtime) ensureProjectClaimBranch(ctx context.Context, task WorkspaceTaskRecord, repo ProjectRepositoryRecord, checkout ProjectCheckoutRecord, branchName, baseBranch, writeScopeJSON string, coordination ProjectCoordinationRecord, hintedBranch ProjectBranchRecord) (ProjectBranchRecord, error) {
	legacyBranchName := projectClaimLegacyBranchName(r.cfg.AgentID, task.ProjectID, task.TaskID)
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.RepoID) != strings.TrimSpace(repo.RepoID) {
			continue
		}
		if strings.TrimSpace(branch.AgentID) != strings.TrimSpace(r.cfg.AgentID) {
			continue
		}
		if projectClaimAdmissionShouldForkRevisionBranch(coordination.PatchQueueItems, branch, task) {
			continue
		}
		hinted := strings.TrimSpace(hintedBranch.BranchID) != "" && strings.TrimSpace(branch.BranchID) == strings.TrimSpace(hintedBranch.BranchID)
		if strings.TrimSpace(branch.ActiveTaskID) != "" && strings.TrimSpace(branch.ActiveTaskID) != strings.TrimSpace(task.TaskID) {
			if hinted {
				return branch, nil
			}
			continue
		}
		storedBranchName := strings.TrimSpace(branch.BranchName)
		if !hinted && projectClaimAdmissionNeedsFreshSuccessorBranch(task, branch) {
			continue
		}
		if !hinted &&
			storedBranchName != branchName &&
			strings.TrimSpace(branch.ActiveTaskID) == strings.TrimSpace(task.TaskID) &&
			projectClaimAdmissionNeedsFreshSuccessorBranch(task, branch) {
			continue
		}
		if hinted || storedBranchName == branchName || storedBranchName == legacyBranchName || strings.TrimSpace(branch.ActiveTaskID) == strings.TrimSpace(task.TaskID) {
			status := projectClaimAdmissionBranchStatusForClaim(branch)
			if branch.CheckoutID == "" || branch.CheckoutID != checkout.CheckoutID || branch.WriteScopeJSON == "" || branch.WriteScopeJSON != writeScopeJSON || !strings.EqualFold(strings.TrimSpace(branch.Status), status) {
				return r.client.RegisterProjectBranch(ctx, ProjectBranchRegisterInput{
					WorkspaceID:    r.cfg.WorkspaceID,
					ProjectID:      task.ProjectID,
					ActorID:        r.cfg.AgentID,
					BranchID:       branch.BranchID,
					RepoID:         repo.RepoID,
					CheckoutID:     checkout.CheckoutID,
					AgentID:        r.cfg.AgentID,
					BranchName:     branchName,
					BranchKind:     "feature",
					BaseBranch:     baseBranch,
					WriteScopeJSON: writeScopeJSON,
					Status:         status,
				})
			}
			return branch, nil
		}
	}
	branch, err := r.client.RegisterProjectBranch(ctx, ProjectBranchRegisterInput{
		WorkspaceID:    r.cfg.WorkspaceID,
		ProjectID:      task.ProjectID,
		ActorID:        r.cfg.AgentID,
		RepoID:         repo.RepoID,
		CheckoutID:     checkout.CheckoutID,
		AgentID:        r.cfg.AgentID,
		BranchName:     branchName,
		BranchKind:     "feature",
		BaseBranch:     baseBranch,
		WriteScopeJSON: writeScopeJSON,
		Status:         "RESERVED",
	})
	if err != nil {
		return ProjectBranchRecord{}, fmt.Errorf("project claim admission branch register: %w", err)
	}
	return branch, nil
}

func projectClaimAdmissionNeedsFreshSuccessorBranch(task WorkspaceTaskRecord, branch ProjectBranchRecord) bool {
	if !runtimeProjectLaneRequiresImplementationGate(task.ProjectLane) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(branch.Status), "READY_FOR_REVIEW") {
		return false
	}
	return !projectClaimAdmissionLooksLikePatchQueueRevisionTask(task)
}

func selectProjectClaimReadyPredecessorByName(branches []ProjectBranchRecord, task WorkspaceTaskRecord, repoID, agentID, branchName string) (ProjectBranchRecord, bool) {
	repoID = strings.TrimSpace(repoID)
	agentID = strings.TrimSpace(agentID)
	branchName = strings.TrimSpace(branchName)
	if repoID == "" || agentID == "" || branchName == "" {
		return ProjectBranchRecord{}, false
	}
	for _, branch := range branches {
		if strings.TrimSpace(branch.RepoID) != repoID || strings.TrimSpace(branch.AgentID) != agentID {
			continue
		}
		if strings.TrimSpace(branch.BranchName) != branchName {
			continue
		}
		if !projectClaimAdmissionNeedsFreshSuccessorBranch(task, branch) {
			continue
		}
		return branch, true
	}
	return ProjectBranchRecord{}, false
}

func projectClaimAdmissionBranchStatusForClaim(branch ProjectBranchRecord) string {
	status := firstNonEmpty(strings.TrimSpace(branch.Status), "RESERVED")
	if strings.EqualFold(status, "ABANDONED") {
		return "RESERVED"
	}
	return status
}

func projectClaimAdmissionShouldForkRevisionBranch(items []ProjectPatchQueueItemRecord, branch ProjectBranchRecord, task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(branch.BranchID) == "" || !runtimeProjectLaneRequiresImplementationGate(task.ProjectLane) {
		return false
	}
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) != strings.TrimSpace(branch.BranchID) {
			continue
		}
		if projectClaimAdmissionRevisionItemMatchesTask(task, item, branch.RepoID) {
			return true
		}
	}
	return false
}

func selectProjectClaimRevisionSourceBranch(items []ProjectPatchQueueItemRecord, branches []ProjectBranchRecord, task WorkspaceTaskRecord, repoID string) (ProjectBranchRecord, bool) {
	return selectProjectClaimRevisionSourceBranchExcluding(items, branches, task, repoID, "")
}

func selectProjectClaimRevisionSourceBranchExcluding(items []ProjectPatchQueueItemRecord, branches []ProjectBranchRecord, task WorkspaceTaskRecord, repoID, excludedBranchID string) (ProjectBranchRecord, bool) {
	if !runtimeProjectLaneRequiresImplementationGate(task.ProjectLane) {
		return ProjectBranchRecord{}, false
	}
	repoID = strings.TrimSpace(repoID)
	excludedBranchID = strings.TrimSpace(excludedBranchID)
	for _, item := range items {
		if !projectClaimAdmissionRevisionItemMatchesTask(task, item, repoID) {
			continue
		}
		for _, branch := range branches {
			if strings.TrimSpace(branch.BranchID) != strings.TrimSpace(item.BranchID) {
				continue
			}
			if excludedBranchID != "" && strings.TrimSpace(branch.BranchID) == excludedBranchID {
				return ProjectBranchRecord{}, false
			}
			if repoID != "" && strings.TrimSpace(branch.RepoID) != repoID {
				return ProjectBranchRecord{}, false
			}
			switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
			case "RESERVED", "ACTIVE", "BLOCKED", "READY_FOR_REVIEW":
			default:
				return ProjectBranchRecord{}, false
			}
			if strings.TrimSpace(branch.BranchName) == "" {
				return ProjectBranchRecord{}, false
			}
			return branch, true
		}
	}
	return ProjectBranchRecord{}, false
}

func projectClaimAdmissionRevisionItemMatchesTask(task WorkspaceTaskRecord, item ProjectPatchQueueItemRecord, repoID string) bool {
	if !projectClaimAdmissionLooksLikePatchQueueRevisionTask(task) {
		return false
	}
	if repoID != "" && strings.TrimSpace(item.RepoID) != strings.TrimSpace(repoID) {
		return false
	}
	if strings.TrimSpace(item.QueueID) == "" ||
		strings.TrimSpace(item.ItemID) == "" ||
		strings.TrimSpace(item.BranchID) == "" ||
		strings.TrimSpace(item.HeadSHA) == "" {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(item.State)) {
	case "BLOCKED", "REJECTED":
	default:
		return false
	}
	queueID, itemID, branchID := runtimeOwnerBoundPatchQueueRefsFromTask(task)
	texts := []string{task.Title, task.Description}
	headSHA := firstNonEmpty(runtimeTaskTextFieldValue(texts, "head_sha"), runtimeTaskTextFieldValue(texts, "head"))
	if strings.TrimSpace(queueID) != "" {
		if !strings.EqualFold(strings.TrimSpace(item.QueueID), strings.TrimSpace(queueID)) {
			return false
		}
	} else if !projectClaimAdmissionTaskContainsRef(task, item.QueueID) {
		return false
	}
	if strings.TrimSpace(itemID) != "" {
		if !strings.EqualFold(strings.TrimSpace(item.ItemID), strings.TrimSpace(itemID)) {
			return false
		}
	} else if !projectClaimAdmissionTaskContainsRef(task, item.ItemID) {
		return false
	}
	if strings.TrimSpace(branchID) != "" {
		if !strings.EqualFold(strings.TrimSpace(item.BranchID), strings.TrimSpace(branchID)) {
			return false
		}
	} else if !projectClaimAdmissionTaskContainsRef(task, item.BranchID) {
		return false
	}
	if strings.TrimSpace(headSHA) != "" {
		return strings.EqualFold(strings.TrimSpace(item.HeadSHA), strings.TrimSpace(headSHA))
	}
	return projectClaimAdmissionTaskContainsRef(task, item.HeadSHA)
}

func projectClaimAdmissionLooksLikePatchQueueRevisionTask(task WorkspaceTaskRecord) bool {
	text := projectClaimAdmissionTaskIdentityText(task)
	hasPatchQueue := runtimeOwnerBoundTaskHasTag(task, "patch-queue", "patch_queue") ||
		strings.Contains(text, "patch queue") ||
		strings.Contains(text, "patch-queue") ||
		strings.Contains(text, "patch_queue") ||
		strings.Contains(text, "patchitem-")
	hasRevision := runtimeOwnerBoundTaskHasTag(task, "revision") ||
		strings.Contains(text, "revision") ||
		strings.Contains(text, "revise") ||
		strings.Contains(text, "revised candidate")
	return hasPatchQueue && hasRevision
}

func projectClaimAdmissionTaskContainsRef(task WorkspaceTaskRecord, ref string) bool {
	return runtimeOwnerBoundTextContainsIdentifier(projectClaimAdmissionTaskIdentityText(task), ref)
}

func projectClaimAdmissionTaskIdentityText(task WorkspaceTaskRecord) string {
	return strings.ToLower(strings.Join([]string{
		strings.TrimSpace(task.TaskID),
		strings.TrimSpace(task.Title),
		strings.TrimSpace(task.Description),
		strings.Join(task.Tags, " "),
		strings.TrimSpace(task.TaskRequirementsJSON),
	}, "\n"))
}

func projectClaimAdmissionCanReuseCheckoutForBranch(checkout ProjectCheckoutRecord, task WorkspaceTaskRecord, branchName string) bool {
	switch strings.ToLower(strings.TrimSpace(checkout.CheckoutKind)) {
	case "review", "validation", "qa", "integration":
		return false
	}
	if activeTaskID := strings.TrimSpace(checkout.ActiveTaskID); activeTaskID != "" && activeTaskID != strings.TrimSpace(task.TaskID) {
		return false
	}
	storedBranchName := strings.TrimSpace(checkout.BranchName)
	legacyBranchName := projectClaimLegacyBranchName(checkout.AgentID, task.ProjectID, task.TaskID)
	return storedBranchName == "" || storedBranchName == strings.TrimSpace(branchName) || storedBranchName == legacyBranchName
}

func projectClaimAdmissionCanReuseDirtyCheckout(checkout ProjectCheckoutRecord, task WorkspaceTaskRecord, branchName string, branches []ProjectBranchRecord) bool {
	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" {
		return false
	}
	if strings.TrimSpace(checkout.ActiveTaskID) == taskID || strings.TrimSpace(checkout.ActiveClaimID) == taskID {
		return true
	}
	if strings.TrimSpace(checkout.ActiveTaskID) != "" || strings.TrimSpace(checkout.ActiveClaimID) != "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(checkout.CheckoutKind)) {
	case "review", "validation", "qa", "integration":
		return false
	}
	if managerProjectCheckoutStatusTerminal(checkout.Status) {
		return false
	}
	checkoutBranchName := strings.TrimSpace(checkout.BranchName)
	if checkoutBranchName == "" {
		return false
	}
	targetBranchName := strings.TrimSpace(branchName)
	legacyBranchName := projectClaimLegacyBranchName(checkout.AgentID, task.ProjectID, task.TaskID)
	if checkoutBranchName != targetBranchName && checkoutBranchName != legacyBranchName {
		return false
	}
	for _, branch := range branches {
		if strings.TrimSpace(branch.CheckoutID) == "" || strings.TrimSpace(branch.CheckoutID) != strings.TrimSpace(checkout.CheckoutID) {
			continue
		}
		if strings.TrimSpace(branch.RepoID) != strings.TrimSpace(checkout.RepoID) ||
			strings.TrimSpace(branch.ProjectID) != strings.TrimSpace(task.ProjectID) ||
			strings.TrimSpace(branch.AgentID) != strings.TrimSpace(checkout.AgentID) ||
			projectCoordinationBranchStatusTerminal(branch.Status) {
			continue
		}
		if activeTaskID := strings.TrimSpace(branch.ActiveTaskID); activeTaskID != "" && activeTaskID != taskID {
			continue
		}
		if activeClaimID := strings.TrimSpace(branch.ActiveClaimID); activeClaimID != "" && activeClaimID != taskID {
			continue
		}
		storedBranchName := strings.TrimSpace(branch.BranchName)
		if storedBranchName == checkoutBranchName || storedBranchName == targetBranchName || storedBranchName == legacyBranchName {
			return true
		}
	}
	return false
}

func selectProjectClaimExistingBranch(branches []ProjectBranchRecord, task WorkspaceTaskRecord, repoID, agentID string) (ProjectBranchRecord, bool) {
	branchID := projectClaimTaskBranchHint(task)
	if branchID == "" {
		return ProjectBranchRecord{}, false
	}
	repoID = strings.TrimSpace(repoID)
	agentID = strings.TrimSpace(agentID)
	for _, branch := range branches {
		if strings.TrimSpace(branch.BranchID) != branchID {
			continue
		}
		if repoID != "" && strings.TrimSpace(branch.RepoID) != repoID {
			return ProjectBranchRecord{}, false
		}
		if agentID != "" && strings.TrimSpace(branch.AgentID) != agentID {
			return ProjectBranchRecord{}, false
		}
		switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
		case "RESERVED", "ACTIVE", "BLOCKED", "READY_FOR_REVIEW":
		default:
			return ProjectBranchRecord{}, false
		}
		if strings.TrimSpace(branch.BranchName) == "" {
			return ProjectBranchRecord{}, false
		}
		return branch, true
	}
	return ProjectBranchRecord{}, false
}

func projectClaimTaskBranchHint(task WorkspaceTaskRecord) string {
	if task.ClaimBranchID != nil {
		if branchID := strings.TrimSpace(*task.ClaimBranchID); branchID != "" {
			return branchID
		}
	}
	for _, text := range []string{task.Description, task.Title} {
		for _, line := range strings.Split(text, "\n") {
			line = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line), "-*"))
			key, value, ok := strings.Cut(line, ":")
			if !ok || !strings.EqualFold(strings.TrimSpace(key), "branch_id") {
				continue
			}
			if branchID := normalizeProjectToolRef(value); branchID != "" {
				return branchID
			}
		}
	}
	return ""
}

func checkoutProjectClaimBranch(ctx context.Context, localPath, branchName, baseBranch string) error {
	branchName = strings.TrimSpace(branchName)
	if branchName == "" {
		return nil
	}
	if err := checkoutProjectClaimBaseBranch(ctx, localPath, baseBranch); err != nil {
		return err
	}
	if _, err := gitCombined(ctx, localPath, "rev-parse", "--verify", "--quiet", branchName); err == nil {
		baseSHA := projectBranchCommitResolveBaseSHA(ctx, localPath, baseBranch)
		branchSHA, _ := gitRevParse(ctx, localPath, branchName)
		if baseSHA != "" && branchSHA != "" {
			switch {
			case branchSHA == baseSHA || gitCommitIsAncestor(ctx, localPath, branchSHA, baseSHA):
				if out, err := gitCombined(ctx, localPath, "checkout", "-B", branchName, baseSHA); err != nil {
					return fmt.Errorf("project claim admission branch reset to current base failed: %w\n%s", err, out)
				}
				return nil
			case gitCommitIsAncestor(ctx, localPath, baseSHA, branchSHA):
				if out, err := gitCombined(ctx, localPath, "checkout", branchName); err != nil {
					return fmt.Errorf("project claim admission branch checkout failed: %w\n%s", err, out)
				}
				return nil
			default:
				if err := quarantineProjectClaimStaleBranch(ctx, localPath, branchName, branchSHA, baseSHA); err != nil {
					return fmt.Errorf("project claim admission blocked stale divergent branch %s: local head %s is not based on current %s %s: %w", branchName, branchSHA, firstNonEmpty(strings.TrimSpace(baseBranch), "base"), baseSHA, err)
				}
				if out, err := gitCombined(ctx, localPath, "checkout", "-b", branchName, baseSHA); err != nil {
					return fmt.Errorf("project claim admission branch recreate from current base failed: %w\n%s", err, out)
				}
				return nil
			}
		}
		if out, err := gitCombined(ctx, localPath, "checkout", branchName); err != nil {
			return fmt.Errorf("project claim admission branch checkout failed: %w\n%s", err, out)
		}
		return nil
	}
	remoteRef := "refs/remotes/origin/" + branchName
	if gitRefExists(ctx, localPath, remoteRef) {
		if out, err := gitCombined(ctx, localPath, "checkout", "-B", branchName, remoteRef); err != nil {
			return fmt.Errorf("project claim admission remote branch checkout failed: %w\n%s", err, out)
		}
		return nil
	}
	if out, err := gitCombined(ctx, localPath, "checkout", "-b", branchName); err != nil {
		return fmt.Errorf("project claim admission branch create failed: %w\n%s", err, out)
	}
	return nil
}

func quarantineProjectClaimStaleBranch(ctx context.Context, localPath, branchName, branchSHA, baseSHA string) error {
	if dirty, err := gitWorktreeDirty(ctx, localPath); err != nil {
		return fmt.Errorf("could not verify checkout cleanliness before stale branch quarantine: %w", err)
	} else if dirty {
		return fmt.Errorf("checkout is dirty; preserve local work and use a fresh checkout/branch or cleanly publish/abandon the stale branch before continuing")
	}
	archiveBranch := projectClaimStaleBranchArchiveName(branchName, branchSHA)
	if gitRefExists(ctx, localPath, "refs/heads/"+archiveBranch) {
		archiveBranch = projectClaimStaleBranchArchiveName(branchName+"-"+shortRefHash(baseSHA), branchSHA)
	}
	if out, err := gitCombined(ctx, localPath, "branch", archiveBranch, branchSHA); err != nil {
		return fmt.Errorf("archive stale local branch failed: %w\n%s", err, out)
	}
	if remoteSHA, err := gitRevParse(ctx, localPath, "refs/remotes/origin/"+branchName); err == nil && strings.TrimSpace(remoteSHA) != "" {
		if out, err := gitCombined(ctx, localPath, "push", "origin", strings.TrimSpace(remoteSHA)+":refs/heads/"+archiveBranch); err != nil {
			return fmt.Errorf("archive stale remote branch failed: %w\n%s", err, out)
		}
		lease := "refs/heads/" + branchName + ":" + strings.TrimSpace(remoteSHA)
		if out, err := gitCombined(ctx, localPath, "push", "--force-with-lease="+lease, "origin", ":refs/heads/"+branchName); err != nil {
			return fmt.Errorf("delete stale remote branch failed: %w\n%s", err, out)
		}
	}
	if out, err := gitCombined(ctx, localPath, "branch", "-D", branchName); err != nil {
		return fmt.Errorf("delete stale local branch failed: %w\n%s", err, out)
	}
	return nil
}

func projectClaimStaleBranchArchiveName(branchName, branchSHA string) string {
	return "rhizome-stale/" + sanitizeRefSegment(branchName) + "-" + shortRefHash(branchSHA)
}

func checkoutProjectClaimBaseBranch(ctx context.Context, localPath, baseBranch string) error {
	baseBranch = strings.TrimSpace(baseBranch)
	if baseBranch == "" {
		return nil
	}
	remoteRef := "refs/remotes/origin/" + baseBranch
	if gitRefExists(ctx, localPath, remoteRef) {
		if out, err := gitCombined(ctx, localPath, "checkout", "-B", baseBranch, remoteRef); err != nil {
			return fmt.Errorf("project claim admission remote base branch checkout failed: %w\n%s", err, out)
		}
		return nil
	}
	if _, err := gitCombined(ctx, localPath, "rev-parse", "--verify", "--quiet", baseBranch); err == nil {
		if out, err := gitCombined(ctx, localPath, "checkout", baseBranch); err != nil {
			return fmt.Errorf("project claim admission base branch checkout failed: %w\n%s", err, out)
		}
		return nil
	}
	if gitCommitObjectExists(ctx, localPath, baseBranch) {
		if out, err := gitCombined(ctx, localPath, "checkout", "--detach", baseBranch); err != nil {
			return fmt.Errorf("project claim admission base commit checkout failed: %w\n%s", err, out)
		}
		return nil
	}
	if out, err := gitCombined(ctx, localPath, "checkout", baseBranch); err != nil {
		return fmt.Errorf("project claim admission base branch checkout failed: %w\n%s", err, out)
	}
	return nil
}

func projectCoordinationFromWork(work *AgentWorkNextResult) (ProjectCoordinationRecord, bool) {
	if work == nil {
		return ProjectCoordinationRecord{}, false
	}
	for _, raw := range []json.RawMessage{work.ProjectCoordination, func() json.RawMessage {
		if work.Packet == nil {
			return nil
		}
		return work.Packet.ProjectCoordination
	}()} {
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		var coordination ProjectCoordinationRecord
		if err := json.Unmarshal(raw, &coordination); err != nil {
			continue
		}
		if coordination.Project.ProjectID != "" || coordination.Profile.ProjectID != "" || len(coordination.Repositories) > 0 {
			return coordination, true
		}
	}
	return ProjectCoordinationRecord{}, false
}

func attachProjectCoordinationToWork(work *AgentWorkNextResult, coordination ProjectCoordinationRecord) {
	if work == nil {
		return
	}
	raw, err := json.Marshal(coordination)
	if err != nil {
		return
	}
	if len(work.ProjectCoordination) == 0 {
		work.ProjectCoordination = append([]byte(nil), raw...)
	}
	if work.Packet != nil && len(work.Packet.ProjectCoordination) == 0 {
		work.Packet.ProjectCoordination = append([]byte(nil), raw...)
	}
}

func runtimeTaskShouldPrepareProjectClaimAdmission(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if roleType := runtimeProjectClaimRequiredRoleType(task.ProjectLane); roleType != "" && roleType != "IMPLEMENTER" {
		return task.RequiresProjectGate != nil && *task.RequiresProjectGate
	}
	if runtimeProjectLaneBypassesImplementationGate(task.ProjectLane) {
		return false
	}
	if runtimeTaskBypassesImplementationGateByStructuredContract(task) {
		return false
	}
	if task.RequiresProjectGate != nil && *task.RequiresProjectGate {
		return true
	}
	return runtimeProjectLaneRequiresImplementationGate(task.ProjectLane)
}

func runtimeProjectClaimAdmissionRequired(task WorkspaceTaskRecord, coordination ProjectCoordinationRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if coordination.Profile.ProjectID == "" {
		return runtimeTaskShouldPrepareProjectClaimAdmission(task)
	}
	if !coordination.Profile.RepoRequired || !strings.EqualFold(strings.TrimSpace(coordination.Profile.RepoStatus), "READY") {
		return false
	}
	if !runtimeProjectPhaseAllowsImplementationWork(coordination.Profile.CurrentPhase) {
		return false
	}
	if roleType := runtimeProjectClaimRequiredRoleType(task.ProjectLane); roleType != "" && roleType != "IMPLEMENTER" {
		return task.RequiresProjectGate != nil && *task.RequiresProjectGate
	}
	if runtimeTaskBypassesImplementationGateByStructuredContract(task) {
		return false
	}
	return runtimeProjectLaneRequiresImplementationGate(task.ProjectLane)
}

func runtimeProjectImplementationGateClosed(task WorkspaceTaskRecord, coordination ProjectCoordinationRecord) (string, bool) {
	if !runtimeTaskShouldPrepareProjectClaimAdmission(task) {
		return "", false
	}
	if runtimeProjectLaneBypassesImplementationGate(task.ProjectLane) {
		return "", false
	}
	status := coordination.GateStatus
	if runtimeProjectGateStatusPresent(status) && !status.ImplementationReady {
		return runtimeProjectGateClosedSummary(status), true
	}
	phase := firstNonEmpty(strings.TrimSpace(status.CurrentPhase), strings.TrimSpace(coordination.Profile.CurrentPhase))
	if phase != "" && !runtimeProjectPhaseAllowsImplementationWork(phase) {
		return "implementation_phase_open: project phase is " + strings.ToLower(strings.TrimSpace(phase)), true
	}
	return "", false
}

func runtimeProjectGateStatusPresent(status ProjectGateStatusRecord) bool {
	return strings.TrimSpace(status.ProjectID) != "" ||
		strings.TrimSpace(status.OverallState) != "" ||
		strings.TrimSpace(status.CurrentPhase) != "" ||
		len(status.Gates) > 0
}

func runtimeProjectGateClosedSummary(status ProjectGateStatusRecord) string {
	for _, gate := range status.Gates {
		if !gate.Required || runtimeProjectGateSatisfied(gate.State) {
			continue
		}
		if strings.TrimSpace(gate.Summary) != "" {
			return strings.TrimSpace(gate.GateKey) + ": " + strings.TrimSpace(gate.Summary)
		}
		return strings.TrimSpace(gate.GateKey) + " is " + strings.ToLower(strings.TrimSpace(gate.State))
	}
	if strings.TrimSpace(status.OverallState) != "" {
		return "project gate state is " + strings.ToLower(strings.TrimSpace(status.OverallState))
	}
	return "project implementation gate is closed"
}

func runtimeProjectGateSatisfied(state string) bool {
	state = strings.ToUpper(strings.TrimSpace(state))
	return state == "SATISFIED" || state == "WAIVED"
}

func selectProjectClaimRepository(coordination ProjectCoordinationRecord) (ProjectRepositoryRecord, bool) {
	for _, repo := range coordination.Repositories {
		if repo.IsCanonical && projectClaimRepositoryReady(repo) {
			return repo, true
		}
	}
	for _, repo := range coordination.Repositories {
		if projectClaimRepositoryReady(repo) {
			return repo, true
		}
	}
	return ProjectRepositoryRecord{}, false
}

func projectClaimRepositoryReady(repo ProjectRepositoryRecord) bool {
	return strings.EqualFold(strings.TrimSpace(repo.RepoStatus), "READY") && strings.TrimSpace(repo.RemoteURL) != ""
}

func selectProjectClaimWriteScope(coordination ProjectCoordinationRecord, agentID string) (string, string, bool) {
	role, ok := selectProjectClaimImplementerRole(coordination, agentID)
	if !ok {
		return "", "", false
	}
	return strings.TrimSpace(role.WriteScopeJSON), strings.TrimSpace(role.RoleID), true
}

func selectProjectActiveClaimWriteScope(coordination ProjectCoordinationRecord, agentID, taskID string) (string, string, bool) {
	agentID = strings.TrimSpace(agentID)
	taskID = strings.TrimSpace(taskID)
	if agentID == "" || taskID == "" {
		return "", "", false
	}
	for _, task := range coordination.Tasks {
		if strings.TrimSpace(task.TaskID) != taskID || strings.TrimSpace(pointerValue(task.ClaimAgentID)) != agentID {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(pointerValue(task.ClaimStatus)), "CLAIMED") {
			continue
		}
		writeScopeJSON := strings.TrimSpace(pointerValue(task.ClaimWriteScopeJSON))
		if writeScopeJSON == "" || !json.Valid([]byte(writeScopeJSON)) || projectRoleAssignWriteScopeEmpty(writeScopeJSON) {
			continue
		}
		return writeScopeJSON, strings.TrimSpace(pointerValue(task.ClaimProjectRoleID)), true
	}
	return "", "", false
}

func selectProjectClaimImplementerRole(coordination ProjectCoordinationRecord, agentID string) (ProjectRoleRecord, bool) {
	return selectProjectClaimRole(coordination, agentID, "IMPLEMENTER")
}

func selectProjectClaimRole(coordination ProjectCoordinationRecord, agentID, roleType string) (ProjectRoleRecord, bool) {
	agentID = strings.TrimSpace(agentID)
	roleType = strings.ToUpper(strings.TrimSpace(roleType))
	if roleType == "" {
		return ProjectRoleRecord{}, false
	}
	for _, role := range coordination.Roles {
		if strings.TrimSpace(role.AgentID) != agentID || !strings.EqualFold(strings.TrimSpace(role.Status), "ACTIVE") {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(role.RoleType), roleType) {
			continue
		}
		if roleType != "IMPLEMENTER" {
			return role, true
		}
		writeScopeJSON := strings.TrimSpace(role.WriteScopeJSON)
		if writeScopeJSON != "" && json.Valid([]byte(writeScopeJSON)) && !projectRoleAssignWriteScopeEmpty(writeScopeJSON) {
			return role, true
		}
	}
	return ProjectRoleRecord{}, false
}

func (r *Runtime) inferProjectTaskWriteScopeJSON(ctx context.Context, task WorkspaceTaskRecord, work *AgentWorkNextResult) string {
	if scope := projectWriteScopeJSONFromPaths(projectTaskWriteScopeHintsForAdmission(task, task.WriteScopeHints)); scope != "" {
		return scope
	}
	if scope := projectWriteScopeJSONFromPaths(projectTaskWriteScopeHintsForAdmission(task, projectWriteScopeHintsFromTaskRequirementsJSON(task.TaskRequirementsJSON))); scope != "" {
		return scope
	}
	var texts []string
	texts = append(texts, task.Title, task.Description, task.TaskRequirementsJSON, strings.Join(task.Tags, " "))
	if work != nil && work.Hydration != nil {
		if work.Hydration.WorkspaceTask != nil {
			if scope := projectWriteScopeJSONFromPaths(projectTaskWriteScopeHintsForAdmission(*work.Hydration.WorkspaceTask, work.Hydration.WorkspaceTask.WriteScopeHints)); scope != "" {
				return scope
			}
			if scope := projectWriteScopeJSONFromPaths(projectTaskWriteScopeHintsForAdmission(*work.Hydration.WorkspaceTask, projectWriteScopeHintsFromTaskRequirementsJSON(work.Hydration.WorkspaceTask.TaskRequirementsJSON))); scope != "" {
				return scope
			}
		}
		hydratedTask := workspaceTaskRecordFromTaskStatus(work.Hydration.Task)
		if scope := projectWriteScopeJSONFromPaths(projectTaskWriteScopeHintsForAdmission(hydratedTask, work.Hydration.Task.WriteScopeHints)); scope != "" {
			return scope
		}
		if scope := projectWriteScopeJSONFromPaths(projectTaskWriteScopeHintsForAdmission(hydratedTask, projectWriteScopeHintsFromTaskRequirementsJSON(work.Hydration.Task.TaskRequirementsJSON))); scope != "" {
			return scope
		}
		for _, doc := range work.Hydration.Docs {
			if workspaceDocLooksTaskScoped(doc, task.TaskID) {
				texts = append(texts, doc.Content)
			}
		}
	}
	for _, text := range texts {
		if scope := projectWriteScopeJSONFromPaths(projectTaskWriteScopeHintsForAdmission(task, extractProjectWriteScopeHints(text))); scope != "" {
			return scope
		}
	}
	if scope := projectWriteScopeJSONFromPaths(inferProjectWriteScopeHintsFromTaskText(task)); scope != "" {
		return scope
	}
	return ""
}

func workspaceDocLooksTaskScoped(doc WorkspaceDocRecord, taskID string) bool {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return false
	}
	docKey := strings.TrimSpace(doc.DocKey)
	if strings.EqualFold(docKey, "task."+taskID) {
		return true
	}
	return strings.Contains(doc.Content, "task_id: "+taskID) || strings.Contains(doc.Content, "- task_id: "+taskID)
}

func extractProjectWriteScopeHints(text string) []string {
	var paths []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line), "-*# "))
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		normalizedKey := strings.ToLower(strings.TrimSpace(key))
		normalizedKey = strings.ReplaceAll(normalizedKey, "-", "_")
		normalizedKey = strings.ReplaceAll(normalizedKey, " ", "_")
		switch normalizedKey {
		case "write_scope_hints", "write_scope_hint", "write_scope":
		default:
			continue
		}
		for _, part := range strings.FieldsFunc(value, func(r rune) bool {
			return r == ',' || r == ';'
		}) {
			paths = append(paths, part)
		}
	}
	return normalizeProjectWriteScopePaths(paths)
}

func projectTaskWriteScopeHintsForAdmission(task WorkspaceTaskRecord, paths []string) []string {
	paths = normalizeProjectWriteScopePaths(paths)
	if len(paths) == 0 {
		return nil
	}
	if projectTaskPreservesExplicitWriteScopeHints(task) {
		return paths
	}
	if projectClaimAdmissionLooksRevisionScopeRepair(task) {
		return paths
	}
	if narrowed := projectLuaAcceptanceWriteScopeHints(task); len(narrowed) > 0 && projectLuaAcceptanceScopeShouldNarrow(paths, narrowed) {
		return narrowed
	}
	if projectWriteScopeHintsNeedSemanticNarrowing(paths) {
		if narrowed := inferProjectWriteScopeHintsFromTaskText(task, paths); len(narrowed) > 0 {
			return narrowed
		}
	}
	return paths
}

func projectTaskPreservesExplicitWriteScopeHints(task WorkspaceTaskRecord) bool {
	payload := projectTaskRequirementsPayload(task.TaskRequirementsJSON)
	if projectBoolFromAny(payload["preserve_write_scope_hints"]) || projectBoolFromAny(payload["write_scope_hints_authoritative"]) {
		return true
	}
	if projectTaskRequirementsAreABPCRecoveryAction(payload) {
		return true
	}
	if projectBoolFromAny(payload["product_first_root"]) {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(projectStringFromAny(firstNonNil(payload["product_slice"], payload["task_slice"])))) {
	case "acceptance_tests", "acceptance-test-matrix", "acceptance_test_matrix", "full_acceptance", "cold_verification":
		return true
	default:
		return false
	}
}

func projectTaskRequirementsAreABPCRecoveryAction(payload map[string]any) bool {
	if len(payload) == 0 {
		return false
	}
	schema := strings.ToLower(strings.TrimSpace(projectStringFromAny(payload["schema"])))
	admissionKind := strings.ToLower(strings.TrimSpace(projectStringFromAny(payload["admission_kind"])))
	taskClass := strings.ToLower(strings.TrimSpace(projectStringFromAny(payload["abpc_task_class"])))
	actionKind := strings.ToLower(strings.TrimSpace(projectStringFromAny(payload["action_kind"])))
	if admissionKind == "abpc_recovery_action" {
		return true
	}
	if schema == "artifact_bound_side_effect_resolution_followup.v1" {
		return true
	}
	return strings.HasPrefix(taskClass, "side_effect_") && actionKind != ""
}

func workspaceTaskRecordFromTaskStatus(task TaskStatus) WorkspaceTaskRecord {
	return WorkspaceTaskRecord{
		TaskID:               task.TaskID,
		Title:                task.Title,
		Description:          task.Description,
		ProjectID:            task.ProjectID,
		ProjectLane:          task.ProjectLane,
		RequiresProjectGate:  task.RequiresProjectGate,
		TaskRequirementsJSON: task.TaskRequirementsJSON,
		WriteScopeHints:      task.WriteScopeHints,
	}
}

func projectWriteScopeHintsFromTaskRequirementsJSON(raw string) []string {
	payload := projectTaskRequirementsPayload(raw)
	return projectStringSliceFromAny(payload["write_scope_hints"])
}

func projectTaskRequirementsPayload(raw string) map[string]any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]any{}
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return map[string]any{}
	}
	return payload
}

func firstNonNil(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func projectStringFromAny(raw any) string {
	if str, ok := raw.(string); ok {
		return str
	}
	return ""
}

func projectBoolFromAny(raw any) bool {
	switch typed := raw.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true", "1", "yes", "y":
			return true
		default:
			return false
		}
	case float64:
		return typed != 0
	default:
		return false
	}
}

func projectStringSliceFromAny(raw any) []string {
	switch typed := raw.(type) {
	case []string:
		return normalizeProjectWriteScopePaths(typed)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if str, ok := item.(string); ok {
				out = append(out, str)
			}
		}
		return normalizeProjectWriteScopePaths(out)
	case string:
		return normalizeProjectWriteScopePaths([]string{typed})
	default:
		return nil
	}
}

func inferProjectWriteScopeHintsFromTaskText(task WorkspaceTaskRecord, extraPaths ...[]string) []string {
	lower := strings.ToLower(strings.TrimSpace(task.TaskID + "\n" + task.Title + "\n" + task.Description + "\n" + strings.Join(task.Tags, " ") + "\n" + task.TaskRequirementsJSON))
	var out []string
	add := func(paths ...string) {
		out = append(out, paths...)
	}
	if projectTaskLooksGoInterpreterScope(task, extraPaths...) {
		if luaScope := projectLuaAcceptanceWriteScopeHints(task); len(luaScope) > 0 {
			return luaScope
		}
		if scaffold := projectGoInterpreterScaffoldWriteScopeHints(task); len(scaffold) > 0 {
			return scaffold
		}
		if primary := projectGoInterpreterPrimaryWriteScopeHints(task, extraPaths...); len(primary) > 0 {
			return primary
		}
		if projectScopeTextContainsAny(lower, "lexer", "lexical", "tokenizer", "tokeniser", "token stream") {
			add("internal/lexer/**", "internal/token/**", "internal/tokens/**")
		}
		if projectScopeTextContainsAny(lower, "parser", "parse", "grammar") {
			add("internal/parser/**", "internal/ast/**")
		}
		if projectScopeTextContainsAny(lower, "evaluator", "evaluation", "evaluate", "eval", "json path", "jsonpath", "path semantics", "query semantics") {
			add("internal/evaluator/**", "internal/runtime/**", "internal/value/**", "internal/path/**", "internal/jsonpath/**")
		}
		if projectScopeTextContainsAny(lower, "built-in", "builtin", "builtins", "map/filter", "map filter", "lambda", "lambdas", "function library") {
			add("internal/builtins/**", "internal/builtin/**", "internal/functions/**", "internal/lambda/**")
		}
		if projectScopeTextContainsAny(lower, "cli", "command line", "file mode", "repl", "read-eval-print") {
			add("cmd/**", "internal/cli/**", "internal/repl/**")
		}
		if projectScopeTextContainsAny(lower, "error model", "diagnostic", "diagnostics") && !projectScopeTextContainsAny(lower, "lexer", "lexical", "tokenizer", "tokeniser") {
			add("internal/errors/**", "internal/diagnostics/**")
		}
		if len(out) > 0 {
			return normalizeProjectWriteScopePaths(out)
		}
	}
	if projectScopeTextContainsAny(lower, "foundation", "toolchain", "app shell", "shared config", "baseline test harness") {
		add(".gitignore", "package*.json", "tsconfig*.json", "vite.config.*", "vitest.config.*", "playwright.config.*", "index.html", "public/**", "src/main.*", "src/App.*", "src/styles.*", "src/styles/**", "src/ui/**", "tests/setup.*", "tests/test-utils/**", "tests/fixtures/**")
		return normalizeProjectWriteScopePaths(out)
	}
	if projectScopeTextContainsAny(lower, "editor", "rich-text", "rich text", "markdown", "shortcut", "autosave", "quote", "quotes", "dash replacement", "blockquote", "divider") {
		add("src/editor/**", "src/lib/editor/**", "tests/editor/**")
	}
	if projectScopeTextContainsAny(lower, "settings", "preferences", "quote style", "auto dash", "auto-dash") {
		add("src/settings/**", "tests/settings/**")
	}
	if projectScopeTextContainsAny(lower, "auth", "sign-in", "signin", "login", "oauth", "google sign") {
		add("src/auth/**", "tests/auth/**")
	}
	if projectScopeTextContainsAny(lower, "profile", "avatar", "author profile") {
		add("src/profile/**", "tests/profile/**")
	}
	if projectScopeTextContainsAny(lower, "article management", "my articles", "article list", "article lifecycle", "draft", "published", "archive", "archiving", "delete article", "article search") {
		add("src/articles/**", "tests/articles/**")
	}
	if projectScopeTextContainsAny(lower, "public article", "public route", "read-only", "readonly", "/p/", "slug", "share url", "viewer") {
		add("src/public/**", "src/routes/**", "tests/public/**")
	}
	if projectScopeTextContainsAny(lower, "import/export", "import article", "export article", "export json", "import json", "serialization") {
		add("src/import-export/**", "src/lib/import-export/**", "tests/import-export/**")
	}
	if len(out) > 0 {
		return normalizeProjectWriteScopePaths(out)
	}
	if projectScopeTextContainsAny(lower, "scaffold", "test harness") {
		add(".gitignore", "package*.json", "tsconfig*.json", "vite.config.*", "vitest.config.*", "playwright.config.*", "index.html", "public/**", "src/main.*", "src/App.*", "src/styles.*", "src/styles/**", "src/ui/**", "tests/setup.*", "tests/test-utils/**", "tests/fixtures/**")
		return normalizeProjectWriteScopePaths(out)
	}
	switch {
	case strings.Contains(lower, "pipeline") || strings.Contains(lower, "algorithm") || strings.Contains(lower, "image-processing") || strings.Contains(lower, "dither"):
		return []string{"src/lib/**", "tests/**"}
	case strings.Contains(lower, "ui") || strings.Contains(lower, "frontend") || strings.Contains(lower, "scaffold") || strings.Contains(lower, "browser"):
		return []string{".gitignore", "package*.json", "tsconfig*.json", "vite.config.*", "index.html", "public/**", "src/main.*", "src/App.*", "src/styles.*", "src/styles/**", "src/ui/**"}
	default:
		return nil
	}
}

func projectGoInterpreterScaffoldWriteScopeHints(task WorkspaceTaskRecord) []string {
	titleText := strings.ToLower(strings.TrimSpace(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.ProjectLane,
	}, "\n")))
	if titleText == "" {
		return nil
	}
	scaffoldByTitle := projectScopeTextContainsAny(titleText,
		"scaffold",
		"skeleton",
		"foundation",
		"repo shell",
		"repo skeleton",
		"module shell",
		"module skeleton",
		"shared go module",
		"seed shared go module",
	)
	fullText := strings.ToLower(strings.TrimSpace(task.TaskID + "\n" + task.Title + "\n" + task.Description + "\n" + strings.Join(task.Tags, " ") + "\n" + task.TaskRequirementsJSON))
	scaffoldByDescription := projectScopeTextContainsAny(fullText, "repo skeleton", "module skeleton", "shared go module") &&
		projectScopeTextContainsAny(fullText, "scaffold", "skeleton", "foundation", "seed")
	if !scaffoldByTitle && !scaffoldByDescription {
		return nil
	}
	if projectScopeTextContainsAny(titleText, "lexer", "lexical", "tokenizer", "tokeniser", "parser", "grammar", "ast", "evaluator", "evaluation", "built-in", "builtin", "builtins", "cli", "repl") {
		return nil
	}
	return normalizeProjectWriteScopePaths([]string{
		".gitignore",
		"README.md",
		"go.mod",
		"go.sum",
		"go.work",
		"go.work.sum",
		"docs/**",
		"testdata/**",
		"tests/scaffold_test.go",
		"cmd/README.md",
		"internal/README.md",
	})
}

func projectGoInterpreterPrimaryWriteScopeHints(task WorkspaceTaskRecord, extraPaths ...[]string) []string {
	text := strings.ToLower(strings.TrimSpace(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.ProjectLane,
	}, "\n")))
	if text == "" {
		return nil
	}
	var out []string
	add := func(paths ...string) {
		out = append(out, paths...)
	}
	hasLexer := projectScopeTextContainsAny(text, "lexer", "lexical", "tokenizer", "tokeniser", "token stream")
	hasParser := projectScopeTextContainsAny(text, "parser", "parse", "grammar", "ast")
	hasEvaluator := projectScopeTextContainsAny(text, "evaluator", "evaluation", "evaluate", " eval ", " eval-", "-eval", "json path", "jsonpath", "query semantics")
	if projectScopeTextContainsAny(text, "no-eval", "no eval", "not eval", "without eval", "no evaluator", "not evaluator", "without evaluator") {
		hasEvaluator = false
	}
	hasBuiltins := projectScopeTextContainsAny(text, "built-in", "builtin", "builtins", "map/filter", "map filter", "lambda semantics", "lambda runtime", "lambda execution", "function library")
	hasCLI := projectScopeTextContainsAny(text, "cli", "command line", "file mode", "repl", "read-eval-print")
	if hasLexer {
		add("internal/lexer/**", "internal/token/**", "internal/tokens/**")
	}
	if hasParser {
		add("internal/parser/**", "internal/ast/**")
	}
	if hasEvaluator {
		add("internal/eval/**", "internal/jsonctx/**", "internal/evaluator/**", "internal/runtime/**", "internal/value/**", "internal/path/**", "internal/jsonpath/**")
	}
	if hasBuiltins && !hasParser {
		add("internal/builtins/**", "internal/builtin/**", "internal/functions/**", "internal/lambda/**")
	}
	if hasCLI {
		if projectScopeTextContainsAny(text, "lua", "conformance", "oracle", "harness") {
			add(projectGoInterpreterHarnessCommandScope(extraPaths...)...)
		} else {
			add(projectGoInterpreterCommandScope(extraPaths...)...)
		}
		if projectScopeTextContainsAny(text, "readme", "reviewer-facing") {
			add("README.md")
		}
	}
	return normalizeProjectWriteScopePaths(out)
}

func projectLuaAcceptanceWriteScopeHints(task WorkspaceTaskRecord) []string {
	text := strings.ToLower(strings.TrimSpace(task.TaskID + "\n" + task.Title + "\n" + task.Description + "\n" + strings.Join(task.Tags, " ") + "\n" + task.TaskRequirementsJSON))
	if !strings.Contains(text, "ac-lua-") {
		return nil
	}
	var out []string
	add := func(paths ...string) {
		out = append(out, paths...)
	}
	has := func(id string) bool {
		return strings.Contains(text, strings.ToLower(id))
	}
	if has("AC-LUA-LEX-01") {
		add("internal/lexer/**", "internal/token/**", "internal/tokens/**")
	}
	if has("AC-LUA-PARSE-01") {
		add("internal/parser/**", "internal/ast/**")
	}
	if has("AC-LUA-SEM-01") {
		add("internal/eval/**", "internal/evaluator/**", "internal/runtime/**", "internal/value/**")
	}
	if has("AC-LUA-FUNC-01") {
		add("internal/eval/**", "internal/evaluator/**", "internal/runtime/**", "internal/value/**", "internal/functions/**")
	}
	if has("AC-LUA-TABLE-01") || has("AC-LUA-META-01") {
		add("internal/eval/**", "internal/evaluator/**", "internal/runtime/**", "internal/value/**", "internal/table/**", "internal/metatable/**")
	}
	if has("AC-LUA-STDLIB-01") {
		add("internal/stdlib/**", "internal/builtins/**", "internal/builtin/**", "internal/functions/**", "internal/runtime/**", "internal/value/**")
	}
	if has("AC-LUA-ERR-01") {
		add("internal/errors/**", "internal/diagnostics/**", "internal/runner/**")
	}
	if has("AC-LUA-CLI-01") {
		add("cmd/glua/**", "internal/cli/**", "internal/repl/**", "internal/runner/**", "scripts/**", "testdata/smoke/**", "tools/oracle/**", "README.md")
	}
	return normalizeProjectWriteScopePaths(out)
}

func projectLuaAcceptanceScopeShouldNarrow(paths, allowed []string) bool {
	paths = normalizeProjectWriteScopePaths(paths)
	allowed = normalizeProjectWriteScopePaths(allowed)
	if len(paths) == 0 || len(allowed) == 0 {
		return len(allowed) > 0
	}
	if projectWriteScopeHintsNeedSemanticNarrowing(paths) {
		return true
	}
	for _, path := range paths {
		if !projectWriteScopePathCoveredByAnyHint(path, allowed) {
			return true
		}
	}
	return false
}

func projectWriteScopePathCoveredByAnyHint(path string, hints []string) bool {
	path = runtimeProjectNormalizeWriteScopePath(path)
	if path == "" {
		return false
	}
	for _, hint := range normalizeProjectWriteScopePaths(hints) {
		if projectWriteScopePathCoveredByHint(path, hint) {
			return true
		}
	}
	return false
}

func projectWriteScopePathCoveredByHint(path, hint string) bool {
	path = strings.Trim(strings.TrimSpace(strings.ReplaceAll(path, `\`, `/`)), "/")
	hint = strings.Trim(strings.TrimSpace(strings.ReplaceAll(hint, `\`, `/`)), "/")
	normalizedPath := runtimeProjectNormalizeWriteScopePath(path)
	normalizedHint := runtimeProjectNormalizeWriteScopePath(hint)
	if normalizedHint == "" || normalizedHint == "*" || normalizedHint == "**" {
		return true
	}
	if normalizedPath == "" {
		return false
	}
	if normalizedPath == normalizedHint {
		return true
	}
	if strings.HasSuffix(hint, "/**") {
		prefix := strings.TrimSuffix(hint, "/**")
		normalizedPrefix := runtimeProjectNormalizeWriteScopePath(prefix)
		return normalizedPath == normalizedPrefix || strings.HasPrefix(normalizedPath, normalizedPrefix+"/")
	}
	if strings.HasSuffix(hint, "/*") {
		prefix := strings.TrimSuffix(hint, "/*")
		normalizedPrefix := runtimeProjectNormalizeWriteScopePath(prefix)
		if normalizedPath == normalizedPrefix || !strings.HasPrefix(normalizedPath, normalizedPrefix+"/") {
			return false
		}
		return !strings.Contains(strings.TrimPrefix(normalizedPath, normalizedPrefix+"/"), "/")
	}
	return false
}

func projectGoInterpreterCommandScope(pathSets ...[]string) []string {
	for _, paths := range pathSets {
		for _, path := range normalizeProjectWriteScopePaths(paths) {
			normalized := runtimeProjectNormalizeWriteScopePath(path)
			if normalized == "cmd" || strings.HasPrefix(normalized, "cmd/") {
				return normalizeProjectWriteScopePaths([]string{path, "internal/cli/**", "internal/repl/**"})
			}
		}
	}
	return []string{"cmd/**", "internal/cli/**", "internal/repl/**"}
}

func projectGoInterpreterHarnessCommandScope(pathSets ...[]string) []string {
	var out []string
	for _, paths := range pathSets {
		for _, path := range normalizeProjectWriteScopePaths(paths) {
			normalized := runtimeProjectNormalizeWriteScopePath(path)
			if normalized == "readme.md" ||
				normalized == "cmd" ||
				strings.HasPrefix(normalized, "cmd/") ||
				normalized == "internal/cli" ||
				strings.HasPrefix(normalized, "internal/cli/") ||
				normalized == "internal/repl" ||
				strings.HasPrefix(normalized, "internal/repl/") ||
				normalized == "internal/runner" ||
				strings.HasPrefix(normalized, "internal/runner/") ||
				normalized == "scripts" ||
				strings.HasPrefix(normalized, "scripts/") ||
				normalized == "testdata" ||
				strings.HasPrefix(normalized, "testdata/") ||
				normalized == "tools/oracle" ||
				strings.HasPrefix(normalized, "tools/oracle/") {
				out = append(out, path)
			}
		}
	}
	if len(out) > 0 {
		return normalizeProjectWriteScopePaths(out)
	}
	return append(projectGoInterpreterCommandScope(pathSets...), "scripts/**", "testdata/**", "tools/oracle/**")
}

func projectTaskLooksGoInterpreterScope(task WorkspaceTaskRecord, extraPaths ...[]string) bool {
	if projectScopePathsContainGoLayout(task.WriteScopeHints) || projectScopePathsContainGoLayout(projectWriteScopeHintsFromTaskRequirementsJSON(task.TaskRequirementsJSON)) {
		return true
	}
	for _, paths := range extraPaths {
		if projectScopePathsContainGoLayout(paths) {
			return true
		}
	}
	text := strings.ToLower(strings.TrimSpace(task.TaskID + "\n" + task.Title + "\n" + task.Description + "\n" + strings.Join(task.Tags, " ") + "\n" + task.TaskRequirementsJSON))
	if text == "" {
		return false
	}
	if projectSemanticTextHasToken(text, "rq", "golang", "lua") ||
		projectScopeTextContainsAny(text, ".go", "go module", "go.mod", "go sum", "go.sum", "go work", "go.work", "interpreter", "json path", "jsonpath", "lexer", "lexical", "tokenizer", "tokeniser", "token stream", "read-eval-print") {
		return true
	}
	return projectSemanticTextHasToken(text, "go") && !projectScopeTextContainsAny(text, "react", "vite", "frontend", "browser", "web app", "typescript", "javascript", "src/")
}

func projectScopePathsContainGoLayout(paths []string) bool {
	for _, path := range paths {
		normalized := runtimeProjectNormalizeWriteScopePath(path)
		switch normalized {
		case "cmd", "internal", "pkg", "go.mod", "go.sum", "go.work":
			return true
		}
		if strings.HasPrefix(normalized, "cmd/") ||
			strings.HasPrefix(normalized, "internal/") ||
			strings.HasPrefix(normalized, "pkg/") ||
			normalized == "scripts" ||
			strings.HasPrefix(normalized, "scripts/") ||
			normalized == "testdata" ||
			strings.HasPrefix(normalized, "testdata/") ||
			normalized == "tools/oracle" ||
			strings.HasPrefix(normalized, "tools/oracle/") {
			return true
		}
	}
	return false
}

func projectWriteScopeHintsNeedSemanticNarrowing(paths []string) bool {
	for _, path := range paths {
		switch runtimeProjectNormalizeWriteScopePath(path) {
		case "*", "**", "src", "app", "web", "client", "tests", "test", "cmd", "internal", "pkg", "lib", "go.mod", "go.sum", "go.work", "readme", "readme.md", "**/*test.go":
			return true
		}
	}
	return false
}

func projectScopeTextContainsAny(text string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func projectSemanticTextHasToken(text string, tokens ...string) bool {
	if text == "" {
		return false
	}
	normalized := strings.NewReplacer("-", " ", "_", " ", "/", " ", ".", " ").Replace(strings.ToLower(text))
	fields := strings.Fields(normalized)
	for _, field := range fields {
		for _, token := range tokens {
			if field == strings.ToLower(strings.TrimSpace(token)) {
				return true
			}
		}
	}
	return false
}

func projectWriteScopeJSONFromPaths(paths []string) string {
	if invalid := invalidProjectWriteScopeHints(paths); len(invalid) > 0 {
		return ""
	}
	paths = normalizeProjectWriteScopePaths(paths)
	if len(paths) == 0 {
		return ""
	}
	raw, err := json.Marshal(map[string][]string{"paths": paths})
	if err != nil {
		return ""
	}
	return string(raw)
}

func validateProjectWriteScopeJSON(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fmt.Errorf("write_scope_json is required")
	}
	if !json.Valid([]byte(raw)) {
		return fmt.Errorf("write_scope_json must be valid JSON")
	}
	paths := projectWriteScopePathsFromJSON(raw)
	if len(paths) == 0 {
		return fmt.Errorf("write_scope_json must include non-empty paths")
	}
	if invalid := invalidProjectWriteScopeHints(paths); len(invalid) > 0 {
		return fmt.Errorf("write_scope_json paths must be repository-relative path globs, not prose labels: %s", strings.Join(invalid, ", "))
	}
	return nil
}

func projectWriteScopePathsFromJSON(raw string) []string {
	var decoded any
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &decoded); err != nil {
		return nil
	}
	var paths []string
	var walk func(any)
	walk = func(value any) {
		switch typed := value.(type) {
		case string:
			paths = append(paths, typed)
		case []any:
			for _, item := range typed {
				walk(item)
			}
		}
	}
	if object, ok := decoded.(map[string]any); ok {
		for _, key := range []string{"paths", "files", "path_prefixes", "write_paths", "scopes"} {
			walk(object[key])
		}
		return paths
	}
	walk(decoded)
	return paths
}

func normalizeProjectWriteScopePaths(paths []string) []string {
	seen := map[string]bool{}
	cleaned := make([]string, 0, len(paths))
	for _, path := range paths {
		path = strings.TrimSpace(path)
		path = strings.Trim(path, "`\"'[]{} ")
		path = strings.ReplaceAll(path, `\`, `/`)
		path = strings.TrimPrefix(path, "./")
		if path == "" || invalidProjectWriteScopePathReason(path) != "" {
			continue
		}
		if seen[path] {
			continue
		}
		seen[path] = true
		cleaned = append(cleaned, path)
	}
	return cleaned
}

func invalidProjectWriteScopeHints(paths []string) []string {
	var invalid []string
	for _, path := range paths {
		path = strings.TrimSpace(path)
		path = strings.Trim(path, "`\"'[]{} ")
		if path == "" {
			continue
		}
		if reason := invalidProjectWriteScopePathReason(path); reason != "" {
			invalid = append(invalid, fmt.Sprintf("%q (%s)", path, reason))
		}
	}
	return invalid
}

func invalidProjectWriteScopePathReason(path string) string {
	path = strings.TrimSpace(strings.ReplaceAll(path, `\`, `/`))
	path = strings.Trim(path, "`\"'[]{} ")
	if path == "" || path == "." {
		return ""
	}
	path = strings.TrimPrefix(path, "./")
	switch {
	case strings.HasPrefix(path, "/"):
		return "absolute paths are not allowed"
	case strings.Contains(path, ":"):
		return "paths must be repo-relative and cannot contain ':'"
	case strings.Contains(path, ".."):
		return "parent-directory traversal is not allowed"
	case strings.IndexFunc(path, unicode.IsSpace) >= 0:
		return "whitespace usually indicates prose rather than a repo path"
	}
	if path == "*" || path == "**" || strings.ContainsAny(path, "/*.") {
		return ""
	}
	if projectWriteScopeAllowedSingleSegment(path) {
		return ""
	}
	return "single-segment scope is ambiguous; use a repo path or glob such as src/**"
}

func projectWriteScopeAllowedSingleSegment(path string) bool {
	switch strings.ToLower(strings.TrimSpace(path)) {
	case "agent", "api", "app", "assets", "cmd", "components", "data", "deploy", "docs", "examples", "internal", "lib", "pkg", "plans", "protocols", "public", "scripts", "spec", "src", "static", "styles", "tasks", "test", "tests", "tools", "ui", "web",
		"dockerfile", "makefile", "readme", "license":
		return true
	default:
		return false
	}
}

func projectWriteScopeJSONIsBroad(writeScopeJSON string) bool {
	var payload struct {
		Paths []string `json:"paths"`
	}
	if json.Unmarshal([]byte(strings.TrimSpace(writeScopeJSON)), &payload) != nil {
		return false
	}
	paths := normalizeProjectWriteScopePaths(payload.Paths)
	return len(paths) == 1 && (paths[0] == "**" || paths[0] == "*")
}

func projectClaimAdmissionShouldPreferRoleScopeForTrustFirstTask(task WorkspaceTaskRecord, taskWriteScopeJSON string, role ProjectRoleRecord) bool {
	roleWriteScopeJSON := strings.TrimSpace(role.WriteScopeJSON)
	taskPaths := projectRoleAssignWriteScopePaths(taskWriteScopeJSON)
	rolePaths := projectRoleAssignWriteScopePaths(roleWriteScopeJSON)
	if len(taskPaths) == 0 || len(rolePaths) == 0 {
		return false
	}
	if projectWriteScopeJSONIsBroad(roleWriteScopeJSON) {
		return false
	}
	if !projectClaimAdmissionScopeOverrideAnchored(taskWriteScopeJSON, roleWriteScopeJSON) {
		return false
	}
	if projectClaimAdmissionLooksBoundaryTransitionRole(role) {
		return true
	}
	if !projectClaimAdmissionWriteScopeLooksCandidateWide(taskPaths) {
		return false
	}
	if projectClaimAdmissionLooksRevisionScopeRepair(task) {
		return true
	}
	return projectClaimAdmissionLooksScopeRepairRole(role)
}

func projectClaimAdmissionScopeOverrideAnchored(taskWriteScopeJSON, overrideWriteScopeJSON string) bool {
	taskPaths := projectRoleAssignWriteScopePaths(taskWriteScopeJSON)
	overridePaths := projectRoleAssignWriteScopePaths(overrideWriteScopeJSON)
	if len(taskPaths) == 0 || len(overridePaths) == 0 {
		return false
	}
	return projectRoleAssignScopeCovers(overridePaths, taskPaths) ||
		projectRoleAssignScopeCovers(taskPaths, overridePaths) ||
		runtimeProjectWriteScopesOverlap(taskPaths, overridePaths)
}

func projectClaimAdmissionShouldPreferRoleScopeForRevision(task WorkspaceTaskRecord, taskWriteScopeJSON, roleWriteScopeJSON string) bool {
	return projectClaimAdmissionShouldPreferRoleScopeForTrustFirstTask(task, taskWriteScopeJSON, ProjectRoleRecord{
		RoleType:       "IMPLEMENTER",
		Status:         "ACTIVE",
		WriteScopeJSON: roleWriteScopeJSON,
	})
}

func projectClaimAdmissionLooksScopeRepairRole(role ProjectRoleRecord) bool {
	text := strings.ToLower(strings.TrimSpace(strings.Join([]string{
		role.RoleID,
		role.Summary,
		role.UpdatedBy,
	}, "\n")))
	if text == "" {
		return false
	}
	if strings.Contains(text, "stale role") ||
		strings.Contains(text, "stale repair role") ||
		strings.Contains(text, "do not override") ||
		strings.Contains(text, "not override") ||
		strings.Contains(text, "should not override") ||
		strings.Contains(text, "must not override") {
		return false
	}
	if projectClaimAdmissionLooksBoundaryTransitionRole(role) {
		return true
	}
	hasRepairIntent := strings.Contains(text, "claim repair") ||
		strings.Contains(text, "blocked admission") ||
		strings.Contains(text, "scope repair") ||
		strings.Contains(text, "boundary expansion") ||
		strings.Contains(text, "expand_boundary") ||
		strings.Contains(text, "expanded boundary") ||
		strings.Contains(text, "boundary transition") ||
		strings.Contains(text, "side-effect resolution") ||
		strings.Contains(text, "side effect resolution") ||
		((strings.Contains(text, "repair") || strings.Contains(text, "repaired")) &&
			(strings.Contains(text, "narrow") || strings.Contains(text, "narrowing"))) ||
		((strings.Contains(text, "expand") || strings.Contains(text, "expanded") || strings.Contains(text, "expansion") || strings.Contains(text, "widen") || strings.Contains(text, "broaden")) &&
			(strings.Contains(text, "scope") || strings.Contains(text, "boundary") || strings.Contains(text, "ownership") || strings.Contains(text, "owner")))
	if !hasRepairIntent {
		return false
	}
	return strings.Contains(text, "scope") ||
		strings.Contains(text, "write_scope") ||
		strings.Contains(text, "write scope") ||
		strings.Contains(text, "ownership") ||
		strings.Contains(text, "owner")
}

func projectClaimAdmissionLooksBoundaryTransitionRole(role ProjectRoleRecord) bool {
	text := strings.ToLower(strings.TrimSpace(strings.Join([]string{
		role.RoleID,
		role.Summary,
		role.UpdatedBy,
	}, "\n")))
	if text == "" {
		return false
	}
	if strings.Contains(text, "stale role") ||
		strings.Contains(text, "do not override") ||
		strings.Contains(text, "not override") ||
		strings.Contains(text, "should not override") ||
		strings.Contains(text, "must not override") {
		return false
	}
	return strings.Contains(text, "expand_boundary") ||
		strings.Contains(text, "boundary expansion") ||
		strings.Contains(text, "expanded boundary") ||
		strings.Contains(text, "boundary transition") ||
		strings.Contains(text, "side-effect resolution") ||
		strings.Contains(text, "side effect resolution") ||
		strings.Contains(text, "abpc side-effect") ||
		strings.Contains(text, "abpc side effect")
}

func projectClaimAdmissionLooksRevisionScopeRepair(task WorkspaceTaskRecord) bool {
	if !runtimeProjectLaneRequiresImplementationGate(task.ProjectLane) {
		return false
	}
	text := strings.ToLower(strings.TrimSpace(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		strings.Join(task.Tags, " "),
	}, "\n")))
	if text == "" {
		return false
	}
	hasRevisionIntent := runtimeOwnerBoundTaskHasTag(task, "revision") ||
		strings.Contains(text, "revision") ||
		strings.Contains(text, "revise") ||
		strings.Contains(text, "unblock integration candidate") ||
		strings.Contains(text, "blocked candidate") ||
		strings.Contains(text, "validation-followup") ||
		strings.Contains(text, "validation followup")
	if !hasRevisionIntent {
		return false
	}
	return strings.Contains(text, "patch queue") ||
		strings.Contains(text, "patch-queue") ||
		strings.Contains(text, "patchq-") ||
		strings.Contains(text, "patchitem-") ||
		strings.Contains(text, "queue_id") ||
		strings.Contains(text, "item_id") ||
		strings.Contains(text, "branch_id") ||
		strings.Contains(text, "blocked candidate")
}

func projectClaimAdmissionWriteScopeLooksCandidateWide(paths []string) bool {
	hasSourceRegion := false
	hasRootSurface := false
	for _, path := range paths {
		normalized := strings.Trim(strings.ToLower(strings.TrimSpace(path)), "/")
		switch normalized {
		case "*", "**", "src", "src/**", "app", "app/**", "web", "web/**", "client", "client/**":
			return true
		}
		if normalized == "" {
			continue
		}
		root := normalized
		if idx := strings.Index(root, "/"); idx >= 0 {
			root = root[:idx]
		}
		switch root {
		case "src", "app", "web", "client":
			hasSourceRegion = true
		case "public":
			hasRootSurface = true
		}
		switch normalized {
		case "package.json", "package-lock.json", "pnpm-lock.yaml", "yarn.lock", "bun.lockb", "vite.config.*", "vite.config.ts", "vite.config.js", "tsconfig*.json", "tsconfig.json", "index.html", "eslint.config.js", "eslint.config.*":
			hasRootSurface = true
		}
	}
	return hasSourceRegion && hasRootSurface && len(paths) >= 4
}

func runtimeProjectLaneRequiresImplementationGate(projectLane string) bool {
	switch strings.ToLower(strings.TrimSpace(projectLane)) {
	case "implementation", "implement", "coding", "code", "frontend", "front-end", "ui", "backend", "back-end", "api", "fullstack", "full-stack":
		return true
	default:
		return false
	}
}

func runtimeProjectTaskRequiresImplementationGate(task WorkspaceTaskRecord) bool {
	if runtimeProjectLaneBypassesImplementationGate(task.ProjectLane) {
		return false
	}
	if runtimeTaskBypassesImplementationGateByStructuredContract(task) {
		return false
	}
	if task.RequiresProjectGate != nil && *task.RequiresProjectGate {
		return true
	}
	return runtimeProjectLaneRequiresImplementationGate(task.ProjectLane)
}

func runtimeTaskBypassesImplementationGateByStructuredContract(task WorkspaceTaskRecord) bool {
	for _, hint := range task.WriteScopeHints {
		if strings.TrimSpace(hint) != "" {
			return false
		}
	}
	return runtimeTaskStructuredProjectEvidenceWork(task)
}

func runtimeTaskStructuredProjectEvidenceWork(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if !runtimeTaskRequirementsContainWorkMode(task, "review", "synthesis", "validation") {
		return false
	}
	if runtimeStringSliceContainsAnyFold(task.Tags,
		"docs",
		"documentation",
		"planning",
		"plan-review",
		"plan_review",
		"product-contract",
		"product_contract",
		"spec-fidelity",
		"spec_fidelity",
		"requirements",
	) {
		return true
	}
	return runtimeStringSliceContainsAnyFold(runtimeTaskRequirementsStringSlice(task, "preferred_skills", "required_skills"),
		"docs",
		"documentation",
		"planning",
		"spec-fidelity",
		"spec_fidelity",
		"requirements",
	)
}

func runtimeTaskRequirementsContainWorkMode(task WorkspaceTaskRecord, modes ...string) bool {
	return runtimeStringSliceContainsAnyFold(runtimeTaskRequirementsStringSlice(task, "required_work_modes", "preferred_work_modes"), modes...)
}

func runtimeTaskRequirementsStringSlice(task WorkspaceTaskRecord, keys ...string) []string {
	payload := projectTaskRequirementsPayload(task.TaskRequirementsJSON)
	var values []string
	for _, key := range keys {
		values = append(values, runtimeStringSliceFromAny(payload[key])...)
	}
	return values
}

func runtimeStringSliceFromAny(raw any) []string {
	switch typed := raw.(type) {
	case []string:
		return runtimeNormalizeStringSlice(typed)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if str, ok := item.(string); ok {
				out = append(out, str)
			}
		}
		return runtimeNormalizeStringSlice(out)
	case string:
		return runtimeNormalizeStringSlice([]string{typed})
	default:
		return nil
	}
}

func runtimeNormalizeStringSlice(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, value)
	}
	return out
}

func runtimeStringSliceContainsAnyFold(values []string, wants ...string) bool {
	for _, value := range values {
		for _, want := range wants {
			if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(want)) {
				return true
			}
		}
	}
	return false
}

func runtimeProjectClaimRequiredRoleType(projectLane string) string {
	lane := strings.ToLower(strings.TrimSpace(projectLane))
	if lane == "" {
		return ""
	}
	if runtimeProjectLaneRequiresImplementationGate(lane) {
		return "IMPLEMENTER"
	}
	switch lane {
	case "review", "reviewer":
		return "REVIEWER"
	case "integration", "integrator", "integrate":
		return "INTEGRATOR"
	default:
		return ""
	}
}

func runtimeProjectLaneBypassesImplementationGate(projectLane string) bool {
	switch strings.ToLower(strings.TrimSpace(projectLane)) {
	case "strategy", "strategic", "planning", "plan", "spec", "specification", "requirements", "design", "framing", "review", "qa", "test", "testing", "verification", "validation", "acceptance", "integration", "integrator", "integrate", "synthesis", "summary", "summarization", "documentation", "docs", "handoff", "report", "final":
		return true
	default:
		return false
	}
}

func runtimeProjectPhaseAllowsImplementationWork(phase string) bool {
	switch strings.ToUpper(strings.TrimSpace(phase)) {
	case "IMPLEMENTATION", "REVIEW", "INTEGRATION", "VALIDATION":
		return true
	default:
		return false
	}
}

func projectClaimBranchName(agentID, projectID, taskID string) string {
	agent := compactRefSegment("agent", agentID)
	projectHash := shortRefHash(firstNonEmpty(projectID, "project"))
	taskHash := shortRefHash(firstNonEmpty(taskID, "manual"))
	return "agent-" + agent + "-p-" + projectHash + "-t-" + taskHash
}

func projectClaimSuccessorBranchName(agentID, projectID, taskID string, predecessor ProjectBranchRecord) string {
	base := projectClaimBranchName(agentID, projectID, taskID)
	lineage := firstNonEmpty(strings.TrimSpace(predecessor.BranchID), strings.TrimSpace(predecessor.BranchName), strings.TrimSpace(predecessor.HeadSHA), "ready")
	return base + "-r-" + shortRefHash(lineage)
}

func projectClaimLegacyBranchName(agentID, projectID, taskID string) string {
	return "agent/" + compactRefSegment("agent", agentID) + "/" + compactRefSegment("project", firstNonEmpty(projectID, "project")) + "/" + compactRefSegment("task", firstNonEmpty(taskID, "manual"))
}

func compactRefSegment(prefix, value string) string {
	const maxRefSegmentLen = 32
	raw := firstNonEmpty(strings.TrimSpace(value), strings.TrimSpace(prefix), "x")
	segment := sanitizeRefSegment(raw)
	if len(segment) <= maxRefSegmentLen {
		return segment
	}
	hash := shortRefHash(raw)
	keep := maxRefSegmentLen - len(hash) - 1
	if keep < 1 {
		return hash[:maxRefSegmentLen]
	}
	head := strings.Trim(segment[:keep], "-.")
	if head == "" {
		head = sanitizeRefSegment(firstNonEmpty(prefix, "x"))
	}
	if len(head) > keep {
		head = strings.Trim(head[:keep], "-.")
	}
	if head == "" {
		head = "x"
	}
	return head + "-" + hash
}

func shortRefHash(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:])[:10]
}

func sanitizeRefSegment(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '.'
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
	segment := strings.Trim(b.String(), "-.")
	if segment == "" {
		segment = "x"
	}
	if len(segment) > 80 {
		segment = strings.Trim(segment[:80], "-.")
	}
	return segment
}

func runtimeMachineID() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		return "unknown-machine"
	}
	return sanitizeRefSegment(host)
}

func sameRuntimePath(left, right string) bool {
	left = strings.TrimSpace(strings.ReplaceAll(left, `\`, `/`))
	right = strings.TrimSpace(strings.ReplaceAll(right, `\`, `/`))
	return strings.EqualFold(strings.TrimRight(left, "/"), strings.TrimRight(right, "/"))
}

func validateProjectCheckoutWorkdir(ctx context.Context, localPath string, repo ProjectRepositoryRecord) error {
	localPath = strings.TrimSpace(localPath)
	if localPath == "" {
		return fmt.Errorf("project claim admission requires a local workdir for repo %s", strings.TrimSpace(repo.RepoID))
	}
	info, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("project claim admission workdir %q is not accessible: %w", localPath, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("project claim admission workdir %q is not a directory", localPath)
	}
	remoteURL := strings.TrimSpace(repo.RemoteURL)
	if remoteURL == "" {
		return nil
	}
	root, err := exec.CommandContext(ctx, "git", "-C", localPath, "rev-parse", "--show-toplevel").Output()
	if err != nil || strings.TrimSpace(string(root)) == "" {
		return fmt.Errorf("project claim admission workdir %q is not a git checkout for repo %s", localPath, strings.TrimSpace(repo.RepoID))
	}
	remotes, err := exec.CommandContext(ctx, "git", "-C", localPath, "remote", "-v").Output()
	if err != nil {
		return fmt.Errorf("project claim admission cannot inspect git remotes for %q: %w", localPath, err)
	}
	if !remoteListContainsRepo(string(remotes), remoteURL) {
		return fmt.Errorf("project claim admission workdir %q remotes do not match project repo %s", localPath, remoteURL)
	}
	return nil
}

func remoteListContainsRepo(remotes, want string) bool {
	want = normalizeGitRemoteForCompare(want)
	if want == "" {
		return true
	}
	for _, line := range strings.Split(remotes, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if normalizeGitRemoteForCompare(fields[1]) == want {
			return true
		}
	}
	return false
}

func normalizeGitRemoteForCompare(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimSuffix(value, ".git")
	value = strings.TrimPrefix(value, "https://")
	value = strings.TrimPrefix(value, "http://")
	value = strings.TrimPrefix(value, "ssh://")
	if strings.HasPrefix(value, "git@github.com:") {
		value = "github.com/" + strings.TrimPrefix(value, "git@github.com:")
	}
	return strings.ToLower(strings.Trim(value, "/"))
}
