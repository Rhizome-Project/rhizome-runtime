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
	completionCoordinationGateEvidence      = "completion_coordination_gate"
	completionCoordinationStateRequested    = "requested"
	completionCoordinationStateReviewReady  = "review_received"
	completionCoordinationRequestTimeoutSec = 300
)

func (r *Runtime) enforceCompletionCoordinationGate(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, trace *TaskRunTrace, workPacket *AgentWorkPacket) (StructuredTaskResult, bool, error) {
	if normalizeOutcome(result.Outcome) != "completed" {
		return result, false, nil
	}
	if gated, applied, err := r.enforceProjectCoordinationCompletionLivenessGate(ctx, task, session, runID, result, workPacket); applied || err != nil {
		return gated, applied, err
	}
	if !r.completionCoordinationTaskRequiresGate(task) {
		return result, false, nil
	}
	if runtimeTrustFirst(r.cfg) {
		if err := r.writeCompletionCoordinationAdvisoryStep(ctx, task, session, runID, result, workPacket, "trust_first_peer_review_advisory", "peer review is advisory in trust_first mode"); err != nil {
			return result, false, err
		}
		return result, false, nil
	}
	state := r.completionCoordinationScratch()
	stateTaskID := strings.TrimSpace(state.CompletionCoordinationTaskID)
	taskID := strings.TrimSpace(task.TaskID)
	if stateTaskID != "" && stateTaskID != taskID {
		if err := r.clearCompletionCoordinationScratch(ctx, stateTaskID); err != nil {
			return result, false, err
		}
		state = r.completionCoordinationScratch()
	}
	if strings.TrimSpace(state.CompletionCoordinationTaskID) == taskID {
		switch strings.TrimSpace(state.CompletionCoordinationState) {
		case completionCoordinationStateReviewReady:
			if err := r.clearCompletionCoordinationScratch(ctx, task.TaskID); err != nil {
				return result, false, err
			}
			return result, false, nil
		case completionCoordinationStateRequested:
			gated, applied, err := r.handleExistingCompletionCoordinationRequest(ctx, task, session, runID, result, workPacket, state)
			if applied || err != nil {
				return gated, applied, err
			}
		}
	}

	if completionCoordinationTraceSatisfied(trace) {
		gated := demoteCompletionForCoordination(result,
			"Completion deferred: verify peer response",
			"An agent_request or reviewer route was attempted in this cycle. Wait for a successful peer response, incorporate it, and then complete in a later cycle.",
		)
		if err := r.writeCompletionCoordinationGateStep(ctx, task, session, runID, gated, workPacket, "", "", "tool_attempt_observed", "peer tool attempt requires durable response before completion"); err != nil {
			return result, false, err
		}
		return gated, true, nil
	}

	peer, ok := r.selectCompletionCoordinationPeer(ctx, state.CompletionCoordinationPeerID)
	if !ok {
		gated := demoteCompletionForCoordination(result,
			"Completion deferred: no peer available",
			fmt.Sprintf("Completion for task %s requires project coordination, but no eligible peer agent is currently available. Keep the task active, surface reviewer scarcity/blocker context, and retry peer review before completing.", task.TaskID),
		)
		if err := r.writeCompletionCoordinationGateStep(ctx, task, session, runID, gated, workPacket, "", "", "peer_unavailable", "no eligible peer available for required project gate"); err != nil {
			return result, false, err
		}
		return gated, true, nil
	}

	created, err := r.requestCompletionCoordinationReview(ctx, task, session, runID, result, peer)
	if err != nil {
		log.Printf("[runtime] completion coordination request failed for task %s: %v", task.TaskID, err)
		gated := demoteCompletionForCoordination(result,
			"Completion deferred: peer coordination request failed",
			fmt.Sprintf("Completion for task %s requires peer coordination, but the runtime could not queue a peer request: %v. Use agent_request to ask an online peer for bounded review before completing.", task.TaskID, err),
		)
		if writeErr := r.writeCompletionCoordinationGateStep(ctx, task, session, runID, gated, workPacket, "", "", "request_failed", err.Error()); writeErr != nil {
			return result, false, writeErr
		}
		return gated, true, nil
	}

	signal := fmt.Sprintf("SYSTEM COORDINATION GATE: completion for task %s was deferred. Runtime queued peer review request %s to %s. Do not complete this task until peer feedback is received and incorporated.", task.TaskID, created.RequestID, peer.AgentID)
	if err := r.setCompletionCoordinationScratch(ctx, task, session, runID, peer.AgentID, created.RequestID, completionCoordinationStateRequested, "", signal); err != nil {
		return result, false, err
	}

	gated := demoteCompletionForCoordination(result,
		"Completion deferred pending peer review",
		fmt.Sprintf("Runtime queued peer review request %s to %s before allowing terminal completion. Wait for peer feedback, incorporate it, and only then return outcome=completed.", created.RequestID, peer.AgentID),
	)
	if err := r.writeCompletionCoordinationGateStep(ctx, task, session, runID, gated, workPacket, peer.AgentID, created.RequestID, completionCoordinationStateRequested, "peer review requested"); err != nil {
		return result, false, err
	}
	return gated, true, nil
}

func (r *Runtime) enforceProjectCoordinationCompletionLivenessGate(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, workPacket *AgentWorkPacket) (StructuredTaskResult, bool, error) {
	if r == nil || r.cfg.Mode != RuntimeModeDaemon || r.client == nil {
		return result, false, nil
	}
	if !projectCoordinationCompletionTaskNeedsLivenessGate(task) {
		return result, false, nil
	}
	projectID := strings.TrimSpace(task.ProjectID)
	coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, projectID)
	if err != nil {
		detail := fmt.Sprintf("Project %s completion liveness could not be read before closing %s: %v", projectID, strings.TrimSpace(task.TaskID), err)
		gated := demoteProjectCoordinationCompletion(result, detail)
		if writeErr := r.writeProjectCoordinationCompletionLivenessStep(ctx, task, session, runID, gated, workPacket, "snapshot_unavailable", detail); writeErr != nil {
			return result, false, writeErr
		}
		return gated, true, nil
	}
	detail, blocked := projectCoordinationCompletionLivenessBlocker(task, coordination)
	if !blocked {
		return result, false, nil
	}
	gated := demoteProjectCoordinationCompletion(result, detail)
	if err := r.writeProjectCoordinationCompletionLivenessStep(ctx, task, session, runID, gated, workPacket, "project_liveness_open", detail); err != nil {
		return result, false, err
	}
	return gated, true, nil
}

func projectCoordinationCompletionTaskNeedsLivenessGate(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if isStrategicLeadCorrectionTask(task) {
		return false
	}
	if runtimeTaskCanStewardProjectLead(task) {
		return true
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if lane == "coordination" || lane == "finalization" || lane == "release" {
		return strings.EqualFold(strings.TrimSpace(task.TaskKind), "COORDINATION")
	}
	return false
}

func projectCoordinationCompletionLivenessBlocker(task WorkspaceTaskRecord, coordination ProjectCoordinationRecord) (string, bool) {
	var blockers []string
	if openTasks := projectCoordinationOpenSiblingTaskSummaries(task, coordination); len(openTasks) > 0 {
		blockers = append(blockers, "open_same_project_tasks="+strings.Join(openTasks, ", "))
	}
	if branchBlockers := projectCoordinationOpenBranchSummaries(coordination); len(branchBlockers) > 0 {
		blockers = append(blockers, "open_project_branches="+strings.Join(branchBlockers, ", "))
	}
	if queueBlockers := projectCoordinationOpenPatchQueueSummaries(coordination); len(queueBlockers) > 0 {
		blockers = append(blockers, "open_patch_queue="+strings.Join(queueBlockers, ", "))
	}
	if len(blockers) == 0 {
		return "", false
	}
	taskID := strings.TrimSpace(task.TaskID)
	detail := fmt.Sprintf("Project root/strategy completion for %s is deferred because project %s still has unresolved execution/review/integration state: %s. Keep the coordination lane alive, wake or delegate the remaining work, resolve real blockers, and complete only after implementation/review/integration/QA evidence has converged.", firstNonEmpty(taskID, "current task"), strings.TrimSpace(task.ProjectID), strings.Join(blockers, "; "))
	return detail, true
}

func projectCoordinationOpenSiblingTaskSummaries(current WorkspaceTaskRecord, coordination ProjectCoordinationRecord) []string {
	currentID := strings.TrimSpace(current.TaskID)
	var out []string
	for _, task := range coordination.Tasks {
		taskID := strings.TrimSpace(task.TaskID)
		if taskID == "" || taskID == currentID {
			continue
		}
		if projectCoordinationTaskStatusTerminal(task.Status) {
			continue
		}
		if !projectCoordinationSiblingTaskBlocksCompletion(task) {
			continue
		}
		out = append(out, projectCoordinationTaskSummary(task))
		if len(out) >= 8 {
			out = append(out, "...")
			break
		}
	}
	return out
}

func projectCoordinationSiblingTaskBlocksCompletion(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.TaskID) == "" {
		return false
	}
	if projectCoordinationTaskStatusTerminal(task.Status) {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(task.TaskKind), "EXECUTION") {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(task.ProjectLane)) {
	case "implementation", "integration", "review", "verification", "validation", "qa", "quality", "test", "testing":
		return true
	default:
		return false
	}
}

func projectCoordinationTaskSummary(task WorkspaceTaskRecord) string {
	parts := []string{strings.TrimSpace(task.TaskID)}
	if status := strings.ToUpper(strings.TrimSpace(task.Status)); status != "" {
		parts = append(parts, status)
	}
	if lane := strings.ToLower(strings.TrimSpace(task.ProjectLane)); lane != "" {
		parts = append(parts, "lane:"+lane)
	}
	return strings.Join(parts, "/")
}

func projectCoordinationTaskStatusTerminal(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "DONE", "COMPLETED", "RESOLVED", "CANCELLED", "CANCELED", "CLOSED", "ARCHIVED", "FAILED":
		return true
	default:
		return false
	}
}

func projectCoordinationOpenBranchSummaries(coordination ProjectCoordinationRecord) []string {
	var out []string
	for _, branch := range coordination.Branches {
		if projectCoordinationBranchStatusTerminal(branch.Status) {
			continue
		}
		branchID := firstNonEmpty(strings.TrimSpace(branch.BranchID), strings.TrimSpace(branch.BranchName))
		if branchID == "" {
			continue
		}
		status := firstNonEmpty(strings.ToUpper(strings.TrimSpace(branch.Status)), "UNKNOWN")
		summary := branchID + "/" + status
		if taskID := strings.TrimSpace(branch.ActiveTaskID); taskID != "" {
			summary += "/task:" + taskID
		}
		out = append(out, summary)
		if len(out) >= 8 {
			out = append(out, "...")
			break
		}
	}
	return out
}

func projectCoordinationBranchStatusTerminal(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "", "MERGED", "CANCELLED", "CANCELED", "ABANDONED", "CLOSED", "ARCHIVED", "SUPERSEDED":
		return true
	default:
		return false
	}
}

func projectCoordinationOpenPatchQueueSummaries(coordination ProjectCoordinationRecord) []string {
	mergedBranches := map[string]bool{}
	for _, branch := range coordination.Branches {
		if strings.EqualFold(strings.TrimSpace(branch.Status), "MERGED") {
			if branchID := strings.TrimSpace(branch.BranchID); branchID != "" {
				mergedBranches[branchID] = true
			}
		}
	}
	var out []string
	for _, item := range coordination.PatchQueueItems {
		state := strings.ToUpper(strings.TrimSpace(item.State))
		if !projectCoordinationPatchQueueStateBlocksCompletion(state, mergedBranches[strings.TrimSpace(item.BranchID)]) {
			continue
		}
		itemID := firstNonEmpty(strings.TrimSpace(item.ItemID), strings.TrimSpace(item.QueueID), strings.TrimSpace(item.BranchID))
		if itemID == "" {
			continue
		}
		summary := itemID + "/" + firstNonEmpty(state, "UNKNOWN")
		if taskID := strings.TrimSpace(item.TaskID); taskID != "" {
			summary += "/task:" + taskID
		}
		out = append(out, summary)
		if len(out) >= 8 {
			out = append(out, "...")
			break
		}
	}
	return out
}

func projectCoordinationPatchQueueStateBlocksCompletion(state string, branchMerged bool) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "PROPOSED", "CLAIMED", "BLOCKED", "RETRYING":
		return true
	case "ACCEPTED":
		return !branchMerged
	default:
		return false
	}
}

func demoteProjectCoordinationCompletion(result StructuredTaskResult, detail string) StructuredTaskResult {
	gated := demoteCompletionForCoordination(result,
		"Project completion deferred: project still has live work",
		"Keep the project coordination/root lane active. Re-read project coordination, delegate or claim the named open tasks, move ready branches through review/integration, and only complete after the project has no unresolved implementation/review/integration/QA state.",
	)
	if strings.TrimSpace(detail) != "" {
		if strings.TrimSpace(gated.Details) != "" {
			gated.Details = strings.TrimSpace(gated.Details) + "\n\n" + strings.TrimSpace(detail)
		} else {
			gated.Details = strings.TrimSpace(detail)
		}
	}
	return gated
}

func (r *Runtime) writeProjectCoordinationCompletionLivenessStep(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, workPacket *AgentWorkPacket, state, detail string) error {
	if r == nil || r.client == nil || strings.TrimSpace(runID) == "" {
		return nil
	}
	_, err := r.client.WriteExecutionStep(ctx, ExecutionStepWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		Phase:       "VERIFY",
		Title:       "Defer project completion until project liveness converges",
		Summary:     result.Summary,
		Status:      "ACTIVE",
		SortOrder:   24,
		Evidence:    prependCapabilitySnapshotEvidence(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID), []string{completionCoordinationGateEvidence, "project_completion_liveness"}),
		Verification: map[string]any{
			"capability_snapshot_id":     daemonRunCapabilitySnapshotID(r.cfg, task, session, runID),
			"capability_snapshot_ref":    capabilitySnapshotEvidenceRef(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID)),
			"coordination_gate":          "project_completion_liveness",
			"state":                      strings.TrimSpace(state),
			"detail":                     strings.TrimSpace(detail),
			"canonical_mutation_applied": false,
			"task_cycle_phase_records": taskCycleProgressPhaseRecords(
				r.cfg,
				task,
				session,
				runID,
				workPacket,
				&result,
				nil,
			),
			"task_cycle_phase_record": taskCyclePhaseRecordEnvelope(
				r.cfg,
				task,
				session,
				runID,
				"PROJECT_LIVENESS_REQUIRED",
				"VERIFY",
				workPacket,
				&result,
				nil,
				7,
			),
		},
	})
	return err
}

func (r *Runtime) writeCompletionCoordinationAdvisoryStep(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, workPacket *AgentWorkPacket, state, detail string) error {
	if r == nil || r.client == nil || strings.TrimSpace(runID) == "" {
		return nil
	}
	_, err := r.client.WriteExecutionStep(ctx, ExecutionStepWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		Phase:       "VERIFY",
		Title:       "Peer coordination advisory",
		Summary:     firstNonEmpty(result.Summary, "Completion peer review gate treated as advisory in trust_first mode"),
		Status:      "COMPLETED",
		SortOrder:   25,
		Evidence:    prependCapabilitySnapshotEvidence(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID), []string{completionCoordinationGateEvidence, "trust_first_advisory"}),
		Verification: map[string]any{
			"capability_snapshot_id":     daemonRunCapabilitySnapshotID(r.cfg, task, session, runID),
			"capability_snapshot_ref":    capabilitySnapshotEvidenceRef(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID)),
			"coordination_gate":          completionCoordinationGateEvidence,
			"coordination_mode":          CoordinationModeTrustFirst,
			"state":                      strings.TrimSpace(state),
			"detail":                     strings.TrimSpace(detail),
			"canonical_mutation_applied": false,
			"task_cycle_phase_records": taskCycleProgressPhaseRecords(
				r.cfg,
				task,
				session,
				runID,
				workPacket,
				&result,
				nil,
			),
			"task_cycle_phase_record": taskCyclePhaseRecordEnvelope(
				r.cfg,
				task,
				session,
				runID,
				"COORDINATION_ADVISORY",
				"VERIFY",
				workPacket,
				&result,
				nil,
				7,
			),
		},
	})
	return err
}

func (r *Runtime) handleExistingCompletionCoordinationRequest(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, workPacket *AgentWorkPacket, state RuntimeScratchState) (StructuredTaskResult, bool, error) {
	requestID := strings.TrimSpace(state.CompletionCoordinationReqID)
	if requestID == "" {
		return result, false, nil
	}

	record, err := r.client.GetAgentRequestResult(ctx, r.cfg.WorkspaceID, requestID)
	if err != nil {
		gated := demoteCompletionForCoordination(result,
			"Completion deferred: peer review status unavailable",
			fmt.Sprintf("Runtime could not read peer review request %s yet: %v. Continue after the peer response is available or ask another online peer with agent_request.", requestID, err),
		)
		if writeErr := r.writeCompletionCoordinationGateStep(ctx, task, session, runID, gated, workPacket, state.CompletionCoordinationPeerID, requestID, "status_unavailable", err.Error()); writeErr != nil {
			return result, false, writeErr
		}
		return gated, true, nil
	}

	status := strings.ToUpper(strings.TrimSpace(record.Status))
	if status == "COMPLETED" {
		reply := firstNonEmpty(formatAgentRequestResponse(record), "peer review completed without a response body")
		signal := fmt.Sprintf("SYSTEM COORDINATION GATE: peer review response for task %s from %s on request %s: %s. Incorporate this feedback before final completion.", task.TaskID, firstNonEmpty(record.ToAgentID, state.CompletionCoordinationPeerID), requestID, truncate(reply, 900))
		if err := r.setCompletionCoordinationScratch(ctx, task, session, runID, firstNonEmpty(record.ToAgentID, state.CompletionCoordinationPeerID), requestID, completionCoordinationStateReviewReady, reply, signal); err != nil {
			return result, false, err
		}
		gated := demoteCompletionForCoordination(result,
			"Completion deferred: peer review received",
			"Peer review feedback is now available in advisory signals. Revise or explicitly account for it in the next cycle before returning outcome=completed.",
		)
		if err := r.writeCompletionCoordinationGateStep(ctx, task, session, runID, gated, workPacket, firstNonEmpty(record.ToAgentID, state.CompletionCoordinationPeerID), requestID, completionCoordinationStateReviewReady, truncate(reply, 600)); err != nil {
			return result, false, err
		}
		return gated, true, nil
	}

	if isCompletedAgentRequest(record) {
		gated := demoteCompletionForCoordination(result,
			"Completion deferred: peer review did not complete",
			fmt.Sprintf("Peer review request %s ended with status %s. Ask another online peer via agent_request before completing.", requestID, firstNonEmpty(status, "UNKNOWN")),
		)
		if err := r.clearCompletionCoordinationScratch(ctx, task.TaskID); err != nil {
			return result, false, err
		}
		if err := r.writeCompletionCoordinationGateStep(ctx, task, session, runID, gated, workPacket, firstNonEmpty(record.ToAgentID, state.CompletionCoordinationPeerID), requestID, status, "peer review did not complete"); err != nil {
			return result, false, err
		}
		return gated, true, nil
	}

	gated := demoteCompletionForCoordination(result,
		"Completion deferred: awaiting peer review",
		fmt.Sprintf("Peer review request %s is still %s. Wait for the response before completing.", requestID, firstNonEmpty(status, "PENDING")),
	)
	if err := r.writeCompletionCoordinationGateStep(ctx, task, session, runID, gated, workPacket, firstNonEmpty(record.ToAgentID, state.CompletionCoordinationPeerID), requestID, firstNonEmpty(status, "PENDING"), "peer review still pending"); err != nil {
		return result, false, err
	}
	return gated, true, nil
}

func (r *Runtime) completionCoordinationTaskRequiresGate(task WorkspaceTaskRecord) bool {
	if r == nil || r.cfg.Mode != RuntimeModeDaemon || strings.TrimSpace(r.cfg.WorkspaceID) == "" || strings.TrimSpace(r.cfg.AgentID) == "" {
		return false
	}
	if task.RequiresProjectGate != nil {
		return *task.RequiresProjectGate
	}
	if isStrategicLeadCorrectionTask(task) {
		return false
	}
	if completionCoordinationExplicitSolo(task) {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(task.Priority)) {
	case "critical", "high":
		return true
	}
	text := strings.ToLower(strings.Join([]string{task.Title, task.Description, task.TaskKind, task.TaskTemplate}, "\n"))
	for _, marker := range []string{"multi-agent", "multiagent", "coordination", "peer review", "review", "autonomous", "автоном", "координац", "ревью", "межагент"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func completionCoordinationExplicitSolo(task WorkspaceTaskRecord) bool {
	text := strings.ToLower(strings.Join([]string{task.Title, task.Description, task.TaskKind, task.TaskTemplate}, "\n"))
	for _, marker := range []string{"solo_ok", "solo allowed", "single-agent", "single agent", "без ревью", "без координации"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func isStrategicLeadCorrectionTask(task WorkspaceTaskRecord) bool {
	if task.RequiresProjectGate != nil && *task.RequiresProjectGate {
		return false
	}
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	if strings.ToUpper(strings.TrimSpace(task.TaskKind)) != "COORDINATION" {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	if lane != "coordination" && lane != "strategy" {
		return false
	}
	taskID := strings.ToLower(strings.TrimSpace(task.TaskID))
	if strings.HasPrefix(taskID, "task-role-scope-") && runtimeTaskHasProjectRoleScopeAuthorityTransition(task) {
		return true
	}
	title := strings.ToLower(strings.TrimSpace(task.Title))
	claimRepairShape := strings.HasPrefix(taskID, "task-project-claim-repair-") &&
		strings.Contains(title, "repair project claim")
	if workspaceTaskHasTag(task, "project-claim-repair") && claimRepairShape {
		return true
	}
	return false
}

func workspaceTaskHasTag(task WorkspaceTaskRecord, want string) bool {
	want = strings.ToLower(strings.TrimSpace(want))
	if want == "" {
		return false
	}
	for _, tag := range task.Tags {
		if strings.ToLower(strings.TrimSpace(tag)) == want {
			return true
		}
	}
	return false
}

func completionCoordinationTraceSatisfied(trace *TaskRunTrace) bool {
	if trace == nil {
		return false
	}
	for _, toolName := range trace.ToolCalls {
		switch strings.ToLower(strings.TrimSpace(toolName)) {
		case "agent_request", "reviewer_route":
			return true
		}
	}
	return false
}

func (r *Runtime) selectCompletionCoordinationPeer(ctx context.Context, excludeAgentID string) (AgentRecord, bool) {
	peers := r.completionCoordinationPeerCandidates(excludeAgentID)
	if len(peers) == 0 && r.client != nil {
		agents, err := r.client.ListWorkspaceAgents(ctx, r.cfg.WorkspaceID)
		if err != nil {
			log.Printf("[runtime] completion coordination peer lookup degraded: %v", err)
		} else {
			peers = completionCoordinationPeerCandidatesFromAgents(agents, r.cfg.AgentID, excludeAgentID)
		}
	}
	if len(peers) == 0 {
		return AgentRecord{}, false
	}
	return peers[0], true
}

func (r *Runtime) completionCoordinationPeerCandidates(excludeAgentID string) []AgentRecord {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	agents := append([]AgentRecord(nil), r.bootstrap.Snapshot.Agents...)
	r.mu.Unlock()
	return completionCoordinationPeerCandidatesFromAgents(agents, r.cfg.AgentID, excludeAgentID)
}

func completionCoordinationPeerCandidatesFromAgents(agents []AgentRecord, selfAgentID, excludeAgentID string) []AgentRecord {
	selfAgentID = strings.TrimSpace(selfAgentID)
	excludeAgentID = strings.TrimSpace(excludeAgentID)
	var peers []AgentRecord
	for _, agent := range agents {
		agentID := strings.TrimSpace(agent.AgentID)
		if agentID == "" || agentID == selfAgentID || agentID == excludeAgentID {
			continue
		}
		if !agent.IsOnline {
			continue
		}
		status := strings.ToUpper(strings.TrimSpace(agent.Status))
		if status == "STOPPED" || status == "OFFLINE" || status == "DELETED" {
			continue
		}
		peers = append(peers, agent)
	}
	sort.SliceStable(peers, func(i, j int) bool {
		return completionCoordinationPeerRank(peers[i]) < completionCoordinationPeerRank(peers[j])
	})
	return peers
}

func completionCoordinationPeerRank(agent AgentRecord) int {
	role := strings.ToLower(strings.TrimSpace(agent.Role))
	switch {
	case strings.Contains(role, "review"):
		return 0
	case strings.Contains(role, "synth"):
		return 1
	default:
		return 2
	}
}

func (r *Runtime) requestCompletionCoordinationReview(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, peer AgentRecord) (AgentRequestCreateResult, error) {
	payload, err := json.Marshal(map[string]any{
		"prompt": buildCompletionCoordinationReviewPrompt(task, session, runID, result),
	})
	if err != nil {
		return AgentRequestCreateResult{}, err
	}
	return r.client.RequestAgent(ctx, AgentRequestInput{
		WorkspaceID: r.cfg.WorkspaceID,
		FromAgentID: r.cfg.AgentID,
		ToAgentID:   strings.TrimSpace(peer.AgentID),
		Method:      "model.ask",
		PayloadJSON: string(payload),
		TimeoutSec:  completionCoordinationRequestTimeoutSec,
	})
}

func buildCompletionCoordinationReviewPrompt(task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult) string {
	var b strings.Builder
	b.WriteString("Review this task before terminal completion. Give bounded, concrete feedback.\n\n")
	b.WriteString(fmt.Sprintf("- task_id: %s\n", strings.TrimSpace(task.TaskID)))
	b.WriteString(fmt.Sprintf("- task_title: %s\n", strings.TrimSpace(task.Title)))
	b.WriteString(fmt.Sprintf("- session_id: %s\n", strings.TrimSpace(session.SessionID)))
	b.WriteString(fmt.Sprintf("- run_id: %s\n", strings.TrimSpace(runID)))
	b.WriteString(fmt.Sprintf("- proposed_summary: %s\n", strings.TrimSpace(result.Summary)))
	if strings.TrimSpace(result.Details) != "" {
		b.WriteString(fmt.Sprintf("- proposed_details: %s\n", truncate(strings.TrimSpace(result.Details), 1200)))
	}
	b.WriteString("\nReturn concise feedback with: approve/blockers, missing tests or review checks, and any revision needed before completion.")
	return b.String()
}

func demoteCompletionForCoordination(result StructuredTaskResult, summary, nextAction string) StructuredTaskResult {
	result.Outcome = "continue"
	result.Summary = strings.TrimSpace(summary)
	result.NextAction = strings.TrimSpace(nextAction)
	result.RequiresHuman = false
	result.OwnerAction = ""
	result.HumanReason = ""
	result.DecisionType = ""
	result.BlockedOn = nil
	result.Materialize = TaskMaterialization{}
	return result
}

func (r *Runtime) writeCompletionCoordinationGateStep(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, workPacket *AgentWorkPacket, peerID, requestID, state, detail string) error {
	if r == nil || r.client == nil || strings.TrimSpace(runID) == "" {
		return nil
	}
	evidence := []string{completionCoordinationGateEvidence}
	if requestID = strings.TrimSpace(requestID); requestID != "" {
		evidence = append(evidence, "agent_request:"+requestID)
	}
	if peerID = strings.TrimSpace(peerID); peerID != "" {
		evidence = append(evidence, "peer_agent:"+peerID)
	}
	_, err := r.client.WriteExecutionStep(ctx, ExecutionStepWriteInput{
		WorkspaceID: r.cfg.WorkspaceID,
		RunID:       runID,
		Phase:       "VERIFY",
		Title:       "Defer completion pending peer coordination",
		Summary:     result.Summary,
		Status:      "ACTIVE",
		SortOrder:   25,
		Evidence:    prependCapabilitySnapshotEvidence(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID), evidence),
		Verification: map[string]any{
			"capability_snapshot_id":     daemonRunCapabilitySnapshotID(r.cfg, task, session, runID),
			"capability_snapshot_ref":    capabilitySnapshotEvidenceRef(daemonRunCapabilitySnapshotID(r.cfg, task, session, runID)),
			"coordination_gate":          completionCoordinationGateEvidence,
			"peer_agent_id":              peerID,
			"request_id":                 requestID,
			"state":                      strings.TrimSpace(state),
			"detail":                     strings.TrimSpace(detail),
			"canonical_mutation_applied": false,
			"task_cycle_phase_records": taskCycleProgressPhaseRecords(
				r.cfg,
				task,
				session,
				runID,
				workPacket,
				&result,
				nil,
			),
			"task_cycle_phase_record": taskCyclePhaseRecordEnvelope(
				r.cfg,
				task,
				session,
				runID,
				"COORDINATION_REQUIRED",
				"VERIFY",
				workPacket,
				&result,
				nil,
				7,
			),
		},
	})
	return err
}

func (r *Runtime) completionCoordinationScratch() RuntimeScratchState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.scratch
}

func (r *Runtime) workspaceDocDraftGateState() WorkspaceDocDraftGateState {
	if r == nil || r.cfg.Mode != RuntimeModeDaemon {
		return WorkspaceDocDraftGateState{}
	}
	if runtimeTrustFirst(r.cfg) {
		return WorkspaceDocDraftGateState{}
	}
	r.mu.Lock()
	var task WorkspaceTaskRecord
	if r.activeTask != nil {
		task = *r.activeTask
	}
	scratch := r.scratch
	runID := firstNonEmpty(strings.TrimSpace(r.activeRunID), strings.TrimSpace(scratch.ActiveRunID), strings.TrimSpace(scratch.CompletionCoordinationRunID))
	r.mu.Unlock()

	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" || !r.completionCoordinationTaskRequiresGate(task) {
		return WorkspaceDocDraftGateState{}
	}
	stateTaskID := strings.TrimSpace(scratch.CompletionCoordinationTaskID)
	state := strings.TrimSpace(scratch.CompletionCoordinationState)
	peerID := strings.TrimSpace(scratch.CompletionCoordinationPeerID)
	requestID := strings.TrimSpace(scratch.CompletionCoordinationReqID)
	if stateTaskID == taskID && state == completionCoordinationStateReviewReady {
		return WorkspaceDocDraftGateState{}
	}
	if stateTaskID != "" && stateTaskID != taskID {
		state = ""
		peerID = ""
		requestID = ""
	}
	if state == "" {
		state = "pre_review"
	}
	return WorkspaceDocDraftGateState{
		Active:    true,
		TaskID:    taskID,
		RunID:     runID,
		PeerID:    peerID,
		RequestID: requestID,
		State:     state,
		Reason:    "Completion for this task requires peer review; this final-looking document remains a draft until peer feedback is incorporated.",
	}
}

func (r *Runtime) setCompletionCoordinationScratch(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID, peerID, requestID, state, reply, advisory string) error {
	return r.updateScratch(ctx, func(s *RuntimeScratchState) {
		s.CompletionCoordinationTaskID = strings.TrimSpace(task.TaskID)
		s.CompletionCoordinationRunID = strings.TrimSpace(runID)
		s.CompletionCoordinationSessionID = strings.TrimSpace(session.SessionID)
		s.CompletionCoordinationPeerID = strings.TrimSpace(peerID)
		s.CompletionCoordinationReqID = strings.TrimSpace(requestID)
		s.CompletionCoordinationState = strings.TrimSpace(state)
		s.CompletionCoordinationAt = time.Now().UTC().Format(time.RFC3339Nano)
		s.CompletionCoordinationReply = truncate(strings.TrimSpace(reply), 1200)
		appendRuntimeAdvisorySignal(s, advisory)
		preserveAuthorityTransition := shouldPreservePendingAuthorityTransition(*s, pendingWorkTrigger{
			Trigger:   "request_resume",
			TaskID:    strings.TrimSpace(task.TaskID),
			SessionID: strings.TrimSpace(session.SessionID),
		})
		if strings.TrimSpace(state) == completionCoordinationStateReviewReady {
			if !preserveAuthorityTransition {
				s.PendingTrigger = "request_resume"
				s.PendingTriggerAt = time.Now().UTC().Format(time.RFC3339Nano)
			}
		}
		if !preserveAuthorityTransition {
			s.PendingTriggerTask = firstNonEmpty(strings.TrimSpace(task.TaskID), s.PendingTriggerTask)
			s.PendingTriggerSession = firstNonEmpty(strings.TrimSpace(session.SessionID), s.PendingTriggerSession)
		}
	})
}

func (r *Runtime) clearCompletionCoordinationScratch(ctx context.Context, taskID string) error {
	state := r.completionCoordinationScratch()
	if strings.TrimSpace(state.CompletionCoordinationTaskID) != strings.TrimSpace(taskID) {
		return nil
	}
	return r.updateScratch(ctx, func(s *RuntimeScratchState) {
		s.CompletionCoordinationTaskID = ""
		s.CompletionCoordinationRunID = ""
		s.CompletionCoordinationSessionID = ""
		s.CompletionCoordinationPeerID = ""
		s.CompletionCoordinationReqID = ""
		s.CompletionCoordinationState = ""
		s.CompletionCoordinationAt = ""
		s.CompletionCoordinationReply = ""
	})
}

func (r *Runtime) refreshCompletionCoordinationResponse(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string) error {
	if r == nil || r.client == nil {
		return nil
	}
	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" {
		return nil
	}
	state := r.completionCoordinationScratch()
	if strings.TrimSpace(state.CompletionCoordinationTaskID) != taskID {
		return nil
	}
	if strings.TrimSpace(state.CompletionCoordinationState) != completionCoordinationStateRequested {
		return nil
	}
	requestID := strings.TrimSpace(state.CompletionCoordinationReqID)
	if requestID == "" {
		return nil
	}
	sessionID := firstNonEmpty(strings.TrimSpace(session.SessionID), strings.TrimSpace(state.CompletionCoordinationSessionID))
	activeRunID := firstNonEmpty(strings.TrimSpace(runID), strings.TrimSpace(state.CompletionCoordinationRunID))
	_, err := r.captureCompletedCompletionCoordinationResponse(ctx, taskID, sessionID, activeRunID, strings.TrimSpace(state.CompletionCoordinationPeerID), requestID)
	return err
}

func appendRuntimeAdvisorySignal(state *RuntimeScratchState, signal string) {
	if state == nil {
		return
	}
	signal = strings.TrimSpace(signal)
	if signal == "" {
		return
	}
	if len(state.AdvisorySignals) == 0 || state.AdvisorySignals[len(state.AdvisorySignals)-1] != signal {
		state.AdvisorySignals = append(state.AdvisorySignals, signal)
	}
	if len(state.AdvisorySignals) > 5 {
		state.AdvisorySignals = state.AdvisorySignals[len(state.AdvisorySignals)-5:]
	}
}

func (r *Runtime) handleAgentResponseRuntimeEvent(ctx context.Context, evt RhizomeEvent) {
	if r == nil {
		return
	}
	selfAgentID := strings.TrimSpace(r.cfg.AgentID)
	eventAgentID := strings.TrimSpace(evt.AgentID)
	payload := completionCoordinationAgentResponsePayload(evt.PayloadJSON)

	r.mu.Lock()
	gateTaskID := strings.TrimSpace(r.scratch.CompletionCoordinationTaskID)
	gateSessionID := strings.TrimSpace(r.scratch.CompletionCoordinationSessionID)
	gateRunID := strings.TrimSpace(r.scratch.CompletionCoordinationRunID)
	gatePeerID := strings.TrimSpace(r.scratch.CompletionCoordinationPeerID)
	gateRequestID := strings.TrimSpace(r.scratch.CompletionCoordinationReqID)
	gateState := strings.TrimSpace(r.scratch.CompletionCoordinationState)
	holdTaskID := strings.TrimSpace(r.scratch.ContinuationHoldTaskID)
	holdSessionID := strings.TrimSpace(r.scratch.ContinuationHoldSessionID)
	holdActive := r.continuationHoldActiveLocked(time.Now().UTC())
	taskID := firstNonEmpty(gateTaskID, payload.TaskID, holdTaskID, r.scratch.ActiveTaskID)
	sessionID := firstNonEmpty(gateSessionID, payload.SessionID, holdSessionID, r.scratch.ActiveSessionID)
	activeSessionStatus := ""
	if r.activeTask != nil {
		taskID = firstNonEmpty(taskID, strings.TrimSpace(r.activeTask.TaskID))
	}
	if r.activeSession != nil {
		sessionID = firstNonEmpty(sessionID, strings.TrimSpace(r.activeSession.SessionID))
		activeSessionStatus = strings.ToUpper(strings.TrimSpace(r.activeSession.Status))
	}
	r.mu.Unlock()

	addressedToSelf := eventAgentID == selfAgentID || payload.FromAgentID == selfAgentID
	if !addressedToSelf {
		return
	}
	if gateState == completionCoordinationStateRequested {
		if gateRequestID != "" && payload.RequestID != "" && gateRequestID != payload.RequestID {
			return
		}
		if gateTaskID != "" && payload.TaskID != "" && gateTaskID != payload.TaskID {
			return
		}
	} else if !holdActive && activeSessionStatus != "BLOCKED" {
		return
	}
	if taskID == "" {
		return
	}
	if gateState == completionCoordinationStateRequested {
		applied, err := r.captureCompletedCompletionCoordinationResponse(ctx, taskID, sessionID, gateRunID, gatePeerID, gateRequestID)
		if err != nil && ctx.Err() == nil {
			log.Printf("[events] completion coordination response capture failed: %v", err)
		}
		if applied {
			return
		}
	}
	if err := r.setPendingWorkTrigger(ctx, "request_resume", taskID, sessionID); err != nil && ctx.Err() == nil {
		log.Printf("[events] agent response resume trigger failed: %v", err)
	}
}

func (r *Runtime) captureCompletedCompletionCoordinationResponse(ctx context.Context, taskID, sessionID, runID, peerID, requestID string) (bool, error) {
	if r == nil || r.client == nil {
		return false, nil
	}
	taskID = strings.TrimSpace(taskID)
	sessionID = strings.TrimSpace(sessionID)
	runID = strings.TrimSpace(runID)
	peerID = strings.TrimSpace(peerID)
	requestID = strings.TrimSpace(requestID)
	if taskID == "" || requestID == "" {
		return false, nil
	}
	record, err := r.client.GetAgentRequestResult(ctx, r.cfg.WorkspaceID, requestID)
	if err != nil {
		return false, err
	}
	if strings.ToUpper(strings.TrimSpace(record.Status)) != "COMPLETED" {
		return false, nil
	}
	reply := firstNonEmpty(formatAgentRequestResponse(record), "peer review completed without a response body")
	if strings.TrimSpace(peerID) == "" {
		peerID = strings.TrimSpace(record.ToAgentID)
	}
	signal := fmt.Sprintf("SYSTEM COORDINATION GATE: peer review response for task %s from %s on request %s: %s. Incorporate this feedback before final completion.", taskID, firstNonEmpty(peerID, record.ToAgentID), requestID, truncate(reply, 900))
	if err := r.setCompletionCoordinationScratch(ctx,
		WorkspaceTaskRecord{TaskID: taskID},
		AgentSessionStateRecord{SessionID: sessionID, TaskID: taskID},
		runID,
		peerID,
		requestID,
		completionCoordinationStateReviewReady,
		reply,
		signal,
	); err != nil {
		return false, err
	}
	r.markHydrationStale()
	r.invalidateFocus()
	r.wakePlanner()
	return true, nil
}

type agentResponseRuntimeEventPayload struct {
	RequestID   string `json:"request_id"`
	FromAgentID string `json:"from_agent_id"`
	ToAgentID   string `json:"to_agent_id"`
	TaskID      string `json:"task_id"`
	SessionID   string `json:"session_id"`
}

func completionCoordinationAgentResponsePayload(raw string) agentResponseRuntimeEventPayload {
	var payload agentResponseRuntimeEventPayload
	if strings.TrimSpace(raw) == "" {
		return payload
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return agentResponseRuntimeEventPayload{}
	}
	payload.RequestID = strings.TrimSpace(payload.RequestID)
	payload.FromAgentID = strings.TrimSpace(payload.FromAgentID)
	payload.ToAgentID = strings.TrimSpace(payload.ToAgentID)
	payload.TaskID = strings.TrimSpace(payload.TaskID)
	payload.SessionID = strings.TrimSpace(payload.SessionID)
	return payload
}
