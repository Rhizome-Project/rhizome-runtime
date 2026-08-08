package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

const (
	idleReflectionNoWorkThreshold                 = 2
	idleReflectionCooldown                        = 15 * time.Minute
	patchQueueDuplicateCooldown                   = 10 * time.Minute
	internalHeartbeatIdleSuppressionSeenThreshold = 3
)

type idleReflectionTarget struct {
	Key                  string
	TaskID               string
	ScopeKind            string
	ScopeID              string
	ProjectID            string
	ProjectTitle         string
	ProjectLane          string
	EmptyProductFrontier bool
	// FrontierBlockedOnlyByRevisionFollowups is a cheap snapshot-level pre-check (R68): the convergence
	// frontier is NOT clear, but every blocking task is a patch-queue revision followup. Only then is it
	// worth fetching the project coordination to apply the precise redundant-rejected-revision override -
	// avoiding a coordination read on every blocked idle cycle.
	FrontierBlockedOnlyByRevisionFollowups bool
	ReflectionScope                        string
	IdleActionPolicy                       string
	Title                                  string
	Description                            string
	AnchorTaskIDs                          []string
	BlockedTaskIDs                         []string
	OpenTaskIDs                            []string
	ActiveTaskIDs                          []string
	StaleTaskIDs                           []string
	TaskRecords                            []WorkspaceTaskRecord
	RecentEvidenceDocKeys                  []string
	ExistingTask                           *WorkspaceTaskRecord
}

func (r *Runtime) resetIdleNoWorkReflection() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.idleNoWorkKey = ""
	r.idleNoWorkCount = 0
	r.mu.Unlock()
}

func internalHeartbeatResultsPromotedPublicWork(results []InternalHeartbeatExecutionResult) bool {
	for _, result := range results {
		if len(result.PromotedRefs) > 0 || strings.EqualFold(strings.TrimSpace(result.Outcome), "backlog_promoted") {
			return true
		}
	}
	return false
}

func internalHeartbeatResultsHandledNoWork(results []InternalHeartbeatExecutionResult) bool {
	for _, result := range results {
		if internalHeartbeatResultHandledNoWork(result) {
			return true
		}
	}
	return false
}

func internalHeartbeatResultHandledNoWork(result InternalHeartbeatExecutionResult) bool {
	if len(result.PromotedRefs) > 0 {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(result.Outcome)) {
	case "backlog_promoted", "backlog_recorded", "capability_ready", "human_input_required", "blocked":
		return true
	default:
		return false
	}
}

func (r *Runtime) idleReflectionSuppressedByInternalHeartbeat(now time.Time) bool {
	if r == nil || r.internalSessions == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if r.internalHeartbeatLocalActivityExceededIdleSuppression(now) {
		return false
	}
	recentSessionCount := 0
	state := r.internalSessions.Snapshot()
	for _, item := range state.Backlog {
		if strings.EqualFold(strings.TrimSpace(item.Status), "open") && !item.Stale && internalHeartbeatBacklogItemRecentEnoughForIdleSuppression(item, now) && !internalHeartbeatBacklogItemExceededIdleSuppression(item) {
			return true
		}
	}
	cutoff := now.Add(-idleReflectionCooldown)
	for _, session := range state.Sessions {
		at := internalHeartbeatParseTime(firstNonEmpty(session.EndedAt, session.StartedAt))
		if !at.IsZero() && !at.Before(cutoff) {
			recentSessionCount++
		}
	}
	return recentSessionCount > 0 && recentSessionCount < internalHeartbeatIdleSuppressionSeenThreshold
}

func (r *Runtime) internalHeartbeatLocalActivityExceededIdleSuppression(now time.Time) bool {
	if r == nil || r.internalSessions == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	state := r.internalSessions.Snapshot()
	for _, item := range state.Backlog {
		if strings.EqualFold(strings.TrimSpace(item.Status), "open") && !item.Stale && internalHeartbeatBacklogItemRecentEnoughForIdleSuppression(item, now) && internalHeartbeatBacklogItemExceededIdleSuppression(item) {
			return true
		}
	}
	cutoff := now.Add(-idleReflectionCooldown)
	recentSessionCount := 0
	for _, session := range state.Sessions {
		at := internalHeartbeatParseTime(firstNonEmpty(session.EndedAt, session.StartedAt))
		if !at.IsZero() && !at.Before(cutoff) {
			recentSessionCount++
		}
	}
	return recentSessionCount >= internalHeartbeatIdleSuppressionSeenThreshold
}

func internalHeartbeatBacklogItemRecentEnoughForIdleSuppression(item AgentPersonalBacklogItem, now time.Time) bool {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cutoff := now.Add(-idleReflectionCooldown)
	for _, raw := range []string{item.LastSeenAt, item.UpdatedAt, item.CreatedAt} {
		at := internalHeartbeatParseTime(raw)
		if !at.IsZero() && !at.Before(cutoff) {
			return true
		}
	}
	return false
}

func internalHeartbeatBacklogItemExceededIdleSuppression(item AgentPersonalBacklogItem) bool {
	return item.SeenCount >= internalHeartbeatIdleSuppressionSeenThreshold || item.PromotionAttempts >= internalHeartbeatIdleSuppressionSeenThreshold
}

func (r *Runtime) backgroundCycleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := runtimePlannerCycleTimeout(r.cfg)
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func (r *Runtime) recordBackgroundCycleTimeoutSignal(ctx context.Context, loop, signal string, err error, at time.Time) {
	if r == nil {
		return
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	if err == nil {
		err = context.DeadlineExceeded
	}
	r.recordLoopFailure(loop, err, at)
	signal = strings.TrimSpace(signal)
	if signal == "" {
		return
	}
	r.mu.Lock()
	appendRuntimeAdvisorySignal(&r.scratch, signal)
	stateCopy := r.scratch
	r.mu.Unlock()
	persistCtx := ctx
	if persistCtx == nil || errors.Is(persistCtx.Err(), context.DeadlineExceeded) || errors.Is(persistCtx.Err(), context.Canceled) {
		persistCtx = context.Background()
	}
	if saveErr := r.saveScratchState(persistCtx, stateCopy); saveErr != nil {
		log.Printf("[runtime] warning: could not persist background cycle timeout signal: %v", saveErr)
	}
}

func idleReflectionShouldSuppressNewGenericTarget(target idleReflectionTarget, internalHeartbeatHandled bool, suppressedByRecentInternalHeartbeat bool) bool {
	if target.ExistingTask != nil || idleReflectionTargetRequiresDeterministicTask(target) {
		return false
	}
	return internalHeartbeatHandled || suppressedByRecentInternalHeartbeat
}

func (r *Runtime) maybeMaterializeIdleReflection(ctx context.Context, work AgentWorkNextResult, trigger pendingWorkTrigger) error {
	if r == nil || r.runtimePaused() {
		return nil
	}
	if r.campaignBuildBreakActive() {
		r.resetIdleNoWorkReflection()
		return nil
	}
	if r.client == nil {
		return nil
	}
	if normalizeWorkTrigger(trigger.Trigger) != "" {
		r.resetIdleNoWorkReflection()
		return nil
	}
	now := time.Now().UTC()
	var internalHeartbeatHandled bool
	if r.internalSessions != nil {
		results, err := r.executeDueInternalHeartbeatsCoordinated(ctx, now)
		if err != nil {
			log.Printf("[planner] typed internal heartbeat skipped: %v", err)
		}
		if internalHeartbeatResultsPromotedPublicWork(results) {
			r.resetIdleNoWorkReflection()
			return nil
		}
		internalHeartbeatHandled = internalHeartbeatResultsHandledNoWork(results)
		if internalHeartbeatHandled && r.internalHeartbeatLocalActivityExceededIdleSuppression(now) {
			internalHeartbeatHandled = false
		}
	}
	policy := metacognitionPolicyForRuntimeConfig(r.cfg)
	trustFirst := runtimeTrustFirst(r.cfg)
	r.mu.Lock()
	snapshot := r.bootstrap.Snapshot
	target := buildIdleReflectionTarget(snapshot, r.cfg.WorkspaceID, now, trustFirst, policy)
	ambientTarget := r.ambientAutonomyTarget(target, snapshot, r.cfg.WorkspaceID, now, policy)
	if strings.TrimSpace(target.Key) == "" {
		if strings.TrimSpace(ambientTarget.Key) == "" {
			r.idleNoWorkKey = ""
			r.idleNoWorkCount = 0
			r.mu.Unlock()
			return nil
		}
		target = ambientTarget
	}
	if r.idleNoWorkKey == target.Key {
		r.idleNoWorkCount++
	} else {
		r.idleNoWorkKey = target.Key
		r.idleNoWorkCount = 1
	}
	count := r.idleNoWorkCount
	cooldownSince := r.idleReflectionAt
	coolingDown := r.idleReflectionKey == target.Key && !cooldownSince.IsZero() && now.Sub(cooldownSince) < idleReflectionCooldown
	if count < idleReflectionNoWorkThreshold || coolingDown {
		r.mu.Unlock()
		if coolingDown && count >= idleReflectionNoWorkThreshold {
			if idleReflectionHasActiveNonIdleWork(target) && trustFirst && target.ExistingTask == nil && idleReflectionActiveWorkMayNeedPatchQueuePruning(snapshot, target) {
				target = r.enrichIdleReflectionTargetWithProjectCoordination(ctx, target, policy, now)
			}
			if idleReflectionHasActiveNonIdleWork(target) {
				return nil
			}
			if evidenceKeys, ok := r.freshProjectEvidenceDocsSince(ctx, target, cooldownSince); ok {
				target.RecentEvidenceDocKeys = evidenceKeys
				target.TaskID = idleReflectionTaskID(target.Key+"|evidence:"+shortHash(strings.Join(evidenceKeys, "|")), now)
				target.ExistingTask = nil
				target.Description = appendFreshProjectEvidenceGuidance(target.Description, evidenceKeys)
			} else {
				return nil
			}
		} else {
			return nil
		}
	} else {
		r.mu.Unlock()
	}

	if trustFirst && target.ExistingTask == nil {
		target = r.enrichIdleReflectionTargetWithProjectCoordination(ctx, target, policy, now)
	}
	if ambient := r.ambientAutonomyTarget(target, snapshot, r.cfg.WorkspaceID, now, policy); !idleReflectionTargetSkipsAmbientAutonomy(target) && strings.TrimSpace(ambient.Key) != "" {
		disposition, err := r.maybeRunAmbientAutonomy(ctx, ambient, work, trigger, now)
		if err != nil {
			log.Printf("[planner] ambient autonomy heartbeat skipped: %v", err)
		}
		if disposition.OwnsNoWork {
			if disposition.SubmittedTask {
				return nil
			}
			if !ambientAutonomyShouldEscalateToIdleDuty(ambient, work, policy, disposition) {
				return nil
			}
			log.Printf("[planner] ambient autonomy produced no bounded task; escalating %s into idle product-quality duty", strings.TrimSpace(ambient.Key))
			target = ambient
		}
	}
	if idleReflectionShouldSuppressNewGenericTarget(target, internalHeartbeatHandled, r.idleReflectionSuppressedByInternalHeartbeat(now)) {
		r.resetIdleNoWorkReflection()
		return nil
	}
	if idleReflectionHasActiveNonIdleWork(target) {
		return nil
	}
	r.mu.Lock()
	r.idleReflectionKey = target.Key
	r.idleReflectionAt = now
	r.mu.Unlock()
	taskID, wake, err := r.ensureIdleReflectionTask(ctx, target, now)
	if err != nil {
		log.Printf("[planner] idle reflection materialization skipped: %v", err)
		return nil
	}
	if wake && strings.TrimSpace(taskID) != "" {
		return r.setPendingWorkTrigger(ctx, "request_resume", taskID, "")
	}
	return nil
}

func (r *Runtime) campaignBuildBreakActive() bool {
	return r != nil && r.cfg.TelicLoopEnabled && strings.TrimSpace(r.cfg.PinnedTaskID) != ""
}

func (r *Runtime) maybeMaterializePatchQueueSupersedeTask(ctx context.Context, work AgentWorkNextResult) (bool, error) {
	if r == nil || r.client == nil || r.runtimePaused() || work.Packet == nil || work.Packet.PatchQueueSupersede == nil {
		return false, nil
	}
	packet := work.Packet
	pq := packet.PatchQueueSupersede
	projectID := firstNonEmpty(strings.TrimSpace(pq.ProjectID), strings.TrimSpace(packet.ProjectID), strings.TrimSpace(work.ProjectID))
	queueID := strings.TrimSpace(pq.QueueID)
	itemID := strings.TrimSpace(pq.ItemID)
	branchID := strings.TrimSpace(pq.BranchID)
	headSHA := strings.TrimSpace(pq.HeadSHA)
	evidenceDocKey := strings.TrimSpace(pq.EvidenceDocKey)
	newItemID := strings.TrimSpace(pq.NewItemID)
	if newItemID == "" {
		newItemID = "supersede-" + shortHash(strings.Join([]string{projectID, queueID, itemID, branchID, headSHA, evidenceDocKey}, "|"))
	}
	if projectID == "" || queueID == "" || itemID == "" || branchID == "" || headSHA == "" || evidenceDocKey == "" || newItemID == "" {
		return false, nil
	}
	pq.ProjectID = projectID
	pq.NewItemID = newItemID
	taskID := patchQueueSupersedeTaskID(projectID, queueID, itemID, branchID, headSHA, evidenceDocKey)
	if r.patchQueueDuplicateTaskRecently(taskID, time.Now().UTC()) {
		return true, nil
	}
	requiresGate := false
	title := "Supersede blocked patch queue item after fresh evidence"
	description := patchQueueSupersedeTaskDescription(packet, *pq, newItemID)
	submitResult, err := r.client.SubmitTask(ctx, TaskSubmitInput{
		WorkspaceID:         r.cfg.WorkspaceID,
		TaskID:              taskID,
		OwnerUserID:         firstNonEmpty(r.cfg.OwnerUserID, r.cfg.AgentID),
		Priority:            "high",
		Title:               title,
		Description:         description,
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		ProjectID:           projectID,
		ProjectLane:         "integration",
		RequiresProjectGate: &requiresGate,
		Tags: []string{
			"project",
			"patch-queue",
			"supersede",
			"integration",
			"queue-stewardship",
			"validation",
			"blocked-unblock",
		},
		LinkedBy: r.cfg.AgentID,
	})
	if err != nil {
		if !isDuplicateTaskSubmitError(err) {
			return false, fmt.Errorf("create patch queue supersede stewardship task: %w", err)
		}
		r.rememberPatchQueueDuplicateTask(taskID, time.Now().UTC())
		log.Printf("[patch-queue] supersede stewardship task %s already exists; leaving scheduler to select it without forcing runtime_switch_task", taskID)
		return true, nil
	} else if strings.TrimSpace(submitResult.TaskID) != "" {
		taskID = strings.TrimSpace(submitResult.TaskID)
	}
	docKey := "task." + taskID
	if err == nil {
		if docErr := r.putDocWithRetry(ctx, docKey, "Task Brief - "+title, patchQueueSupersedeTaskDocContent(packet, *pq, newItemID)); docErr != nil {
			log.Printf("[patch-queue] supersede stewardship task %s created but doc materialization failed: %v", taskID, docErr)
		}
	}
	if strings.TrimSpace(taskID) != "" {
		if wakeErr := r.setPendingWorkTrigger(ctx, "runtime_switch_task", taskID, ""); wakeErr != nil {
			return true, wakeErr
		}
	}
	return true, nil
}

func (r *Runtime) maybeMaterializePatchQueueClaimStewardshipTask(ctx context.Context, work AgentWorkNextResult) (bool, error) {
	if r == nil || r.client == nil || r.runtimePaused() || work.Packet == nil || work.Packet.PatchQueueClaim == nil {
		return false, nil
	}
	packet := work.Packet
	pq := packet.PatchQueueClaim
	projectID := firstNonEmpty(strings.TrimSpace(pq.ProjectID), strings.TrimSpace(packet.ProjectID), strings.TrimSpace(work.ProjectID))
	queueID := strings.TrimSpace(pq.QueueID)
	itemID := strings.TrimSpace(pq.ItemID)
	branchID := strings.TrimSpace(pq.BranchID)
	claimedBy := strings.TrimSpace(pq.ClaimedBy)
	claimExpiresAt := strings.TrimSpace(pq.ClaimExpiresAt)
	if projectID == "" || queueID == "" || itemID == "" {
		return false, nil
	}
	pq.ProjectID = projectID
	taskID := patchQueueClaimStewardshipTaskID(projectID, queueID, itemID, branchID, claimedBy, claimExpiresAt)
	requiresGate := false
	title := "Resolve claimed patch queue item lifecycle"
	description := patchQueueClaimStewardshipTaskDescription(packet, *pq)
	submitResult, err := r.client.SubmitTask(ctx, TaskSubmitInput{
		WorkspaceID:  r.cfg.WorkspaceID,
		TaskID:       taskID,
		OwnerUserID:  firstNonEmpty(r.cfg.OwnerUserID, r.cfg.AgentID),
		Priority:     "high",
		Title:        title,
		Description:  description,
		TaskKind:     "COORDINATION",
		TaskTemplate: "generic",
		ProjectID:    projectID,
		// RPF-58B(i): the steward task carries its own deterministic contract instead of
		// relying on title/description text heuristics. Lane "review" + a non-integration
		// patch_queue_task_kind means the required-tool gate resolves to
		// project_patch_queue_lifecycle (claim->decide-or-release), never
		// project_patch_queue_integrate (which refuses a CLAIMED, non-ACCEPTED item and made
		// zeta's steward session BLOCK in R58).
		ProjectLane:         "review",
		RequiresProjectGate: &requiresGate,
		TaskRequirements: map[string]any{
			"patch_queue_task_kind": "claim_stewardship",
			"required_tool":         "project_patch_queue_lifecycle",
			"project_id":            projectID,
			"queue_id":              queueID,
			"item_id":               itemID,
			"branch_id":             branchID,
		},
		Tags:     patchQueueClaimStewardshipTaskTags(*pq),
		LinkedBy: r.cfg.AgentID,
	})
	if err != nil {
		if !isDuplicateTaskSubmitError(err) {
			return false, fmt.Errorf("create patch queue claim stewardship task: %w", err)
		}
		log.Printf("[patch-queue] claim stewardship task %s already exists; leaving scheduler to select it without forcing runtime_switch_task", taskID)
		return true, nil
	} else if strings.TrimSpace(submitResult.TaskID) != "" {
		taskID = strings.TrimSpace(submitResult.TaskID)
	}
	docKey := "task." + taskID
	if docErr := r.putDocWithRetry(ctx, docKey, "Task Brief - "+title, patchQueueClaimStewardshipTaskDocContent(packet, *pq)); docErr != nil {
		log.Printf("[patch-queue] claim stewardship task %s created but doc materialization failed: %v", taskID, docErr)
	}
	if strings.TrimSpace(taskID) != "" {
		if wakeErr := r.setPendingWorkTrigger(ctx, "runtime_switch_task", taskID, ""); wakeErr != nil {
			return true, wakeErr
		}
	}
	return true, nil
}

func (r *Runtime) maybeMaterializePatchQueueSubmitHandoffTask(ctx context.Context, work AgentWorkNextResult) (bool, error) {
	if r == nil || r.client == nil || r.runtimePaused() || work.Packet == nil || work.Packet.OwnerBound == nil {
		return false, nil
	}
	packet := work.Packet
	if !strings.EqualFold(strings.TrimSpace(packet.WorkType), "project_patch_queue_submit_handoff_available") {
		return false, nil
	}
	ob := packet.OwnerBound
	projectID := firstNonEmpty(strings.TrimSpace(packet.ProjectID), strings.TrimSpace(work.ProjectID))
	branchID := strings.TrimSpace(ob.BranchID)
	branchName := strings.TrimSpace(ob.BranchName)
	headSHA := strings.TrimSpace(ob.HeadSHA)
	reviewDocKey := strings.TrimSpace(ob.ReviewDocKey)
	requiredAgentID := strings.TrimSpace(ob.RequiredAgentID)
	if projectID == "" || branchID == "" || headSHA == "" || reviewDocKey == "" || requiredAgentID == "" {
		return false, nil
	}
	if requiredAgentID != strings.TrimSpace(r.cfg.AgentID) {
		return false, nil
	}
	branch := ProjectBranchRecord{
		ProjectID:    projectID,
		BranchID:     branchID,
		BranchName:   branchName,
		HeadSHA:      headSHA,
		ReviewDocKey: reviewDocKey,
	}
	if coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, projectID); err == nil {
		if coordinatedBranch, ok := patchQueueSubmitHandoffBranchFromCoordination(coordination.Branches, branchID, branchName, headSHA, requiredAgentID); ok {
			branch = coordinatedBranch
			projectID = firstNonEmpty(strings.TrimSpace(branch.ProjectID), projectID)
			branchID = firstNonEmpty(strings.TrimSpace(branch.BranchID), branchID)
			branchName = firstNonEmpty(strings.TrimSpace(branch.BranchName), branchName)
			headSHA = firstNonEmpty(strings.TrimSpace(branch.HeadSHA), headSHA)
			reviewDocKey = firstNonEmpty(strings.TrimSpace(branch.ReviewDocKey), reviewDocKey)
		}
	} else {
		log.Printf("[patch-queue] submit handoff could not hydrate branch envelope for %s: %v", branchID, err)
	}
	taskID := patchQueueSubmitHandoffTaskID(projectID, branchID, headSHA, reviewDocKey)
	if r.patchQueueDuplicateTaskRecently(taskID, time.Now().UTC()) {
		return true, nil
	}
	requiresGate := false
	title := "Submit READY_FOR_REVIEW branch to patch queue"
	description := patchQueueSubmitHandoffTaskDescription(packet, branch, requiredAgentID)
	requirements := map[string]any{
		"schema":              "rhizome_project_patch_queue_submit_handoff_v1",
		"required_transition": "project_patch_queue_submit",
		"project_id":          projectID,
		"branch_id":           branchID,
		"branch_name":         branchName,
		"head_sha":            headSHA,
		"review_doc_key":      reviewDocKey,
		"required_agent_id":   requiredAgentID,
		"controlled_queue":    true,
		"repo_authority_mode": "repoauthority_controlled_queue",
	}
	addPatchQueueSubmitEnvelopeRequirements(requirements, branch)
	submitResult, err := r.client.SubmitTask(ctx, TaskSubmitInput{
		WorkspaceID:         r.cfg.WorkspaceID,
		TaskID:              taskID,
		OwnerUserID:         firstNonEmpty(r.cfg.OwnerUserID, r.cfg.AgentID),
		Priority:            "critical",
		Title:               title,
		Description:         description,
		TaskKind:            "EXECUTION",
		TaskTemplate:        "integration",
		TaskClass:           "INTEGRATION",
		TaskClassSource:     "EXPLICIT",
		ProjectID:           projectID,
		ProjectLane:         "integration",
		RequiresProjectGate: &requiresGate,
		TaskRequirements:    requirements,
		Tags: []string{
			"project",
			"patch-queue",
			"integration",
			"owner-bound",
			"owner-bound-kind:patch_queue_submit",
			"owner-branch:" + branchID,
			"required-agent:" + requiredAgentID,
			"queue-handoff",
			"review-ready-handoff",
		},
		LinkedBy: r.cfg.AgentID,
	})
	if err != nil {
		if !isDuplicateTaskSubmitError(err) {
			return false, fmt.Errorf("create patch queue submit handoff task: %w", err)
		}
		r.rememberPatchQueueDuplicateTask(taskID, time.Now().UTC())
		log.Printf("[patch-queue] submit handoff task %s already exists; waking owner-bound scheduler path", taskID)
	} else if strings.TrimSpace(submitResult.TaskID) != "" {
		taskID = strings.TrimSpace(submitResult.TaskID)
	}
	docKey := "task." + taskID
	if docErr := r.putDocWithRetry(ctx, docKey, "Task Brief - "+title, patchQueueSubmitHandoffTaskDocContent(packet, branch, requiredAgentID)); docErr != nil {
		log.Printf("[patch-queue] submit handoff task %s created but doc materialization failed: %v", taskID, docErr)
	}
	if strings.TrimSpace(taskID) != "" {
		if wakeErr := r.setPendingWorkTrigger(ctx, "runtime_switch_task", taskID, ""); wakeErr != nil {
			return true, wakeErr
		}
	}
	return true, nil
}

func patchQueueSubmitHandoffTaskID(projectID, branchID, headSHA, reviewDocKey string) string {
	seed := strings.Join([]string{projectID, branchID, headSHA, reviewDocKey}, "|")
	return sanitizeRefSegment("task-patchq-submit-" + compactRefSegment("project", projectID) + "-" + shortHash(seed))
}

func patchQueueSubmitHandoffTaskDescription(packet *AgentWorkPacket, branch ProjectBranchRecord, requiredAgentID string) string {
	var b strings.Builder
	b.WriteString("Patch queue submit handoff task created from agent.work.next frontier `project_patch_queue_submit_handoff_available`.\n\n")
	b.WriteString("A branch is READY_FOR_REVIEW, but the patch queue handoff receipt is missing. Review packet evidence alone is not a queue handoff receipt.\n\n")
	if packet != nil && strings.TrimSpace(packet.WhyNow) != "" {
		b.WriteString("Why now: " + strings.TrimSpace(packet.WhyNow) + "\n\n")
	}
	b.WriteString("Required action:\n")
	b.WriteString("- Call `project_patch_queue_submit` for this exact owned branch.\n")
	b.WriteString("- Do not create validation-only follow-up work until the queue item exists or the tool returns a typed blocker.\n")
	b.WriteString("- If the branch/head changed, rerun project_branch_review_ready first, then submit the current READY_FOR_REVIEW branch.\n\n")
	b.WriteString(projectPatchQueueSubmitArgsBlock(branch, requiredAgentID))
	return b.String()
}

func patchQueueSubmitHandoffTaskDocContent(packet *AgentWorkPacket, branch ProjectBranchRecord, requiredAgentID string) string {
	var b strings.Builder
	b.WriteString("# Patch Queue Submit Handoff\n\n")
	b.WriteString("This task was materialized because `agent.work.next` surfaced a READY_FOR_REVIEW branch without a patch queue item for the same branch/head.\n\n")
	b.WriteString("## Frontier\n\n")
	b.WriteString("- work_type: project_patch_queue_submit_handoff_available\n")
	if packet != nil {
		b.WriteString("- coordination_state: " + strings.TrimSpace(packet.CoordinationState) + "\n")
		b.WriteString("- preferred_transition: " + strings.TrimSpace(packet.PreferredTransition) + "\n")
		b.WriteString("- why_now: " + strings.TrimSpace(packet.WhyNow) + "\n")
	}
	b.WriteString("\n## Exact Branch\n\n")
	b.WriteString(projectPatchQueueSubmitArgsBlock(branch, requiredAgentID))
	b.WriteString("\n## Guardrails\n\n")
	b.WriteString("- `project_branch_review_ready` is the branch READY_FOR_REVIEW receipt, not the patch queue handoff receipt.\n")
	b.WriteString("- `project_patch_queue_submit` must create the queue item before reviewers treat the branch as a shared integration candidate.\n")
	b.WriteString("- This task is owner-bound to the branch owner; non-owners should not claim it.\n")
	return b.String()
}

func patchQueueSubmitHandoffBranchFromCoordination(branches []ProjectBranchRecord, branchID, branchName, headSHA, requiredAgentID string) (ProjectBranchRecord, bool) {
	branchID = strings.TrimSpace(branchID)
	branchName = strings.TrimSpace(branchName)
	headSHA = strings.TrimSpace(headSHA)
	requiredAgentID = strings.TrimSpace(requiredAgentID)
	for _, branch := range branches {
		if branchID != "" && strings.TrimSpace(branch.BranchID) != branchID {
			continue
		}
		if branchID == "" && branchName != "" && strings.TrimSpace(branch.BranchName) != branchName {
			continue
		}
		if headSHA != "" && strings.TrimSpace(branch.HeadSHA) != "" && strings.TrimSpace(branch.HeadSHA) != headSHA {
			continue
		}
		if requiredAgentID != "" && strings.TrimSpace(branch.AgentID) != "" && strings.TrimSpace(branch.AgentID) != requiredAgentID {
			continue
		}
		return branch, true
	}
	return ProjectBranchRecord{}, false
}

func patchQueueClaimStewardshipTaskID(projectID, queueID, itemID, branchID, claimedBy, claimExpiresAt string) string {
	seed := strings.Join([]string{projectID, queueID, itemID, branchID, claimedBy, claimExpiresAt}, "|")
	return sanitizeRefSegment("task-patchq-claim-steward-" + compactRefSegment("project", projectID) + "-" + shortHash(seed))
}

func patchQueueClaimStewardshipTaskTags(pq AgentWorkPatchQueueClaim) []string {
	tags := []string{"project", "patch-queue", "integration", "queue-stewardship", "claim-stewardship", "claimed-decision"}
	if !pq.ClaimActive {
		tags = append(tags, "claim-expired")
	}
	if pq.OperationBindingPresent {
		tags = append(tags, "operation-bound")
	}
	return tags
}

func patchQueueClaimStewardshipTaskDescription(packet *AgentWorkPacket, pq AgentWorkPatchQueueClaim) string {
	var b strings.Builder
	b.WriteString("Patch queue claim stewardship task created from agent.work.next frontier `project_patch_queue_claim_stewardship_available`.\n\n")
	b.WriteString("A CLAIMED patch queue item is visible as live coordination debt but has no terminal decision. Recheck current queue state before every lifecycle mutation.\n\n")
	if packet != nil && strings.TrimSpace(packet.WhyNow) != "" {
		b.WriteString("Why now: " + strings.TrimSpace(packet.WhyNow) + "\n\n")
	}
	b.WriteString("Required action:\n")
	if pq.ClaimActive {
		b.WriteString("- If this agent still owns the active claim, record reviewer advisory and then accept/reject/block/cancel as appropriate.\n")
		b.WriteString("- If the claim is no longer active by the time you act, call `project_patch_queue_lifecycle` with `action=claim` first to renew/reclaim and obtain a fresh claim token.\n")
	} else {
		b.WriteString("- First call `project_patch_queue_lifecycle` with `action=claim` for this exact queue/item; this reclaims the expired item and returns a fresh claim token.\n")
		b.WriteString("- After reclaiming, record reviewer advisory if needed, then accept/reject/block/cancel, or release only your own fresh claim if no decision is justified.\n")
		b.WriteString("- Do not attempt to release another agent's stale claim directly.\n")
	}
	if pq.OperationBindingPresent {
		b.WriteString("- Operation binding is already present, so prefer a terminal decision or cancel; do not use release as the cleanup path.\n")
	}
	b.WriteString("\nproject_patch_queue_lifecycle selector:\n")
	b.WriteString("- project_id: " + strings.TrimSpace(pq.ProjectID) + "\n")
	b.WriteString("- queue_id: " + strings.TrimSpace(pq.QueueID) + "\n")
	b.WriteString("- item_id: " + strings.TrimSpace(pq.ItemID) + "\n")
	if strings.TrimSpace(pq.BranchID) != "" {
		b.WriteString("- branch_id: " + strings.TrimSpace(pq.BranchID) + "\n")
	}
	if strings.TrimSpace(pq.HeadSHA) != "" {
		b.WriteString("- expected_head_sha: " + strings.TrimSpace(pq.HeadSHA) + "\n")
	}
	if len(pq.AllowedActions) > 0 {
		b.WriteString("- allowed_actions_after_recheck: " + strings.Join(pq.AllowedActions, ", ") + "\n")
	}
	return b.String()
}

func patchQueueClaimStewardshipTaskDocContent(packet *AgentWorkPacket, pq AgentWorkPatchQueueClaim) string {
	var b strings.Builder
	b.WriteString("# Patch Queue Claim Stewardship\n\n")
	b.WriteString("This task was materialized because `agent.work.next` surfaced a CLAIMED patch queue item that still needs lifecycle stewardship.\n\n")
	b.WriteString("## Frontier\n\n")
	b.WriteString("- work_type: project_patch_queue_claim_stewardship_available\n")
	if packet != nil {
		b.WriteString("- coordination_state: " + strings.TrimSpace(packet.CoordinationState) + "\n")
		b.WriteString("- preferred_transition: " + strings.TrimSpace(packet.PreferredTransition) + "\n")
		b.WriteString("- why_now: " + strings.TrimSpace(packet.WhyNow) + "\n")
	}
	b.WriteString("\n## Exact Queue Item\n\n")
	b.WriteString("- project_id: " + strings.TrimSpace(pq.ProjectID) + "\n")
	b.WriteString("- queue_id: " + strings.TrimSpace(pq.QueueID) + "\n")
	b.WriteString("- item_id: " + strings.TrimSpace(pq.ItemID) + "\n")
	b.WriteString("- branch_id: " + strings.TrimSpace(pq.BranchID) + "\n")
	b.WriteString("- branch_name: " + strings.TrimSpace(pq.BranchName) + "\n")
	b.WriteString("- head_sha: " + strings.TrimSpace(pq.HeadSHA) + "\n")
	b.WriteString("- state: " + strings.TrimSpace(pq.State) + "\n")
	b.WriteString("- claimed_by: " + strings.TrimSpace(pq.ClaimedBy) + "\n")
	b.WriteString("- claim_expires_at: " + strings.TrimSpace(pq.ClaimExpiresAt) + "\n")
	b.WriteString(fmt.Sprintf("- claim_active_at_packet_time: %t\n", pq.ClaimActive))
	b.WriteString(fmt.Sprintf("- operation_binding_present: %t\n", pq.OperationBindingPresent))
	if len(pq.AllowedActions) > 0 {
		b.WriteString("- allowed_actions: " + strings.Join(pq.AllowedActions, ", ") + "\n")
	}
	if strings.TrimSpace(pq.ReviewDocKey) != "" || strings.TrimSpace(pq.EvidenceDocKey) != "" || strings.TrimSpace(pq.DecisionDocKey) != "" {
		b.WriteString("\n## Evidence Links\n\n")
		b.WriteString("- review_doc_key: " + strings.TrimSpace(pq.ReviewDocKey) + "\n")
		b.WriteString("- evidence_doc_key: " + strings.TrimSpace(pq.EvidenceDocKey) + "\n")
		b.WriteString("- decision_doc_key: " + strings.TrimSpace(pq.DecisionDocKey) + "\n")
	}
	b.WriteString("\n## Guardrails\n\n")
	b.WriteString("- Recheck current patch queue state before mutation; another reviewer may have acted after this packet was generated.\n")
	b.WriteString("- Reclaim expired items with `action=claim`; never release another agent's stale claim directly.\n")
	b.WriteString("- This is metadata lifecycle stewardship only; do not edit files or mutate git from this task.\n")
	if pq.OperationBindingPresent {
		b.WriteString("- Operation binding is present; resolve with a terminal decision or cancel rather than release.\n")
	}
	return b.String()
}

func (r *Runtime) patchQueueDuplicateTaskRecently(taskID string, now time.Time) bool {
	if r == nil {
		return false
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.patchQueueDuplicateTasks) == 0 {
		return false
	}
	at, ok := r.patchQueueDuplicateTasks[taskID]
	if !ok || at.IsZero() || now.Sub(at) >= patchQueueDuplicateCooldown {
		delete(r.patchQueueDuplicateTasks, taskID)
		return false
	}
	return true
}

func (r *Runtime) rememberPatchQueueDuplicateTask(taskID string, now time.Time) {
	if r == nil {
		return
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.patchQueueDuplicateTasks == nil {
		r.patchQueueDuplicateTasks = map[string]time.Time{}
	}
	r.patchQueueDuplicateTasks[taskID] = now
}

func patchQueueSupersedeTaskID(projectID, queueID, itemID, branchID, headSHA, evidenceDocKey string) string {
	seed := strings.Join([]string{projectID, queueID, itemID, branchID, headSHA, evidenceDocKey}, "|")
	return sanitizeRefSegment("task-patchq-supersede-" + compactRefSegment("project", projectID) + "-" + shortHash(seed))
}

func patchQueueSupersedeTaskDescription(packet *AgentWorkPacket, pq AgentWorkPatchQueueSupersede, newItemID string) string {
	var b strings.Builder
	b.WriteString("Patch queue stewardship task created from agent.work.next frontier `project_patch_queue_supersede_available`.\n\n")
	b.WriteString("A BLOCKED patch queue item has fresh exact evidence after its decision. Create or reuse a live supersession item; do not mutate git from this task.\n\n")
	if packet != nil && strings.TrimSpace(packet.WhyNow) != "" {
		b.WriteString("Why now: " + strings.TrimSpace(packet.WhyNow) + "\n\n")
	}
	b.WriteString("Required action:\n")
	b.WriteString("- Call `project_patch_queue_lifecycle` with `action=supersede` and the exact arguments below.\n")
	b.WriteString("- After a live PROPOSED successor exists, continue normal review/integration through patch queue review/claim/decision.\n")
	b.WriteString("- If the evidence no longer matches branch/head, publish a doc explaining the drift and block this task with that reason.\n\n")
	b.WriteString("project_patch_queue_lifecycle args:\n")
	b.WriteString("- project_id: " + strings.TrimSpace(pq.ProjectID) + "\n")
	b.WriteString("- action: supersede\n")
	b.WriteString("- queue_id: " + strings.TrimSpace(pq.QueueID) + "\n")
	b.WriteString("- item_id: " + strings.TrimSpace(pq.ItemID) + "\n")
	b.WriteString("- new_item_id: " + strings.TrimSpace(newItemID) + "\n")
	b.WriteString("- branch_id: " + strings.TrimSpace(pq.BranchID) + "\n")
	b.WriteString("- expected_head_sha: " + strings.TrimSpace(pq.HeadSHA) + "\n")
	b.WriteString("- evidence_doc_key: " + strings.TrimSpace(pq.EvidenceDocKey) + "\n")
	if strings.TrimSpace(pq.DecisionDocKey) != "" {
		b.WriteString("- blocked_decision_doc_key: " + strings.TrimSpace(pq.DecisionDocKey) + "\n")
	}
	if strings.TrimSpace(pq.ReviewDocKey) != "" {
		b.WriteString("- review_doc_key: " + strings.TrimSpace(pq.ReviewDocKey) + "\n")
	}
	return b.String()
}

func patchQueueSupersedeTaskDocContent(packet *AgentWorkPacket, pq AgentWorkPatchQueueSupersede, newItemID string) string {
	var b strings.Builder
	b.WriteString("# Patch Queue Supersede Stewardship\n\n")
	b.WriteString("This task was materialized by the runtime because `agent.work.next` surfaced a blocked patch queue item with fresh exact evidence.\n\n")
	b.WriteString("## Frontier\n\n")
	b.WriteString("- work_type: project_patch_queue_supersede_available\n")
	if packet != nil {
		b.WriteString("- coordination_state: " + strings.TrimSpace(packet.CoordinationState) + "\n")
		b.WriteString("- preferred_transition: " + strings.TrimSpace(packet.PreferredTransition) + "\n")
		b.WriteString("- why_now: " + strings.TrimSpace(packet.WhyNow) + "\n")
	}
	b.WriteString("\n## Exact Arguments\n\n")
	b.WriteString("- project_id: " + strings.TrimSpace(pq.ProjectID) + "\n")
	b.WriteString("- action: supersede\n")
	b.WriteString("- queue_id: " + strings.TrimSpace(pq.QueueID) + "\n")
	b.WriteString("- item_id: " + strings.TrimSpace(pq.ItemID) + "\n")
	b.WriteString("- new_item_id: " + strings.TrimSpace(newItemID) + "\n")
	b.WriteString("- branch_id: " + strings.TrimSpace(pq.BranchID) + "\n")
	b.WriteString("- branch_name: " + strings.TrimSpace(pq.BranchName) + "\n")
	b.WriteString("- expected_head_sha: " + strings.TrimSpace(pq.HeadSHA) + "\n")
	b.WriteString("- evidence_doc_key: " + strings.TrimSpace(pq.EvidenceDocKey) + "\n")
	b.WriteString("- blocked_decision_doc_key: " + strings.TrimSpace(pq.DecisionDocKey) + "\n")
	b.WriteString("- review_doc_key: " + strings.TrimSpace(pq.ReviewDocKey) + "\n\n")
	b.WriteString("## Guardrails\n\n")
	b.WriteString("- This is a metadata-only patch queue lifecycle step; do not edit files or mutate git while performing supersession.\n")
	b.WriteString("- Do not rely on private local paths or `model.ask` as evidence. Use workspace docs/evidence docs and patch queue state.\n")
	b.WriteString("- If supersession already exists, verify it names this old queue/item, branch/head, and evidence doc, then continue with normal review/integration.\n")
	return b.String()
}

func ambientAutonomyShouldEscalateToIdleDuty(target idleReflectionTarget, work AgentWorkNextResult, policy AgentMetacognitionPolicy, disposition ambientAutonomyDisposition) bool {
	policy = describeMetacognitionPolicy(policy)
	if !policy.CanOpenReflectionTasks || strings.TrimSpace(target.TaskID) == "" {
		return false
	}
	if work.Packet != nil && work.Packet.Gate != nil && strings.EqualFold(strings.TrimSpace(work.Packet.Gate.GateState), "closed") {
		return false
	}
	if disposition.CoolingDown && target.ExistingTask == nil {
		return false
	}
	if idleReflectionHasActiveNonIdleWork(target) {
		return false
	}
	return true
}

func idleReflectionTargetRequiresDeterministicTask(target idleReflectionTarget) bool {
	if len(uniqueTrimmedCSVStrings(target.RecentEvidenceDocKeys)) > 0 {
		return true
	}
	return idleReflectionTargetSkipsAmbientAutonomy(target)
}

func idleReflectionTargetSkipsAmbientAutonomy(target idleReflectionTarget) bool {
	text := strings.ToLower(strings.Join([]string{
		target.Key,
		target.Title,
		target.Description,
	}, "\n"))
	return containsAnySignal(text, []string{
		"visual_acceptance_gap",
		"visual acceptance debt",
		"canonical integration hint",
		"blocked patch queue convergence hint",
	})
}

func idleReflectionHasActiveNonIdleWork(target idleReflectionTarget) bool {
	return len(target.ActiveTaskIDs) > 0
}

func idleReflectionActiveWorkMayNeedPatchQueuePruning(snapshot WorkspaceSnapshot, target idleReflectionTarget) bool {
	if len(target.ActiveTaskIDs) == 0 {
		return false
	}
	tasksByID := make(map[string]WorkspaceTaskRecord, len(snapshot.Tasks))
	for _, task := range snapshot.Tasks {
		if taskID := strings.TrimSpace(task.TaskID); taskID != "" {
			tasksByID[taskID] = task
		}
	}
	hasPatchQueueContinuation := false
	for _, taskID := range target.ActiveTaskIDs {
		task, ok := tasksByID[strings.TrimSpace(taskID)]
		if !ok {
			return false
		}
		isPatchQueueContinuation := idleReflectionTaskIsOpenPatchQueueContinuation(task)
		if idleReflectionTaskHasActiveOwnership(task) && !isPatchQueueContinuation {
			return false
		}
		if !isPatchQueueContinuation {
			return false
		}
		hasPatchQueueContinuation = true
	}
	return hasPatchQueueContinuation
}

func buildIdleReflectionTarget(snapshot WorkspaceSnapshot, workspaceID string, now time.Time, trustFirst bool, policy AgentMetacognitionPolicy) idleReflectionTarget {
	target := idleReflectionTarget{}
	policy = describeMetacognitionPolicy(policy)
	if !policy.CanOpenReflectionTasks {
		return target
	}
	target.ReflectionScope = strings.TrimSpace(policy.ReflectionScope)
	target.IdleActionPolicy = strings.TrimSpace(policy.IdleActionPolicy)
	openTasks := make([]WorkspaceTaskRecord, 0, len(snapshot.Tasks))
	for _, task := range snapshot.Tasks {
		if taskSubmitTaskIsTerminal(task) || isIdleReflectionTask(task) {
			continue
		}
		openTasks = append(openTasks, task)
	}
	if !trustFirst && idleReflectionSuppressedByActiveWork(openTasks) {
		return target
	}

	if projectID := dominantIdleReflectionProjectID(openTasks, snapshot.Projects); strings.TrimSpace(projectID) != "" {
		projectTitle := projectTitleByID(snapshot.Projects, projectID)
		target.ScopeKind = "project"
		target.ScopeID = projectID
		target.ProjectID = projectID
		target.ProjectTitle = projectTitle
		target.ProjectLane = "qa"
		target.Key = "project:" + projectID
		target.Title = idleReflectionTitle(policy, "Product quality iteration: inspect and improve "+firstNonEmpty(projectTitle, projectID))
		target.EmptyProductFrontier = idleReflectionProjectConvergenceFrontierIsClear(openTasks, projectID)
		target.FrontierBlockedOnlyByRevisionFollowups = idleReflectionFrontierBlockedOnlyByRevisionFollowups(openTasks, projectID)
		target.Description = idleReflectionDescription(target.ScopeKind, firstNonEmpty(projectTitle, projectID), openTasks, policy, target.EmptyProductFrontier)
	} else {
		if idleReflectionHasUnprojectedOpenWork(openTasks) {
			return idleReflectionTarget{}
		}
		target.ScopeKind = "workspace"
		target.ScopeID = firstNonEmpty(workspaceID, snapshot.Workspace.WorkspaceID, "workspace")
		target.ProjectLane = "qa"
		target.Key = "workspace:" + target.ScopeID
		target.Title = idleReflectionTitle(policy, "Product quality iteration: inspect idle workspace and create useful work")
		target.Description = idleReflectionDescription(target.ScopeKind, firstNonEmpty(snapshot.Workspace.Title, target.ScopeID), openTasks, policy, len(openTasks) == 0)
	}

	for _, task := range openTasks {
		if target.ProjectID != "" && strings.TrimSpace(task.ProjectID) != target.ProjectID {
			continue
		}
		taskID := strings.TrimSpace(task.TaskID)
		if taskID == "" {
			continue
		}
		target.OpenTaskIDs = append(target.OpenTaskIDs, taskID)
		if taskClaimStatus(task) == "BLOCKED" || strings.EqualFold(strings.TrimSpace(task.Status), "BLOCKED") {
			target.BlockedTaskIDs = append(target.BlockedTaskIDs, taskID)
		} else if idleReflectionTaskHasActiveOwnership(task) || idleReflectionTaskIsOpenPatchQueueContinuation(task) {
			target.ActiveTaskIDs = append(target.ActiveTaskIDs, taskID)
		}
		if len(target.AnchorTaskIDs) < 8 {
			target.AnchorTaskIDs = append(target.AnchorTaskIDs, taskID)
		}
		target.TaskRecords = append(target.TaskRecords, task)
	}

	if existing, ok := findOpenIdleReflectionTaskForTarget(snapshot.Tasks, target); ok {
		existingCopy := existing
		target.TaskID = strings.TrimSpace(existing.TaskID)
		target.ExistingTask = &existingCopy
		return target
	}

	target.TaskID = idleReflectionTaskID(target.Key, now)
	if existing, ok := findBootstrapTask(snapshot.Tasks, target.TaskID); ok {
		existingCopy := existing
		target.ExistingTask = &existingCopy
	}
	return target
}

func idleReflectionTaskIsOpenPatchQueueContinuation(task WorkspaceTaskRecord) bool {
	if taskSubmitTaskIsTerminal(task) || strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	switch lane {
	case "review", "reviewer", "validation", "verification", "qa", "integration", "integrator":
	default:
		return false
	}
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		lane,
		strings.Join(task.Tags, " "),
	}, "\n"))
	if !containsAnySignal(text, []string{"patch_queue", "patch-queue", "patch queue", "patchq", "queue_id", "item_id", "branch_id"}) {
		return false
	}
	return containsAnySignal(text, []string{"review", "validate", "validation", "verify", "verification", "smoke", "integration", "integrate", "accepted", "blocked", "candidate"})
}

func (r *Runtime) freshProjectEvidenceDocsSince(ctx context.Context, target idleReflectionTarget, since time.Time) ([]string, bool) {
	if r == nil || r.client == nil || strings.TrimSpace(target.ProjectID) == "" || since.IsZero() {
		return nil, false
	}
	docs, err := r.client.ListDocs(ctx, r.cfg.WorkspaceID, 100)
	if err != nil {
		log.Printf("[planner] fresh project evidence doc scan skipped for %s: %v", target.ProjectID, err)
		return nil, false
	}
	keys := make([]string, 0, 4)
	for _, doc := range docs {
		if len(keys) >= 4 {
			break
		}
		if doc.ArchivedAt != nil {
			continue
		}
		updatedAt := parseIdleReflectionDocUpdatedAt(doc.UpdatedAt)
		if updatedAt.IsZero() || !updatedAt.After(since) {
			continue
		}
		if !idleReflectionDocMatchesProject(doc, target) || !idleReflectionDocLooksLikeEvidenceSignal(doc) {
			continue
		}
		if key := strings.TrimSpace(doc.DocKey); key != "" {
			keys = append(keys, key)
		}
	}
	keys = uniqueTrimmedCSVStrings(keys)
	return keys, len(keys) > 0
}

func parseIdleReflectionDocUpdatedAt(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed
	}
	return time.Time{}
}

func idleReflectionDocMatchesProject(doc WorkspaceDocRecord, target idleReflectionTarget) bool {
	text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title}, "\n"))
	for _, needle := range idleReflectionProjectNeedles(target) {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func idleReflectionProjectNeedles(target idleReflectionTarget) []string {
	values := []string{
		strings.TrimSpace(target.ProjectID),
		strings.TrimSpace(target.ProjectTitle),
		strings.ReplaceAll(strings.TrimSpace(target.ProjectID), "-", " "),
		strings.ReplaceAll(strings.TrimSpace(target.ProjectID), "_", " "),
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" || len(value) < 4 {
			continue
		}
		out = append(out, value)
	}
	return uniqueTrimmedCSVStrings(out)
}

func idleReflectionDocLooksLikeEvidenceSignal(doc WorkspaceDocRecord) bool {
	text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title}, "\n"))
	return containsAnySignal(text, []string{
		"evidence",
		"validation",
		"browser_smoke",
		"browser smoke",
		"smoke",
		"canonical_validation",
		"review decision",
		"gap",
		"failure",
		"blocked",
		"mismatch",
		"result",
	})
}

func appendFreshProjectEvidenceGuidance(description string, docKeys []string) string {
	docKeys = uniqueTrimmedCSVStrings(docKeys)
	if len(docKeys) == 0 {
		return description
	}
	var b strings.Builder
	b.WriteString(strings.TrimSpace(description))
	if b.Len() > 0 {
		b.WriteString("\n\n")
	}
	b.WriteString("Fresh evidence wake signal:\n")
	b.WriteString("- A project-scoped evidence/review/result document changed after the previous idle reflection cooldown began.\n")
	b.WriteString("- Treat this as a novelty signal that overrides the ordinary reflection cooldown; inspect these docs before deciding no work remains.\n")
	b.WriteString("- If the evidence is negative or shows product/provenance mismatch, create or claim the smallest repair/validation follow-up instead of writing another passive note.\n")
	b.WriteString("- fresh_evidence_doc_keys: " + strings.Join(docKeys, ", ") + "\n")
	return b.String()
}

type idleReflectionIntegrationHint struct {
	QueueID      string
	ItemID       string
	BranchID     string
	BranchName   string
	HeadSHA      string
	Decision     string
	ReviewDocKey string
}

type idleReflectionBlockedQueueHint struct {
	QueueID        string
	ItemID         string
	BranchID       string
	BranchName     string
	OwnerAgentID   string
	HeadSHA        string
	Decision       string
	DecisionDocKey string
	ReviewDocKey   string
}

type idleReflectionVisualAcceptanceGap struct {
	QueueID         string
	ItemID          string
	ProjectID       string
	BranchID        string
	BranchName      string
	HeadSHA         string
	Decision        string
	DecisionDocKey  string
	ReviewDocKey    string
	EvidenceDocKey  string
	EvidenceDocKeys []string
	Reasons         []string
	Missing         []string
	BlockingSignals []string
}

func (r *Runtime) enrichIdleReflectionTargetWithProjectCoordination(ctx context.Context, target idleReflectionTarget, policy AgentMetacognitionPolicy, now time.Time) idleReflectionTarget {
	if r == nil || r.client == nil || strings.TrimSpace(target.ProjectID) == "" {
		return target
	}
	target = r.enrichIdleReflectionTargetWithReflectionBoard(ctx, target, policy)
	coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, target.ProjectID)
	if err != nil {
		log.Printf("[planner] idle reflection project coordination hint skipped for %s: %v", target.ProjectID, err)
		return target
	}
	target = idleReflectionTargetWithProjectCoordination(target, coordination, policy)
	if gap, ok := r.acceptedPatchQueueVisualAcceptanceGap(ctx, target, coordination); ok {
		return idleReflectionTargetWithVisualAcceptanceGap(target, gap, policy, now)
	}
	return target
}

func (r *Runtime) enrichIdleReflectionTargetWithReflectionBoard(ctx context.Context, target idleReflectionTarget, policy AgentMetacognitionPolicy) idleReflectionTarget {
	if r == nil || r.client == nil || strings.TrimSpace(target.ProjectID) == "" {
		return target
	}
	docKey := "project." + strings.TrimSpace(target.ProjectID) + ".reflection_board"
	doc, ok, err := r.client.GetDoc(ctx, r.cfg.WorkspaceID, docKey)
	if err != nil {
		log.Printf("[planner] idle reflection board hint skipped for %s: %v", target.ProjectID, err)
		return target
	}
	if !ok || strings.TrimSpace(doc.Content) == "" {
		return target
	}
	return idleReflectionTargetWithReflectionBoard(target, docKey, doc.Content, policy)
}

func idleReflectionTargetWithProjectCoordination(target idleReflectionTarget, coordination ProjectCoordinationRecord, policy AgentMetacognitionPolicy) idleReflectionTarget {
	target = idleReflectionTargetWithoutResolvedPatchQueueTasks(target, coordination)
	hint, ok := acceptedPatchQueueIntegrationHint(coordination)
	if ok {
		target.ProjectLane = "integration"
		target.Title = "Integration convergence pass: merge accepted project candidate"
		target.Description = appendAcceptedPatchQueueIntegrationGuidance(target.Description, target.ProjectID, hint, policy)
		return target
	}
	blockedHint, ok := blockedPatchQueueConvergenceHint(coordination)
	if ok {
		target.ProjectLane = "integration"
		target.Title = "Patch queue convergence pass: resolve blocked candidate evidence"
		target.Description = appendBlockedPatchQueueConvergenceGuidance(target.Description, target.ProjectID, blockedHint, policy)
	}
	return target
}

func (r *Runtime) acceptedPatchQueueVisualAcceptanceGap(ctx context.Context, target idleReflectionTarget, coordination ProjectCoordinationRecord) (idleReflectionVisualAcceptanceGap, bool) {
	if r == nil || r.client == nil {
		return idleReflectionVisualAcceptanceGap{}, false
	}
	docsByKey := r.visualAcceptanceDocsForProjectCoordination(ctx, target, coordination)
	return acceptedPatchQueueVisualAcceptanceGapFromDocs(coordination, docsByKey)
}

func (r *Runtime) visualAcceptanceDocsForProjectCoordination(ctx context.Context, target idleReflectionTarget, coordination ProjectCoordinationRecord) map[string]WorkspaceDocRecord {
	docsByKey := map[string]WorkspaceDocRecord{}
	branchesByID := projectBranchesByID(coordination.Branches)
	hasAcceptedItem := false
	hasAcceptedUIPathset := false
	for _, item := range coordination.PatchQueueItems {
		if !strings.EqualFold(strings.TrimSpace(item.State), "ACCEPTED") {
			continue
		}
		hasAcceptedItem = true
		if len(uiFacingSignalsForPatchQueueItem(nil, item, item.DecisionSummary)) > 0 {
			hasAcceptedUIPathset = true
		}
		branch := branchesByID[strings.TrimSpace(item.BranchID)]
		for _, docKey := range visualAcceptanceDocKeysForPatchQueueItem(item, branch) {
			if strings.TrimSpace(docKey) == "" {
				continue
			}
			if _, ok := docsByKey[docKey]; ok {
				continue
			}
			doc, ok, err := r.client.GetDoc(ctx, r.cfg.WorkspaceID, docKey)
			if err != nil {
				log.Printf("[planner] visual acceptance doc fetch skipped for %s: %v", docKey, err)
				continue
			}
			if ok && doc.ArchivedAt == nil {
				docsByKey[strings.TrimSpace(doc.DocKey)] = doc
			}
		}
	}
	if !hasAcceptedItem || !hasAcceptedUIPathset {
		return docsByKey
	}
	docs, err := r.client.ListDocs(ctx, r.cfg.WorkspaceID, 200)
	if err != nil {
		log.Printf("[planner] visual acceptance project doc scan skipped for %s: %v", target.ProjectID, err)
		return docsByKey
	}
	for _, doc := range docs {
		docKey := strings.TrimSpace(doc.DocKey)
		if docKey == "" || doc.ArchivedAt != nil {
			continue
		}
		if _, ok := docsByKey[docKey]; ok {
			continue
		}
		if !idleReflectionDocMatchesProject(doc, target) || !idleReflectionDocLooksLikeVisualAcceptanceSignal(doc) {
			continue
		}
		docsByKey[docKey] = doc
	}
	return docsByKey
}

func acceptedPatchQueueVisualAcceptanceGapFromDocs(coordination ProjectCoordinationRecord, docsByKey map[string]WorkspaceDocRecord) (idleReflectionVisualAcceptanceGap, bool) {
	branchesByID := projectBranchesByID(coordination.Branches)
	var selected idleReflectionVisualAcceptanceGap
	var selectedItem ProjectPatchQueueItemRecord
	hasSelected := false
	for _, item := range coordination.PatchQueueItems {
		if !strings.EqualFold(strings.TrimSpace(item.State), "ACCEPTED") {
			continue
		}
		if openVisualAcceptanceFollowupTaskExists(coordination.Tasks, item) {
			continue
		}
		branch := branchesByID[strings.TrimSpace(item.BranchID)]
		docs := visualAcceptanceDocsForPatchQueueItem(item, branch, docsByKey)
		gate := visualAcceptanceGateForPatchQueueItem(docs, item, item.DecisionSummary)
		if !gate.Required || gate.Satisfied {
			continue
		}
		gap := idleReflectionVisualAcceptanceGap{
			QueueID:         strings.TrimSpace(item.QueueID),
			ItemID:          strings.TrimSpace(item.ItemID),
			ProjectID:       strings.TrimSpace(firstNonEmpty(item.ProjectID, coordination.Project.ProjectID)),
			BranchID:        strings.TrimSpace(item.BranchID),
			BranchName:      strings.TrimSpace(branch.BranchName),
			HeadSHA:         firstNonEmpty(strings.TrimSpace(item.HeadSHA), strings.TrimSpace(branch.HeadSHA)),
			Decision:        strings.TrimSpace(item.DecisionSummary),
			DecisionDocKey:  strings.TrimSpace(item.DecisionDocKey),
			ReviewDocKey:    firstNonEmpty(strings.TrimSpace(item.ReviewDocKey), strings.TrimSpace(branch.ReviewDocKey)),
			EvidenceDocKey:  strings.TrimSpace(item.EvidenceDocKey),
			EvidenceDocKeys: uniqueTrimmedCSVStrings(gate.EvidenceDocKeys),
			Reasons:         uniqueTrimmedCSVStrings(gate.Reasons),
			Missing:         uniqueTrimmedCSVStrings(gate.Missing),
			BlockingSignals: uniqueTrimmedCSVStrings(gate.BlockingSignals),
		}
		if !hasSelected || patchQueueItemLooksNewer(item, selectedItem) {
			selected = gap
			selectedItem = item
			hasSelected = true
		}
	}
	return selected, hasSelected
}

func projectBranchesByID(branches []ProjectBranchRecord) map[string]ProjectBranchRecord {
	branchesByID := map[string]ProjectBranchRecord{}
	for _, branch := range branches {
		branchID := strings.TrimSpace(branch.BranchID)
		if branchID != "" {
			branchesByID[branchID] = branch
		}
	}
	return branchesByID
}

func visualAcceptanceDocKeysForPatchQueueItem(item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) []string {
	return uniqueTrimmedCSVStrings([]string{
		strings.TrimSpace(item.DecisionDocKey),
		strings.TrimSpace(item.ReviewDocKey),
		strings.TrimSpace(item.EvidenceDocKey),
		strings.TrimSpace(item.ReviewerAdvisory.ReviewDocKey),
		strings.TrimSpace(branch.ReviewDocKey),
	})
}

func visualAcceptanceDocsForPatchQueueItem(item ProjectPatchQueueItemRecord, branch ProjectBranchRecord, docsByKey map[string]WorkspaceDocRecord) []WorkspaceDocRecord {
	if len(docsByKey) == 0 {
		return nil
	}
	directKeys := map[string]bool{}
	for _, docKey := range visualAcceptanceDocKeysForPatchQueueItem(item, branch) {
		if strings.TrimSpace(docKey) != "" {
			directKeys[strings.TrimSpace(docKey)] = true
		}
	}
	identifiers := visualAcceptanceCandidateIdentifiers(item, branch)
	outByKey := map[string]WorkspaceDocRecord{}
	for docKey, doc := range docsByKey {
		normalizedDocKey := strings.TrimSpace(firstNonEmpty(doc.DocKey, docKey))
		if normalizedDocKey == "" || doc.ArchivedAt != nil {
			continue
		}
		if directKeys[normalizedDocKey] {
			outByKey[normalizedDocKey] = doc
			continue
		}
		text := strings.ToLower(strings.Join([]string{normalizedDocKey, doc.Title, doc.Content}, "\n"))
		if !idleReflectionTextLooksLikeVisualAcceptanceSignal(text) {
			continue
		}
		for _, identifier := range identifiers {
			if strings.Contains(text, strings.ToLower(identifier)) {
				outByKey[normalizedDocKey] = doc
				break
			}
		}
	}
	out := make([]WorkspaceDocRecord, 0, len(outByKey))
	for _, doc := range outByKey {
		out = append(out, doc)
	}
	return out
}

func visualAcceptanceCandidateIdentifiers(item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) []string {
	identifiers := []string{
		item.QueueID,
		item.ItemID,
		item.BranchID,
		branch.BranchID,
		branch.BranchName,
		item.HeadSHA,
		branch.HeadSHA,
	}
	head := firstNonEmpty(strings.TrimSpace(item.HeadSHA), strings.TrimSpace(branch.HeadSHA))
	if len(head) > 12 {
		identifiers = append(identifiers, head[:12])
	}
	return uniqueTrimmedCSVStrings(identifiers)
}

func idleReflectionDocLooksLikeVisualAcceptanceSignal(doc WorkspaceDocRecord) bool {
	text := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title, doc.Content}, "\n"))
	return idleReflectionTextLooksLikeVisualAcceptanceSignal(text)
}

func idleReflectionTextLooksLikeVisualAcceptanceSignal(text string) bool {
	return containsAnySignal(text, []string{
		"rhizome_visual_acceptance_v1",
		"visual_acceptance",
		"visual acceptance",
		"visual_verdict",
		"viewport_matrix",
		"screenshot",
		"screenshots",
		"overlap",
		"clipping",
		"readability",
		"layout check",
	})
}

func openVisualAcceptanceFollowupTaskExists(tasks []WorkspaceTaskRecord, item ProjectPatchQueueItemRecord) bool {
	branchID := strings.TrimSpace(item.BranchID)
	itemID := strings.TrimSpace(item.ItemID)
	queueID := strings.TrimSpace(item.QueueID)
	for _, task := range tasks {
		if taskSubmitTaskIsTerminal(task) {
			continue
		}
		text := strings.ToLower(strings.Join([]string{
			task.TaskID,
			task.Title,
			task.Description,
			task.ProjectLane,
			strings.Join(task.Tags, " "),
		}, "\n"))
		if !containsAnySignal(text, []string{"visual acceptance", "visual_acceptance", "rhizome_visual_acceptance_v1", "visual qa", "screenshot", "viewport"}) {
			continue
		}
		if itemID != "" && strings.Contains(text, strings.ToLower(itemID)) {
			return true
		}
		if itemID == "" && queueID != "" && strings.Contains(text, strings.ToLower(queueID)) {
			return true
		}
		if itemID == "" && queueID == "" && branchID != "" && strings.Contains(text, strings.ToLower(branchID)) {
			return true
		}
	}
	return false
}

func patchQueueItemLooksNewer(left, right ProjectPatchQueueItemRecord) bool {
	leftTime := patchQueueItemObservationTime(left)
	rightTime := patchQueueItemObservationTime(right)
	if leftTime.IsZero() && rightTime.IsZero() {
		return strings.TrimSpace(left.ItemID) > strings.TrimSpace(right.ItemID)
	}
	if rightTime.IsZero() {
		return true
	}
	if leftTime.IsZero() {
		return false
	}
	return leftTime.After(rightTime)
}

func patchQueueItemObservationTime(item ProjectPatchQueueItemRecord) time.Time {
	for _, value := range []string{item.DecidedAt, item.UpdatedAt, item.CreatedAt} {
		if parsed := parseIdleReflectionDocUpdatedAt(value); !parsed.IsZero() {
			return parsed
		}
	}
	return time.Time{}
}

func idleReflectionTargetWithVisualAcceptanceGap(target idleReflectionTarget, gap idleReflectionVisualAcceptanceGap, policy AgentMetacognitionPolicy, now time.Time) idleReflectionTarget {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if strings.TrimSpace(target.ProjectID) == "" {
		target.ProjectID = strings.TrimSpace(gap.ProjectID)
	}
	target.ProjectLane = "qa"
	target.Key = idleReflectionVisualAcceptanceGapKey(target, gap)
	target.TaskID = idleReflectionTaskID(target.Key, now)
	target.ExistingTask = nil
	target.Title = "Visual acceptance pass: verify accepted UI candidate"
	target.Description = appendVisualAcceptanceGapGuidance(target.Description, target.ProjectID, gap, policy)
	return target
}

func idleReflectionVisualAcceptanceGapKey(target idleReflectionTarget, gap idleReflectionVisualAcceptanceGap) string {
	projectID := firstNonEmpty(strings.TrimSpace(target.ProjectID), strings.TrimSpace(gap.ProjectID), "project")
	head := strings.TrimSpace(gap.HeadSHA)
	if len(head) > 12 {
		head = head[:12]
	}
	seed := strings.Join(uniqueTrimmedCSVStrings([]string{
		projectID,
		strings.TrimSpace(gap.QueueID),
		strings.TrimSpace(gap.ItemID),
		strings.TrimSpace(gap.BranchID),
		head,
	}), ":")
	return "project:" + projectID + "|visual_acceptance_gap:" + shortHash(seed)
}

func appendVisualAcceptanceGapGuidance(description, projectID string, gap idleReflectionVisualAcceptanceGap, policy AgentMetacognitionPolicy) string {
	var b strings.Builder
	b.WriteString(strings.TrimSpace(description))
	if b.Len() > 0 {
		b.WriteString("\n\n")
	}
	head := strings.TrimSpace(gap.HeadSHA)
	if len(head) > 12 {
		head = head[:12]
	}
	b.WriteString("Visual acceptance debt:\n")
	b.WriteString("- Project coordination has an ACCEPTED UI-facing patch queue candidate without durable visual acceptance evidence. Treat the ACCEPTED state as insufficient until the UI has been checked like a real user would see it.\n")
	b.WriteString("- Publish a workspace doc containing schema: rhizome_visual_acceptance_v1, screenshot refs or local artifact paths, viewport_matrix for desktop plus narrow/mobile, first viewport/empty state, primary path, post-action/output/result state, overlap/clipping/contrast/readability/responsive fit/typography/hierarchy/spacing/usability checks, primary-surface geometry/density checks, and visual_verdict: pass only when there are no blocking findings.\n")
	b.WriteString("- If screenshots reveal broken layout, unreadable text, laggy primary flows, stale previews, suspicious resizing/stretching, or a board/grid/canvas/editor/game surface with implausible aspect ratio, tiny cells, wrapping, bad density, or mode/preset/difficulty-specific fit, do not close this as pass. Create or claim the smallest repair task and link it to this queue/item/branch.\n")
	if strings.TrimSpace(projectID) != "" {
		b.WriteString("- Project: " + strings.TrimSpace(projectID) + "\n")
	}
	if strings.TrimSpace(gap.QueueID) != "" {
		b.WriteString("- queue_id: " + strings.TrimSpace(gap.QueueID) + "\n")
	}
	if strings.TrimSpace(gap.ItemID) != "" {
		b.WriteString("- item_id: " + strings.TrimSpace(gap.ItemID) + "\n")
	}
	if strings.TrimSpace(gap.BranchID) != "" {
		b.WriteString("- branch_id: " + strings.TrimSpace(gap.BranchID) + "\n")
	}
	if strings.TrimSpace(gap.BranchName) != "" || head != "" {
		b.WriteString(fmt.Sprintf("- Candidate branch: %s @ %s.\n", firstNonEmpty(strings.TrimSpace(gap.BranchName), "-"), firstNonEmpty(head, "-")))
	}
	if len(gap.Reasons) > 0 {
		b.WriteString("- UI-facing signals: " + strings.Join(gap.Reasons, ", ") + "\n")
	}
	if len(gap.Missing) > 0 {
		b.WriteString("- Missing visual evidence: " + strings.Join(gap.Missing, "; ") + "\n")
	}
	if len(gap.BlockingSignals) > 0 {
		b.WriteString("- Blocking visual signals already seen in docs: " + strings.Join(gap.BlockingSignals, ", ") + "\n")
	}
	if strings.TrimSpace(gap.DecisionDocKey) != "" {
		b.WriteString("- Decision evidence: " + strings.TrimSpace(gap.DecisionDocKey) + "\n")
	}
	if strings.TrimSpace(gap.ReviewDocKey) != "" {
		b.WriteString("- Review evidence: " + strings.TrimSpace(gap.ReviewDocKey) + "\n")
	}
	if strings.TrimSpace(gap.EvidenceDocKey) != "" {
		b.WriteString("- Implementation evidence: " + strings.TrimSpace(gap.EvidenceDocKey) + "\n")
	}
	if len(gap.EvidenceDocKeys) > 0 {
		b.WriteString("- Candidate visual docs inspected: " + strings.Join(gap.EvidenceDocKeys, ", ") + "\n")
	}
	if strings.TrimSpace(gap.Decision) != "" {
		b.WriteString("- Acceptance decision summary: " + oneLine(gap.Decision) + "\n")
	}
	policy = describeMetacognitionPolicy(policy)
	if strings.TrimSpace(policy.ReflectionScope) != "" {
		b.WriteString("- Keep the pass inside reflection_scope=" + strings.TrimSpace(policy.ReflectionScope) + ", but convert any concrete defect into task_submit repair work instead of writing another generic reflection.\n")
	}
	return b.String()
}

func idleReflectionTargetWithoutResolvedPatchQueueTasks(target idleReflectionTarget, coordination ProjectCoordinationRecord) idleReflectionTarget {
	if strings.TrimSpace(target.ProjectID) == "" || (len(coordination.Tasks) == 0 && len(target.TaskRecords) == 0) {
		return target
	}
	tasksByID := make(map[string]WorkspaceTaskRecord, len(coordination.Tasks)+len(target.TaskRecords))
	for _, task := range target.TaskRecords {
		taskID := strings.TrimSpace(task.TaskID)
		if taskID != "" {
			tasksByID[taskID] = task
		}
	}
	for _, task := range coordination.Tasks {
		taskID := strings.TrimSpace(task.TaskID)
		if taskID != "" {
			tasksByID[taskID] = task
		}
	}
	candidateIDs := uniqueTrimmedCSVStrings(target.OpenTaskIDs, target.BlockedTaskIDs, target.ActiveTaskIDs, target.AnchorTaskIDs)
	staleIDs := make([]string, 0)
	for _, taskID := range candidateIDs {
		task, ok := tasksByID[taskID]
		if !ok {
			continue
		}
		if stale, _ := idleReflectionPatchQueueTaskResolvedByCoordination(task, coordination); stale {
			staleIDs = append(staleIDs, taskID)
		}
	}
	staleIDs = uniqueTrimmedCSVStrings(staleIDs)
	if len(staleIDs) == 0 {
		return target
	}
	target.StaleTaskIDs = uniqueTrimmedCSVStrings(target.StaleTaskIDs, staleIDs)
	target.OpenTaskIDs = idleReflectionRemoveTaskIDs(target.OpenTaskIDs, staleIDs)
	target.BlockedTaskIDs = idleReflectionRemoveTaskIDs(target.BlockedTaskIDs, staleIDs)
	target.ActiveTaskIDs = idleReflectionRemoveTaskIDs(target.ActiveTaskIDs, staleIDs)
	target.AnchorTaskIDs = idleReflectionRemoveTaskIDs(target.AnchorTaskIDs, staleIDs)
	target.Description = appendResolvedPatchQueueTaskGuidance(target.Description, staleIDs)
	return target
}

func idleReflectionRemoveTaskIDs(values, remove []string) []string {
	if len(values) == 0 || len(remove) == 0 {
		return values
	}
	removeSet := make(map[string]struct{}, len(remove))
	for _, value := range remove {
		if value = strings.TrimSpace(value); value != "" {
			removeSet[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, drop := removeSet[trimmed]; drop {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

type idleReflectionPatchQueueTaskRefs struct {
	QueueID              string
	ItemID               string
	BranchID             string
	HeadSHA              string
	PatchQueueTask       bool
	SupersedeTask        bool
	ClaimStewardshipTask bool
	OwnerSubmitTask      bool
	RevisionTask         bool
	ReviewTask           bool
	ValidationTask       bool
	IntegrationTask      bool
}

func idleReflectionPatchQueueTaskResolvedByCoordination(task WorkspaceTaskRecord, coordination ProjectCoordinationRecord) (bool, string) {
	if taskSubmitTaskIsTerminal(task) {
		return false, ""
	}
	refs := idleReflectionPatchQueueTaskRefsFromTask(task)
	if !refs.PatchQueueTask {
		return false, ""
	}
	if refs.OwnerSubmitTask && !refs.RevisionTask && refs.BranchID != "" && refs.HeadSHA != "" {
		if decision, ok := latestProjectPatchQueueTerminalItemForBranchHead(coordination.PatchQueueItems, refs.BranchID, refs.HeadSHA); ok && projectPatchQueuePositiveTerminalState(decision.State) {
			return true, "referenced branch/head already has an ACCEPTED patch queue decision"
		}
	}
	item, ok := idleReflectionPatchQueueReferencedItem(coordination.PatchQueueItems, refs)
	if !ok {
		return false, ""
	}
	state := strings.ToUpper(strings.TrimSpace(item.State))
	if refs.ClaimStewardshipTask && state != "CLAIMED" {
		return true, "referenced patch queue item is no longer CLAIMED"
	}
	if successor, ok := idleReflectionPatchQueueSuccessorItem(coordination.PatchQueueItems, item); ok {
		return true, fmt.Sprintf("referenced patch queue item is superseded by %s/%s in state %s", strings.TrimSpace(successor.QueueID), strings.TrimSpace(successor.ItemID), strings.ToUpper(strings.TrimSpace(successor.State)))
	}
	if projectPatchQueueNegativeTerminalState(item.State) && (refs.SupersedeTask || refs.OwnerSubmitTask || refs.ReviewTask || refs.ValidationTask) {
		if accepted, ok := idleReflectionNewerAcceptedProjectCandidate(coordination, item); ok {
			return true, fmt.Sprintf("referenced negative patch queue item is historical after newer accepted candidate %s/%s", strings.TrimSpace(accepted.QueueID), strings.TrimSpace(accepted.ItemID))
		}
	}
	if projectPatchQueuePositiveTerminalState(item.State) && (refs.SupersedeTask || refs.OwnerSubmitTask || refs.ReviewTask || refs.ValidationTask) {
		return true, "referenced patch queue item is already ACCEPTED"
	}
	if projectPatchQueuePositiveTerminalState(item.State) && !refs.IntegrationTask && (refs.QueueID != "" && refs.ItemID != "") {
		return true, "referenced patch queue item is already ACCEPTED"
	}
	return false, ""
}

func idleReflectionPatchQueueTaskRefsFromTask(task WorkspaceTaskRecord) idleReflectionPatchQueueTaskRefs {
	queueID, itemID, branchID := runtimeOwnerBoundPatchQueueRefsFromTask(task)
	texts := []string{task.Title, task.Description}
	queueID = firstNonEmpty(
		runtimeTaskTagValue(task, "queue:", "queue=", "queue-id:", "queue-id=", "queue_id:", "queue_id="),
		queueID,
	)
	itemID = firstNonEmpty(
		runtimeTaskTagValue(task, "item:", "item=", "item-id:", "item-id=", "item_id:", "item_id="),
		itemID,
	)
	branchID = firstNonEmpty(
		runtimeTaskTagValue(task, "branch:", "branch=", "branch-id:", "branch-id=", "branch_id:", "branch_id=", "owner-branch:", "owner-branch=", "owner_branch:", "owner_branch="),
		branchID,
	)
	headSHA := firstNonEmpty(
		runtimeTaskTagValue(task, "head:", "head=", "head-sha:", "head-sha=", "head_sha:", "head_sha=", "expected-head:", "expected-head=", "expected-head-sha:", "expected-head-sha=", "expected_head_sha:", "expected_head_sha="),
		runtimeTaskTextFieldValue(texts, "head_sha"),
		runtimeTaskTextFieldValue(texts, "expected_head_sha"),
		runtimeTaskTextFieldValue(texts, "expected head sha"),
	)
	lane := strings.ToLower(strings.TrimSpace(task.ProjectLane))
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		lane,
		strings.Join(task.Tags, " "),
	}, "\n"))
	patchQueueTask := containsAnySignal(text, []string{"patch_queue", "patch-queue", "patch queue", "patchq", "queue_id", "item_id", "project_patch_queue"})
	revisionTask := containsAnySignal(text, []string{
		"revision",
		"revise",
		"owner-bound-kind:patch_queue_revision",
		"owner_bound_kind:patch_queue_revision",
		"patch_queue_revision",
	})
	submitTask := containsAnySignal(text, []string{
		"owner-submit",
		"owner submit",
		"owner requeue submit",
		"branch-owner-submit",
		"branch owner submit",
		"owner-bound-kind:patch_queue_submit",
		"owner_bound_kind:patch_queue_submit",
	})
	return idleReflectionPatchQueueTaskRefs{
		QueueID:              strings.TrimSpace(queueID),
		ItemID:               strings.TrimSpace(itemID),
		BranchID:             strings.TrimSpace(branchID),
		HeadSHA:              strings.TrimSpace(headSHA),
		PatchQueueTask:       patchQueueTask,
		SupersedeTask:        containsAnySignal(text, []string{"supersede", "requeue", "project_patch_queue_supersede_available", "action=supersede", "action: supersede"}),
		ClaimStewardshipTask: containsAnySignal(text, []string{"claim-stewardship", "claim stewardship", "claimed patch queue item", "project_patch_queue_claim_stewardship_available"}),
		OwnerSubmitTask:      submitTask || (runtimeTaskHasOwnerBoundSignal(task) && !revisionTask),
		RevisionTask:         revisionTask,
		ReviewTask:           lane == "review" || lane == "reviewer" || containsAnySignal(text, []string{"independent review", "review patch queue", "review candidate"}),
		ValidationTask:       lane == "validation" || lane == "verification" || containsAnySignal(text, []string{"validate", "validation", "verification", "browser smoke", "browser-smoke", "smoke"}),
		IntegrationTask:      lane == "integration" || lane == "integrator" || containsAnySignal(text, []string{"project_patch_queue_integrate", "integrate accepted", "integration convergence", "merge accepted"}),
	}
}

func idleReflectionPatchQueueReferencedItem(items []ProjectPatchQueueItemRecord, refs idleReflectionPatchQueueTaskRefs) (ProjectPatchQueueItemRecord, bool) {
	queueID := strings.TrimSpace(refs.QueueID)
	itemID := strings.TrimSpace(refs.ItemID)
	if queueID != "" && itemID != "" {
		for _, item := range items {
			if strings.TrimSpace(item.QueueID) == queueID && strings.TrimSpace(item.ItemID) == itemID {
				return item, true
			}
		}
		return ProjectPatchQueueItemRecord{}, false
	}
	branchID := strings.TrimSpace(refs.BranchID)
	headSHA := strings.TrimSpace(refs.HeadSHA)
	if branchID != "" && headSHA != "" {
		var selected ProjectPatchQueueItemRecord
		count := 0
		for _, item := range items {
			if strings.TrimSpace(item.BranchID) != branchID || strings.TrimSpace(item.HeadSHA) != headSHA {
				continue
			}
			selected = item
			count++
		}
		if count == 1 {
			return selected, true
		}
		return ProjectPatchQueueItemRecord{}, false
	}
	if branchID != "" && queueID == "" && itemID == "" {
		var selected ProjectPatchQueueItemRecord
		count := 0
		for _, item := range items {
			if strings.TrimSpace(item.BranchID) != branchID {
				continue
			}
			selected = item
			count++
		}
		if count == 1 {
			return selected, true
		}
	}
	return ProjectPatchQueueItemRecord{}, false
}

func idleReflectionPatchQueueSuccessorItem(items []ProjectPatchQueueItemRecord, source ProjectPatchQueueItemRecord) (ProjectPatchQueueItemRecord, bool) {
	sourceQueueID := strings.TrimSpace(source.QueueID)
	sourceItemID := strings.TrimSpace(source.ItemID)
	if sourceQueueID == "" || sourceItemID == "" {
		return ProjectPatchQueueItemRecord{}, false
	}
	for _, item := range items {
		if strings.TrimSpace(item.QueueID) == sourceQueueID && strings.TrimSpace(item.ItemID) == sourceItemID {
			continue
		}
		if strings.TrimSpace(item.SupersedesQueueID) != sourceQueueID || strings.TrimSpace(item.SupersedesItemID) != sourceItemID {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(item.State)) {
		case "PROPOSED", "CLAIMED", "ACCEPTED", "BLOCKED":
			return item, true
		}
	}
	return ProjectPatchQueueItemRecord{}, false
}

func idleReflectionNewerAcceptedProjectCandidate(coordination ProjectCoordinationRecord, source ProjectPatchQueueItemRecord) (ProjectPatchQueueItemRecord, bool) {
	sourceUpdatedAt := strings.TrimSpace(source.UpdatedAt)
	branchesByID := make(map[string]ProjectBranchRecord, len(coordination.Branches))
	for _, branch := range coordination.Branches {
		if branchID := strings.TrimSpace(branch.BranchID); branchID != "" {
			branchesByID[branchID] = branch
		}
	}
	var selected ProjectPatchQueueItemRecord
	found := false
	for _, item := range coordination.PatchQueueItems {
		if !projectPatchQueuePositiveTerminalState(item.State) {
			continue
		}
		if strings.TrimSpace(item.QueueID) == strings.TrimSpace(source.QueueID) && strings.TrimSpace(item.ItemID) == strings.TrimSpace(source.ItemID) {
			continue
		}
		itemUpdatedAt := strings.TrimSpace(item.UpdatedAt)
		if sourceUpdatedAt != "" && itemUpdatedAt != "" {
			if itemUpdatedAt <= sourceUpdatedAt {
				continue
			}
		} else if branch, ok := branchesByID[strings.TrimSpace(item.BranchID)]; !ok || !projectBranchLooksIntegrated(branch) {
			continue
		}
		if !idleReflectionAcceptedCandidateCoversNegativeSource(source, item) {
			continue
		}
		if !found || itemUpdatedAt > strings.TrimSpace(selected.UpdatedAt) {
			selected = item
			found = true
		}
	}
	if !found {
		return ProjectPatchQueueItemRecord{}, false
	}
	return selected, true
}

func idleReflectionAcceptedCandidateCoversNegativeSource(source, accepted ProjectPatchQueueItemRecord) bool {
	if strings.TrimSpace(source.ProjectID) != "" && strings.TrimSpace(accepted.ProjectID) != "" && strings.TrimSpace(source.ProjectID) != strings.TrimSpace(accepted.ProjectID) {
		return false
	}
	if strings.TrimSpace(source.RepoID) != "" && strings.TrimSpace(accepted.RepoID) != "" && strings.TrimSpace(source.RepoID) != strings.TrimSpace(accepted.RepoID) {
		return false
	}
	if strings.TrimSpace(accepted.SupersedesQueueID) == strings.TrimSpace(source.QueueID) && strings.TrimSpace(accepted.SupersedesItemID) == strings.TrimSpace(source.ItemID) && strings.TrimSpace(source.QueueID) != "" && strings.TrimSpace(source.ItemID) != "" {
		return true
	}
	if strings.TrimSpace(source.BranchID) != "" && strings.TrimSpace(source.BranchID) == strings.TrimSpace(accepted.BranchID) {
		return true
	}
	if strings.TrimSpace(source.HeadSHA) != "" && strings.TrimSpace(source.HeadSHA) == strings.TrimSpace(accepted.HeadSHA) {
		return true
	}
	return idleReflectionPatchQueuePathsetsOverlap(projectPatchQueueFollowupPathset(source), projectPatchQueueFollowupPathset(accepted))
}

func idleReflectionPatchQueuePathsetsOverlap(left, right []string) bool {
	left = uniqueTrimmedCSVStrings(left)
	right = uniqueTrimmedCSVStrings(right)
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	for _, l := range left {
		for _, r := range right {
			if strings.TrimSpace(l) == "" || strings.TrimSpace(r) == "" {
				continue
			}
			if pathsetIsCoveredByScope([]string{l}, []string{r}) || pathsetIsCoveredByScope([]string{r}, []string{l}) {
				return true
			}
		}
	}
	return false
}

func appendResolvedPatchQueueTaskGuidance(description string, staleTaskIDs []string) string {
	staleTaskIDs = uniqueTrimmedCSVStrings(staleTaskIDs)
	if len(staleTaskIDs) == 0 {
		return description
	}
	var b strings.Builder
	b.WriteString(strings.TrimSpace(description))
	if b.Len() > 0 {
		b.WriteString("\n\n")
	}
	b.WriteString("Resolved patch queue task guidance:\n")
	b.WriteString("- Current project coordination shows these open-task hints are stale or already superseded; do not treat them as active blockers for no-task heartbeat, reflection, or follow-up creation.\n")
	b.WriteString("- ignored_stale_task_ids: " + strings.Join(staleTaskIDs, ", ") + "\n")
	b.WriteString("- Prefer current patch queue item state, successor links, and accepted/integration hints over older workspace task rows or reflection board mentions of those task ids.\n")
	return b.String()
}

func idleReflectionTargetWithReflectionBoard(target idleReflectionTarget, docKey, content string, policy AgentMetacognitionPolicy) idleReflectionTarget {
	policy = describeMetacognitionPolicy(policy)
	if !reflectionBoardHasActiveThread(content, policy.ReflectionScope) {
		return target
	}
	target.Title = idleReflectionTitle(policy, "Project reflection pass: join active reflection board thread")
	target.Description = appendReflectionBoardJoinGuidance(target.Description, docKey, policy)
	return target
}

func appendReflectionBoardJoinGuidance(description, docKey string, policy AgentMetacognitionPolicy) string {
	var b strings.Builder
	b.WriteString(strings.TrimSpace(description))
	if b.Len() > 0 {
		b.WriteString("\n\n")
	}
	policy = describeMetacognitionPolicy(policy)
	b.WriteString("Shared reflection board hint:\n")
	b.WriteString("- An active in-scope reflection thread already exists in " + strings.TrimSpace(docKey) + ".\n")
	b.WriteString("- Join, update, or critique that board thread before opening a new reflection direction.\n")
	b.WriteString("- If a concrete product or coordination bug emerges, convert it into the smallest task_submit follow-up allowed by idle_action_policy=" + strings.TrimSpace(policy.IdleActionPolicy) + ".\n")
	return b.String()
}

func reflectionBoardHasActiveThread(content, targetScope string) bool {
	targetScope = normalizeReflectionScope(targetScope)
	for _, entry := range reflectionBoardEntries(content) {
		if reflectionBoardEntryTerminal(entry) || !reflectionBoardEntryActive(entry) {
			continue
		}
		entryScope := normalizeReflectionScope(reflectionBoardEntryField(entry, []string{"reflection_scope", "scope"}))
		if entryScope == "" || targetScope == "" || entryScope == targetScope || targetScope == reflectionScopeGlobal {
			return true
		}
	}
	return false
}

func reflectionBoardEntries(content string) []string {
	content = strings.ReplaceAll(strings.TrimSpace(content), "\r\n", "\n")
	if content == "" {
		return nil
	}
	raw := strings.Split(content, "\n\n")
	entries := make([]string, 0, len(raw))
	for _, entry := range raw {
		if trimmed := strings.TrimSpace(entry); trimmed != "" {
			entries = append(entries, strings.ToLower(trimmed))
		}
	}
	return entries
}

func reflectionBoardEntryActive(entry string) bool {
	return containsAnySignal(entry, []string{
		"status: active",
		"status=active",
		"status: open",
		"status=open",
		"status: in_progress",
		"status=in_progress",
		"review_status: open",
		"review_status=open",
	})
}

func reflectionBoardEntryTerminal(entry string) bool {
	return containsAnySignal(entry, []string{
		"status: resolved",
		"status=resolved",
		"status: closed",
		"status=closed",
		"status: done",
		"status=done",
		"status: completed",
		"status=completed",
	})
}

func reflectionBoardEntryField(entry string, fields []string) string {
	for _, line := range strings.Split(strings.ToLower(entry), "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(line, "-"))
		for _, field := range fields {
			field = strings.ToLower(strings.TrimSpace(field))
			if field == "" {
				continue
			}
			for _, sep := range []string{":", "="} {
				prefix := field + sep
				if strings.HasPrefix(line, prefix) {
					return strings.TrimSpace(strings.TrimPrefix(line, prefix))
				}
			}
		}
	}
	return ""
}

func acceptedPatchQueueIntegrationHint(coordination ProjectCoordinationRecord) (idleReflectionIntegrationHint, bool) {
	branchesByID := map[string]ProjectBranchRecord{}
	for _, branch := range coordination.Branches {
		branchID := strings.TrimSpace(branch.BranchID)
		if branchID != "" {
			branchesByID[branchID] = branch
		}
	}
	for _, item := range coordination.PatchQueueItems {
		if !strings.EqualFold(strings.TrimSpace(item.State), "ACCEPTED") {
			continue
		}
		branchID := strings.TrimSpace(item.BranchID)
		if branchID == "" {
			continue
		}
		branch, ok := branchesByID[branchID]
		if !ok || strings.TrimSpace(branch.BranchID) == "" {
			continue
		}
		if projectBranchLooksIntegrated(branch) {
			continue
		}
		headSHA := firstNonEmpty(strings.TrimSpace(item.HeadSHA), strings.TrimSpace(branch.HeadSHA))
		if headSHA == "" {
			continue
		}
		return idleReflectionIntegrationHint{
			QueueID:      strings.TrimSpace(item.QueueID),
			ItemID:       strings.TrimSpace(item.ItemID),
			BranchID:     branchID,
			BranchName:   strings.TrimSpace(branch.BranchName),
			HeadSHA:      headSHA,
			Decision:     strings.TrimSpace(item.DecisionSummary),
			ReviewDocKey: firstNonEmpty(strings.TrimSpace(item.ReviewDocKey), strings.TrimSpace(branch.ReviewDocKey)),
		}, true
	}
	return idleReflectionIntegrationHint{}, false
}

func blockedPatchQueueConvergenceHint(coordination ProjectCoordinationRecord) (idleReflectionBlockedQueueHint, bool) {
	branchesByID := map[string]ProjectBranchRecord{}
	for _, branch := range coordination.Branches {
		branchID := strings.TrimSpace(branch.BranchID)
		if branchID != "" {
			branchesByID[branchID] = branch
		}
	}
	for _, item := range coordination.PatchQueueItems {
		if !strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED") {
			continue
		}
		if openPatchQueueFollowupTaskExists(coordination.Tasks, item) {
			continue
		}
		branchID := strings.TrimSpace(item.BranchID)
		if branchID == "" {
			continue
		}
		branch, ok := branchesByID[branchID]
		if !ok || strings.TrimSpace(branch.BranchID) == "" {
			continue
		}
		if projectBranchLooksIntegrated(branch) {
			continue
		}
		headSHA := firstNonEmpty(strings.TrimSpace(item.HeadSHA), strings.TrimSpace(branch.HeadSHA))
		if headSHA == "" {
			continue
		}
		return idleReflectionBlockedQueueHint{
			QueueID:        strings.TrimSpace(item.QueueID),
			ItemID:         strings.TrimSpace(item.ItemID),
			BranchID:       branchID,
			BranchName:     strings.TrimSpace(branch.BranchName),
			OwnerAgentID:   strings.TrimSpace(branch.AgentID),
			HeadSHA:        headSHA,
			Decision:       strings.TrimSpace(item.DecisionSummary),
			DecisionDocKey: strings.TrimSpace(item.DecisionDocKey),
			ReviewDocKey:   firstNonEmpty(strings.TrimSpace(item.ReviewDocKey), strings.TrimSpace(branch.ReviewDocKey)),
		}, true
	}
	return idleReflectionBlockedQueueHint{}, false
}

func openPatchQueueFollowupTaskExists(tasks []WorkspaceTaskRecord, item ProjectPatchQueueItemRecord) bool {
	strongNeedles := uniqueTrimmedCSVStrings([]string{
		item.ItemID,
		item.BranchID,
	})
	queueNeedle := strings.TrimSpace(item.QueueID)
	if len(strongNeedles) == 0 && queueNeedle == "" {
		return false
	}
	for _, task := range tasks {
		if taskSubmitTaskIsTerminal(task) {
			continue
		}
		text := strings.ToLower(strings.Join([]string{
			task.TaskID,
			task.Title,
			task.Description,
			task.ProjectLane,
			strings.Join(task.Tags, " "),
		}, "\n"))
		if !containsAnySignal(text, []string{"patch-queue", "patch queue", "patchq", "queue-facing", "requeue", "validation", "revision", "integration"}) {
			continue
		}
		strongMatches := 0
		for _, needle := range strongNeedles {
			if strings.Contains(text, strings.ToLower(strings.TrimSpace(needle))) {
				strongMatches++
			}
		}
		if strongMatches > 0 {
			return true
		}
		if len(strongNeedles) == 0 && queueNeedle != "" && strings.Contains(text, strings.ToLower(queueNeedle)) {
			return true
		}
	}
	return false
}

func projectBranchLooksIntegrated(branch ProjectBranchRecord) bool {
	switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
	case "MERGED", "ARCHIVED", "ABANDONED":
		return true
	default:
		return false
	}
}

func appendAcceptedPatchQueueIntegrationGuidance(description, projectID string, hint idleReflectionIntegrationHint, policy AgentMetacognitionPolicy) string {
	var b strings.Builder
	b.WriteString(strings.TrimSpace(description))
	if b.Len() > 0 {
		b.WriteString("\n\n")
	}
	head := strings.TrimSpace(hint.HeadSHA)
	if len(head) > 12 {
		head = head[:12]
	}
	b.WriteString("Canonical integration hint:\n")
	b.WriteString("- Project coordination exposes an ACCEPTED patch queue candidate that is not yet marked MERGED.\n")
	b.WriteString("- Do not treat a seed/default branch as proof that the product is missing while this reviewed candidate exists.\n")
	b.WriteString("- This task is the integration/convergence lane, not a request for another scaffold from seed.\n")
	b.WriteString(fmt.Sprintf("- Call project_patch_queue_integrate with project_id=%s", firstNonEmpty(strings.TrimSpace(projectID), strings.TrimSpace(hint.BranchID), "project")))
	if strings.TrimSpace(hint.QueueID) != "" {
		b.WriteString(", queue_id=" + strings.TrimSpace(hint.QueueID))
	}
	if strings.TrimSpace(hint.ItemID) != "" {
		b.WriteString(", item_id=" + strings.TrimSpace(hint.ItemID))
	}
	if strings.TrimSpace(hint.BranchID) != "" {
		b.WriteString(", branch_id=" + strings.TrimSpace(hint.BranchID))
	}
	b.WriteString(", then run a bounded build/smoke on the canonical target branch before opening improvement follow-ups.\n")
	if strings.TrimSpace(hint.BranchName) != "" || head != "" {
		b.WriteString(fmt.Sprintf("- Candidate branch: %s @ %s.\n", firstNonEmpty(strings.TrimSpace(hint.BranchName), "-"), firstNonEmpty(head, "-")))
	}
	if strings.TrimSpace(hint.ReviewDocKey) != "" {
		b.WriteString("- Review evidence: " + strings.TrimSpace(hint.ReviewDocKey) + "\n")
	}
	if strings.TrimSpace(hint.Decision) != "" {
		b.WriteString("- Acceptance decision: " + oneLine(hint.Decision) + "\n")
	}
	b.WriteString("- If integration fails because the source branch is unavailable or conflicts, record that fresh blocker and create/claim the smallest unblock task. Do not open duplicate seed scaffolding unless all reviewed candidates are invalid.\n")
	policy = describeMetacognitionPolicy(policy)
	if strings.TrimSpace(policy.ReflectionScope) != "" {
		b.WriteString("- Keep the pass inside reflection_scope=" + strings.TrimSpace(policy.ReflectionScope) + " while using integration as the next concrete reality check.\n")
	}
	return b.String()
}

func appendBlockedPatchQueueConvergenceGuidance(description, projectID string, hint idleReflectionBlockedQueueHint, policy AgentMetacognitionPolicy) string {
	var b strings.Builder
	b.WriteString(strings.TrimSpace(description))
	if b.Len() > 0 {
		b.WriteString("\n\n")
	}
	head := strings.TrimSpace(hint.HeadSHA)
	if len(head) > 12 {
		head = head[:12]
	}
	suggestedItemID := projectPatchQueueRequeueItemID(hint)
	b.WriteString("Blocked patch queue convergence hint:\n")
	b.WriteString("- Project coordination exposes a BLOCKED patch queue candidate for the current branch/head. Resolve this queue-facing state before treating the product as complete or opening unrelated polish work.\n")
	b.WriteString("- First inspect the decision doc, review packet, and newer validation/QA docs. Newer evidence only supersedes the blocker when it names the same branch_id and head_sha and was produced after the BLOCKED decision.\n")
	if strings.TrimSpace(hint.OwnerAgentID) != "" {
		b.WriteString("- Branch owner: owner_agent_id=" + strings.TrimSpace(hint.OwnerAgentID) + ". Code-changing revisions still belong to that owner, but a reviewer/integrator/strategic lead can perform an evidence-bound same-head lifecycle supersede when fresh exact validation evidence exists.\n")
	}
	b.WriteString("- If newer validation evidence passes, use project_patch_queue_lifecycle action=supersede/requeue with a fresh new_item_id plus validation_doc_key/evidence_doc_key, or ask an eligible reviewer/integrator/strategic lead to do that exact lifecycle action. Do not rely on read-only model.ask as the mutation itself. Any code-changing revision follow-up must use owner-bound tags: owner-bound, owner-bound-kind:patch_queue_submit, owner-branch:" + strings.TrimSpace(hint.BranchID))
	if strings.TrimSpace(hint.OwnerAgentID) != "" {
		b.WriteString(", required-agent:" + strings.TrimSpace(hint.OwnerAgentID))
	}
	b.WriteString(". Agents without fresh exact evidence should create or delegate that queue-facing follow-up instead of only writing a recommendation.\n")
	b.WriteString("- If the blocker is a real product/code defect, create or claim the smallest revision task and publish a new branch/head rather than requeueing the same evidence.\n")
	b.WriteString(fmt.Sprintf("- Suggested requeue args: project_id=%s", firstNonEmpty(strings.TrimSpace(projectID), strings.TrimSpace(hint.BranchID), "project")))
	b.WriteString(", action=supersede")
	if strings.TrimSpace(hint.QueueID) != "" {
		b.WriteString(", queue_id=" + strings.TrimSpace(hint.QueueID))
	}
	if strings.TrimSpace(hint.ItemID) != "" {
		b.WriteString(", item_id=" + strings.TrimSpace(hint.ItemID))
	}
	if suggestedItemID != "" {
		b.WriteString(", new_item_id=" + suggestedItemID)
	}
	if strings.TrimSpace(hint.BranchID) != "" {
		b.WriteString(", branch_id=" + strings.TrimSpace(hint.BranchID))
	}
	b.WriteString(".\n")
	if strings.TrimSpace(hint.BranchName) != "" || head != "" {
		b.WriteString(fmt.Sprintf("- Candidate branch: %s @ %s.\n", firstNonEmpty(strings.TrimSpace(hint.BranchName), "-"), firstNonEmpty(head, "-")))
	}
	if strings.TrimSpace(hint.DecisionDocKey) != "" {
		b.WriteString("- Decision evidence: " + strings.TrimSpace(hint.DecisionDocKey) + "\n")
	}
	if strings.TrimSpace(hint.ReviewDocKey) != "" {
		b.WriteString("- Review evidence: " + strings.TrimSpace(hint.ReviewDocKey) + "\n")
	}
	if strings.TrimSpace(hint.Decision) != "" {
		b.WriteString("- Blocked decision: " + oneLine(hint.Decision) + "\n")
	}
	policy = describeMetacognitionPolicy(policy)
	if strings.TrimSpace(policy.ReflectionScope) != "" {
		b.WriteString("- Keep the pass inside reflection_scope=" + strings.TrimSpace(policy.ReflectionScope) + " and convert findings into at most the configured number of concrete queue/revision tasks.\n")
	}
	return b.String()
}

func projectPatchQueueRequeueItemID(hint idleReflectionBlockedQueueHint) string {
	base := firstNonEmpty(strings.TrimSpace(hint.ItemID), strings.TrimSpace(hint.BranchID), "patchitem")
	head := strings.TrimSpace(hint.HeadSHA)
	if len(head) > 12 {
		head = head[:12]
	}
	if head != "" {
		base += "-requeue-" + head
	} else {
		base += "-requeue"
	}
	return sanitizeRefSegment(base)
}

func idleReflectionTitle(policy AgentMetacognitionPolicy, fallback string) string {
	switch policy.IdleActionPolicy {
	case idlePolicyReviewArtifact:
		return "Artifact quality iteration: inspect evidence and concrete gaps"
	case idlePolicyJoinExistingReflection:
		return "Project reflection pass: join active quality work or fill a gap"
	case idlePolicyOpenUncoveredDirection:
		return "Project metacognition pass: inspect plan, blockers, and next improvements"
	default:
		return fallback
	}
}

func findOpenIdleReflectionTaskForTarget(tasks []WorkspaceTaskRecord, target idleReflectionTarget) (WorkspaceTaskRecord, bool) {
	for _, task := range tasks {
		if taskSubmitTaskIsTerminal(task) || !isIdleReflectionTask(task) {
			continue
		}
		if !idleReflectionTaskScopeMatchesTarget(task, target) {
			continue
		}
		if target.ProjectID != "" && strings.TrimSpace(task.ProjectID) != target.ProjectID {
			continue
		}
		if target.ProjectID == "" && strings.TrimSpace(task.ProjectID) != "" {
			continue
		}
		return task, true
	}
	return WorkspaceTaskRecord{}, false
}

func idleReflectionTaskScopeMatchesTarget(task WorkspaceTaskRecord, target idleReflectionTarget) bool {
	targetScope := normalizeReflectionScope(target.ReflectionScope)
	taskScope := idleReflectionTaskScope(task)
	if targetScope == "" || taskScope == "" {
		return true
	}
	return targetScope == taskScope || targetScope == reflectionScopeGlobal
}

func idleReflectionTaskScope(task WorkspaceTaskRecord) string {
	for _, tag := range task.Tags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if strings.HasPrefix(tag, "metacognition-scope-") {
			if scope := normalizeReflectionScope(strings.TrimPrefix(tag, "metacognition-scope-")); scope != "" {
				return scope
			}
		}
	}
	text := strings.ToLower(strings.Join([]string{
		task.TaskID,
		task.Title,
		task.Description,
		task.ProjectLane,
		strings.Join(task.Tags, " "),
	}, " "))
	switch {
	case containsAnySignal(text, []string{"global metacognition", "global reflection", "systemic reflection", "workspace metacognition"}):
		return reflectionScopeGlobal
	case containsAnySignal(text, []string{"project metacognition", "project reflection", "project-wide reflection", "project wide reflection"}):
		return reflectionScopeProject
	case containsAnySignal(text, []string{"artifact metacognition", "artifact reflection", "artifact quality iteration", "review reflection", "qa reflection"}):
		return reflectionScopeArtifact
	case strings.Contains(strings.ToLower(strings.TrimSpace(task.TaskID)), "idle-reflection") && strings.TrimSpace(task.ProjectID) != "":
		return reflectionScopeProject
	case strings.Contains(strings.ToLower(strings.TrimSpace(task.TaskID)), "idle-reflection"):
		return reflectionScopeGlobal
	default:
		return ""
	}
}

func idleReflectionSuppressedByActiveWork(tasks []WorkspaceTaskRecord) bool {
	hasBlocked := false
	for _, task := range tasks {
		if idleReflectionTaskIsBlocked(task) {
			hasBlocked = true
			continue
		}
		if idleReflectionTaskHasActiveOwnership(task) {
			return true
		}
	}
	if hasBlocked {
		return false
	}
	return false
}

func idleReflectionHasUnprojectedOpenWork(tasks []WorkspaceTaskRecord) bool {
	for _, task := range tasks {
		if taskSubmitTaskIsTerminal(task) || isIdleReflectionTask(task) {
			continue
		}
		if strings.TrimSpace(task.ProjectID) == "" {
			return true
		}
	}
	return false
}

func idleReflectionTaskIsBlocked(task WorkspaceTaskRecord) bool {
	return strings.EqualFold(strings.TrimSpace(task.Status), "BLOCKED") || taskClaimStatus(task) == "BLOCKED"
}

func idleReflectionTaskHasActiveOwnership(task WorkspaceTaskRecord) bool {
	status := strings.ToUpper(strings.TrimSpace(task.Status))
	claimStatus := taskClaimStatus(task)
	if claimStatus == "CLAIMED" || claimStatus == "ACTIVE" {
		return true
	}
	if status != "RUNNING" && status != "ACTIVE" {
		return false
	}
	return strings.TrimSpace(pointerValue(task.ClaimAgentID)) != ""
}

func dominantIdleReflectionProjectID(tasks []WorkspaceTaskRecord, projects []ProjectRecord) string {
	type projectScore struct {
		projectID string
		score     int
	}
	scores := map[string]projectScore{}
	for _, task := range tasks {
		projectID := strings.TrimSpace(task.ProjectID)
		if projectID == "" {
			continue
		}
		score := scores[projectID]
		score.projectID = projectID
		score.score++
		switch {
		case taskClaimStatus(task) == "BLOCKED" || strings.EqualFold(strings.TrimSpace(task.Status), "BLOCKED"):
			score.score += 20
		case strings.EqualFold(strings.TrimSpace(task.Status), "RUNNING"):
			score.score += 10
		}
		scores[projectID] = score
	}
	best := projectScore{}
	for _, score := range scores {
		if score.score > best.score || (score.score == best.score && score.projectID < best.projectID) {
			best = score
		}
	}
	if best.projectID != "" {
		return best.projectID
	}
	for _, project := range projects {
		if strings.EqualFold(strings.TrimSpace(project.Status), "ACTIVE") {
			return strings.TrimSpace(project.ProjectID)
		}
	}
	return ""
}

func projectTitleByID(projects []ProjectRecord, projectID string) string {
	projectID = strings.TrimSpace(projectID)
	for _, project := range projects {
		if strings.TrimSpace(project.ProjectID) == projectID {
			return strings.TrimSpace(project.Title)
		}
	}
	return ""
}

func idleReflectionTaskID(scopeKey string, now time.Time) string {
	scope := compactRefSegment("idle", scopeKey)
	bucket := now.UTC().Truncate(idleReflectionCooldown).Format("20060102-1504")
	return sanitizeRefSegment("task-idle-reflection-" + scope + "-" + bucket)
}

func isIdleReflectionTask(task WorkspaceTaskRecord) bool {
	if strings.Contains(strings.ToLower(strings.TrimSpace(task.TaskID)), "idle-reflection") {
		return true
	}
	for _, tag := range task.Tags {
		if strings.EqualFold(strings.TrimSpace(tag), "anti-idle") || strings.EqualFold(strings.TrimSpace(tag), "meta-reflection") {
			return true
		}
	}
	return strings.Contains(strings.ToLower(strings.TrimSpace(task.Title)), "meta-reflection")
}

// idleReflectionProjectConvergenceFrontierIsClear reports whether the project has no open
// CONVERGENCE-BLOCKING (spec-required) task. openTasks must already exclude terminal and
// idle-reflection tasks (buildIdleReflectionTarget's filter).
//
// CONVERGENCE-CLUSTER ROOT FIX: this used to require zero open non-reflection tasks ("empty
// frontier"). That is the wrong trigger - a live roster on a finished product mints discretionary
// housekeeping (polish, side-effect classification, claim/role/repo repair, coordination meta)
// indefinitely, so the EMPTY PRODUCT FRONTIER protocol never fired and the project never converged.
// Convergeable is "no open BLOCKING task remains": discretionary work does not keep the frontier
// open. taskIsConvergenceBlocking is byte-identical to the server DONE gate's predicate, so the
// protocol fires under exactly the condition evaluateProjectPhaseTerminalAdmissionTx will admit.
func idleReflectionProjectConvergenceFrontierIsClear(openTasks []WorkspaceTaskRecord, projectID string) bool {
	// No patch-queue context (nil items/branches) -> the R68 redundant rejected-revision override never
	// fires, so this is the strict frontier-clear predicate. Callers that hold patch-queue state use
	// idleReflectionProjectConvergenceFrontierIsClearWithPatchQueue directly.
	return idleReflectionProjectConvergenceFrontierIsClearWithPatchQueue(openTasks, nil, projectID)
}

// runtimeProjectFinalizationConvergeable reports whether the project is CONVERGEABLE: its blocking
// frontier is clear (no open spec-rooted task remains -> the product is spec-complete and the lead
// should be committing the attestation, not generating more coordination churn). This is the R67
// finalization-liveness signal used to FREEZE discretionary coordination minting at finalization. It
// reuses the byte-identical-with-the-server-gate convergence-blocking predicate, so "convergeable"
// means exactly "no spec-rooted blocking task" - it is NOT "easy work ran out". Errs toward FALSE
// (allow minting): requires a loaded, non-empty project task frontier (an empty/unknown slice would
// vacuously report clear and over-suppress).
func runtimeProjectFinalizationConvergeable(coordination ProjectCoordinationRecord, projectID string) bool {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" || len(coordination.Tasks) == 0 {
		return false
	}
	return idleReflectionProjectConvergenceFrontierIsClearWithPatchQueue(coordination.Tasks, coordination.PatchQueueItems, projectID)
}

func idleReflectionDescription(scopeKind, scopeLabel string, openTasks []WorkspaceTaskRecord, policy AgentMetacognitionPolicy, emptyProductFrontier bool) string {
	policy = describeMetacognitionPolicy(policy)
	var b strings.Builder
	b.WriteString("Rhizome profile-scoped product-quality iteration task.\n\n")
	b.WriteString("The runtime observed repeated agent.work.next no-work responses while the workspace may still contain a live product or coordination context. ")
	b.WriteString("Treat idle capacity as useful product work, but keep the initiative width aligned with this agent's metacognition profile.\n\n")
	b.WriteString("Metacognition profile:\n")
	b.WriteString("- reflection_scope: " + policy.ReflectionScope + "\n")
	b.WriteString("- idle_action_policy: " + policy.IdleActionPolicy + "\n")
	b.WriteString(fmt.Sprintf("- max_new_tasks_per_idle_cycle: %d\n", policy.MaxNewTasksPerIdleCycle))
	b.WriteString("- scope_guidance: " + policy.ScopeDescription + "\n")
	b.WriteString("- idle_guidance: " + policy.IdleBehaviorDescription + "\n")
	b.WriteString("- task_creation: " + policy.TaskCreationDescription + "\n\n")
	if strings.TrimSpace(policy.RealityCheckDescription) != "" {
		b.WriteString("- reality_check_quality_bar: " + strings.TrimSpace(policy.RealityCheckDescription) + "\n\n")
	}
	b.WriteString("Scope: ")
	b.WriteString(firstNonEmpty(scopeKind, "workspace"))
	if strings.TrimSpace(scopeLabel) != "" {
		b.WriteString(" / ")
		b.WriteString(strings.TrimSpace(scopeLabel))
	}
	b.WriteString("\n\n")
	if emptyProductFrontier {
		b.WriteString("PRODUCT CONVERGENCE PROTOCOL (active for this reflection):\n")
		b.WriteString("This scope currently has no open spec-required (blocking) product or integration task; only discretionary work (polish, quality iteration, side-effect classification, claim/role/repo repair, coordination meta, reflection) may remain, and that does NOT justify deferring convergence. Do NOT mint additional discretionary work, \"hold position\", \"wait for a verification delta\", \"avoid coordination churn\", or re-confirm coordination-board claim annotations to keep the frontier busy - none of those are valid outcomes here, and minting more discretionary tasks to avoid converging is a protocol violation. Exactly one of two outcomes is required:\n")
		b.WriteString("1. Continue the product: verify durable state directly (patch queue decision receipts, integrated branch history, shipped tests) instead of trusting board annotations, then compare the shipped product against project.<project_id>.product_contract, project.<project_id>.acceptance_criteria, and the original operator/root specification. If any concrete specification item is missing or only partially covered, open exactly one bounded implementation task (project_lane=implementation) that quotes the uncovered specification item and names its acceptance check, and set that task's task_requirements.acceptance_criteria_refs to the uncovered AC-ID(s) so it is machine-anchored to the operator spec and stays a convergence-blocking task until that criterion is covered by integrated state.\n")
		if strings.EqualFold(strings.TrimSpace(scopeKind), "project") {
			b.WriteString("2. Terminalize the project as converged: the convergence oracle is the EXTERNAL operator spec, not \"easy work ran out\". Read project.<project_id>.source_requirements_trace and enumerate EVERY acceptance-criteria ID listed under acceptance_criteria_refs - that full set is the authoritative checklist (do NOT shorten it). For each required AC-ID, confirm an INTEGRATED product artifact covers it (a patch-queue item in state INTEGRATED, verified from durable state). If any required AC-ID is not yet covered by integrated state, that is a real specification gap: take outcome 1 and open exactly one bounded implementation task quoting that AC-ID (with task_requirements.acceptance_criteria_refs naming the uncovered AC-ID so it is a machine-anchored, convergence-blocking task) - never declare convergence with an uncovered criterion, and never substitute discretionary polish for an uncovered criterion.\n")
			b.WriteString("   Only when every required AC-ID is covered, the strategic lead records the DONE-evidence attestation as a workspace doc at key project.<project_id>.acceptance_coverage, schema rhizome_acceptance_coverage_v1, with one markdown list entry per required AC-ID in the form `- <AC-ID> | <integrated artifact ref> | <one-line justification>`, where the artifact ref is the integrated item's queue_id/item_id (or branch_id, head_sha, or source task_id). Then advance the project phase to DONE via project_phase_transition with a short evidence summary. The DONE transition is REJECTED until every required AC-ID has an attestation entry naming an artifact that exists as integrated state, so publish the acceptance_coverage doc BEFORE requesting DONE. If you are not the strategic lead, post a convergence recommendation update addressed to the lead with your per-AC-ID coverage evidence, then complete this reflection.\n")
		} else {
			b.WriteString("2. Terminalize the workspace as converged: if a genuine coverage pass finds no remaining specification gap, publish a convergence evidence doc (what was verified, why no substantial improvement remains) and complete this reflection.\n")
		}
		b.WriteString("Completing this reflection with only a hold/wait/monitor conclusion while the frontier stays empty and convergence is not recorded is a protocol violation.\n\n")
	}
	b.WriteString("Expected behavior:\n")
	b.WriteString("- First inspect visible tasks, shared docs, recent updates, existing meta-reflection/anti-idle work, and the shared reflection board in this scope. For project work, use project.<project_id>.reflection_board as the default board. Join or extend a relevant active reflection if one exists instead of duplicating it.\n")
	b.WriteString("- Inspect current tasks, shared docs, recent updates, project coordination, patch queue state, and workspace.tension.frontier.\n")
	b.WriteString("- If your reflection could change product direction, request bounded critique from a suitable peer or reviewer before turning it into implementation work.\n")
	b.WriteString("- Compare any runnable product against the operator/root task and project acceptance criteria; if the checklist is missing or vague, create/refine project.<project_id>.acceptance_criteria before declaring quality complete.\n")
	b.WriteString("- Check Product Contract and Critical Plan Review evidence. If missing, create/refine project.<project_id>.product_contract or project.<project_id>.plan_review before judging implementation readiness.\n")
	b.WriteString("- If a runnable product exists, perform a bounded reality check that matches reflection_scope: build/test/browser smoke, static UX review, accessibility pass, spec-fidelity review, edge-case review, or test-gap analysis.\n")
	if strings.TrimSpace(policy.RealityCheckDescription) != "" {
		b.WriteString("- For UI/UX reality checks, do not accept a product from code shape, screenshots, nonblank canvas, or green build/test output alone. Name at least one real user scenario, the artifact/version observed, and either evidence-backed findings or explicit no-finding evidence.\n")
	}
	b.WriteString(fmt.Sprintf("- If the product is only MVP-functional or misses acceptance criteria, create at most %d concrete task_submit follow-up(s) for the highest-value spec-fidelity, UX, polish, test, accessibility, or feature improvements; use project_lane=implementation for code changes and project_lane=qa/review/validation for audits and smokes.\n", policy.MaxNewTasksPerIdleCycle))
	b.WriteString("- If you identify a concrete next step, create or claim the smallest real task before completing this reflection. A result doc that merely recommends future work is incomplete unless max_new_tasks_per_idle_cycle=0 or all useful follow-ups already exist.\n")
	b.WriteString("- If active work is blocked, identify whether the blocker is current or stale, then create or claim a bounded unblock task instead of polling.\n")
	b.WriteString("- If work was duplicated or stale, document the canonical path and create/claim cleanup or integration work where useful.\n")
	b.WriteString("- If the workspace is genuinely complete after a quality pass, publish concise evidence explaining why no substantial next improvement remains and complete this task.\n")
	b.WriteString("- Do not stop at a passive status note when a concrete review, test, polish, or improvement task can be created or claimed.\n")
	if len(openTasks) > 0 {
		b.WriteString("\nOpen task hints:\n")
		for _, task := range takeIdleReflectionTasks(openTasks, 8) {
			b.WriteString(fmt.Sprintf("- %s [%s/%s]: %s\n", strings.TrimSpace(task.TaskID), strings.TrimSpace(task.Status), taskClaimStatus(task), firstNonEmpty(task.Title, task.Description)))
		}
	}
	return b.String()
}

func takeIdleReflectionTasks(tasks []WorkspaceTaskRecord, limit int) []WorkspaceTaskRecord {
	if limit <= 0 || len(tasks) <= limit {
		return tasks
	}
	return tasks[:limit]
}

func (r *Runtime) ensureIdleReflectionTask(ctx context.Context, target idleReflectionTarget, now time.Time) (string, bool, error) {
	taskID := strings.TrimSpace(target.TaskID)
	if taskID == "" {
		return "", false, nil
	}
	// FF3 (lead-targeted convergence): when the convergence frontier is clear, ONLY the strategic lead
	// may run the convergence reflection - only the lead can write project.<id>.acceptance_coverage and
	// transition DONE (the server rejects a non-lead phase transition). A non-lead that runs it can only
	// post a recommendation and resolve it, starving the lead of the convergence turn (observed R64:
	// epsilon ran it; alpha never converged). So a non-lead neither creates nor claims it while an active
	// lead exists. If the lead is missing/lapsed or coordination is unknown, fall through so convergence
	// is never deadlocked.
	// R68 keystone (operator-signed): the WorkspaceSnapshot that built `target` carries no patch-queue
	// context, so a frontier that is clear EXCEPT for a redundant rejected-revision over-production task
	// reads as not-clear. Re-evaluate against the project coordination (items + branches) so the lead can
	// converge a spec-complete product whose only remaining "blocker" is over-produced rework. The
	// override errs toward blocking; the server DONE gate re-applies the same predicate plus the
	// external-spec coverage floor, so an over-eager upgrade here cannot produce a false DONE.
	if !target.EmptyProductFrontier && target.FrontierBlockedOnlyByRevisionFollowups && strings.TrimSpace(target.ProjectID) != "" {
		if r.projectFrontierClearIgnoringRedundantRejectedRevisions(ctx, strings.TrimSpace(target.ProjectID)) {
			target.EmptyProductFrontier = true
		}
	}
	if target.EmptyProductFrontier && strings.TrimSpace(target.ProjectID) != "" {
		if leadID, leadActive, ok := r.activeProjectLeadID(ctx, strings.TrimSpace(target.ProjectID), now); ok && leadActive && leadID != "" && leadID != r.cfg.AgentID {
			return "", false, nil
		}
	}
	if target.ExistingTask != nil {
		if taskSubmitTaskIsTerminal(*target.ExistingTask) {
			return "", false, nil
		}
		return taskID, isTaskCandidate(*target.ExistingTask, r.cfg.AgentID), nil
	}

	requiresGate := false
	policy := metacognitionPolicyForRuntimeConfig(r.cfg)
	input := TaskSubmitInput{
		WorkspaceID:         r.cfg.WorkspaceID,
		TaskID:              taskID,
		OwnerUserID:         firstNonEmpty(r.cfg.OwnerUserID, r.cfg.AgentID),
		Priority:            "normal",
		Title:               target.Title,
		Description:         target.Description,
		TaskKind:            "EXECUTION",
		TaskTemplate:        "generic",
		ProjectID:           target.ProjectID,
		ProjectLane:         firstNonEmpty(target.ProjectLane, "qa"),
		RequiresProjectGate: &requiresGate,
		Tags: uniqueTrimmedCSVStrings([]string{
			"meta-reflection",
			"anti-idle",
			"product-quality",
			"post-mvp",
			"qa",
			"planning",
			"metacognition-scope-" + policySafeTag(policy.ReflectionScope),
			"idle-policy-" + policySafeTag(policy.IdleActionPolicy),
		}),
		LinkedBy: r.cfg.AgentID,
	}
	if target.EmptyProductFrontier && strings.TrimSpace(target.ProjectID) != "" {
		input.TaskRequirements = map[string]any{
			"schema":              "task_requirements.v1",
			"runtime_contract":    "empty_product_frontier.v1",
			"required_transition": "empty_product_frontier_outcome",
			"allowed_terminal_receipts": []string{
				"task_submit",
				"project_phase_transition",
			},
			"project_id": strings.TrimSpace(target.ProjectID),
		}
	}
	_, err := r.client.SubmitTask(ctx, input)
	if err != nil {
		if isDuplicateTaskSubmitError(err) {
			return taskID, false, nil
		}
		return "", false, err
	}

	docKey := "task." + taskID
	docTitle := "Task Brief - " + target.Title
	if err := r.putDocWithRetry(ctx, docKey, docTitle, idleReflectionDocContent(target, now)); err != nil {
		log.Printf("[planner] idle reflection task %s created but doc materialization failed: %v", taskID, err)
	}
	return taskID, true, nil
}

func policySafeTag(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "_", "-")
	value = strings.ReplaceAll(value, " ", "-")
	if value == "" {
		return "unspecified"
	}
	return value
}

func idleReflectionDocContent(target idleReflectionTarget, now time.Time) string {
	var b strings.Builder
	b.WriteString("# Idle Product-Quality Task\n\n")
	b.WriteString("- task_id: " + strings.TrimSpace(target.TaskID) + "\n")
	b.WriteString("- created_at: " + now.UTC().Format(time.RFC3339Nano) + "\n")
	b.WriteString("- scope_kind: " + strings.TrimSpace(target.ScopeKind) + "\n")
	b.WriteString("- scope_id: " + strings.TrimSpace(target.ScopeID) + "\n")
	if strings.TrimSpace(target.ProjectID) != "" {
		b.WriteString("- project_id: " + strings.TrimSpace(target.ProjectID) + "\n")
		b.WriteString("- reflection_board_doc_key: project." + strings.TrimSpace(target.ProjectID) + ".reflection_board\n")
		b.WriteString("- product_contract_doc_key: project." + strings.TrimSpace(target.ProjectID) + ".product_contract\n")
		b.WriteString("- plan_review_doc_key: project." + strings.TrimSpace(target.ProjectID) + ".plan_review\n")
	}
	if len(target.OpenTaskIDs) > 0 {
		b.WriteString("- open_task_ids: " + strings.Join(target.OpenTaskIDs, ", ") + "\n")
	}
	if len(target.BlockedTaskIDs) > 0 {
		b.WriteString("- blocked_task_ids: " + strings.Join(target.BlockedTaskIDs, ", ") + "\n")
	}
	if len(target.ActiveTaskIDs) > 0 {
		b.WriteString("- active_task_ids: " + strings.Join(target.ActiveTaskIDs, ", ") + "\n")
	}
	if len(target.StaleTaskIDs) > 0 {
		b.WriteString("- ignored_stale_task_ids: " + strings.Join(target.StaleTaskIDs, ", ") + "\n")
	}
	if target.EmptyProductFrontier {
		b.WriteString("- empty_product_frontier: true\n")
	}
	if len(target.RecentEvidenceDocKeys) > 0 {
		b.WriteString("- fresh_evidence_doc_keys: " + strings.Join(uniqueTrimmedCSVStrings(target.RecentEvidenceDocKeys), ", ") + "\n")
	}
	b.WriteString("\n")
	b.WriteString(strings.TrimSpace(target.Description))
	b.WriteString("\n")
	return b.String()
}

func isDuplicateTaskSubmitError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "unique constraint") ||
		strings.Contains(text, "already exists") ||
		strings.Contains(text, "duplicate")
}
