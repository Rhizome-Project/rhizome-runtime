package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type ProjectPatchQueueLifecycleTool struct {
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	runtimeBinding func() AgentRuntimeBinding
}

func NewProjectPatchQueueLifecycleTool(client *RhizomeClient, workspaceID, agentID string) *ProjectPatchQueueLifecycleTool {
	return &ProjectPatchQueueLifecycleTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
	}
}

func (t *ProjectPatchQueueLifecycleTool) Name() string { return "project_patch_queue_lifecycle" }

// projectPatchQueueLifecycleDecisionRecordedMarker is emitted in the integration_guidance of a
// successful accept/block/reject decision (and only a decision - claim/reconcile/consume do not
// produce it). The required-tool review gate (runtime_noop_progress.go) matches this exact marker
// to recognize that a DURABLE DECISION happened, so a review session cannot satisfy its
// required_tool=project_patch_queue_lifecycle gate merely by CLAIMING the item (RPF-58A). Producer
// and matcher share this constant so they cannot drift.
const projectPatchQueueLifecycleDecisionRecordedMarker = "Patch queue decision was recorded as shared integration evidence. No git mutation was performed."

func (t *ProjectPatchQueueLifecycleTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectPatchQueueLifecycleTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectPatchQueueLifecycleTool) Description() string {
	return "Claim, release, repair receipts, record reviewer advisory evidence, consume decision continuations, or record an integration decision for a durable project patch queue item. It does not merge, push, rebase, or mutate git state."
}

func (t *ProjectPatchQueueLifecycleTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id": map[string]any{"type": "string", "description": "Project id."},
			"action": map[string]any{
				"type":        "string",
				"description": "Lifecycle action: claim, release, reconcile_review_task, consume_continuation, reviewer_advisory, accept, reject, block, cancel, supersede, or requeue.",
				"enum":        []string{"claim", "release", "reconcile_review_task", "consume_continuation", "reviewer_advisory", "accept", "reject", "block", "cancel", "supersede", "requeue"},
			},
			"queue_id": map[string]any{"type": "string", "description": "Patch queue id."},
			"item_id":  map[string]any{"type": "string", "description": "Patch queue item id."},
			"branch_id": map[string]any{
				"type":        "string",
				"description": "Optional branch id when queue_id/item_id are unknown.",
			},
			"claim_token": map[string]any{
				"type":        "string",
				"description": "Claim fence token returned by claim. Defaults to visible item claim_token when present.",
			},
			"outbox_id": map[string]any{
				"type":        "string",
				"description": "Optional decision continuation outbox id for consume_continuation.",
			},
			"lease_seconds": map[string]any{
				"type":        "integer",
				"description": "Optional claim lease duration for claim action.",
			},
			"decision_summary": map[string]any{
				"type":        "string",
				"description": "Required for accept/reject/block/cancel decisions.",
			},
			"decision_doc_key": map[string]any{
				"type":        "string",
				"description": "Optional workspace doc key with integration decision evidence. For UI-facing ACCEPTED decisions, this or the review/advisory evidence should point to a rhizome_visual_acceptance_v1 packet. When project source refs exist, ACCEPTED decisions must also include rhizome_spec_fidelity_review_v1 or source_fidelity_status: passed plus checked_source_doc_keys; use BLOCKED_SPEC_DRIFT instead of ACCEPTED for runnable adjacent artifacts that miss source anchors.",
			},
			"checked_source_doc_keys": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Optional source doc keys explicitly checked by an ACCEPTED source-fidelity review. Defaults from project source refs/trace docs when available.",
			},
			"advisory_summary": map[string]any{
				"type":        "string",
				"description": "Optional reviewer advisory summary for reviewer_advisory action.",
			},
			"advisory_scope": map[string]any{
				"type":        "string",
				"description": "Optional reviewer advisory scope. Use lane_correctness for same-head candidate defects; use integration_completeness only for integration-level blockers.",
				"enum":        []string{"lane_correctness", "integration_completeness"},
			},
			"advisory_verdict": map[string]any{
				"type":        "string",
				"description": "Optional reviewer advisory verdict. Use repair_required for a late lane defect that should defeat ACCEPTED before integration.",
				"enum":        []string{"reviewed", "repair_required"},
			},
			"review_doc_key": map[string]any{
				"type":        "string",
				"description": "Workspace doc key containing reviewer advisory evidence. Required for same-head late negative advisories against ACCEPTED items.",
			},
			"head_sha": map[string]any{
				"type":        "string",
				"description": "Optional head_sha guard for reviewer_advisory; defaults to the selected patch queue item head.",
			},
			"new_item_id": map[string]any{
				"type":        "string",
				"description": "Fresh item id for supersede/requeue when a BLOCKED same-head item has new validation evidence.",
			},
			"requeue_item_id": map[string]any{
				"type":        "string",
				"description": "Alias for new_item_id.",
			},
			"validation_doc_key": map[string]any{
				"type":        "string",
				"description": "Fresh validation evidence doc key for supersede/requeue.",
			},
			"evidence_doc_key": map[string]any{
				"type":        "string",
				"description": "Alias for validation_doc_key.",
			},
		},
		"required": []string{"project_id", "action"},
	}
}

func (t *ProjectPatchQueueLifecycleTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_patch_queue_lifecycle is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	projectID, err := resolveProjectToolActiveProjectID("project_patch_queue_lifecycle", t.workspaceID, projectToolRefArg(args, "project_id"), t.runtimeBinding)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	action := normalizeProjectPatchQueueLifecycleAction(stringArg(args, "action"))
	if action == "" {
		return &ToolResult{Output: "action must be one of claim, release, reconcile_review_task, consume_continuation, reviewer_advisory, accept, reject, block, cancel, supersede, or requeue", IsError: true}
	}
	if action == "consume_continuation" &&
		strings.TrimSpace(stringArg(args, "outbox_id")) != "" {
		return t.consumeContinuation(ctx, projectID, ProjectPatchQueueItemRecord{}, stringArg(args, "outbox_id"))
	}
	coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_lifecycle failed reading project coordination for %s: %v", projectID, err), IsError: true}
	}
	item, err := selectProjectPatchQueueLifecycleItem(coordination.PatchQueueItems, stringArg(args, "queue_id"), stringArg(args, "item_id"), stringArg(args, "branch_id"))
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	switch action {
	case "claim":
		return t.claim(ctx, projectID, item, intArg(args["lease_seconds"], 0), stringArg(args, "claim_token"))
	case "release":
		return t.release(ctx, projectID, item, stringArg(args, "claim_token"))
	case "reconcile_review_task":
		return t.reconcileReviewTask(ctx, projectID, item)
	case "consume_continuation":
		return t.consumeContinuation(ctx, projectID, item, stringArg(args, "outbox_id"))
	case "reviewer_advisory":
		return t.recordReviewerAdvisory(ctx, projectID, item, args)
	case "supersede", "requeue":
		return t.supersede(ctx, projectID, item, args)
	default:
		return t.decide(ctx, projectID, item, action, stringArg(args, "decision_summary"), stringArg(args, "decision_doc_key"), stringArg(args, "claim_token"), stringListArg(args, "checked_source_doc_keys"))
	}
}

func (t *ProjectPatchQueueLifecycleTool) reconcileReviewTask(ctx context.Context, projectID string, item ProjectPatchQueueItemRecord) *ToolResult {
	status, eventID, repaired, err := t.client.ReconcileProjectPatchQueueReviewTask(ctx, ProjectPatchQueueReviewTaskReconcileInput{
		WorkspaceID: t.workspaceID,
		ProjectID:   projectID,
		ActorID:     t.agentID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_lifecycle reconcile_review_task failed for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
	}
	payload := map[string]any{
		"action":               "reconcile_review_task",
		"project_id":           projectID,
		"queue_id":             item.QueueID,
		"item_id":              item.ItemID,
		"patch_queue_state":    item.State,
		"review_task":          status,
		"review_task_event_id": eventID,
		"repaired":             repaired,
		"guidance":             "Durable patch queue review task receipt is present. Continue with the visible review task or lifecycle decision; no git mutation was performed.",
	}
	return projectPatchQueueLifecycleJSONResult(payload, fmt.Sprintf("project_patch_queue_lifecycle reconcile_review_task completed for %s/%s", item.QueueID, item.ItemID))
}

func (t *ProjectPatchQueueLifecycleTool) consumeContinuation(ctx context.Context, projectID string, item ProjectPatchQueueItemRecord, outboxID string) *ToolResult {
	status, continuation, consumed, created, err := t.client.ConsumeProjectPatchQueueDecisionContinuation(ctx, ProjectPatchQueueDecisionContinuationConsumeInput{
		WorkspaceID: t.workspaceID,
		ProjectID:   projectID,
		ActorID:     t.agentID,
		OutboxID:    strings.TrimSpace(outboxID),
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
	})
	if err != nil {
		target := firstNonEmpty(strings.TrimSpace(outboxID), strings.TrimSpace(item.QueueID)+"/"+strings.TrimSpace(item.ItemID))
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_lifecycle consume_continuation failed for %s: %v", target, err), IsError: true}
	}
	queueID := firstNonEmpty(strings.TrimSpace(item.QueueID), strings.TrimSpace(continuation.QueueID))
	itemID := firstNonEmpty(strings.TrimSpace(item.ItemID), strings.TrimSpace(continuation.ItemID))
	payload := map[string]any{
		"action":         "consume_continuation",
		"project_id":     projectID,
		"queue_id":       queueID,
		"item_id":        itemID,
		"outbox_id":      continuation.OutboxID,
		"continuation":   continuation,
		"task":           status,
		"consumed":       consumed,
		"created":        created,
		"guidance":       "Pending patch queue decision continuation was materialized or reused as a visible task. No git mutation was performed.",
		"next_tool_hint": "claim or execute the returned continuation task according to its project_lane and task_requirements_json",
	}
	return projectPatchQueueLifecycleJSONResult(payload, fmt.Sprintf("project_patch_queue_lifecycle consume_continuation completed for %s/%s", queueID, itemID))
}

func (t *ProjectPatchQueueLifecycleTool) claim(ctx context.Context, projectID string, item ProjectPatchQueueItemRecord, leaseSeconds int, claimToken string) *ToolResult {
	if !strings.EqualFold(strings.TrimSpace(item.State), "PROPOSED") && !projectPatchQueueLifecycleClaimReclaimable(item, t.agentID) {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_lifecycle cannot claim item %s/%s in state %s", item.QueueID, item.ItemID, item.State), IsError: true}
	}
	claimed, err := t.client.ClaimProjectPatchQueueItem(ctx, ProjectPatchQueueClaimInput{
		WorkspaceID:  t.workspaceID,
		ProjectID:    projectID,
		ActorID:      t.agentID,
		QueueID:      item.QueueID,
		ItemID:       item.ItemID,
		ClaimToken:   strings.TrimSpace(claimToken),
		LeaseSeconds: leaseSeconds,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_lifecycle claim failed for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
	}
	return projectPatchQueueLifecycleResult("claim", claimed, "Patch queue item is claimed for integration review. No git mutation was performed.")
}

func (t *ProjectPatchQueueLifecycleTool) release(ctx context.Context, projectID string, item ProjectPatchQueueItemRecord, claimToken string) *ToolResult {
	if strings.TrimSpace(item.ClaimedBy) != t.agentID {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_lifecycle release requires this agent to own claim %s/%s", item.QueueID, item.ItemID), IsError: true}
	}
	token := firstNonEmpty(strings.TrimSpace(claimToken), strings.TrimSpace(item.ClaimToken))
	if token == "" {
		return &ToolResult{Output: "claim_token is required to release a patch queue claim", IsError: true}
	}
	released, err := t.client.ReleaseProjectPatchQueueClaim(ctx, ProjectPatchQueueReleaseInput{
		WorkspaceID: t.workspaceID,
		ProjectID:   projectID,
		ActorID:     t.agentID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ClaimToken:  token,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_lifecycle release failed for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
	}
	return projectPatchQueueLifecycleResult("release", released, "Patch queue claim was released back to PROPOSED. No git mutation was performed.")
}

func (t *ProjectPatchQueueLifecycleTool) recordReviewerAdvisory(ctx context.Context, projectID string, item ProjectPatchQueueItemRecord, args map[string]any) *ToolResult {
	summary := strings.TrimSpace(stringArg(args, "advisory_summary"))
	scope := normalizeProjectPatchQueueLifecycleAdvisoryScope(stringArg(args, "advisory_scope"))
	verdict := strings.ToLower(strings.TrimSpace(stringArg(args, "advisory_verdict")))
	reviewDocKey := strings.TrimSpace(firstNonEmpty(stringArg(args, "review_doc_key"), stringArg(args, "decision_doc_key")))
	headSHA := strings.TrimSpace(firstNonEmpty(stringArg(args, "head_sha"), item.HeadSHA))
	defeatsAccepted := projectPatchQueueLifecycleAdvisoryDefeatsAccepted(item, summary, scope, verdict)
	if strings.EqualFold(scope, "integration_completeness") && strings.EqualFold(strings.TrimSpace(item.State), "ACCEPTED") {
		return &ToolResult{Output: "project_patch_queue_lifecycle reviewer_advisory refused to defeat an ACCEPTED lane item with integration_completeness scope; bind full-product concerns to the integration validation task instead.", IsError: true}
	}
	if !defeatsAccepted && strings.TrimSpace(item.ClaimedBy) != t.agentID {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_lifecycle reviewer_advisory requires this agent to own claim %s/%s", item.QueueID, item.ItemID), IsError: true}
	}
	if !defeatsAccepted && strings.EqualFold(strings.TrimSpace(item.RepoAuthorityMode), "repoauthority_controlled_queue") && !item.RollbackEvidenceAccepted {
		return projectPatchQueueLifecycleJSONResult(map[string]any{
			"action":                     "reviewer_advisory",
			"project_id":                 projectID,
			"queue_id":                   item.QueueID,
			"item_id":                    item.ItemID,
			"state":                      item.State,
			"repo_authority_mode":        item.RepoAuthorityMode,
			"rollback_evidence_accepted": false,
			"reviewer_advisory_recorded": false,
			"no_git_mutation":            true,
			"next_action":                "use_accept_reject_or_block_decision_for_ordinary_lane_review",
			"integration_guidance":       "reviewer_advisory is a post-CAS controlled-queue evidence gate and requires rollback evidence. For an ordinary READY_FOR_REVIEW or repair candidate review, record a normal accept/reject/block decision with project_patch_queue_lifecycle instead of blocking the candidate on missing rollback evidence.",
		}, fmt.Sprintf("project_patch_queue_lifecycle reviewer_advisory is not applicable for %s/%s without rollback evidence", item.QueueID, item.ItemID))
	}
	if defeatsAccepted && reviewDocKey == "" {
		return &ToolResult{Output: "project_patch_queue_lifecycle reviewer_advisory requires review_doc_key when defeating an ACCEPTED item with same-head lane-correctness evidence", IsError: true}
	}
	token := firstNonEmpty(strings.TrimSpace(stringArg(args, "claim_token")), strings.TrimSpace(item.ClaimToken))
	if token == "" && !defeatsAccepted {
		return &ToolResult{Output: "claim_token is required to record reviewer advisory evidence", IsError: true}
	}
	if verdict == "" {
		if defeatsAccepted {
			verdict = "repair_required"
		} else {
			verdict = "reviewed"
		}
	}
	if scope == "" && defeatsAccepted {
		scope = "lane_correctness"
	}
	recorded, err := t.client.RecordProjectPatchQueueReviewerAdvisory(ctx, ProjectPatchQueueReviewerAdvisoryRecordInput{
		WorkspaceID: t.workspaceID,
		ProjectID:   projectID,
		ActorID:     t.agentID,
		QueueID:     item.QueueID,
		ItemID:      item.ItemID,
		ReviewerAdvisory: PatchQueueReviewerAdvisory{
			Summary:           summary,
			Scope:             scope,
			Verdict:           verdict,
			ReviewDocKey:      reviewDocKey,
			HeadSHA:           headSHA,
			DefeatsAcceptance: defeatsAccepted,
		},
		ClaimToken: token,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_lifecycle reviewer_advisory failed for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
	}
	guidance := "Reviewer advisory evidence was recorded and remains advisory-only. No git mutation was performed."
	if defeatsAccepted {
		guidance = "Reviewer advisory evidence defeated the accepted same-head lane candidate and routed repair through patch queue continuation. No git mutation was performed."
	}
	return projectPatchQueueLifecycleResult("reviewer_advisory", recorded, guidance)
}

func normalizeProjectPatchQueueLifecycleAdvisoryScope(scope string) string {
	scope = strings.ToLower(strings.TrimSpace(scope))
	scope = strings.ReplaceAll(scope, "-", "_")
	scope = strings.ReplaceAll(scope, " ", "_")
	switch scope {
	case "lane", "lane_defect", "candidate_defect", "candidate_correctness", "lane_correctness":
		return "lane_correctness"
	case "integration", "full_product", "full_product_completeness", "product_completeness", "integration_completeness":
		return "integration_completeness"
	default:
		return scope
	}
}

func projectPatchQueueLifecycleAdvisoryDefeatsAccepted(item ProjectPatchQueueItemRecord, summary, scope, verdict string) bool {
	if !strings.EqualFold(strings.TrimSpace(item.State), "ACCEPTED") {
		return false
	}
	if strings.EqualFold(scope, "integration_completeness") {
		return false
	}
	if strings.EqualFold(scope, "lane_correctness") {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(verdict), "repair_required") {
		return true
	}
	return projectPatchQueueLifecycleNegativeAdvisorySummary(summary)
}

func projectPatchQueueLifecycleNegativeAdvisorySummary(summary string) bool {
	text := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(summary)), " "))
	if text == "" {
		return false
	}
	for _, marker := range []string{
		"defect",
		"bug",
		"broken",
		"incorrect",
		"invalid",
		"reject",
		"not accepted-ready",
		"not accepted ready",
		"repair needed",
		"repair required",
		"follow-up repair",
		"follow up repair",
		"blocking",
		"fails",
		"failed",
		"wrong",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func (t *ProjectPatchQueueLifecycleTool) decide(ctx context.Context, projectID string, item ProjectPatchQueueItemRecord, action, summary, docKey, claimToken string, checkedSourceDocKeys []string) *ToolResult {
	if strings.TrimSpace(item.ClaimedBy) != t.agentID {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_lifecycle decision requires this agent to own claim %s/%s", item.QueueID, item.ItemID), IsError: true}
	}
	token := firstNonEmpty(strings.TrimSpace(claimToken), strings.TrimSpace(item.ClaimToken))
	if token == "" {
		return &ToolResult{Output: "claim_token is required to record a patch queue decision", IsError: true}
	}
	if strings.TrimSpace(summary) == "" {
		return &ToolResult{Output: "decision_summary is required for patch queue decisions", IsError: true}
	}
	decision, err := projectPatchQueueLifecycleDecision(action)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	if strings.EqualFold(decision, "BLOCKED") && projectPatchQueueLifecycleSummaryLooksLikeActorAuthorityGap(summary) {
		return &ToolResult{Output: "project_patch_queue_lifecycle refused to record BLOCKED for a reviewer/integrator authority gap. BLOCKED is for candidate defects or missing candidate evidence; release the item, request/repair the role, or record a non-terminal review/advisory note instead.", IsError: true}
	}
	// R22-F2: blocking is a QUALITY verdict about the candidate, never a scheduling action.
	// In R22 the submitter, unable to find a free reviewer (both peers were pinned in
	// side-effect lanes and refused delegation), self-BLOCKED a healthy candidate - which
	// converted a temporary capacity gap into a terminal-looking queue state plus a repair
	// cascade. Refuse submitter blocks whose stated reason is reviewer availability.
	if strings.EqualFold(decision, "BLOCKED") &&
		strings.TrimSpace(item.SubmittedBy) == t.agentID &&
		projectPatchQueueLifecycleSummaryLooksLikeReviewerCapacityGap(summary) {
		return &ToolResult{Output: "project_patch_queue_lifecycle refused to record BLOCKED: blocking is a QUALITY verdict about the candidate, not a scheduling action. Reviewer-unavailability resolves itself: release your claim and leave the item PROPOSED - the review task stays selectable and reviewer capacity frees when peer sessions end. Block your own candidate only for a concrete defect or missing candidate evidence.", IsError: true}
	}
	if strings.EqualFold(decision, "ACCEPTED") {
		docs, err := t.visualAcceptanceCandidateDocs(ctx, item, summary, docKey)
		if err != nil {
			return &ToolResult{Output: err.Error(), IsError: true}
		}
		checkedSourceDocKeys, err = t.patchQueueDecisionCheckedSourceDocKeys(ctx, projectID, checkedSourceDocKeys, docs)
		if err != nil {
			return &ToolResult{Output: err.Error(), IsError: true}
		}
		gate := visualAcceptanceGateForPatchQueueItem(docs, item, summary)
		if gate.Required && !gate.Satisfied {
			return &ToolResult{Output: visualAcceptanceGateError("project_patch_queue_lifecycle", item, gate), IsError: true}
		}
	}
	decided, err := t.client.DecideProjectPatchQueueItem(ctx, ProjectPatchQueueDecisionInput{
		WorkspaceID:          t.workspaceID,
		ProjectID:            projectID,
		ActorID:              t.agentID,
		QueueID:              item.QueueID,
		ItemID:               item.ItemID,
		Decision:             decision,
		DecisionSummary:      strings.TrimSpace(summary),
		DecisionDocKey:       strings.TrimSpace(docKey),
		CheckedSourceDocKeys: checkedSourceDocKeys,
		ClaimToken:           token,
	})
	if err != nil {
		base := fmt.Sprintf("project_patch_queue_lifecycle decision failed for %s/%s: %v", item.QueueID, item.ItemID, err)
		if hint := projectPatchQueueLifecycleSourceFidelityRemediation(err, decision, projectID); hint != "" {
			base += "\n" + hint
		}
		return &ToolResult{Output: base, IsError: true}
	}
	guidance := projectPatchQueueLifecycleDecisionRecordedMarker
	extra := map[string]any{}
	switch strings.ToUpper(strings.TrimSpace(decided.State)) {
	case "ACCEPTED":
		guidance = projectPatchQueueLifecycleDecisionRecordedMarker + " Acceptance is not a canonical baseline update; call project_patch_queue_integrate or project_patch_queue_followup(auto) next so the accepted candidate becomes visible integration work."
		followup := NewProjectPatchQueueFollowupTool(t.client, t.workspaceID, t.agentID, "").Execute(ctx, map[string]any{
			"project_id":          projectID,
			"queue_id":            decided.QueueID,
			"item_id":             decided.ItemID,
			"followup_kind":       "integration",
			"extra_context":       "Created automatically after ACCEPTED patch queue decision so the accepted candidate becomes visible canonical integration work.",
			"dependency_task_ids": []string{},
		})
		followupPayload := projectPatchQueueLifecycleToolResultPayload(followup)
		extra["integration_followup"] = followupPayload
		extra["next_action_contract"] = projectPatchQueueLifecycleNextActionContract(decided, "integration", followupPayload)
	case "BLOCKED":
		guidance = projectPatchQueueLifecycleDecisionRecordedMarker + " A blocked candidate is not a terminal resting state; a visible validation or revision follow-up was requested automatically so agents can unblock or revise the candidate."
		followup := NewProjectPatchQueueFollowupTool(t.client, t.workspaceID, t.agentID, "").Execute(ctx, map[string]any{
			"project_id":                 projectID,
			"queue_id":                   decided.QueueID,
			"item_id":                    decided.ItemID,
			"followup_kind":              "auto",
			"extra_context":              "Created automatically after BLOCKED patch queue decision so missing validation evidence or required revisions become visible follow-up work instead of ending coordination.",
			"dependency_task_ids":        []string{},
			"skip_visual_receipt_lookup": true,
		})
		followupPayload := projectPatchQueueLifecycleToolResultPayload(followup)
		extra["followup"] = followupPayload
		extra["next_action_contract"] = projectPatchQueueLifecycleNextActionContract(decided, "auto", followupPayload)
	case "REJECTED":
		guidance = projectPatchQueueLifecycleDecisionRecordedMarker + " A rejected candidate needs visible revision or replacement work; a follow-up was requested automatically so agents can keep product progress moving."
		followup := NewProjectPatchQueueFollowupTool(t.client, t.workspaceID, t.agentID, "").Execute(ctx, map[string]any{
			"project_id":          projectID,
			"queue_id":            decided.QueueID,
			"item_id":             decided.ItemID,
			"followup_kind":       "revision",
			"extra_context":       "Created automatically after REJECTED patch queue decision so concrete review findings become visible revision work instead of ending coordination.",
			"dependency_task_ids": []string{},
		})
		followupPayload := projectPatchQueueLifecycleToolResultPayload(followup)
		extra["followup"] = followupPayload
		extra["next_action_contract"] = projectPatchQueueLifecycleNextActionContract(decided, "revision", followupPayload)
	}
	return projectPatchQueueLifecycleResult(action, decided, guidance, extra)
}

// projectPatchQueueLifecycleSourceFidelityRemediation turns the server's opaque
// source-fidelity ACCEPTED rejection (CD-1, the Run25 class) into an actionable instruction so
// an integrator/reviewer is not dead-ended by a cryptic error. It deliberately does NOT
// re-implement the server gate (that would reintroduce tool/server drift, CD-2); it only
// enriches an error that is already source-fidelity-shaped.
func projectPatchQueueLifecycleSourceFidelityRemediation(err error, decision, projectID string) string {
	if err == nil || !strings.EqualFold(strings.TrimSpace(decision), "ACCEPTED") {
		return ""
	}
	lower := strings.ToLower(err.Error())
	if !containsAnySignal(lower, []string{
		"source-fidelity",
		"source_fidelity_status",
		"checked_source_doc_keys",
		"rhizome_spec_fidelity_review_v1",
	}) {
		return ""
	}
	refsDoc := projectSourceRefsDocKey(projectID)
	return strings.Join([]string{
		"This project enforces source-fidelity on ACCEPTED decisions. To proceed, either:",
		fmt.Sprintf("  (a) include in your decision_summary or decision_doc a `source_fidelity_status: passed` line (or a rhizome_spec_fidelity_review_v1 block) AND `checked_source_doc_keys:` listing every source_doc_key from %s; or", refsDoc),
		"  (b) if the candidate actually diverges from the source spec, record BLOCKED_SPEC_DRIFT instead of ACCEPTED.",
	}, "\n")
}

func (t *ProjectPatchQueueLifecycleTool) patchQueueDecisionCheckedSourceDocKeys(ctx context.Context, projectID string, explicit []string, decisionDocs []WorkspaceDocRecord) ([]string, error) {
	keys := uniqueTrimmedCSVStrings(explicit)
	if len(keys) > 0 || !patchQueueLifecycleDocsContainSourceFidelitySignal(decisionDocs) {
		return keys, nil
	}
	for _, docKey := range uniqueTrimmedCSVStrings([]string{
		projectSourceRefsDocKey(projectID),
		projectSourceRequirementsTraceDocKey(projectID),
	}) {
		doc, ok, err := t.client.GetDoc(ctx, t.workspaceID, docKey)
		if err != nil {
			return nil, fmt.Errorf("project_patch_queue_lifecycle source-fidelity gate failed reading doc %s: %v", docKey, err)
		}
		if !ok {
			continue
		}
		keys = append(keys, sourceDocKeysFromSourceRefsText(doc.Content)...)
	}
	return uniqueTrimmedCSVStrings(keys), nil
}

func patchQueueLifecycleDocsContainSourceFidelitySignal(docs []WorkspaceDocRecord) bool {
	var b strings.Builder
	for _, doc := range docs {
		b.WriteString("\n")
		b.WriteString(doc.Content)
	}
	text := strings.ToLower(b.String())
	return strings.Contains(text, "rhizome_spec_fidelity_review_v1") ||
		strings.Contains(text, "source_fidelity_status") ||
		strings.Contains(text, "source fidelity status")
}

func (t *ProjectPatchQueueLifecycleTool) visualAcceptanceCandidateDocs(ctx context.Context, item ProjectPatchQueueItemRecord, summary, decisionDocKey string) ([]WorkspaceDocRecord, error) {
	docKeys := uniqueTrimmedCSVStrings([]string{
		strings.TrimSpace(decisionDocKey),
		strings.TrimSpace(item.DecisionDocKey),
		strings.TrimSpace(item.ReviewDocKey),
		strings.TrimSpace(item.EvidenceDocKey),
		strings.TrimSpace(item.ReviewerAdvisory.ReviewDocKey),
	})
	docs := make([]WorkspaceDocRecord, 0, len(docKeys)+1)
	if strings.TrimSpace(summary) != "" {
		docs = append(docs, WorkspaceDocRecord{
			DocKey:  "inline.decision_summary",
			Title:   "Inline patch queue decision summary",
			Content: summary,
		})
	}
	for _, key := range docKeys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		doc, ok, err := t.client.GetDoc(ctx, t.workspaceID, key)
		if err != nil {
			return nil, fmt.Errorf("project_patch_queue_lifecycle visual_acceptance_gate failed reading doc %s: %v", key, err)
		}
		if !ok {
			docs = append(docs, WorkspaceDocRecord{DocKey: key, Title: "Missing workspace doc", Content: ""})
			continue
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

func stringListArg(args map[string]any, key string) []string {
	if args == nil {
		return nil
	}
	value, ok := args[key]
	if !ok || value == nil {
		return nil
	}
	switch typed := value.(type) {
	case []string:
		return uniqueTrimmedCSVStrings(typed)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			out = append(out, fmt.Sprint(item))
		}
		return uniqueTrimmedCSVStrings(out)
	case string:
		return uniqueTrimmedCSVStrings([]string{typed})
	default:
		return uniqueTrimmedCSVStrings([]string{fmt.Sprint(typed)})
	}
}

func (t *ProjectPatchQueueLifecycleTool) supersede(ctx context.Context, projectID string, item ProjectPatchQueueItemRecord, args map[string]any) *ToolResult {
	if !strings.EqualFold(strings.TrimSpace(item.State), "BLOCKED") {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_lifecycle supersede requires a BLOCKED item, got %s", item.State), IsError: true}
	}
	newItemID := firstNonEmpty(strings.TrimSpace(stringArg(args, "new_item_id")), strings.TrimSpace(stringArg(args, "requeue_item_id")), strings.TrimSpace(stringArg(args, "item_id")))
	if newItemID == "" || newItemID == strings.TrimSpace(item.ItemID) {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_lifecycle supersede requires new_item_id/requeue_item_id different from blocked item_id %s", item.ItemID), IsError: true}
	}
	evidenceDocKey := firstNonEmpty(strings.TrimSpace(stringArg(args, "validation_doc_key")), strings.TrimSpace(stringArg(args, "evidence_doc_key")))
	if evidenceDocKey == "" {
		return &ToolResult{Output: "project_patch_queue_lifecycle supersede requires validation_doc_key or evidence_doc_key", IsError: true}
	}
	superseded, alreadyQueued, err := t.client.SupersedeProjectPatchQueueItem(ctx, ProjectPatchQueueSupersedeInput{
		WorkspaceID:    t.workspaceID,
		ProjectID:      projectID,
		ActorID:        t.agentID,
		QueueID:        item.QueueID,
		ItemID:         item.ItemID,
		NewItemID:      newItemID,
		EvidenceDocKey: evidenceDocKey,
		MaxAttempts:    intArg(args["max_attempts"], 0),
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_lifecycle supersede failed for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
	}
	guidance := "A fresh PROPOSED patch queue item was created from newer evidence. Review/integration must claim and decide the fresh item; no git mutation was performed."
	extra := map[string]any{
		"already_queued": alreadyQueued,
		"superseded": map[string]any{
			"queue_id": item.QueueID,
			"item_id":  item.ItemID,
			"state":    item.State,
		},
	}
	return projectPatchQueueLifecycleResult("supersede", superseded, guidance, extra)
}

// projectPatchQueueLifecycleSummaryLooksLikeReviewerCapacityGap (R22-F2) recognizes block
// summaries whose stated reason is reviewer AVAILABILITY rather than a candidate defect.
// Conservative phrase set: genuine defect summaries pass through untouched.
func projectPatchQueueLifecycleSummaryLooksLikeReviewerCapacityGap(summary string) bool {
	text := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(summary)), " "))
	if text == "" {
		return false
	}
	for _, marker := range []string{
		"no reviewer",
		"no eligible reviewer",
		"no independent reviewer",
		"reviewer unavailable",
		"reviewers unavailable",
		"reviewer is unavailable",
		"reviewers are busy",
		"reviewer is busy",
		"reviewers busy",
		"review capacity",
		"reviewer capacity",
		"delegation refused",
		"delegation was refused",
		"peers refused",
		"peers are busy",
		"no one to review",
		"nobody to review",
		"awaiting reviewer",
		"waiting for a reviewer",
		"waiting for reviewer",
		"review remains pending",
		"review still pending",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func projectPatchQueueLifecycleSummaryLooksLikeActorAuthorityGap(summary string) bool {
	text := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(summary)), " "))
	if text == "" {
		return false
	}
	authoritySignal := false
	for _, marker := range []string{
		"integrator authority",
		"integration authority",
		"active integrator",
		"active integration role",
		"lacks integrator",
		"lack integrator",
		"missing integrator",
		"without integrator",
		"lacks integration role",
		"missing integration role",
		"controlled-queue completion",
		"controlled queue completion",
	} {
		if strings.Contains(text, marker) {
			authoritySignal = true
			break
		}
	}
	if !authoritySignal {
		return false
	}
	for _, actorMarker := range []string{
		"reviewer",
		"actor",
		"principal",
		"agent",
		"claimed by",
		"claimant",
		"i lack",
		"i lacks",
		"iota lacks",
		"epsilon lacks",
		"delta lacks",
		"because",
	} {
		if strings.Contains(text, actorMarker) {
			return true
		}
	}
	return false
}

func projectPatchQueueLifecycleResult(action string, item ProjectPatchQueueItemRecord, guidance string, extras ...map[string]any) *ToolResult {
	payload := map[string]any{
		"action":                     action,
		"project_id":                 item.ProjectID,
		"repo_id":                    item.RepoID,
		"branch_id":                  item.BranchID,
		"queue_id":                   item.QueueID,
		"item_id":                    item.ItemID,
		"repo_authority_mode":        item.RepoAuthorityMode,
		"context_digest":             item.ContextDigest,
		"state":                      item.State,
		"claimed_by":                 item.ClaimedBy,
		"claim_token":                item.ClaimToken,
		"claim_expires_at":           item.ClaimExpiresAt,
		"decision_doc_key":           item.DecisionDocKey,
		"decision_summary":           item.DecisionSummary,
		"decided_by":                 item.DecidedBy,
		"decided_at":                 item.DecidedAt,
		"supersedes_queue_id":        item.SupersedesQueueID,
		"supersedes_item_id":         item.SupersedesItemID,
		"evidence_doc_key":           item.EvidenceDocKey,
		"reviewer_advisory_digest":   item.ReviewerAdvisoryDigest,
		"operator_enablement_digest": item.OperatorEnablementDigest,
		"no_git_mutation":            true,
		"integration_guidance":       guidance,
	}
	for _, extra := range extras {
		for key, value := range extra {
			if strings.TrimSpace(key) == "" || value == nil {
				continue
			}
			payload[key] = value
		}
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_lifecycle %s completed for %s/%s", action, item.QueueID, item.ItemID)}
	}
	return &ToolResult{Output: string(raw)}
}

func projectPatchQueueLifecycleJSONResult(payload map[string]any, fallback string) *ToolResult {
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fallback}
	}
	return &ToolResult{Output: string(raw)}
}

func projectPatchQueueLifecycleToolResultPayload(result *ToolResult) map[string]any {
	if result == nil {
		return map[string]any{"is_error": true, "output": "follow-up tool returned no result"}
	}
	payload := map[string]any{
		"is_error": result.IsError,
		"output":   strings.TrimSpace(result.Output),
	}
	var parsed any
	if err := json.Unmarshal([]byte(strings.TrimSpace(result.Output)), &parsed); err == nil {
		payload["payload"] = parsed
	}
	return payload
}

func projectPatchQueueLifecycleNextActionContract(item ProjectPatchQueueItemRecord, requestedKind string, followupPayload map[string]any) map[string]any {
	followupTaskID := projectPatchQueueLifecycleStringFromPayload(followupPayload, "task_id")
	resolvedKind := firstNonEmpty(projectPatchQueueLifecycleStringFromPayload(followupPayload, "followup_kind"), strings.TrimSpace(requestedKind))
	followupError := false
	if raw, ok := followupPayload["is_error"].(bool); ok {
		followupError = raw
	}
	contract := map[string]any{
		"contract_version":              "patch_queue_lifecycle_next_action_v1",
		"queue_id":                      strings.TrimSpace(item.QueueID),
		"item_id":                       strings.TrimSpace(item.ItemID),
		"branch_id":                     strings.TrimSpace(item.BranchID),
		"head_sha":                      strings.TrimSpace(item.HeadSHA),
		"terminal_state":                strings.ToUpper(strings.TrimSpace(item.State)),
		"followup_kind":                 resolvedKind,
		"followup_task_id":              followupTaskID,
		"current_work_transition":       "complete_or_release_current_patch_queue_lifecycle_task",
		"current_session_must_converge": true,
		"work_next_guidance":            "Do not keep a private patch-queue decision session active after creating visible follow-up work. Finish the current lifecycle task truthfully, then let agent.work.next select the follow-up or another live task.",
		"stale_session_risk":            "An active lifecycle/review session can hide the newly created follow-up from this agent's next heartbeat.",
	}
	if followupError {
		contract["followup_visibility"] = "not_visible_submit_failed"
		contract["repair_guidance"] = "Repair the follow-up submit error before claiming or delegating a deterministic follow-up task id."
	} else if followupTaskID != "" {
		contract["followup_visibility"] = "visible_task_created_or_hydrated"
		contract["next_claim_guidance"] = "Claim or delegate the visible follow-up task_id instead of continuing private integration/review loops."
	} else {
		contract["followup_visibility"] = "no_followup_task_id_returned"
		contract["repair_guidance"] = "Hydrate current project coordination before assuming the follow-up exists."
	}
	return contract
}

func projectPatchQueueLifecycleStringFromPayload(payload map[string]any, key string) string {
	key = strings.TrimSpace(key)
	if key == "" || len(payload) == 0 {
		return ""
	}
	var walk func(any) string
	walk = func(value any) string {
		switch typed := value.(type) {
		case map[string]any:
			if found, ok := typed[key]; ok {
				if str := strings.TrimSpace(fmt.Sprint(found)); str != "" && str != "<nil>" {
					return str
				}
			}
			for _, nested := range typed {
				if str := walk(nested); str != "" {
					return str
				}
			}
		case []any:
			for _, nested := range typed {
				if str := walk(nested); str != "" {
					return str
				}
			}
		}
		return ""
	}
	return walk(payload)
}

func normalizeProjectPatchQueueLifecycleAction(value string) string {
	action := strings.ToLower(strings.TrimSpace(value))
	action = strings.ReplaceAll(action, "-", "_")
	action = strings.ReplaceAll(action, " ", "_")
	switch action {
	case "claim", "release", "reconcile_review_task", "consume_continuation", "reviewer_advisory", "accept", "reject", "block", "cancel", "supersede", "requeue":
		return action
	case "reconcile-review-task", "reconcile review task", "reconcile_patch_queue_review_task_receipt":
		return "reconcile_review_task"
	case "consume-continuation", "consume continuation", "consume_decision_continuation", "decision_continuation":
		return "consume_continuation"
	case "review", "reviewed", "reviewer-advisory", "reviewer advisory":
		return "reviewer_advisory"
	case "accepted", "approve", "approved":
		return "accept"
	case "rejected":
		return "reject"
	case "blocked":
		return "block"
	case "canceled", "cancelled":
		return "cancel"
	case "superseded", "requeued":
		return "supersede"
	default:
		return ""
	}
}

func projectPatchQueueLifecycleActionNeedsIntegrationAuthority(action string) bool {
	switch normalizeProjectPatchQueueLifecycleAction(action) {
	case "claim", "reviewer_advisory":
		return true
	default:
		return false
	}
}

func projectPatchQueueLifecycleDecision(action string) (string, error) {
	switch normalizeProjectPatchQueueLifecycleAction(action) {
	case "accept":
		return "ACCEPTED", nil
	case "reject":
		return "REJECTED", nil
	case "block":
		return "BLOCKED", nil
	case "cancel":
		return "CANCELED", nil
	default:
		return "", fmt.Errorf("action must be accept, reject, block, or cancel for decision")
	}
}

func selectProjectPatchQueueLifecycleItem(items []ProjectPatchQueueItemRecord, queueID, itemID, branchID string) (ProjectPatchQueueItemRecord, error) {
	queueID = strings.TrimSpace(queueID)
	itemID = strings.TrimSpace(itemID)
	branchID = strings.TrimSpace(branchID)
	if queueID == "" && itemID == "" && branchID == "" {
		return ProjectPatchQueueItemRecord{}, fmt.Errorf("queue_id/item_id or branch_id is required")
	}
	matches := []ProjectPatchQueueItemRecord{}
	for _, item := range items {
		if queueID != "" && strings.TrimSpace(item.QueueID) != queueID {
			continue
		}
		if itemID != "" && strings.TrimSpace(item.ItemID) != itemID {
			continue
		}
		if branchID != "" && strings.TrimSpace(item.BranchID) != branchID {
			continue
		}
		matches = append(matches, item)
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		ids := make([]string, 0, len(matches))
		for _, item := range matches {
			ids = append(ids, strings.TrimSpace(item.QueueID)+"/"+strings.TrimSpace(item.ItemID))
		}
		return ProjectPatchQueueItemRecord{}, fmt.Errorf("patch queue item selector is ambiguous for queue_id=%q item_id=%q branch_id=%q; matches=%s; provide exact queue_id and item_id", queueID, itemID, branchID, strings.Join(uniqueTrimmedCSVStrings(ids), ", "))
	}
	return ProjectPatchQueueItemRecord{}, fmt.Errorf("patch queue item not found in project coordination")
}

func projectAgentHasPatchQueueIntegrationAuthority(coordination ProjectCoordinationRecord, agentID string) bool {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return false
	}
	if coordination.StrategicLead != nil &&
		strings.TrimSpace(coordination.StrategicLead.AgentID) == agentID &&
		strings.EqualFold(strings.TrimSpace(coordination.StrategicLead.Status), "ACTIVE") {
		return true
	}
	for _, role := range coordination.Roles {
		if strings.TrimSpace(role.AgentID) == agentID &&
			strings.EqualFold(strings.TrimSpace(role.Status), "ACTIVE") &&
			strings.EqualFold(strings.TrimSpace(role.RoleType), "INTEGRATOR") {
			return true
		}
	}
	return false
}

func projectAgentHasPatchQueueEvidenceAuthority(coordination ProjectCoordinationRecord, agentID string) bool {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return false
	}
	if coordination.StrategicLead != nil &&
		strings.TrimSpace(coordination.StrategicLead.AgentID) == agentID &&
		strings.EqualFold(strings.TrimSpace(coordination.StrategicLead.Status), "ACTIVE") {
		return true
	}
	for _, role := range coordination.Roles {
		if strings.TrimSpace(role.AgentID) != agentID ||
			!strings.EqualFold(strings.TrimSpace(role.Status), "ACTIVE") {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(role.RoleType)) {
		case "INTEGRATOR", "REVIEWER":
			return true
		}
	}
	return false
}

func projectPatchQueueLifecycleClaimReclaimable(item ProjectPatchQueueItemRecord, agentID string) bool {
	if !strings.EqualFold(strings.TrimSpace(item.State), "CLAIMED") {
		return false
	}
	if strings.TrimSpace(item.ClaimedBy) == strings.TrimSpace(agentID) {
		return true
	}
	expiresAt := strings.TrimSpace(item.ClaimExpiresAt)
	if expiresAt == "" {
		return true
	}
	parsed, err := time.Parse(time.RFC3339Nano, expiresAt)
	if err != nil {
		return true
	}
	return !parsed.After(time.Now().UTC())
}
