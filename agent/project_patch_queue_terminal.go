package main

import (
	"fmt"
	"strings"
)

func latestProjectPatchQueueTerminalItemForBranchHead(items []ProjectPatchQueueItemRecord, branchID, headSHA string) (ProjectPatchQueueItemRecord, bool) {
	branchID = strings.TrimSpace(branchID)
	headSHA = strings.TrimSpace(headSHA)
	if branchID == "" || headSHA == "" {
		return ProjectPatchQueueItemRecord{}, false
	}
	var selected ProjectPatchQueueItemRecord
	found := false
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) != branchID || strings.TrimSpace(item.HeadSHA) != headSHA {
			continue
		}
		if !projectPatchQueueTerminalState(item.State) {
			continue
		}
		if !found || strings.TrimSpace(item.UpdatedAt) > strings.TrimSpace(selected.UpdatedAt) {
			selected = item
			found = true
		}
	}
	if !found {
		return ProjectPatchQueueItemRecord{}, false
	}
	return selected, true
}

func projectPatchQueueItemExistsForBranchHead(items []ProjectPatchQueueItemRecord, branchID, headSHA string) bool {
	branchID = strings.TrimSpace(branchID)
	headSHA = strings.TrimSpace(headSHA)
	if branchID == "" || headSHA == "" {
		return false
	}
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) == branchID && strings.TrimSpace(item.HeadSHA) == headSHA {
			return true
		}
	}
	return false
}

func projectPatchQueueTerminalState(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "ACCEPTED", "INTEGRATED", "REJECTED", "BLOCKED", "CANCELED":
		return true
	default:
		return false
	}
}

func projectPatchQueuePositiveTerminalState(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "ACCEPTED", "INTEGRATED":
		return true
	default:
		return false
	}
}

func projectPatchQueueNegativeTerminalState(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "REJECTED", "BLOCKED":
		return true
	default:
		return false
	}
}

func projectPatchQueueTerminalDecisionPayload(item ProjectPatchQueueItemRecord) map[string]any {
	return map[string]any{
		"queue_id":         strings.TrimSpace(item.QueueID),
		"item_id":          strings.TrimSpace(item.ItemID),
		"branch_id":        strings.TrimSpace(item.BranchID),
		"state":            strings.ToUpper(strings.TrimSpace(item.State)),
		"head_sha":         strings.TrimSpace(item.HeadSHA),
		"decision_summary": strings.TrimSpace(item.DecisionSummary),
		"decision_doc_key": strings.TrimSpace(item.DecisionDocKey),
		"decided_by":       strings.TrimSpace(item.DecidedBy),
		"decided_at":       strings.TrimSpace(item.DecidedAt),
	}
}

func projectPatchQueueSameHeadRejectedMessage(toolName string, item ProjectPatchQueueItemRecord) string {
	summary := strings.TrimSpace(item.DecisionSummary)
	if summary == "" {
		summary = "no decision summary recorded"
	}
	return fmt.Sprintf("%s cannot clear same-head REJECTED patch queue item %s/%s for branch_id=%s head_sha=%s by republishing review evidence. Fix the rejection in the owned checkout, create a new commit/head with project_branch_commit, publish project_branch_review_ready for the new head, then submit a fresh patch queue item with a new item_id or claim the existing revision follow-up. Decision: %s", toolName, strings.TrimSpace(item.QueueID), strings.TrimSpace(item.ItemID), strings.TrimSpace(item.BranchID), strings.TrimSpace(item.HeadSHA), summary)
}

func projectPatchQueueSameHeadBlockedGuidance(item ProjectPatchQueueItemRecord) string {
	summary := strings.TrimSpace(item.DecisionSummary)
	if summary == "" {
		summary = "no decision summary recorded"
	}
	return fmt.Sprintf("Same-head patch queue item %s/%s remains BLOCKED. Refreshing review evidence is allowed when the blocker is missing validation evidence, but completion/integration still needs a queue-facing resolution: an eligible reviewer/integrator/strategic lead should call project_patch_queue_lifecycle action=supersede with a fresh new_item_id and validation_doc_key/evidence_doc_key after evidence refresh. This creates a fresh PROPOSED item; reviewers still decide it. Decision: %s", strings.TrimSpace(item.QueueID), strings.TrimSpace(item.ItemID), summary)
}
