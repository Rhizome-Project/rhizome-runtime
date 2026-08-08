package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type projectPatchQueueSelectorCandidate struct {
	QueueID                 string `json:"queue_id"`
	ItemID                  string `json:"item_id"`
	State                   string `json:"state"`
	ProjectID               string `json:"project_id,omitempty"`
	RepoID                  string `json:"repo_id,omitempty"`
	BranchID                string `json:"branch_id,omitempty"`
	ReviewDocKey            string `json:"review_doc_key,omitempty"`
	EvidenceDocKey          string `json:"evidence_doc_key,omitempty"`
	HeadSHA                 string `json:"head_sha,omitempty"`
	BaseSHA                 string `json:"base_sha,omitempty"`
	SupersedesQueueID       string `json:"supersedes_queue_id,omitempty"`
	SupersedesItemID        string `json:"supersedes_item_id,omitempty"`
	ClaimedBy               string `json:"claimed_by,omitempty"`
	ClaimExpiresAt          string `json:"claim_expires_at,omitempty"`
	DecisionSummary         string `json:"decision_summary,omitempty"`
	DecidedBy               string `json:"decided_by,omitempty"`
	UpdatedAt               string `json:"updated_at,omitempty"`
	VisualEvidenceDocKey    string `json:"visual_evidence_doc_key,omitempty"`
	VisualEvidenceVerdict   string `json:"visual_evidence_verdict,omitempty"`
	VisualEvidenceBlocking  bool   `json:"visual_evidence_blocking,omitempty"`
	VisualEvidenceRoute     string `json:"visual_evidence_route,omitempty"`
	SuggestedNextTransition string `json:"suggested_next_transition,omitempty"`
}

func projectPatchQueueSelectorCandidates(items []ProjectPatchQueueItemRecord) []projectPatchQueueSelectorCandidate {
	out := make([]projectPatchQueueSelectorCandidate, 0, len(items))
	for _, item := range items {
		out = append(out, projectPatchQueueSelectorCandidate{
			QueueID:           strings.TrimSpace(item.QueueID),
			ItemID:            strings.TrimSpace(item.ItemID),
			State:             strings.TrimSpace(item.State),
			ProjectID:         strings.TrimSpace(item.ProjectID),
			RepoID:            strings.TrimSpace(item.RepoID),
			BranchID:          strings.TrimSpace(item.BranchID),
			ReviewDocKey:      strings.TrimSpace(item.ReviewDocKey),
			EvidenceDocKey:    strings.TrimSpace(item.EvidenceDocKey),
			HeadSHA:           strings.TrimSpace(item.HeadSHA),
			BaseSHA:           strings.TrimSpace(item.BaseSHA),
			SupersedesQueueID: strings.TrimSpace(item.SupersedesQueueID),
			SupersedesItemID:  strings.TrimSpace(item.SupersedesItemID),
			ClaimedBy:         strings.TrimSpace(item.ClaimedBy),
			ClaimExpiresAt:    strings.TrimSpace(item.ClaimExpiresAt),
			DecisionSummary:   strings.TrimSpace(item.DecisionSummary),
			DecidedBy:         strings.TrimSpace(item.DecidedBy),
			UpdatedAt:         strings.TrimSpace(item.UpdatedAt),
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		left := out[i]
		right := out[j]
		for _, pair := range [][2]string{
			{left.State, right.State},
			{left.QueueID, right.QueueID},
			{left.ItemID, right.ItemID},
			{left.RepoID, right.RepoID},
			{left.BranchID, right.BranchID},
		} {
			if pair[0] != pair[1] {
				return pair[0] < pair[1]
			}
		}
		return false
	})
	return out
}

func projectPatchQueueSelectorAmbiguousError(matches []ProjectPatchQueueItemRecord) error {
	payload := map[string]any{
		"error":             "patch_queue_selector_ambiguous",
		"candidate_count":   len(matches),
		"candidates":        projectPatchQueueSelectorCandidates(matches),
		"selector_guidance": "Retry with exact queue_id and item_id, or call project_patch_queue_list with project_id plus optional repo_id/branch_id/state to refresh candidates.",
		"branch_id_warning": "branch_id can match superseded, blocked, rejected, and accepted records after revision loops.",
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("patch queue selector is ambiguous; provide queue_id and item_id")
	}
	return fmt.Errorf("patch queue selector is ambiguous: %s", string(raw))
}

func projectPatchQueueSelectorNotFoundError() error {
	return fmt.Errorf("patch queue item not found in project coordination; call project_patch_queue_list with project_id and optional repo_id/branch_id/state to discover current candidates")
}

func projectPatchQueueSelectorMissingError() error {
	return fmt.Errorf("queue_id/item_id or branch_id is required; call project_patch_queue_list with project_id and optional repo_id/state to discover current candidates")
}
