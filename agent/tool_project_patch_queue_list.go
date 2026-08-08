package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type ProjectPatchQueueListTool struct {
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	runtimeBinding func() AgentRuntimeBinding
}

func NewProjectPatchQueueListTool(client *RhizomeClient, workspaceID, agentID string) *ProjectPatchQueueListTool {
	return &ProjectPatchQueueListTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
	}
}

func (t *ProjectPatchQueueListTool) Name() string { return "project_patch_queue_list" }

func (t *ProjectPatchQueueListTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectPatchQueueListTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectPatchQueueListTool) Description() string {
	return "List durable project patch queue candidates and exact queue_id/item_id selectors without mutating git or coordination state."
}

func (t *ProjectPatchQueueListTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id": map[string]any{"type": "string", "description": "Project id."},
			"repo_id":    map[string]any{"type": "string", "description": "Optional repository filter."},
			"branch_id":  map[string]any{"type": "string", "description": "Optional branch filter."},
			"state": map[string]any{
				"type":        "string",
				"description": "Optional queue state filter.",
				"enum":        []string{"PROPOSED", "CLAIMED", "ACCEPTED", "REJECTED", "BLOCKED", "CANCELED"},
			},
		},
		"required": []string{"project_id"},
	}
}

func (t *ProjectPatchQueueListTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_patch_queue_list is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	projectID, err := resolveProjectToolActiveProjectID("project_patch_queue_list", t.workspaceID, projectToolRefArg(args, "project_id"), t.runtimeBinding)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	input := ProjectPatchQueueListInput{
		WorkspaceID: t.workspaceID,
		ProjectID:   projectID,
		RepoID:      projectToolRefArg(args, "repo_id"),
		BranchID:    projectToolRefArg(args, "branch_id"),
		State:       strings.ToUpper(strings.TrimSpace(stringArg(args, "state"))),
	}
	items, err := t.client.ListProjectPatchQueueItems(ctx, input)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_list failed for %s: %v", projectID, err), IsError: true}
	}
	candidates, visualLookupErrors := projectPatchQueueSelectorCandidatesWithVisualEvidence(ctx, t.client, t.workspaceID, items)
	payload := map[string]any{
		"project_id":        projectID,
		"filters":           projectPatchQueueListFilters(input),
		"count":             len(items),
		"patch_queue_items": candidates,
		"selector_guidance": "Use queue_id and item_id from this output for project_patch_queue_lifecycle, project_patch_queue_integrate, project_patch_queue_followup, CAS, or materialization calls. branch_id alone can be ambiguous after revisions/supersession.",
	}
	if len(visualLookupErrors) > 0 {
		payload["visual_evidence_lookup_errors"] = visualLookupErrors
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_list found %d item(s) for %s", len(items), projectID)}
	}
	return &ToolResult{Output: string(raw)}
}

func projectPatchQueueListFilters(input ProjectPatchQueueListInput) map[string]string {
	filters := map[string]string{}
	if value := strings.TrimSpace(input.RepoID); value != "" {
		filters["repo_id"] = value
	}
	if value := strings.TrimSpace(input.BranchID); value != "" {
		filters["branch_id"] = value
	}
	if value := strings.TrimSpace(input.State); value != "" {
		filters["state"] = value
	}
	return filters
}

func projectPatchQueueSelectorCandidatesWithVisualEvidence(ctx context.Context, client *RhizomeClient, workspaceID string, items []ProjectPatchQueueItemRecord) ([]projectPatchQueueSelectorCandidate, []string) {
	candidates := projectPatchQueueSelectorCandidates(items)
	if client == nil || strings.TrimSpace(workspaceID) == "" {
		return candidates, nil
	}
	var lookupErrors []string
	for i := range candidates {
		if !strings.EqualFold(strings.TrimSpace(candidates[i].State), "BLOCKED") {
			continue
		}
		item := ProjectPatchQueueItemRecord{
			QueueID:   candidates[i].QueueID,
			ItemID:    candidates[i].ItemID,
			ProjectID: candidates[i].ProjectID,
			RepoID:    candidates[i].RepoID,
			BranchID:  candidates[i].BranchID,
			HeadSHA:   candidates[i].HeadSHA,
		}
		receipt, ok, err := patchQueueVisualEvidenceReceiptForItem(ctx, client, workspaceID, item)
		if err != nil {
			selector := firstNonEmpty(strings.TrimSpace(candidates[i].ItemID), strings.TrimSpace(candidates[i].BranchID), strings.TrimSpace(candidates[i].QueueID), "unknown_item")
			lookupErrors = append(lookupErrors, fmt.Sprintf("%s: %v", selector, err))
			continue
		}
		if !ok {
			continue
		}
		candidates[i].VisualEvidenceDocKey = strings.TrimSpace(receipt.DocKey)
		candidates[i].VisualEvidenceVerdict = strings.TrimSpace(receipt.Verdict)
		candidates[i].VisualEvidenceBlocking = receipt.Blocking
		if receipt.Blocking {
			candidates[i].VisualEvidenceRoute = "blocking_visual_evidence_exists; call project_patch_queue_followup to create or claim a revision follow-up instead of requesting another missing-evidence validation task"
			candidates[i].SuggestedNextTransition = "project_patch_queue_followup"
		} else {
			candidates[i].VisualEvidenceRoute = "passing_visual_evidence_exists; call project_patch_queue_lifecycle with action=supersede/requeue when reviewer authority is required"
			candidates[i].SuggestedNextTransition = "project_patch_queue_lifecycle"
		}
	}
	return candidates, lookupErrors
}
