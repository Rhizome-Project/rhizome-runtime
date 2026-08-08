package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type ProjectPatchQueueSubmitTool struct {
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	workdir        string
	runtimeBinding func() AgentRuntimeBinding
}

func NewProjectPatchQueueSubmitTool(client *RhizomeClient, workspaceID, agentID string) *ProjectPatchQueueSubmitTool {
	return &ProjectPatchQueueSubmitTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
	}
}

func (t *ProjectPatchQueueSubmitTool) WithWorkdir(workdir string) *ProjectPatchQueueSubmitTool {
	if t != nil {
		t.workdir = strings.TrimSpace(workdir)
	}
	return t
}

func (t *ProjectPatchQueueSubmitTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectPatchQueueSubmitTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectPatchQueueSubmitTool) Name() string { return "project_patch_queue_submit" }

func (t *ProjectPatchQueueSubmitTool) Description() string {
	return "Submit an owned READY_FOR_REVIEW project branch into Rhizome's durable patch queue. It does not merge, push, rebase, or mutate git state."
}

func (t *ProjectPatchQueueSubmitTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id": map[string]any{"type": "string", "description": "Project id."},
			"branch_id":  map[string]any{"type": "string", "description": "READY_FOR_REVIEW project branch id."},
			"branch_name": map[string]any{
				"type":        "string",
				"description": "Existing READY_FOR_REVIEW branch name when branch_id is unknown.",
			},
			"repo_id": map[string]any{"type": "string", "description": "Optional repository id guard."},
			"queue_id": map[string]any{
				"type":        "string",
				"description": "Optional stable queue id. Defaults to project/repository scope.",
			},
			"item_id": map[string]any{
				"type":        "string",
				"description": "Optional stable queue item id. Defaults to branch scope.",
			},
			"review_doc_key": map[string]any{
				"type":        "string",
				"description": "Optional review doc key guard. Must match the READY_FOR_REVIEW branch.",
			},
			"pathset_json": map[string]any{
				"type":        "string",
				"description": "Optional JSON pathset override. Defaults to the branch write scope and cannot widen it.",
			},
			"base_ref": map[string]any{
				"type":        "string",
				"description": "Optional base ref override. Defaults to branch base_branch.",
			},
			"base_sha": map[string]any{
				"type":        "string",
				"description": "Optional base SHA override. Defaults to branch base_sha.",
			},
			"head_sha": map[string]any{
				"type":        "string",
				"description": "Optional head SHA override. Defaults to branch head_sha.",
			},
			"repo_authority_mode": map[string]any{
				"type":        "string",
				"description": "Optional authority mode. Runtime submissions use repoauthority_controlled_queue; patch_only_temp_repo is legacy/invalid and cannot be newly submitted.",
			},
			"controlled_queue": map[string]any{
				"type":        "boolean",
				"description": "Submit as repoauthority_controlled_queue with complete runtime/repo/base binding refs. This is the default strict runtime path; the tool fails before RPC if refs cannot be built.",
			},
			"local_path": map[string]any{
				"type":        "string",
				"description": "Optional local checkout path used for read-only git evidence. Defaults to the branch checkout path or agent workdir.",
			},
			"candidate_paths": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Optional concrete candidate paths for scoped pathsets. If omitted, the tool derives them from git diff base_sha..head_sha.",
			},
			"task_id": map[string]any{
				"type":        "string",
				"description": "Optional task binding override. Defaults to active runtime task.",
			},
			"session_id": map[string]any{
				"type":        "string",
				"description": "Optional session binding override. Defaults to active runtime session.",
			},
			"run_id": map[string]any{
				"type":        "string",
				"description": "Optional parent task run binding override. Defaults to active runtime run; this is not the mutation operation id.",
			},
			"capability_snapshot_id": map[string]any{
				"type":        "string",
				"description": "Optional capability snapshot binding override. Defaults to active runtime capability snapshot.",
			},
			"capability_snapshot_schema": map[string]any{
				"type":        "string",
				"description": "Optional capability snapshot schema. Defaults to daemon_capability_snapshot.v1.",
			},
			"repo_lease_id": map[string]any{
				"type":        "string",
				"description": "Concrete durable repo lease evidence id. Required for controlled_queue.",
			},
			"lease_term": map[string]any{
				"type":        "integer",
				"description": "Positive durable repo lease term. Required for controlled_queue.",
			},
			"base_tree_hash": map[string]any{
				"type":        "string",
				"description": "Optional base git tree hash. Defaults to read-only git rev-parse base_sha^{tree}.",
			},
			"base_file_hashes_json": map[string]any{
				"type":        "string",
				"description": "Optional JSON object of concrete base file content digests. Defaults to read-only git show base_sha:path for candidate paths.",
			},
			"max_attempts": map[string]any{
				"type":        "integer",
				"description": "Optional bounded retry max attempts for the queue item.",
			},
			"supersedes_queue_id": map[string]any{
				"type":        "string",
				"description": "Required when requeueing the same branch/head after a BLOCKED item, or when replacing a legacy patch-only proposal: queue id being superseded.",
			},
			"supersedes_item_id": map[string]any{
				"type":        "string",
				"description": "Required when requeueing the same branch/head after a BLOCKED item, or when replacing a legacy patch-only proposal: item id being superseded.",
			},
			"validation_doc_key": map[string]any{
				"type":        "string",
				"description": "Evidence doc key proving the old BLOCKED reason was addressed for the same branch/head.",
			},
			"evidence_doc_key": map[string]any{
				"type":        "string",
				"description": "Alias for validation_doc_key when the proof is a broader review, smoke, or QA evidence document.",
			},
		},
		"required": []string{"project_id"},
	}
}

func (t *ProjectPatchQueueSubmitTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_patch_queue_submit is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	projectID, err := resolveProjectToolActiveProjectID("project_patch_queue_submit", t.workspaceID, projectToolRefArg(args, "project_id"), t.runtimeBinding)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	branchID := projectToolRefArg(args, "branch_id")
	branchName := projectToolRefArg(args, "branch_name")
	if branchID == "" && branchName == "" {
		return &ToolResult{Output: "branch_id or branch_name is required", IsError: true}
	}
	requestedAuthorityMode := strings.TrimSpace(stringArg(args, "repo_authority_mode"))
	if requestedAuthorityMode == "patch_only_temp_repo" {
		return &ToolResult{Output: "project_patch_queue_submit no longer creates patch_only_temp_repo proposals; use controlled_queue semantics with durable runtime/repo/base refs", IsError: true}
	}
	coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_submit failed reading project coordination for %s: %v", projectID, err), IsError: true}
	}
	repoID := projectToolRefArg(args, "repo_id")
	queueID := strings.TrimSpace(stringArg(args, "queue_id"))
	itemID := strings.TrimSpace(stringArg(args, "item_id"))
	if branchEvidence, ok, err := selectProjectPatchQueueBranchEvidence(coordination.Branches, t.agentID, branchID, branchName, repoID); err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	} else if ok {
		if terminalDecision, ok := latestProjectPatchQueueTerminalItemForBranchHead(coordination.PatchQueueItems, branchEvidence.BranchID, branchEvidence.HeadSHA); ok &&
			projectPatchQueuePositiveTerminalState(terminalDecision.State) &&
			projectPatchQueueSubmitHasQueueSelector(queueID, itemID) &&
			!projectPatchQueueSubmitSelectorMatches(terminalDecision, queueID, itemID) {
			return &ToolResult{Output: fmt.Sprintf("project_patch_queue_submit refuses fresh queue identity for same-head ACCEPTED patch queue item %s/%s on branch_id=%s head_sha=%s; integration, rebuild, or explicit supersession must consume that accepted boundary before a new item_id is allowed", terminalDecision.QueueID, terminalDecision.ItemID, branchEvidence.BranchID, branchEvidence.HeadSHA), IsError: true}
		}
		if terminalDecision, ok := projectPatchQueueSubmitTerminalNoopCandidate(coordination.PatchQueueItems, branchEvidence.BranchID, branchEvidence.HeadSHA, queueID, itemID); ok {
			if projectPatchQueuePositiveTerminalState(terminalDecision.State) {
				return projectPatchQueueSubmitResult(projectPatchQueueSubmitOutput{
					Item:                 terminalDecision,
					Branch:               branchEvidence,
					TerminalNoop:         true,
					AlreadyAccepted:      true,
					AlreadyIntegrated:    projectPatchQueueBranchIntegrated(branchEvidence),
					NoGitMutation:        true,
					AutoMergeEnabled:     false,
					RecommendedTaskState: "completed",
					NextAction:           "close_stale_owner_submit_task",
					DoNotRetrySubmit:     true,
					DoNotCreateNewBranch: true,
					Guidance:             "This branch/head already has an ACCEPTED durable patch queue decision. Treat this stale owner-submit as satisfied; do not create a replacement branch or submit another queue item.",
				})
			}
			if projectPatchQueueNegativeTerminalState(terminalDecision.State) {
				if strings.EqualFold(strings.TrimSpace(terminalDecision.State), "REJECTED") {
					return &ToolResult{Output: projectPatchQueueSameHeadRejectedMessage("project_patch_queue_submit", terminalDecision), IsError: true}
				}
				if projectBranchTerminalState(branchEvidence.Status) {
					return &ToolResult{Output: fmt.Sprintf("project_patch_queue_submit cannot requeue terminal branch %s in status %s for same-head %s. %s", branchEvidence.BranchID, firstNonEmpty(branchEvidence.Status, "UNKNOWN"), terminalDecision.HeadSHA, projectPatchQueueSameHeadBlockedGuidance(terminalDecision)), IsError: true}
				}
			}
		}
		if terminalDecision, ok := latestProjectPatchQueueTerminalItemForBranchHead(coordination.PatchQueueItems, branchEvidence.BranchID, branchEvidence.HeadSHA); ok && projectPatchQueueNegativeTerminalState(terminalDecision.State) && projectBranchTerminalState(branchEvidence.Status) {
			if strings.EqualFold(strings.TrimSpace(terminalDecision.State), "REJECTED") {
				return &ToolResult{Output: projectPatchQueueSameHeadRejectedMessage("project_patch_queue_submit", terminalDecision), IsError: true}
			}
			return &ToolResult{Output: fmt.Sprintf("project_patch_queue_submit cannot requeue terminal branch %s in status %s for same-head %s. %s", branchEvidence.BranchID, firstNonEmpty(branchEvidence.Status, "UNKNOWN"), terminalDecision.HeadSHA, projectPatchQueueSameHeadBlockedGuidance(terminalDecision)), IsError: true}
		}
		if projectBranchTerminalState(branchEvidence.Status) {
			return &ToolResult{Output: projectPatchQueueTerminalBranchMessage("project_patch_queue_submit", branchEvidence), IsError: true}
		}
	}
	branch, err := selectProjectPatchQueueBranch(coordination.Branches, t.agentID, branchID, branchName, repoID)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	branchHeadSHA := strings.TrimSpace(branch.HeadSHA)
	supersedesQueueID := ""
	supersedesItemID := ""
	evidenceDocKey := ""
	legacyPatchOnlyReplacement := ProjectPatchQueueItemRecord{}
	legacyPatchOnlyReplacementOK := false
	if terminalDecision, ok := latestProjectPatchQueueTerminalItemForBranchHead(coordination.PatchQueueItems, branch.BranchID, branchHeadSHA); ok && projectPatchQueueNegativeTerminalState(terminalDecision.State) {
		if strings.EqualFold(strings.TrimSpace(terminalDecision.State), "REJECTED") {
			return &ToolResult{Output: projectPatchQueueSameHeadRejectedMessage("project_patch_queue_submit", terminalDecision), IsError: true}
		}
		var err error
		supersedesQueueID, supersedesItemID, evidenceDocKey, err = t.validateBlockedRequeueEvidence(ctx, args, terminalDecision)
		if err != nil {
			return &ToolResult{Output: err.Error() + " " + projectPatchQueueSameHeadBlockedGuidance(terminalDecision), IsError: true}
		}
	}
	if existing, ok := findProjectPatchQueueItemForBranch(coordination.PatchQueueItems, branch.BranchID); ok {
		if projectPatchQueueSubmitWantsControlled(args) && !projectPatchQueueSubmitExistingControlledReady(existing) {
			if !projectPatchQueueSubmitLegacyPatchOnlyProposal(existing, branch) {
				return &ToolResult{Output: fmt.Sprintf("existing patch queue item %s/%s is not a complete controlled_queue proposal; use a new item_id after revision or clear the stale proposal before operation/CAS gates", existing.QueueID, existing.ItemID), IsError: true}
			}
			if queueID != "" && queueID != strings.TrimSpace(existing.QueueID) {
				return &ToolResult{Output: fmt.Sprintf("project_patch_queue_submit legacy patch-only cancel+replace must keep queue_id=%s and use a fresh item_id", strings.TrimSpace(existing.QueueID)), IsError: true}
			}
			replacementItemID, err := projectPatchQueueSubmitControlledReplacementItemID(existing, strings.TrimSpace(stringArg(args, "item_id")))
			if err != nil {
				return &ToolResult{Output: err.Error(), IsError: true}
			}
			queueID = strings.TrimSpace(existing.QueueID)
			itemID = replacementItemID
			supersedesQueueID = strings.TrimSpace(existing.QueueID)
			supersedesItemID = strings.TrimSpace(existing.ItemID)
			legacyPatchOnlyReplacement = existing
			legacyPatchOnlyReplacementOK = true
		} else {
			return projectPatchQueueSubmitResult(projectPatchQueueSubmitOutput{
				Item:             existing,
				Branch:           branch,
				AlreadyQueued:    true,
				NoGitMutation:    true,
				AutoMergeEnabled: false,
				Guidance:         "Branch was already visible in the durable patch queue. Integration still requires explicit review/validation gates.",
			})
		}
	}
	if _, ok := selectProjectBranchReviewRepo(coordination.Repositories, branch.RepoID); !ok {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_submit requires branch repo %s to be READY with remote_url", branch.RepoID), IsError: true}
	}
	reviewDocKey := firstNonEmpty(projectToolRefArg(args, "review_doc_key"), branch.ReviewDocKey)
	if reviewDocKey == "" || reviewDocKey != strings.TrimSpace(branch.ReviewDocKey) {
		return &ToolResult{Output: "review_doc_key must match the READY_FOR_REVIEW branch evidence", IsError: true}
	}
	existingPathset, err := pathsetFromWriteScopeJSON(branch.WriteScopeJSON)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("branch write_scope_json pathset is invalid: %v", err), IsError: true}
	}
	if len(existingPathset) == 0 {
		return &ToolResult{Output: "branch write_scope_json must include non-empty paths before patch queue submission", IsError: true}
	}
	pathsetJSON := strings.TrimSpace(stringArg(args, "pathset_json"))
	if pathsetJSON != "" {
		pathset, err := pathsetFromWriteScopeJSON(pathsetJSON)
		if err != nil {
			return &ToolResult{Output: fmt.Sprintf("pathset_json pathset is invalid: %v", err), IsError: true}
		}
		if len(pathset) == 0 {
			return &ToolResult{Output: "pathset_json must include non-empty paths", IsError: true}
		}
		if !pathsetIsCoveredByScope(pathset, existingPathset) {
			return &ToolResult{Output: "pathset_json cannot widen branch write_scope_json", IsError: true}
		}
	}
	if branchHeadSHA == "" {
		return &ToolResult{Output: "READY_FOR_REVIEW branch must have head_sha before patch queue submission", IsError: true}
	}
	if headOverride := strings.TrimSpace(stringArg(args, "head_sha")); headOverride != "" && headOverride != branchHeadSHA {
		return &ToolResult{Output: "head_sha override must match the READY_FOR_REVIEW branch evidence", IsError: true}
	}
	branchBaseRef := strings.TrimSpace(branch.BaseBranch)
	if baseRefOverride := strings.TrimSpace(stringArg(args, "base_ref")); baseRefOverride != "" && branchBaseRef != "" && baseRefOverride != branchBaseRef {
		return &ToolResult{Output: "base_ref override must match the READY_FOR_REVIEW branch evidence", IsError: true}
	}
	branchBaseSHA := strings.TrimSpace(branch.BaseSHA)
	if branchBaseSHA != "" && branchHeadSHA == branchBaseSHA {
		return &ToolResult{Output: "READY_FOR_REVIEW branch head_sha must differ from base_sha before patch queue submission", IsError: true}
	}
	if baseSHAOverride := strings.TrimSpace(stringArg(args, "base_sha")); baseSHAOverride != "" && branchBaseSHA != "" && baseSHAOverride != branchBaseSHA {
		return &ToolResult{Output: "base_sha override must match the READY_FOR_REVIEW branch evidence", IsError: true}
	}
	input := ProjectPatchQueueSubmitInput{
		WorkspaceID:       t.workspaceID,
		ProjectID:         projectID,
		ActorID:           t.agentID,
		QueueID:           queueID,
		ItemID:            itemID,
		RepoID:            branch.RepoID,
		BranchID:          branch.BranchID,
		ReviewDocKey:      reviewDocKey,
		SupersedesQueueID: supersedesQueueID,
		SupersedesItemID:  supersedesItemID,
		EvidenceDocKey:    evidenceDocKey,
		RepoAuthorityMode: firstNonEmpty(requestedAuthorityMode, "repoauthority_controlled_queue"),
		PathsetJSON:       pathsetJSON,
		BaseRef:           branchBaseRef,
		BaseSHA:           branchBaseSHA,
		HeadSHA:           branchHeadSHA,
		AutoMerge:         false,
		MaxAttempts:       intArg(args["max_attempts"], 0),
	}
	if boolArg(args, "controlled_queue", false) {
		input.RepoAuthorityMode = "repoauthority_controlled_queue"
	}
	if input.RepoAuthorityMode == "repoauthority_controlled_queue" {
		var err error
		input, err = t.withControlledQueueBinding(ctx, args, coordination, branch, existingPathset, input)
		if err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_patch_queue_submit cannot build controlled_queue binding refs for branch %s: %v", branch.BranchID, err), IsError: true}
		}
	}
	item, err := t.client.SubmitProjectPatchQueueItem(ctx, input)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_submit failed submitting branch %s: %v", branch.BranchID, err), IsError: true}
	}
	return projectPatchQueueSubmitResult(projectPatchQueueSubmitOutput{
		Item:             item,
		Branch:           branch,
		AlreadyQueued:    false,
		NoGitMutation:    true,
		AutoMergeEnabled: false,
		Guidance:         projectPatchQueueSubmitGuidance(legacyPatchOnlyReplacementOK, legacyPatchOnlyReplacement),
	})
}

func (t *ProjectPatchQueueSubmitTool) validateBlockedRequeueEvidence(ctx context.Context, args map[string]any, terminalDecision ProjectPatchQueueItemRecord) (string, string, string, error) {
	requestedItemID := strings.TrimSpace(stringArg(args, "item_id"))
	if requestedItemID == "" {
		return "", "", "", fmt.Errorf("project_patch_queue_submit requires a fresh item_id and validation evidence for same-head BLOCKED state.")
	}
	oldItemID := strings.TrimSpace(terminalDecision.ItemID)
	if oldItemID != "" && requestedItemID == oldItemID {
		return "", "", "", fmt.Errorf("project_patch_queue_submit requeue item_id must differ from superseded BLOCKED item_id %s.", oldItemID)
	}
	supersedesItemID := strings.TrimSpace(stringArg(args, "supersedes_item_id"))
	if oldItemID != "" && supersedesItemID != oldItemID {
		return "", "", "", fmt.Errorf("project_patch_queue_submit same-head BLOCKED requeue requires supersedes_item_id=%s.", oldItemID)
	}
	oldQueueID := strings.TrimSpace(terminalDecision.QueueID)
	if supersedesQueueID := strings.TrimSpace(stringArg(args, "supersedes_queue_id")); oldQueueID != "" && supersedesQueueID != "" && supersedesQueueID != oldQueueID {
		return "", "", "", fmt.Errorf("project_patch_queue_submit supersedes_queue_id must match blocked queue_id=%s.", oldQueueID)
	}
	evidenceDocKey := firstNonEmpty(
		strings.TrimSpace(stringArg(args, "validation_doc_key")),
		strings.TrimSpace(stringArg(args, "evidence_doc_key")),
	)
	if evidenceDocKey == "" {
		return "", "", "", fmt.Errorf("project_patch_queue_submit same-head BLOCKED requeue requires validation_doc_key or evidence_doc_key proving the old blocker is superseded for branch_id=%s head_sha=%s.", strings.TrimSpace(terminalDecision.BranchID), strings.TrimSpace(terminalDecision.HeadSHA))
	}
	doc, ok, err := t.client.GetDoc(ctx, t.workspaceID, evidenceDocKey)
	if err != nil {
		return "", "", "", fmt.Errorf("project_patch_queue_submit failed reading evidence_doc_key %s: %v", evidenceDocKey, err)
	}
	if !ok {
		return "", "", "", fmt.Errorf("project_patch_queue_submit evidence_doc_key %s was not found", evidenceDocKey)
	}
	evidenceText := strings.ToLower(strings.Join([]string{doc.DocKey, doc.Title, doc.Content}, "\n"))
	for _, required := range []string{terminalDecision.BranchID, terminalDecision.HeadSHA, oldQueueID, oldItemID} {
		required = strings.ToLower(strings.TrimSpace(required))
		if required == "" {
			continue
		}
		if !strings.Contains(evidenceText, required) {
			return "", "", "", fmt.Errorf("project_patch_queue_submit evidence_doc_key %s must name the same branch_id/head_sha and superseded queue/item; missing %s", evidenceDocKey, required)
		}
	}
	return oldQueueID, oldItemID, evidenceDocKey, nil
}

type projectPatchQueueSubmitOutput struct {
	Item                 ProjectPatchQueueItemRecord
	Branch               ProjectBranchRecord
	AlreadyQueued        bool
	TerminalNoop         bool
	AlreadyAccepted      bool
	AlreadyIntegrated    bool
	NoGitMutation        bool
	AutoMergeEnabled     bool
	RecommendedTaskState string
	NextAction           string
	DoNotRetrySubmit     bool
	DoNotCreateNewBranch bool
	Guidance             string
}

func projectPatchQueueSubmitResult(output projectPatchQueueSubmitOutput) *ToolResult {
	payload := map[string]any{
		"project_id":           output.Item.ProjectID,
		"repo_id":              output.Item.RepoID,
		"branch_id":            output.Item.BranchID,
		"branch_name":          output.Branch.BranchName,
		"review_doc_key":       output.Item.ReviewDocKey,
		"supersedes_queue_id":  output.Item.SupersedesQueueID,
		"supersedes_item_id":   output.Item.SupersedesItemID,
		"evidence_doc_key":     output.Item.EvidenceDocKey,
		"queue_id":             output.Item.QueueID,
		"item_id":              output.Item.ItemID,
		"state":                output.Item.State,
		"pathset":              output.Item.Pathset,
		"base_ref":             output.Item.BaseRef,
		"base_sha":             output.Item.BaseSHA,
		"head_sha":             output.Item.HeadSHA,
		"already_queued":       output.AlreadyQueued,
		"no_git_mutation":      output.NoGitMutation,
		"auto_merge_enabled":   output.AutoMergeEnabled,
		"repo_authority_mode":  output.Item.RepoAuthorityMode,
		"integration_guidance": output.Guidance,
	}
	if output.TerminalNoop {
		payload["terminal_noop"] = true
	}
	if output.AlreadyAccepted {
		payload["already_accepted"] = true
	}
	if output.AlreadyIntegrated {
		payload["already_integrated"] = true
	}
	if output.RecommendedTaskState != "" {
		payload["recommended_task_outcome"] = output.RecommendedTaskState
	}
	if output.NextAction != "" {
		payload["next_action"] = output.NextAction
	}
	if output.DoNotRetrySubmit {
		payload["do_not_retry_submit"] = true
	}
	if output.DoNotCreateNewBranch {
		payload["do_not_create_new_branch"] = true
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_submit completed for %s/%s", output.Item.QueueID, output.Item.ItemID)}
	}
	return &ToolResult{Output: string(raw)}
}

func selectProjectPatchQueueBranch(branches []ProjectBranchRecord, agentID, branchID, branchName, repoID string) (ProjectBranchRecord, error) {
	branch, err := selectProjectBranchForOwnedTool("project_patch_queue_submit", branches, agentID, branchID, branchName, repoID)
	if err != nil {
		return ProjectBranchRecord{}, fmt.Errorf("project_patch_queue_submit requires an existing READY_FOR_REVIEW branch owned by this agent: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(branch.Status), "READY_FOR_REVIEW") {
		return ProjectBranchRecord{}, fmt.Errorf("project_patch_queue_submit requires branch %s to be READY_FOR_REVIEW, got %s. Recovery: run project_branch_commit with push=true for the owned checkout/current HEAD, then project_branch_review_ready with verification evidence, then retry project_patch_queue_submit", branch.BranchID, branch.Status)
	}
	if strings.TrimSpace(branch.ReviewDocKey) == "" {
		return ProjectBranchRecord{}, fmt.Errorf("project_patch_queue_submit requires branch %s to have review_doc_key. Recovery: run project_branch_review_ready with verification evidence, then retry project_patch_queue_submit", branch.BranchID)
	}
	return branch, nil
}

func selectProjectPatchQueueBranchEvidence(branches []ProjectBranchRecord, agentID, branchID, branchName, repoID string) (ProjectBranchRecord, bool, error) {
	agentID = strings.TrimSpace(agentID)
	branchID = strings.TrimSpace(branchID)
	branchName = strings.TrimSpace(branchName)
	repoID = strings.TrimSpace(repoID)
	var matches []ProjectBranchRecord
	for _, branch := range branches {
		if branchID != "" && strings.TrimSpace(branch.BranchID) != branchID {
			continue
		}
		if branchID == "" && branchName != "" && strings.TrimSpace(branch.BranchName) != branchName {
			continue
		}
		if repoID != "" && strings.TrimSpace(branch.RepoID) != repoID {
			continue
		}
		if strings.TrimSpace(branch.AgentID) != "" && strings.TrimSpace(branch.AgentID) != agentID {
			return ProjectBranchRecord{}, false, fmt.Errorf("%s", projectBranchOwnerRoutingMessage("project_patch_queue_submit", agentID, branch))
		}
		if strings.TrimSpace(branch.BranchID) != "" && strings.TrimSpace(branch.BranchName) != "" {
			matches = append(matches, branch)
		}
	}
	if len(matches) == 0 {
		return ProjectBranchRecord{}, false, nil
	}
	if branchID == "" && len(matches) > 1 {
		return ProjectBranchRecord{}, false, fmt.Errorf("project_patch_queue_submit branch_name is ambiguous across repositories; provide branch_id or repo_id")
	}
	return matches[0], true, nil
}

func projectPatchQueueTerminalBranchMessage(toolName string, branch ProjectBranchRecord) string {
	return fmt.Sprintf("%s cannot submit terminal branch %s in status %s because no ACCEPTED patch queue item exists for branch_id=%s head_sha=%s. Do not create a replacement branch or re-submit solely to satisfy this stale owner-submit; inspect the existing project/patch-queue state and close or supersede the task only with fresh evidence.", toolName, firstNonEmpty(branch.BranchID, "<unknown>"), firstNonEmpty(branch.Status, "UNKNOWN"), firstNonEmpty(branch.BranchID, "<unknown>"), firstNonEmpty(branch.HeadSHA, "<unknown>"))
}

func projectPatchQueueBranchIntegrated(branch ProjectBranchRecord) bool {
	switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
	case "MERGED", "INTEGRATED":
		return true
	default:
		return false
	}
}

func projectPatchQueueSubmitTerminalNoopCandidate(items []ProjectPatchQueueItemRecord, branchID, headSHA, queueID, itemID string) (ProjectPatchQueueItemRecord, bool) {
	queueID = strings.TrimSpace(queueID)
	itemID = strings.TrimSpace(itemID)
	if !projectPatchQueueSubmitHasQueueSelector(queueID, itemID) {
		return latestProjectPatchQueueTerminalItemForBranchHead(items, branchID, headSHA)
	}
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
		if queueID != "" && strings.TrimSpace(item.QueueID) != queueID {
			continue
		}
		if itemID != "" && strings.TrimSpace(item.ItemID) != itemID {
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

func projectPatchQueueSubmitHasQueueSelector(queueID, itemID string) bool {
	return strings.TrimSpace(queueID) != "" || strings.TrimSpace(itemID) != ""
}

func projectPatchQueueSubmitSelectorMatches(item ProjectPatchQueueItemRecord, queueID, itemID string) bool {
	queueID = strings.TrimSpace(queueID)
	itemID = strings.TrimSpace(itemID)
	if queueID != "" && strings.TrimSpace(item.QueueID) != queueID {
		return false
	}
	if itemID != "" && strings.TrimSpace(item.ItemID) != itemID {
		return false
	}
	return true
}

func findProjectPatchQueueItemForBranch(items []ProjectPatchQueueItemRecord, branchID string) (ProjectPatchQueueItemRecord, bool) {
	branchID = strings.TrimSpace(branchID)
	for _, item := range items {
		if strings.TrimSpace(item.BranchID) == branchID && projectPatchQueueActiveState(item.State) {
			return item, true
		}
	}
	return ProjectPatchQueueItemRecord{}, false
}

func projectPatchQueueActiveState(state string) bool {
	switch strings.ToUpper(strings.TrimSpace(state)) {
	case "PROPOSED", "CLAIMED":
		return true
	default:
		return false
	}
}

func (t *ProjectPatchQueueSubmitTool) withControlledQueueBinding(ctx context.Context, args map[string]any, coordination ProjectCoordinationRecord, branch ProjectBranchRecord, branchPathset []string, input ProjectPatchQueueSubmitInput) (ProjectPatchQueueSubmitInput, error) {
	pathset := branchPathset
	if strings.TrimSpace(input.PathsetJSON) != "" {
		parsed, err := pathsetFromWriteScopeJSON(input.PathsetJSON)
		if err != nil {
			return input, fmt.Errorf("pathset_json pathset is invalid: %w", err)
		}
		pathset = parsed
	}
	bindingRefs, err := projectPatchQueueSubmitControlledBindingRefs(ctx, args, coordination, branch, pathset, input, t.workdir)
	if err != nil {
		return input, err
	}
	binding := AgentRuntimeBinding{}
	if t.runtimeBinding != nil {
		binding = t.runtimeBinding()
	}
	taskID := firstNonEmpty(strings.TrimSpace(stringArg(args, "task_id")), binding.TaskID)
	sessionID := firstNonEmpty(strings.TrimSpace(stringArg(args, "session_id")), binding.SessionID)
	runID := firstNonEmpty(strings.TrimSpace(stringArg(args, "run_id")), binding.RunID)
	capabilitySnapshotID := firstNonEmpty(strings.TrimSpace(stringArg(args, "capability_snapshot_id")), binding.CapabilitySnapshotID)
	capabilitySnapshotSchema := firstNonEmpty(strings.TrimSpace(stringArg(args, "capability_snapshot_schema")), binding.CapabilitySnapshotSchema, daemonCapabilitySnapshotSchema)
	queueID := firstNonEmpty(strings.TrimSpace(input.QueueID), sanitizeRefSegment("patchq-"+input.ProjectID+"-"+input.RepoID))
	itemID := firstNonEmpty(strings.TrimSpace(input.ItemID), sanitizeRefSegment("patchitem-"+input.BranchID))
	input.QueueID = queueID
	input.ItemID = itemID
	input.TaskID = taskID
	input.SessionID = sessionID
	input.RunID = runID
	input.AgentID = t.agentID
	input.PrincipalType = firstNonEmpty(strings.TrimSpace(stringArg(args, "principal_type")), "agent")
	input.PrincipalID = firstNonEmpty(strings.TrimSpace(stringArg(args, "principal_id")), t.agentID)
	input.CapabilitySnapshotID = capabilitySnapshotID
	input.CapabilitySnapshotSchema = capabilitySnapshotSchema
	input.RepoRoot = bindingRefs.RepoRoot
	input.BaseTreeHash = bindingRefs.BaseTreeHash
	input.BaseFileHashes = bindingRefs.BaseFileHashes
	input.BaseFileHashesJSON = ""
	input.RepoLeaseID = strings.TrimSpace(stringArg(args, "repo_lease_id"))
	input.LeaseTerm = int64(intArg(args["lease_term"], 0))
	if input.RepoLeaseID == "" && input.LeaseTerm <= 0 {
		input.RepoLeaseID = projectPatchQueueGeneratedRepoLeaseID(input.WorkspaceID, input.ProjectID, queueID, itemID, input.AgentID, runID)
		input.LeaseTerm = 1
	}
	if err := projectPatchQueueRequireControlledSubmitRefs(input); err != nil {
		return input, err
	}
	return input, nil
}

type projectPatchQueueSubmitBindingRefs struct {
	RepoRoot       string
	BaseTreeHash   string
	BaseFileHashes map[string]string
}

func projectPatchQueueSubmitControlledBindingRefs(ctx context.Context, args map[string]any, coordination ProjectCoordinationRecord, branch ProjectBranchRecord, pathset []string, input ProjectPatchQueueSubmitInput, workdir string) (projectPatchQueueSubmitBindingRefs, error) {
	localPaths := projectPatchQueueSubmitLocalPathCandidates(args, coordination, branch, workdir)
	if len(localPaths) == 0 {
		return projectPatchQueueSubmitBindingRefs{}, fmt.Errorf("no local_path candidates are available")
	}
	rawBaseHashes := strings.TrimSpace(stringArg(args, "base_file_hashes_json"))
	explicitCandidatePaths := stringSliceArg(args, "candidate_paths")
	explicitBaseTreeHash := strings.TrimSpace(stringArg(args, "base_tree_hash"))
	failures := make([]string, 0, len(localPaths))
	for _, localPath := range localPaths {
		repoRoot, err := projectPatchQueueResolveRepoRoot(localPath)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", localPath, err))
			continue
		}
		candidatePaths, err := projectPatchQueueConcreteCandidatePaths(ctx, repoRoot, input.BaseSHA, input.HeadSHA, pathset, explicitCandidatePaths)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: candidate_paths: %v", repoRoot, err))
			continue
		}
		baseFileHashes, err := projectPatchQueueSubmitBaseFileHashes(ctx, repoRoot, input.BaseSHA, input.HeadSHA, pathset, candidatePaths, rawBaseHashes)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: base_file_hashes: %v", repoRoot, err))
			continue
		}
		baseTreeHash := explicitBaseTreeHash
		if baseTreeHash == "" {
			baseTreeHash, err = projectPatchQueueGitTreeHash(ctx, repoRoot, input.BaseSHA)
			if err != nil {
				failures = append(failures, fmt.Sprintf("%s: base_tree_hash: %v", repoRoot, err))
				continue
			}
		}
		return projectPatchQueueSubmitBindingRefs{
			RepoRoot:       repoRoot,
			BaseTreeHash:   baseTreeHash,
			BaseFileHashes: baseFileHashes,
		}, nil
	}
	return projectPatchQueueSubmitBindingRefs{}, fmt.Errorf("unable to derive binding refs from %d local_path candidate(s): %s", len(localPaths), strings.Join(failures, "; "))
}

func projectPatchQueueSubmitBaseFileHashes(ctx context.Context, repoRoot, baseSHA, headSHA string, pathset, candidatePaths []string, rawJSON string) (map[string]string, error) {
	hashes, err := projectPatchQueueBaseFileHashes(ctx, repoRoot, baseSHA, headSHA, pathset, candidatePaths, rawJSON)
	if err == nil {
		return hashes, nil
	}
	if strings.TrimSpace(rawJSON) == "" || !projectPatchQueueBaseFileHashMismatch(err) {
		return nil, err
	}
	// The tool owns controlled-queue evidence. If the caller supplied a stale
	// base hash, prefer the recorded base tree over self-supplied digest text.
	return projectPatchQueueBaseFileHashes(ctx, repoRoot, baseSHA, headSHA, pathset, candidatePaths, "")
}

func projectPatchQueueBaseFileHashMismatch(err error) bool {
	text := strings.ToLower(strings.TrimSpace(fmt.Sprint(err)))
	return strings.Contains(text, "base_file_hashes[") && strings.Contains(text, "mismatch")
}

func projectPatchQueueSubmitLocalPathCandidates(args map[string]any, coordination ProjectCoordinationRecord, branch ProjectBranchRecord, workdir string) []string {
	paths := make([]string, 0, 4)
	seen := map[string]struct{}{}
	add := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		key := strings.ToLower(path)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		paths = append(paths, path)
	}
	add(stringArg(args, "local_path"))
	add(projectBranchReviewCheckoutPath(coordination.Checkouts, branch.CheckoutID))
	for _, checkout := range coordination.Checkouts {
		if !strings.EqualFold(strings.TrimSpace(checkout.Status), "ACTIVE") {
			continue
		}
		if projectPatchQueueSubmitCheckoutMatchesBranch(checkout, branch) {
			add(checkout.LocalPath)
		}
	}
	for _, checkout := range coordination.Checkouts {
		if strings.EqualFold(strings.TrimSpace(checkout.Status), "ACTIVE") {
			continue
		}
		if projectPatchQueueSubmitCheckoutMatchesBranch(checkout, branch) {
			add(checkout.LocalPath)
		}
	}
	add(workdir)
	return paths
}

func projectPatchQueueSubmitCheckoutMatchesBranch(checkout ProjectCheckoutRecord, branch ProjectBranchRecord) bool {
	if strings.TrimSpace(checkout.LocalPath) == "" {
		return false
	}
	if strings.TrimSpace(branch.ProjectID) != "" && strings.TrimSpace(checkout.ProjectID) != strings.TrimSpace(branch.ProjectID) {
		return false
	}
	if strings.TrimSpace(branch.RepoID) != "" && strings.TrimSpace(checkout.RepoID) != strings.TrimSpace(branch.RepoID) {
		return false
	}
	if strings.TrimSpace(branch.CheckoutID) != "" && strings.TrimSpace(checkout.CheckoutID) == strings.TrimSpace(branch.CheckoutID) {
		return true
	}
	if strings.TrimSpace(branch.AgentID) != "" &&
		strings.TrimSpace(checkout.AgentID) != "" &&
		strings.TrimSpace(checkout.AgentID) != strings.TrimSpace(branch.AgentID) {
		return false
	}
	if strings.TrimSpace(branch.BranchName) != "" && strings.TrimSpace(checkout.BranchName) == strings.TrimSpace(branch.BranchName) {
		return true
	}
	if strings.TrimSpace(branch.HeadSHA) != "" && strings.TrimSpace(checkout.HeadSHA) == strings.TrimSpace(branch.HeadSHA) {
		return true
	}
	return strings.TrimSpace(branch.BaseSHA) != "" &&
		strings.TrimSpace(checkout.BaseSHA) == strings.TrimSpace(branch.BaseSHA) &&
		strings.TrimSpace(branch.BranchName) != ""
}

func projectPatchQueueSubmitWantsControlled(args map[string]any) bool {
	mode := strings.TrimSpace(stringArg(args, "repo_authority_mode"))
	return mode == "" || boolArg(args, "controlled_queue", false) || mode == "repoauthority_controlled_queue"
}

func projectPatchQueueSubmitLegacyPatchOnlyProposal(item ProjectPatchQueueItemRecord, branch ProjectBranchRecord) bool {
	return strings.TrimSpace(item.RepoAuthorityMode) == "patch_only_temp_repo" &&
		strings.EqualFold(strings.TrimSpace(item.State), "PROPOSED") &&
		strings.TrimSpace(item.BranchID) == strings.TrimSpace(branch.BranchID)
}

func projectPatchQueueSubmitControlledReplacementItemID(existing ProjectPatchQueueItemRecord, requestedItemID string) (string, error) {
	oldItemID := strings.TrimSpace(existing.ItemID)
	requestedItemID = strings.TrimSpace(requestedItemID)
	if requestedItemID != "" {
		if requestedItemID == oldItemID {
			return "", fmt.Errorf("project_patch_queue_submit controlled_queue replacement for legacy patch-only proposal %s/%s requires a fresh item_id", strings.TrimSpace(existing.QueueID), oldItemID)
		}
		return requestedItemID, nil
	}
	stem := sanitizeRefSegment(oldItemID)
	if stem == "" || stem == "x" {
		stem = sanitizeRefSegment(firstNonEmpty(strings.TrimSpace(existing.BranchID), "patchitem"))
	}
	if strings.HasSuffix(stem, "-controlled") {
		return stem + "-replacement", nil
	}
	return stem + "-controlled", nil
}

func projectPatchQueueSubmitGuidance(legacyPatchOnlyReplacement bool, oldItem ProjectPatchQueueItemRecord) string {
	if legacyPatchOnlyReplacement {
		return fmt.Sprintf("Legacy patch-only proposal %s/%s was canceled as historical evidence and replaced with a controlled_queue item. Integration still requires explicit operation/CAS/materialization/review gates.",
			strings.TrimSpace(oldItem.QueueID),
			strings.TrimSpace(oldItem.ItemID),
		)
	}
	return "Branch is now visible in the durable patch queue. Integration still requires explicit review/validation gates."
}

func projectPatchQueueSubmitExistingControlledReady(item ProjectPatchQueueItemRecord) bool {
	return strings.TrimSpace(item.RepoAuthorityMode) == "repoauthority_controlled_queue" &&
		strings.TrimSpace(item.TaskID) != "" &&
		strings.TrimSpace(item.SessionID) != "" &&
		strings.TrimSpace(item.RunID) != "" &&
		strings.TrimSpace(item.AgentID) != "" &&
		strings.TrimSpace(item.PrincipalType) != "" &&
		strings.TrimSpace(item.PrincipalID) != "" &&
		strings.TrimSpace(item.CapabilitySnapshotID) != "" &&
		strings.TrimSpace(item.CapabilitySnapshotSchema) != "" &&
		strings.TrimSpace(item.RepoRoot) != "" &&
		strings.TrimSpace(item.BaseTreeHash) != "" &&
		strings.TrimSpace(item.ContextDigest) != "" &&
		strings.TrimSpace(item.RepoLeaseID) != "" &&
		item.LeaseTerm > 0
}

func projectPatchQueueRequireControlledSubmitRefs(input ProjectPatchQueueSubmitInput) error {
	missing := []string{}
	for field, value := range map[string]string{
		"task_id":                    input.TaskID,
		"session_id":                 input.SessionID,
		"run_id":                     input.RunID,
		"agent_id":                   input.AgentID,
		"principal_type":             input.PrincipalType,
		"principal_id":               input.PrincipalID,
		"capability_snapshot_id":     input.CapabilitySnapshotID,
		"capability_snapshot_schema": input.CapabilitySnapshotSchema,
		"repo_root":                  input.RepoRoot,
		"base_tree_hash":             input.BaseTreeHash,
		"repo_lease_id":              input.RepoLeaseID,
	} {
		if strings.TrimSpace(value) == "" {
			missing = append(missing, field)
		}
	}
	if input.LeaseTerm <= 0 {
		missing = append(missing, "lease_term")
	}
	if len(missing) > 0 {
		return fmt.Errorf("controlled_queue requires complete binding refs: missing %s", strings.Join(missing, ", "))
	}
	return nil
}
