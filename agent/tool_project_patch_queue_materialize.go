package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

const (
	projectPatchQueueMaterializationSchema        = "repo_patch_materialization.v1"
	projectPatchQueueMaterializationEncodingUTF8  = "utf-8"
	projectPatchQueueMaterializationMaxFileBytes  = int64(1 << 20)
	projectPatchQueueMaterializationMaxTotalBytes = int64(4 << 20)
)

type ProjectPatchQueueMaterializeTool struct {
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	workdir        string
	runtimeBinding func() AgentRuntimeBinding
}

func NewProjectPatchQueueMaterializeTool(client *RhizomeClient, workspaceID, agentID, workdir string) *ProjectPatchQueueMaterializeTool {
	return &ProjectPatchQueueMaterializeTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
		workdir:     strings.TrimSpace(workdir),
	}
}

func (t *ProjectPatchQueueMaterializeTool) Name() string { return "project_patch_queue_materialize" }

func (t *ProjectPatchQueueMaterializeTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectPatchQueueMaterializeTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectPatchQueueMaterializeTool) Description() string {
	return "Record exact UTF-8 candidate file contents for a claimed, CAS-verified project patch queue item. It reads files from a checkout and records evidence only; it does not apply, merge, push, rebase, switch branches, or mutate git state."
}

func (t *ProjectPatchQueueMaterializeTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id": map[string]any{"type": "string", "description": "Project id."},
			"queue_id":   map[string]any{"type": "string", "description": "Patch queue id."},
			"item_id":    map[string]any{"type": "string", "description": "Patch queue item id."},
			"branch_id": map[string]any{
				"type":        "string",
				"description": "Optional branch id when queue_id/item_id are unknown.",
			},
			"claim_token": map[string]any{
				"type":        "string",
				"description": "Claim fence token returned by project_patch_queue_lifecycle claim. Defaults to the visible item claim_token when present.",
			},
			"local_path": map[string]any{
				"type":        "string",
				"description": "Optional local checkout path. Defaults to the item branch checkout path or agent workdir.",
			},
		},
		"required": []string{"project_id"},
	}
}

func (t *ProjectPatchQueueMaterializeTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_patch_queue_materialize is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	projectID, err := resolveProjectToolActiveProjectID("project_patch_queue_materialize", t.workspaceID, projectToolRefArg(args, "project_id"), t.runtimeBinding)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_materialize failed reading project coordination for %s: %v", projectID, err), IsError: true}
	}
	if !projectAgentHasPatchQueueEvidenceAuthority(coordination, t.agentID) {
		return &ToolResult{Output: "project_patch_queue_materialize requires this agent to hold an active REVIEWER/INTEGRATOR role or strategic lead lease. Reviewers can also record ACCEPT/BLOCK/REJECT with project_patch_queue_lifecycle without materialization when the task is only a lane review.", IsError: true}
	}
	item, err := selectProjectPatchQueueMaterializationItem(coordination.PatchQueueItems, stringArg(args, "queue_id"), stringArg(args, "item_id"), stringArg(args, "branch_id"), t.agentID)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	if !strings.EqualFold(strings.TrimSpace(item.State), "CLAIMED") || strings.TrimSpace(item.ClaimedBy) != t.agentID {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_materialize requires this agent to own an active CLAIMED item, got state=%s claimed_by=%s", item.State, item.ClaimedBy), IsError: true}
	}
	claimToken := firstNonEmpty(strings.TrimSpace(stringArg(args, "claim_token")), strings.TrimSpace(item.ClaimToken))
	if claimToken == "" {
		return &ToolResult{Output: "claim_token is required to record patch materialization evidence", IsError: true}
	}
	if !projectPatchQueueItemCASApplied(item) {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_materialize requires applied CAS evidence on item %s/%s before recording candidate bytes", item.QueueID, item.ItemID), IsError: true}
	}
	if strings.TrimSpace(item.MaterializationDigest) != "" || item.MaterializationAccepted {
		return projectPatchQueueMaterializeResult(projectPatchQueueMaterializeOutput{
			ProjectID:               projectID,
			Item:                    item,
			AlreadyMaterialized:     true,
			NoGitMutation:           true,
			MaterializedFileDigests: projectPatchQueueMaterializedFileDigestMap(item.Materialization.Files),
			Guidance:                "Patch materialization was already recorded. Continue with reviewer/operator gates instead of duplicating evidence.",
		})
	}

	task := WorkspaceTaskRecord{ProjectID: projectID}
	if t.runtimeBinding != nil {
		binding := t.runtimeBinding()
		task.TaskID = strings.TrimSpace(binding.TaskID)
		task.ProjectID = firstNonEmpty(strings.TrimSpace(binding.ProjectID), projectID)
	}
	localPath := firstNonEmpty(strings.TrimSpace(stringArg(args, "local_path")), projectPatchQueueMaterializationCheckoutPath(coordination, item, t.agentID, task), t.workdir)
	materializationPaths, err := projectPatchQueueMaterializationPaths(item)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_materialize cannot derive concrete materialization paths for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
	}
	if repoRoot, err := projectPatchQueueResolveRepoRoot(localPath); err == nil {
		if projectPatchQueueGitWorktreeAvailable(ctx, repoRoot) {
			if err := projectPatchQueueVerifyGitCheckoutReady(ctx, repoRoot, item, materializationPaths); err != nil {
				return &ToolResult{Output: fmt.Sprintf("project_patch_queue_materialize checkout identity check failed for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
			}
		} else if strings.TrimSpace(item.RepoAuthorityMode) == "repoauthority_controlled_queue" {
			return &ToolResult{Output: fmt.Sprintf("project_patch_queue_materialize requires a git worktree for controlled queue item %s/%s", item.QueueID, item.ItemID), IsError: true}
		}
	} else if strings.TrimSpace(item.RepoAuthorityMode) == "repoauthority_controlled_queue" {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_materialize cannot resolve controlled checkout for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
	}
	materialization, fileDigests, err := buildProjectPatchQueueMaterializationFromCheckout(localPath, projectID, item, materializationPaths)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_materialize failed building materialization for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
	}
	recorded, err := t.client.RecordProjectPatchQueueMaterialization(ctx, ProjectPatchQueueMaterializationRecordInput{
		WorkspaceID:     t.workspaceID,
		ProjectID:       projectID,
		ActorID:         t.agentID,
		QueueID:         item.QueueID,
		ItemID:          item.ItemID,
		Materialization: materialization,
		ClaimToken:      claimToken,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_materialize failed recording materialization for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
	}
	return projectPatchQueueMaterializeResult(projectPatchQueueMaterializeOutput{
		ProjectID:               projectID,
		Item:                    recorded,
		LocalPath:               localPath,
		AlreadyMaterialized:     false,
		NoGitMutation:           true,
		MaterializedFileDigests: fileDigests,
		Guidance:                "Candidate file contents are now recorded as durable patch materialization evidence. This did not apply, merge, push, rebase, switch branches, or mutate git state.",
	})
}

type projectPatchQueueMaterializeOutput struct {
	ProjectID               string
	Item                    ProjectPatchQueueItemRecord
	LocalPath               string
	AlreadyMaterialized     bool
	NoGitMutation           bool
	MaterializedFileDigests map[string]string
	Guidance                string
}

func projectPatchQueueMaterializeResult(output projectPatchQueueMaterializeOutput) *ToolResult {
	payload := map[string]any{
		"project_id":                  output.ProjectID,
		"repo_id":                     output.Item.RepoID,
		"branch_id":                   output.Item.BranchID,
		"queue_id":                    output.Item.QueueID,
		"item_id":                     output.Item.ItemID,
		"state":                       output.Item.State,
		"claimed_by":                  output.Item.ClaimedBy,
		"cas_patch_digest":            output.Item.CASPatchDigest,
		"cas_evaluation_digest":       output.Item.CASEvaluationDigest,
		"materialization_digest":      output.Item.MaterializationDigest,
		"materialized_file_count":     len(output.MaterializedFileDigests),
		"materialized_file_digests":   output.MaterializedFileDigests,
		"already_materialized":        output.AlreadyMaterialized,
		"no_git_mutation":             output.NoGitMutation,
		"materialization_guidance":    output.Guidance,
		"raw_candidate_content_shown": false,
	}
	if strings.TrimSpace(output.LocalPath) != "" {
		payload["local_path"] = strings.TrimSpace(output.LocalPath)
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_materialize completed for %s/%s", output.Item.QueueID, output.Item.ItemID)}
	}
	return &ToolResult{Output: string(raw)}
}

func selectProjectPatchQueueMaterializationItem(items []ProjectPatchQueueItemRecord, queueID, itemID, branchID, agentID string) (ProjectPatchQueueItemRecord, error) {
	queueID = strings.TrimSpace(queueID)
	itemID = strings.TrimSpace(itemID)
	branchID = strings.TrimSpace(branchID)
	agentID = strings.TrimSpace(agentID)
	if queueID == "" && itemID == "" && branchID == "" {
		return ProjectPatchQueueItemRecord{}, fmt.Errorf("queue_id/item_id or branch_id is required")
	}
	var matches []ProjectPatchQueueItemRecord
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
	if len(matches) == 0 {
		return ProjectPatchQueueItemRecord{}, fmt.Errorf("patch queue item not found in project coordination")
	}
	if queueID != "" && itemID != "" {
		if len(matches) == 1 {
			return matches[0], nil
		}
		return ProjectPatchQueueItemRecord{}, fmt.Errorf("patch queue selector is ambiguous; provide exact queue_id and item_id")
	}
	var active []ProjectPatchQueueItemRecord
	for _, item := range matches {
		if strings.EqualFold(strings.TrimSpace(item.State), "CLAIMED") &&
			strings.TrimSpace(item.ClaimedBy) == agentID &&
			projectPatchQueueItemCASApplied(item) {
			active = append(active, item)
		}
	}
	switch len(active) {
	case 1:
		return active[0], nil
	case 0:
		if len(matches) == 1 {
			return matches[0], nil
		}
		return ProjectPatchQueueItemRecord{}, fmt.Errorf("patch queue selector is ambiguous or has no claimed CAS-verified item for this agent; provide queue_id and item_id")
	default:
		return ProjectPatchQueueItemRecord{}, fmt.Errorf("patch queue selector is ambiguous; multiple claimed CAS-verified items match branch_id, provide queue_id and item_id")
	}
}

func projectPatchQueueItemCASApplied(item ProjectPatchQueueItemRecord) bool {
	if !item.CASEvidenceAccepted {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(item.CASStatus), "APPLIED") {
		return false
	}
	if strings.TrimSpace(item.CASPatchDigest) == "" || strings.TrimSpace(item.CASEvaluationDigest) == "" {
		return false
	}
	if len(item.CASResult.Paths) == 0 {
		return false
	}
	for _, p := range item.CASResult.Paths {
		if !strings.EqualFold(strings.TrimSpace(p.Status), "APPLIED") {
			return false
		}
		if strings.TrimSpace(p.Path) == "" || strings.TrimSpace(p.CandidateHash) == "" {
			return false
		}
	}
	return true
}

func projectPatchQueueMaterializationCheckoutPath(coordination ProjectCoordinationRecord, item ProjectPatchQueueItemRecord, agentID string, task WorkspaceTaskRecord) string {
	var selectedBranch ProjectBranchRecord
	hasBranch := false
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.BranchID) == strings.TrimSpace(item.BranchID) {
			selectedBranch = branch
			hasBranch = true
			if checkout, ok := selectProjectCompletionCheckout(coordination, branch, agentID, task); ok {
				return strings.TrimSpace(checkout.LocalPath)
			}
			if path := projectBranchReviewCheckoutPath(coordination.Checkouts, branch.CheckoutID); path != "" {
				return path
			}
			break
		}
	}
	claimAgentID := firstNonEmpty(strings.TrimSpace(item.ClaimedBy), strings.TrimSpace(agentID))
	if hasBranch && strings.TrimSpace(selectedBranch.BranchName) != "" {
		for _, checkout := range coordination.Checkouts {
			if managerProjectCheckoutStatusTerminal(checkout.Status) {
				continue
			}
			if strings.TrimSpace(checkout.RepoID) != strings.TrimSpace(item.RepoID) {
				continue
			}
			if strings.TrimSpace(checkout.AgentID) != claimAgentID {
				continue
			}
			if !strings.EqualFold(strings.TrimSpace(checkout.BranchName), strings.TrimSpace(selectedBranch.BranchName)) {
				continue
			}
			if strings.TrimSpace(checkout.LocalPath) != "" {
				return strings.TrimSpace(checkout.LocalPath)
			}
		}
	}
	for _, checkout := range coordination.Checkouts {
		if managerProjectCheckoutStatusTerminal(checkout.Status) {
			continue
		}
		if strings.TrimSpace(checkout.RepoID) == strings.TrimSpace(item.RepoID) &&
			strings.TrimSpace(checkout.AgentID) == claimAgentID &&
			strings.TrimSpace(checkout.LocalPath) != "" {
			return strings.TrimSpace(checkout.LocalPath)
		}
	}
	return ""
}

func projectPatchQueueMaterializationPaths(item ProjectPatchQueueItemRecord) ([]string, error) {
	pathset := projectPatchQueueFollowupPathset(item)
	if len(pathset) == 0 {
		return nil, fmt.Errorf("patch queue item pathset is empty")
	}
	paths := make([]string, 0, len(item.CASResult.Paths))
	seen := map[string]struct{}{}
	for _, casPath := range item.CASResult.Paths {
		if !strings.EqualFold(strings.TrimSpace(casPath.Status), "APPLIED") {
			continue
		}
		normalized, err := normalizeReviewPath(casPath.Path)
		if err != nil {
			return nil, fmt.Errorf("CAS path %q is invalid: %w", casPath.Path, err)
		}
		if !pathsetIsCoveredByScope([]string{normalized}, pathset) {
			return nil, fmt.Errorf("CAS path %s is not covered by patch queue pathset", normalized)
		}
		if _, ok := seen[normalized]; ok {
			return nil, fmt.Errorf("CAS path %s is duplicated", normalized)
		}
		seen[normalized] = struct{}{}
		paths = append(paths, normalized)
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("applied CAS evidence has no concrete paths")
	}
	return paths, nil
}

func buildProjectPatchQueueMaterializationFromCheckout(localPath, projectID string, item ProjectPatchQueueItemRecord, materializationPaths []string) (PatchMaterialization, map[string]string, error) {
	root, err := filepath.Abs(strings.TrimSpace(localPath))
	if err != nil || strings.TrimSpace(localPath) == "" {
		return PatchMaterialization{}, nil, fmt.Errorf("local_path is required and must be absolute/resolvable")
	}
	rootEval, err := filepath.EvalSymlinks(root)
	if err == nil {
		root = rootEval
	}
	casPaths := projectPatchQueueCASPathMap(item)
	files := make([]PatchMaterializedFile, 0, len(materializationPaths))
	fileDigests := make(map[string]string, len(materializationPaths))
	var totalBytes int64
	for _, path := range materializationPaths {
		normalized, err := normalizeReviewPath(path)
		if err != nil {
			return PatchMaterialization{}, nil, fmt.Errorf("pathset path %q is invalid: %w", path, err)
		}
		casPath, ok := casPaths[normalized]
		if !ok {
			return PatchMaterialization{}, nil, fmt.Errorf("path %s is missing from applied CAS evidence", normalized)
		}
		content, size, err := readProjectPatchQueueMaterializedFile(root, normalized)
		if err != nil {
			return PatchMaterialization{}, nil, err
		}
		totalBytes += size
		if totalBytes > projectPatchQueueMaterializationMaxTotalBytes {
			return PatchMaterialization{}, nil, fmt.Errorf("materialization exceeds total size limit %d bytes", projectPatchQueueMaterializationMaxTotalBytes)
		}
		digest := projectPatchQueueMaterializationContentDigest(content)
		if candidateHash := strings.TrimSpace(casPath.CandidateHash); candidateHash != "" && candidateHash != digest {
			return PatchMaterialization{}, nil, fmt.Errorf("path %s content digest %s does not match CAS candidate_hash %s", normalized, digest, candidateHash)
		}
		fileDigests[normalized] = digest
		files = append(files, PatchMaterializedFile{
			Path:            normalized,
			ChangeKind:      strings.TrimSpace(casPath.ChangeKind),
			BaseHash:        strings.TrimSpace(casPath.BaseHash),
			CandidateHash:   strings.TrimSpace(casPath.CandidateHash),
			ContentEncoding: projectPatchQueueMaterializationEncodingUTF8,
			Content:         content,
			ContentDigest:   digest,
		})
	}
	return PatchMaterialization{
		Schema:              projectPatchQueueMaterializationSchema,
		WorkspaceID:         item.WorkspaceID,
		ProjectID:           projectID,
		QueueID:             item.QueueID,
		ItemID:              item.ItemID,
		OperationID:         item.OperationID,
		OperationKind:       item.OperationKind,
		CASPatchDigest:      item.CASPatchDigest,
		CASEvaluationDigest: item.CASEvaluationDigest,
		Files:               files,
		RecordedBy:          item.ClaimedBy,
	}, fileDigests, nil
}

func readProjectPatchQueueMaterializedFile(root, normalizedPath string) (string, int64, error) {
	full := filepath.Join(root, filepath.FromSlash(normalizedPath))
	cleanFull := filepath.Clean(full)
	if err := requirePathInsideRoot(root, cleanFull); err != nil {
		return "", 0, err
	}
	evalFull, err := filepath.EvalSymlinks(cleanFull)
	if err == nil {
		if err := requirePathInsideRoot(root, evalFull); err != nil {
			return "", 0, err
		}
		cleanFull = evalFull
	}
	info, err := os.Stat(cleanFull)
	if err != nil {
		return "", 0, fmt.Errorf("read %s: %w", normalizedPath, err)
	}
	if info.IsDir() {
		return "", 0, fmt.Errorf("path %s is a directory, not a materializable file", normalizedPath)
	}
	if info.Size() > projectPatchQueueMaterializationMaxFileBytes {
		return "", 0, fmt.Errorf("path %s exceeds file size limit %d bytes", normalizedPath, projectPatchQueueMaterializationMaxFileBytes)
	}
	data, err := os.ReadFile(cleanFull)
	if err != nil {
		return "", 0, fmt.Errorf("read %s: %w", normalizedPath, err)
	}
	if !utf8.Valid(data) {
		return "", 0, fmt.Errorf("path %s is not valid UTF-8", normalizedPath)
	}
	return string(data), int64(len(data)), nil
}

func requirePathInsideRoot(root, full string) error {
	rel, err := filepath.Rel(root, full)
	if err != nil {
		return fmt.Errorf("cannot resolve materialized path under checkout: %w", err)
	}
	if rel == "." || rel == ".." || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("materialized path %s escapes checkout root %s", full, root)
	}
	return nil
}

func projectPatchQueueCASPathMap(item ProjectPatchQueueItemRecord) map[string]CASPatchPathResult {
	out := make(map[string]CASPatchPathResult, len(item.CASResult.Paths))
	for _, p := range item.CASResult.Paths {
		normalized, err := normalizeReviewPath(p.Path)
		if err != nil {
			continue
		}
		out[normalized] = p
	}
	return out
}

func projectPatchQueueMaterializedFileDigestMap(files []PatchMaterializedFile) map[string]string {
	out := make(map[string]string, len(files))
	for _, file := range files {
		path := strings.TrimSpace(file.Path)
		digest := strings.TrimSpace(file.ContentDigest)
		if path != "" && digest != "" {
			out[path] = digest
		}
	}
	return out
}

func projectPatchQueueMaterializationContentDigest(content string) string {
	sum := sha256.Sum256([]byte(content))
	return "sha256:" + hex.EncodeToString(sum[:])
}
