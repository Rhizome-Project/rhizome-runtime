package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const (
	projectPatchQueueCASApplySchema          = "repo_cas_patch_apply.v1"
	projectPatchQueueCASStatusApplied        = "applied"
	projectPatchQueueCASChangeAdd            = "add"
	projectPatchQueueCASChangeModify         = "modify"
	projectPatchQueueTestEvidenceSchema      = "repo_patch_queue_test_evidence.v1"
	projectPatchQueueTestStatusPassed        = "passed"
	projectPatchQueueOperationKindPatchApply = "repo_patch_apply"
)

type ProjectPatchQueueCASTool struct {
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	workdir        string
	runtimeBinding func() AgentRuntimeBinding
}

func NewProjectPatchQueueCASTool(client *RhizomeClient, workspaceID, agentID, workdir string) *ProjectPatchQueueCASTool {
	return &ProjectPatchQueueCASTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
		workdir:     strings.TrimSpace(workdir),
	}
}

func (t *ProjectPatchQueueCASTool) Name() string { return "project_patch_queue_cas_record" }

func (t *ProjectPatchQueueCASTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectPatchQueueCASTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectPatchQueueCASTool) Description() string {
	return "Bind a claimed controlled patch queue item to a mutation operation and record CAS/test evidence from an immutable git head. It records evidence only; it does not apply, merge, push, rebase, switch branches, or mutate git state."
}

func (t *ProjectPatchQueueCASTool) Parameters() map[string]any {
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
			"candidate_paths": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Optional concrete candidate paths for scoped pathsets. If omitted, the tool derives them from git diff base_sha..head_sha.",
			},
			"operation_id": map[string]any{
				"type":        "string",
				"description": "Optional mutation operation id. Omit by default so Rhizome creates the operation ledger.",
			},
			"operation_kind": map[string]any{
				"type":        "string",
				"description": "Optional mutation operation kind. Defaults to repo_patch_apply.",
			},
			"test_name": map[string]any{
				"type":        "string",
				"description": "Name of the verification already performed for this candidate.",
			},
			"test_command": map[string]any{
				"type":        "string",
				"description": "Verification command already performed for this candidate.",
			},
			"test_status": map[string]any{
				"type":        "string",
				"description": "Verification status. Must be passed for merge admission.",
			},
			"test_exit_code": map[string]any{
				"type":        "integer",
				"description": "Verification exit code. Must be 0 for merge admission.",
			},
			"test_output_digest": map[string]any{
				"type":        "string",
				"description": "Optional sha256 output digest. Defaults to digest of test_output_summary.",
			},
			"test_output_summary": map[string]any{
				"type":        "string",
				"description": "Short verification output summary. Raw long output should stay out of tool output.",
			},
			"test_duration_ms": map[string]any{
				"type":        "integer",
				"description": "Optional verification duration in milliseconds.",
			},
		},
		"required": []string{"project_id"},
	}
}

func (t *ProjectPatchQueueCASTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_patch_queue_cas_record is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	projectID, err := resolveProjectToolActiveProjectID("project_patch_queue_cas_record", t.workspaceID, projectToolRefArg(args, "project_id"), t.runtimeBinding)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_cas_record failed reading project coordination for %s: %v", projectID, err), IsError: true}
	}
	if !projectAgentHasPatchQueueEvidenceAuthority(coordination, t.agentID) {
		return &ToolResult{Output: "project_patch_queue_cas_record requires this agent to hold an active REVIEWER/INTEGRATOR role or strategic lead lease. Reviewers can also record ACCEPT/BLOCK/REJECT with project_patch_queue_lifecycle without CAS when the task is only a lane review.", IsError: true}
	}
	item, err := selectProjectPatchQueueCASItem(coordination.PatchQueueItems, stringArg(args, "queue_id"), stringArg(args, "item_id"), stringArg(args, "branch_id"), t.agentID)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	if !strings.EqualFold(strings.TrimSpace(item.State), "CLAIMED") || strings.TrimSpace(item.ClaimedBy) != t.agentID {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_cas_record requires this agent to own an active CLAIMED item, got state=%s claimed_by=%s", item.State, item.ClaimedBy), IsError: true}
	}
	if strings.TrimSpace(item.RepoAuthorityMode) != "repoauthority_controlled_queue" {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_cas_record requires repoauthority_controlled_queue item, got %s", item.RepoAuthorityMode), IsError: true}
	}
	claimToken := firstNonEmpty(strings.TrimSpace(stringArg(args, "claim_token")), strings.TrimSpace(item.ClaimToken))
	if claimToken == "" {
		return &ToolResult{Output: "claim_token is required to record CAS evidence", IsError: true}
	}
	if projectPatchQueueItemCASApplied(item) {
		return projectPatchQueueCASResult(projectPatchQueueCASOutput{
			ProjectID:        projectID,
			Item:             item,
			AlreadyRecorded:  true,
			NoGitMutation:    true,
			CandidateDigests: projectPatchQueueCASCandidateDigestMap(item.CASResult),
			Guidance:         "Applied CAS evidence was already recorded. Continue with materialization/reviewer/operator gates instead of duplicating evidence.",
		})
	}
	if projectPatchQueueCASEvidencePresentButNotApplied(item) {
		return &ToolResult{Output: fmt.Sprintf("patch queue item %s/%s already has non-applied or partial CAS evidence; create revision/follow-up instead of overwriting evidence", item.QueueID, item.ItemID), IsError: true}
	}
	task := WorkspaceTaskRecord{ProjectID: projectID}
	if t.runtimeBinding != nil {
		binding := t.runtimeBinding()
		task.TaskID = strings.TrimSpace(binding.TaskID)
		task.ProjectID = firstNonEmpty(strings.TrimSpace(binding.ProjectID), projectID)
	}
	localPath := firstNonEmpty(strings.TrimSpace(stringArg(args, "local_path")), projectPatchQueueMaterializationCheckoutPath(coordination, item, t.agentID, task), t.workdir)
	repoRoot, err := projectPatchQueueResolveRepoRoot(localPath)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_cas_record cannot resolve checkout for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
	}
	pathset := projectPatchQueueFollowupPathset(item)
	if len(pathset) == 0 {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_cas_record cannot derive pathset for %s/%s", item.QueueID, item.ItemID), IsError: true}
	}
	candidatePaths, err := projectPatchQueueConcreteCandidatePaths(ctx, repoRoot, item.BaseSHA, item.HeadSHA, pathset, stringSliceArg(args, "candidate_paths"))
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_cas_record cannot derive concrete candidate paths for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
	}
	if err := projectPatchQueueVerifyGitCheckoutReady(ctx, repoRoot, item, candidatePaths); err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_cas_record checkout identity check failed for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
	}
	if !item.OperationBindingAccepted || strings.TrimSpace(item.OperationID) == "" {
		bound, err := t.client.BindProjectPatchQueueMutationOperation(ctx, ProjectPatchQueueOperationBindInput{
			WorkspaceID:       t.workspaceID,
			ProjectID:         projectID,
			ActorID:           t.agentID,
			QueueID:           item.QueueID,
			ItemID:            item.ItemID,
			OperationID:       strings.TrimSpace(stringArg(args, "operation_id")),
			OperationKind:     strings.TrimSpace(stringArg(args, "operation_kind")),
			MutationPathsJSON: strings.TrimSpace(item.PathsetJSON),
			ClaimToken:        claimToken,
		})
		if err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_patch_queue_cas_record failed binding operation for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
		}
		item = bound
	}
	if err := projectPatchQueueCASOperationReady(item); err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_cas_record cannot build CAS evidence for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
	}
	casResult, candidateDigests, err := buildProjectPatchQueueCASEvidenceFromGitHead(ctx, repoRoot, item, candidatePaths)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_cas_record failed building CAS evidence for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
	}
	testEvidence, err := projectPatchQueueCASTestEvidence(args)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_cas_record verification evidence is invalid: %v", err), IsError: true}
	}
	recorded, err := t.client.RecordProjectPatchQueueCASEvidence(ctx, ProjectPatchQueueCASRecordInput{
		WorkspaceID:  t.workspaceID,
		ProjectID:    projectID,
		ActorID:      t.agentID,
		QueueID:      item.QueueID,
		ItemID:       item.ItemID,
		CASResult:    casResult,
		TestEvidence: testEvidence,
		ClaimToken:   claimToken,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_cas_record failed recording CAS evidence for %s/%s: %v", item.QueueID, item.ItemID, err), IsError: true}
	}
	return projectPatchQueueCASResult(projectPatchQueueCASOutput{
		ProjectID:        projectID,
		Item:             recorded,
		LocalPath:        repoRoot,
		AlreadyRecorded:  false,
		NoGitMutation:    true,
		CandidateDigests: candidateDigests,
		Guidance:         "Operation binding and applied CAS/test evidence are now durable. This did not apply, merge, push, rebase, switch branches, or mutate git state.",
	})
}

type projectPatchQueueCASOutput struct {
	ProjectID        string
	Item             ProjectPatchQueueItemRecord
	LocalPath        string
	AlreadyRecorded  bool
	NoGitMutation    bool
	CandidateDigests map[string]string
	Guidance         string
}

func projectPatchQueueCASResult(output projectPatchQueueCASOutput) *ToolResult {
	payload := map[string]any{
		"project_id":                  output.ProjectID,
		"repo_id":                     output.Item.RepoID,
		"branch_id":                   output.Item.BranchID,
		"queue_id":                    output.Item.QueueID,
		"item_id":                     output.Item.ItemID,
		"state":                       output.Item.State,
		"claimed_by":                  output.Item.ClaimedBy,
		"operation_id":                output.Item.OperationID,
		"operation_kind":              output.Item.OperationKind,
		"operation_bound_by":          output.Item.OperationBoundBy,
		"cas_status":                  output.Item.CASStatus,
		"cas_patch_digest":            output.Item.CASPatchDigest,
		"cas_evaluation_digest":       output.Item.CASEvaluationDigest,
		"candidate_file_digests":      output.CandidateDigests,
		"already_recorded":            output.AlreadyRecorded,
		"no_git_mutation":             output.NoGitMutation,
		"cas_guidance":                output.Guidance,
		"raw_candidate_content_shown": false,
	}
	if strings.TrimSpace(output.LocalPath) != "" {
		payload["local_path"] = strings.TrimSpace(output.LocalPath)
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_patch_queue_cas_record completed for %s/%s", output.Item.QueueID, output.Item.ItemID)}
	}
	return &ToolResult{Output: string(raw)}
}

func selectProjectPatchQueueCASItem(items []ProjectPatchQueueItemRecord, queueID, itemID, branchID, agentID string) (ProjectPatchQueueItemRecord, error) {
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
		if strings.EqualFold(strings.TrimSpace(item.State), "CLAIMED") && strings.TrimSpace(item.ClaimedBy) == agentID {
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
		return ProjectPatchQueueItemRecord{}, fmt.Errorf("patch queue selector is ambiguous or has no claimed item for this agent; provide queue_id and item_id")
	default:
		return ProjectPatchQueueItemRecord{}, fmt.Errorf("patch queue selector is ambiguous; multiple claimed items match branch_id, provide queue_id and item_id")
	}
}

func projectPatchQueueCASEvidencePresentButNotApplied(item ProjectPatchQueueItemRecord) bool {
	return item.CASEvidenceAccepted ||
		strings.TrimSpace(item.CASEvidenceSchema) != "" ||
		strings.TrimSpace(item.CASStatus) != "" ||
		strings.TrimSpace(item.CASPatchDigest) != "" ||
		strings.TrimSpace(item.CASEvaluationDigest) != "" ||
		strings.TrimSpace(item.CASResultJSON) != "" ||
		len(item.CASResult.Paths) > 0 ||
		len(item.CASResult.Issues) > 0 ||
		strings.TrimSpace(item.CASTestEvidenceJSON) != "" ||
		strings.TrimSpace(item.CASTestEvidenceDigest) != ""
}

func projectPatchQueueCASOperationReady(item ProjectPatchQueueItemRecord) error {
	for field, value := range map[string]string{
		"operation_id":                   item.OperationID,
		"operation_kind":                 firstNonEmpty(item.OperationKind, projectPatchQueueOperationKindPatchApply),
		"operation_context_digest":       item.OperationContextDigest,
		"operation_lease_context_digest": item.OperationLeaseContextDigest,
		"operation_mutation_paths_json":  item.OperationMutationPathsJSON,
		"repo_lease_id":                  item.RepoLeaseID,
		"context_digest":                 item.ContextDigest,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required after operation binding", field)
		}
	}
	if !item.OperationBindingAccepted {
		return fmt.Errorf("operation binding must be accepted")
	}
	return nil
}

func buildProjectPatchQueueCASEvidenceFromGitHead(ctx context.Context, repoRoot string, item ProjectPatchQueueItemRecord, candidatePaths []string) (CASPatchApplyResult, map[string]string, error) {
	paths := make([]CASPatchPathResult, 0, len(candidatePaths))
	candidateDigests := make(map[string]string, len(candidatePaths))
	for _, candidatePath := range candidatePaths {
		normalized, err := normalizeReviewPath(candidatePath)
		if err != nil {
			return CASPatchApplyResult{}, nil, err
		}
		baseHash := strings.TrimSpace(item.BaseFileHashes[normalized])
		candidateHash, err := projectPatchQueueGitObjectContentDigest(ctx, repoRoot, item.HeadSHA, normalized)
		if err != nil {
			return CASPatchApplyResult{}, nil, fmt.Errorf("candidate head hash for %s: %w", normalized, err)
		}
		pathResult := CASPatchPathResult{
			Path:          normalized,
			Status:        projectPatchQueueCASStatusApplied,
			CandidateHash: candidateHash,
		}
		if baseHash == "" {
			baseExists, err := projectPatchQueueGitObjectExists(ctx, repoRoot, item.BaseSHA, normalized)
			if err != nil {
				return CASPatchApplyResult{}, nil, fmt.Errorf("base path probe for %s: %w", normalized, err)
			}
			if baseExists {
				return CASPatchApplyResult{}, nil, fmt.Errorf("base_file_hashes[%s] is missing on controlled queue item for an existing base file; resubmit with complete base hashes", normalized)
			}
			pathResult.ChangeKind = projectPatchQueueCASChangeAdd
		} else {
			pathResult.BaseHash = baseHash
			pathResult.CurrentHash = baseHash
		}
		candidateDigests[normalized] = candidateHash
		paths = append(paths, pathResult)
	}
	sort.Slice(paths, func(i, j int) bool { return paths[i].Path < paths[j].Path })
	return CASPatchApplyResult{
		Schema:        projectPatchQueueCASApplySchema,
		Status:        projectPatchQueueCASStatusApplied,
		PatchID:       strings.TrimSpace(item.OperationID),
		PatchDigest:   projectPatchQueueCASPatchDigest(candidateDigests),
		ContextDigest: strings.TrimSpace(item.OperationContextDigest),
		Paths:         paths,
	}, candidateDigests, nil
}

func projectPatchQueueCASPatchDigest(candidateHashes map[string]string) string {
	paths := make([]string, 0, len(candidateHashes))
	for path := range candidateHashes {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	var b strings.Builder
	b.WriteString(projectPatchQueueCASApplySchema)
	for _, path := range paths {
		b.WriteByte(0)
		b.WriteString(path)
		b.WriteByte(0)
		b.WriteString(strings.TrimSpace(candidateHashes[path]))
	}
	return "sha256:" + projectPatchQueueHexDigest(b.String())
}

func projectPatchQueueCASTestEvidence(args map[string]any) (PatchQueueTestEvidence, error) {
	name := firstNonEmpty(strings.TrimSpace(stringArg(args, "test_name")), "patch queue CAS validation")
	command := strings.TrimSpace(stringArg(args, "test_command"))
	if command == "" {
		return PatchQueueTestEvidence{}, fmt.Errorf("test_command is required")
	}
	status := firstNonEmpty(strings.TrimSpace(stringArg(args, "test_status")), projectPatchQueueTestStatusPassed)
	if status != projectPatchQueueTestStatusPassed {
		return PatchQueueTestEvidence{}, fmt.Errorf("test_status must be %s", projectPatchQueueTestStatusPassed)
	}
	exitCode := intArg(args["test_exit_code"], 0)
	if exitCode != 0 {
		return PatchQueueTestEvidence{}, fmt.Errorf("test_exit_code must be 0")
	}
	outputSummary := firstNonEmpty(strings.TrimSpace(stringArg(args, "test_output_summary")), "passed")
	outputDigest := firstNonEmpty(strings.TrimSpace(stringArg(args, "test_output_digest")), "sha256:"+projectPatchQueueHexDigest(outputSummary))
	digestHex := strings.TrimPrefix(outputDigest, "sha256:")
	if !strings.HasPrefix(outputDigest, "sha256:") || len(digestHex) != 64 {
		return PatchQueueTestEvidence{}, fmt.Errorf("test_output_digest must be canonical sha256")
	}
	if _, err := hex.DecodeString(digestHex); err != nil {
		return PatchQueueTestEvidence{}, fmt.Errorf("test_output_digest must be canonical sha256")
	}
	return PatchQueueTestEvidence{
		Schema:         projectPatchQueueTestEvidenceSchema,
		Name:           name,
		Command:        command,
		Status:         status,
		ExitCode:       exitCode,
		OutputDigest:   outputDigest,
		OutputSummary:  outputSummary,
		DurationMillis: int64(intArg(args["test_duration_ms"], 0)),
	}, nil
}

func projectPatchQueueHexDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func projectPatchQueueCASCandidateDigestMap(result CASPatchApplyResult) map[string]string {
	out := make(map[string]string, len(result.Paths))
	for _, pathResult := range result.Paths {
		if strings.TrimSpace(pathResult.Path) != "" && strings.TrimSpace(pathResult.CandidateHash) != "" {
			out[strings.TrimSpace(pathResult.Path)] = strings.TrimSpace(pathResult.CandidateHash)
		}
	}
	return out
}
