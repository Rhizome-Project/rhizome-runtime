package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

func (r *Runtime) enforceProjectImplementationCompletionEvidenceGate(ctx context.Context, task WorkspaceTaskRecord, result StructuredTaskResult) (StructuredTaskResult, bool, error) {
	if normalizeOutcome(result.Outcome) != "completed" || !runtimeProjectCompletionEvidenceGateRequired(task) {
		return result, false, nil
	}
	if runtimeTrustFirst(r.cfg) {
		return r.deferProjectImplementationCompletionUntilGitEvidenceCurrent(ctx, task, result)
	}
	return r.deferProjectImplementationCompletionUntilGitEvidenceCurrent(ctx, task, result)
}

func (r *Runtime) enforcePostReceiptTerminalCompletionEvidenceGate(ctx context.Context, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string, result StructuredTaskResult, trace *TaskRunTrace, workPacket *AgentWorkPacket) (StructuredTaskResult, error) {
	if normalizeOutcome(result.Outcome) != "completed" {
		return result, nil
	}
	gated, _, err := r.enforceCompletionCoordinationGate(ctx, task, session, runID, result, trace, workPacket)
	if err != nil || normalizeOutcome(gated.Outcome) != "completed" {
		return gated, err
	}
	gated, _, err = r.enforceProjectPatchQueueIntegrationAssemblyGate(task, gated, trace)
	if err != nil || normalizeOutcome(gated.Outcome) != "completed" {
		return gated, err
	}
	gated, _, err = r.enforceProjectImplementationCompletionEvidenceGate(ctx, task, gated)
	return gated, err
}

func (r *Runtime) enforceProjectPatchQueueIntegrationAssemblyGate(task WorkspaceTaskRecord, result StructuredTaskResult, trace *TaskRunTrace) (StructuredTaskResult, bool, error) {
	if normalizeOutcome(result.Outcome) != "completed" || !runtimeProjectTaskLooksPatchQueueIntegrationConvergence(task) {
		return result, false, nil
	}
	if projectPatchQueueIntegrationAssemblyEvidenceComplete(result, trace) {
		return result, false, nil
	}
	return demoteProjectPatchQueueIntegrationCompletion(result), true, nil
}

func (r *Runtime) enforceProjectImplementationSelfActionableGitEvidenceBlock(ctx context.Context, task WorkspaceTaskRecord, result StructuredTaskResult) (StructuredTaskResult, bool, error) {
	if normalizeOutcome(result.Outcome) != "blocked" || !runtimeProjectCompletionEvidenceGateRequired(task) || !projectImplementationGitEvidenceBlockLooksSelfActionable(result) {
		return result, false, nil
	}
	if runtimeTrustFirst(r.cfg) {
		if runtimeProjectLaneRequiresImplementationGate(task.ProjectLane) {
			return demoteProjectImplementationCompletion(result, "self-owned project implementation git publication is incomplete; trust_first does not make code ownership/review evidence optional"), true, nil
		}
		decision, ok, err := r.currentProjectCompletionNegativePatchQueueDecision(ctx, task)
		if ok {
			return demoteProjectImplementationCompletionForPatchQueueDecision(result, decision), true, nil
		}
		if err != nil {
			return trustFirstProjectImplementationGitEvidenceBlockAdvisory(result, fmt.Sprintf("negative patch queue check unavailable: %v", err)), true, nil
		}
		return trustFirstProjectImplementationGitEvidenceBlockAdvisory(result, ""), true, nil
	}
	return r.deferProjectImplementationCompletionUntilGitEvidenceCurrent(ctx, task, result)
}

func (r *Runtime) enforceProjectImplementationRecoverableFailureGate(ctx context.Context, task WorkspaceTaskRecord, result StructuredTaskResult) (StructuredTaskResult, bool, error) {
	if normalizeOutcome(result.Outcome) != "failed" || !runtimeProjectCompletionEvidenceGateRequired(task) {
		return result, false, nil
	}
	detail, ok, err := r.recoverableProjectImplementationWorkDetail(ctx, task)
	if err != nil {
		return result, false, err
	}
	if !ok {
		return result, false, nil
	}
	return recoverableProjectImplementationFailure(result, detail), true, nil
}

func (r *Runtime) recoverableProjectImplementationWorkDetail(ctx context.Context, task WorkspaceTaskRecord) (string, bool, error) {
	if r == nil || r.client == nil || strings.TrimSpace(task.ProjectID) == "" {
		return "", false, nil
	}
	coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, task.ProjectID)
	if err != nil {
		return "", false, fmt.Errorf("project implementation failure recovery evidence cannot be verified: %w", err)
	}
	branch, ok := selectProjectCompletionBranch(coordination, r.cfg.AgentID, task)
	if !ok {
		return "", false, nil
	}
	checkout, ok := selectProjectCompletionCheckout(coordination, branch, r.cfg.AgentID, task)
	if !ok || strings.TrimSpace(checkout.LocalPath) == "" {
		return "", false, nil
	}
	localPath := strings.TrimSpace(checkout.LocalPath)
	if strings.TrimSpace(r.cfg.Workdir) != "" {
		if err := ensureProjectCheckoutMaterializePath(r.cfg.Workdir, localPath); err != nil {
			return "", false, nil
		}
	}
	if !isGitCheckout(ctx, localPath) {
		return "", false, nil
	}
	dirty, err := gitWorktreeDirty(ctx, localPath)
	if err != nil {
		return "", false, fmt.Errorf("project checkout %s dirty-state check failed during failure recovery: %w", localPath, err)
	}
	if dirty {
		return fmt.Sprintf("recoverable_project_work: checkout %s has uncommitted product changes on branch %s (%s)", localPath, firstNonEmpty(branch.BranchID, "<unknown-branch>"), firstNonEmpty(branch.Status, "UNKNOWN")), true, nil
	}
	head, err := gitRevParse(ctx, localPath, "HEAD")
	if err != nil || strings.TrimSpace(head) == "" {
		return "", false, nil
	}
	head = strings.TrimSpace(head)
	branchHead := strings.TrimSpace(branch.HeadSHA)
	branchBase := strings.TrimSpace(branch.BaseSHA)
	if branchHead != "" && !strings.EqualFold(strings.TrimSpace(branch.Status), "READY_FOR_REVIEW") {
		if branchBase == "" || branchHead != branchBase {
			return fmt.Sprintf("recoverable_project_work: branch %s has committed head %s with status %s but is not READY_FOR_REVIEW", firstNonEmpty(branch.BranchID, "<unknown-branch>"), branchHead, firstNonEmpty(branch.Status, "UNKNOWN")), true, nil
		}
	}
	if branchHead == "" && branchBase != "" && head != branchBase {
		return fmt.Sprintf("recoverable_project_work: checkout %s has local committed head %s beyond registered base %s without branch publication evidence", localPath, head, branchBase), true, nil
	}
	if detail, ok := r.recoverableProjectImplementationRealityCheckDetail(ctx, task); ok {
		return detail, true, nil
	}
	return "", false, nil
}

func (r *Runtime) recoverableProjectImplementationRealityCheckDetail(ctx context.Context, task WorkspaceTaskRecord) (string, bool) {
	if r == nil || r.client == nil || strings.TrimSpace(task.TaskID) == "" {
		return "", false
	}
	bundle := r.taskHydration(ctx, &task)
	if bundle == nil {
		return "", false
	}
	var selected WorkspaceDocRecord
	found := false
	for _, doc := range bundle.Docs {
		key := strings.ToLower(strings.TrimSpace(doc.DocKey))
		if !(strings.HasSuffix(key, ".artifact_reality_check") ||
			strings.HasSuffix(key, ".candidate_provenance") ||
			strings.HasSuffix(key, ".provenance_review_status") ||
			strings.HasSuffix(key, ".review_ready_status")) {
			continue
		}
		text := strings.Join([]string{doc.DocKey, doc.Title, doc.Content}, "\n")
		if !projectImplementationRealityCheckTextLooksSelfActionable(text) {
			continue
		}
		if !found || strings.TrimSpace(doc.UpdatedAt) > strings.TrimSpace(selected.UpdatedAt) {
			selected = doc
			found = true
		}
	}
	if !found {
		return "", false
	}
	content := strings.TrimSpace(selected.Content)
	if len(content) > 360 {
		content = strings.TrimSpace(content[:360]) + "..."
	}
	return fmt.Sprintf("recoverable_project_work: task evidence doc %s says the owned implementation lane is not review-ready and needs repair: %s", strings.TrimSpace(selected.DocKey), content), true
}

func (r *Runtime) deferProjectImplementationCompletionUntilGitEvidenceCurrent(ctx context.Context, task WorkspaceTaskRecord, result StructuredTaskResult) (StructuredTaskResult, bool, error) {
	if r == nil || r.client == nil {
		return demoteProjectImplementationCompletion(result, "project implementation completion evidence cannot be verified because runtime client is unavailable"), true, nil
	}
	coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, task.ProjectID)
	if err != nil {
		return demoteProjectImplementationCompletion(result, fmt.Sprintf("project implementation completion evidence cannot be verified: %v", err)), true, nil
	}
	branch, ok := selectProjectCompletionBranch(coordination, r.cfg.AgentID, task)
	if !ok {
		return demoteProjectImplementationCompletion(result, "project implementation completion requires an owned branch bound to the active task before terminal completion"), true, nil
	}
	if !strings.EqualFold(strings.TrimSpace(branch.Status), "READY_FOR_REVIEW") {
		return demoteProjectImplementationCompletion(result, fmt.Sprintf("project branch %s is %s, not READY_FOR_REVIEW; run project_branch_commit if needed, then project_branch_review_ready with current verification evidence", branch.BranchID, firstNonEmpty(branch.Status, "UNKNOWN"))), true, nil
	}
	if strings.TrimSpace(branch.HeadSHA) == "" {
		return demoteProjectImplementationCompletion(result, fmt.Sprintf("project branch %s is READY_FOR_REVIEW without head_sha evidence", branch.BranchID)), true, nil
	}
	checkout, ok := selectProjectCompletionCheckout(coordination, branch, r.cfg.AgentID, task)
	if !ok || strings.TrimSpace(checkout.LocalPath) == "" {
		return demoteProjectImplementationCompletion(result, fmt.Sprintf("project branch %s has no registered checkout path for final cleanliness verification", branch.BranchID)), true, nil
	}
	localPath := strings.TrimSpace(checkout.LocalPath)
	if strings.TrimSpace(r.cfg.Workdir) != "" {
		if err := ensureProjectCheckoutMaterializePath(r.cfg.Workdir, localPath); err != nil {
			return demoteProjectImplementationCompletion(result, fmt.Sprintf("project checkout path is not verifiable from this agent workdir: %v", err)), true, nil
		}
	}
	if !isGitCheckout(ctx, localPath) {
		return demoteProjectImplementationCompletion(result, fmt.Sprintf("project checkout %s is not a readable git checkout", localPath)), true, nil
	}
	dirty, err := gitWorktreeDirty(ctx, localPath)
	if err != nil {
		return demoteProjectImplementationCompletion(result, fmt.Sprintf("project checkout %s dirty-state check failed: %v", localPath, err)), true, nil
	}
	if dirty {
		return demoteProjectImplementationCompletion(result, fmt.Sprintf("project checkout %s has uncommitted changes; commit them with project_branch_commit and publish a fresh project_branch_review_ready packet before completing", localPath)), true, nil
	}
	head, err := gitRevParse(ctx, localPath, "HEAD")
	if err != nil || strings.TrimSpace(head) == "" {
		return demoteProjectImplementationCompletion(result, fmt.Sprintf("project checkout %s HEAD cannot be read: %v", localPath, err)), true, nil
	}
	if strings.TrimSpace(head) != strings.TrimSpace(branch.HeadSHA) {
		return demoteProjectImplementationCompletion(result, fmt.Sprintf("project checkout HEAD %s does not match READY_FOR_REVIEW branch evidence %s; republish project_branch_review_ready after committing the latest revision", strings.TrimSpace(head), strings.TrimSpace(branch.HeadSHA))), true, nil
	}
	repo, ok := selectProjectBranchReviewRepo(coordination.Repositories, branch.RepoID)
	if !ok {
		return demoteProjectImplementationCompletion(result, fmt.Sprintf("project branch %s has no READY repository with remote_url for publication verification", branch.BranchID)), true, nil
	}
	remoteHead, err := projectRemoteBranchHead(ctx, repo, branch.BranchName)
	if err != nil {
		return demoteProjectImplementationCompletion(result, fmt.Sprintf("project branch %s publication cannot be verified on the shared project remote: %v", branch.BranchID, err)), true, nil
	}
	if strings.TrimSpace(remoteHead) == "" {
		return demoteProjectImplementationCompletion(result, fmt.Sprintf("project branch %s is READY_FOR_REVIEW locally but branch %q is not published on the shared project remote; run project_branch_commit with push=true, then project_branch_review_ready again", branch.BranchID, branch.BranchName)), true, nil
	}
	if strings.TrimSpace(remoteHead) != strings.TrimSpace(branch.HeadSHA) {
		return demoteProjectImplementationCompletion(result, fmt.Sprintf("project branch %s remote head %s does not match READY_FOR_REVIEW head evidence %s; run project_branch_commit with push=true for the current HEAD, then project_branch_review_ready again", branch.BranchID, strings.TrimSpace(remoteHead), strings.TrimSpace(branch.HeadSHA))), true, nil
	}
	if decision, ok := latestProjectPatchQueueTerminalItemForBranchHead(coordination.PatchQueueItems, branch.BranchID, branch.HeadSHA); ok && projectPatchQueueNegativeTerminalState(decision.State) {
		return demoteProjectImplementationCompletionForPatchQueueDecision(result, decision), true, nil
	}
	if !projectPatchQueueItemExistsForBranchHead(coordination.PatchQueueItems, branch.BranchID, branch.HeadSHA) {
		detail := fmt.Sprintf(
			"implementation task %s published branch %s at head %s as READY_FOR_REVIEW, but no patch queue item exists for that branch/head. Review packet evidence is not the queue handoff receipt; call project_patch_queue_submit for this READY_FOR_REVIEW branch before completing or opening validation-only follow-up work.\n\n%s",
			strings.TrimSpace(task.TaskID),
			strings.TrimSpace(branch.BranchID),
			strings.TrimSpace(branch.HeadSHA),
			projectPatchQueueSubmitArgsBlock(branch, r.cfg.AgentID),
		)
		return demoteProjectImplementationCompletionForPatchQueueSubmitHandoff(result, detail), true, nil
	}
	return result, false, nil
}

func (r *Runtime) enforceTrustFirstProjectImplementationCompletionEvidenceGate(ctx context.Context, task WorkspaceTaskRecord, result StructuredTaskResult) (StructuredTaskResult, bool, error) {
	decision, ok, err := r.currentProjectCompletionNegativePatchQueueDecision(ctx, task)
	if err != nil {
		return trustFirstProjectImplementationCompletionAdvisory(result, fmt.Sprintf("project git publication evidence could not be checked: %v", err)), false, nil
	}
	if ok {
		return demoteProjectImplementationCompletionForPatchQueueDecision(result, decision), true, nil
	}
	return r.annotateTrustFirstProjectImplementationCompletionAdvisory(ctx, task, result), false, nil
}

func (r *Runtime) currentProjectCompletionNegativePatchQueueDecision(ctx context.Context, task WorkspaceTaskRecord) (ProjectPatchQueueItemRecord, bool, error) {
	if r == nil || r.client == nil || strings.TrimSpace(task.ProjectID) == "" {
		return ProjectPatchQueueItemRecord{}, false, nil
	}
	coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, task.ProjectID)
	if err != nil {
		return ProjectPatchQueueItemRecord{}, false, err
	}
	branch, ok := selectProjectCompletionBranch(coordination, r.cfg.AgentID, task)
	if !ok || strings.TrimSpace(branch.HeadSHA) == "" {
		return ProjectPatchQueueItemRecord{}, false, nil
	}
	decision, ok := latestProjectPatchQueueTerminalItemForBranchHead(coordination.PatchQueueItems, branch.BranchID, branch.HeadSHA)
	if !ok || !projectPatchQueueNegativeTerminalState(decision.State) {
		return ProjectPatchQueueItemRecord{}, false, nil
	}
	return decision, true, nil
}

func (r *Runtime) annotateTrustFirstProjectImplementationCompletionAdvisory(ctx context.Context, task WorkspaceTaskRecord, result StructuredTaskResult) StructuredTaskResult {
	advisory := r.trustFirstProjectImplementationCompletionAdvisory(ctx, task)
	if strings.TrimSpace(advisory) == "" {
		return result
	}
	return trustFirstProjectImplementationCompletionAdvisory(result, advisory)
}

func (r *Runtime) trustFirstProjectImplementationCompletionAdvisory(ctx context.Context, task WorkspaceTaskRecord) string {
	if r == nil || r.client == nil {
		return "project git publication evidence could not be checked because runtime client is unavailable; trust_first treats this as advisory telemetry"
	}
	coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, task.ProjectID)
	if err != nil {
		return fmt.Sprintf("project git publication evidence could not be checked: %v; trust_first treats this as advisory telemetry", err)
	}
	branch, ok := selectProjectCompletionBranch(coordination, r.cfg.AgentID, task)
	if !ok {
		return "no owned branch is bound to this project task; trust_first treats branch lifecycle evidence as advisory telemetry"
	}
	if !strings.EqualFold(strings.TrimSpace(branch.Status), "READY_FOR_REVIEW") {
		return fmt.Sprintf("project branch %s is %s, not READY_FOR_REVIEW; trust_first allows completion but records the branch lifecycle gap as advisory telemetry", branch.BranchID, firstNonEmpty(branch.Status, "UNKNOWN"))
	}
	if strings.TrimSpace(branch.HeadSHA) == "" {
		return fmt.Sprintf("project branch %s is READY_FOR_REVIEW without head_sha evidence; trust_first records this as advisory telemetry", branch.BranchID)
	}
	checkout, ok := selectProjectCompletionCheckout(coordination, branch, r.cfg.AgentID, task)
	if !ok || strings.TrimSpace(checkout.LocalPath) == "" {
		return fmt.Sprintf("project branch %s has no registered checkout path for final cleanliness verification; trust_first records this as advisory telemetry", branch.BranchID)
	}
	localPath := strings.TrimSpace(checkout.LocalPath)
	if strings.TrimSpace(r.cfg.Workdir) != "" {
		if err := ensureProjectCheckoutMaterializePath(r.cfg.Workdir, localPath); err != nil {
			return fmt.Sprintf("project checkout path is not verifiable from this agent workdir: %v; trust_first records this as advisory telemetry", err)
		}
	}
	if !isGitCheckout(ctx, localPath) {
		return fmt.Sprintf("project checkout %s is not a readable git checkout; trust_first records this as advisory telemetry", localPath)
	}
	dirty, err := gitWorktreeDirty(ctx, localPath)
	if err != nil {
		return fmt.Sprintf("project checkout %s dirty-state check failed: %v; trust_first records this as advisory telemetry", localPath, err)
	}
	if dirty {
		return fmt.Sprintf("project checkout %s has uncommitted changes; trust_first records this as advisory telemetry instead of blocking completion", localPath)
	}
	head, err := gitRevParse(ctx, localPath, "HEAD")
	if err != nil || strings.TrimSpace(head) == "" {
		return fmt.Sprintf("project checkout %s HEAD cannot be read: %v; trust_first records this as advisory telemetry", localPath, err)
	}
	if strings.TrimSpace(head) != strings.TrimSpace(branch.HeadSHA) {
		return fmt.Sprintf("project checkout HEAD %s does not match READY_FOR_REVIEW branch evidence %s; trust_first records this as advisory telemetry", strings.TrimSpace(head), strings.TrimSpace(branch.HeadSHA))
	}
	repo, ok := selectProjectBranchReviewRepo(coordination.Repositories, branch.RepoID)
	if !ok {
		return fmt.Sprintf("project branch %s has no READY repository with remote_url for publication verification; trust_first records this as advisory telemetry", branch.BranchID)
	}
	remoteHead, err := projectRemoteBranchHead(ctx, repo, branch.BranchName)
	if err != nil {
		return fmt.Sprintf("project branch %s publication cannot be verified on the shared project remote: %v; trust_first records this as advisory telemetry", branch.BranchID, err)
	}
	if strings.TrimSpace(remoteHead) == "" {
		return fmt.Sprintf("project branch %s is READY_FOR_REVIEW locally but branch %q is not published on the shared project remote; trust_first records this as advisory telemetry", branch.BranchID, branch.BranchName)
	}
	if strings.TrimSpace(remoteHead) != strings.TrimSpace(branch.HeadSHA) {
		return fmt.Sprintf("project branch %s remote head %s does not match READY_FOR_REVIEW head evidence %s; trust_first records this as advisory telemetry", branch.BranchID, strings.TrimSpace(remoteHead), strings.TrimSpace(branch.HeadSHA))
	}
	return ""
}

func trustFirstProjectImplementationCompletionAdvisory(result StructuredTaskResult, advisory string) StructuredTaskResult {
	advisory = strings.TrimSpace(advisory)
	if advisory == "" {
		return result
	}
	note := "trust_first_git_publication_advisory: " + advisory
	result.Details = appendAutonomyNote(result.Details, note)
	if strings.TrimSpace(result.NextAction) == "" {
		result.NextAction = "Keep moving with product, review, or integration work; publish branch/review-ready evidence when useful, but do not treat missing git bookkeeping as an external blocker in trust_first mode."
	}
	return result
}

func trustFirstProjectImplementationGitEvidenceBlockAdvisory(result StructuredTaskResult, advisory string) StructuredTaskResult {
	result.Outcome = "continue"
	result.Summary = firstNonEmpty(result.Summary, "Project git publication bookkeeping treated as trust_first advisory telemetry")
	note := "trust_first_git_publication_advisory: self-owned git publication evidence is advisory in trust_first mode when no current-head negative patch queue decision exists"
	if strings.TrimSpace(advisory) != "" {
		note += "; " + strings.TrimSpace(advisory)
	}
	result.Details = appendAutonomyNote(result.Details, note)
	result.NextAction = trustFirstGitPublicationAdvisoryNextAction(result.NextAction)
	result.BlockedOn = nil
	result.Materialize = TaskMaterialization{}
	return result
}

func trustFirstGitPublicationAdvisoryNextAction(current string) string {
	const advisoryNextAction = "Continue with the next useful product, validation, review, or integration move; publish branch/review-ready evidence when useful, but do not wait solely on git bookkeeping."
	if strings.TrimSpace(current) == "" {
		return advisoryNextAction
	}
	if projectImplementationGitEvidenceBlockLooksSelfActionable(StructuredTaskResult{NextAction: current}) || containsAny(current, "push=true", "ready_for_review") {
		return advisoryNextAction
	}
	return current
}

func projectImplementationGitEvidenceBlockLooksSelfActionable(result StructuredTaskResult) bool {
	text := strings.ToLower(strings.Join([]string{
		result.Summary,
		result.Details,
		result.NextAction,
		result.Materialize.DocKey,
		result.Materialize.DocTitle,
		result.Materialize.DocContent,
		blockedDetails(result.BlockedOn),
	}, "\n"))
	return containsAny(text,
		"project_branch_commit",
		"project_branch_review_ready",
		"project git evidence",
		"git evidence",
		"uncommitted changes",
		"dirty checkout",
		"dirty-state",
		"ready_for_review",
		"review-ready",
		"evidence_gap",
	)
}

func runtimeProjectCompletionEvidenceGateRequired(task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(task.ProjectID) == "" {
		return false
	}
	// R49: a structured docs/spec-fidelity planning-evidence task (lane=implementation but
	// producing planning docs - product_contract / plan_review - not a git/patch-queue
	// publication, empty write-scope) must NOT be subject to the implementation
	// completion-evidence gate. Mirrors the R46 claim-admission bypass in
	// runtimeProjectTaskRequiresImplementationGate. Without it the gate demands an "owned git
	// publication step" the docs task can never produce, deferring completion forever while the
	// agent would otherwise loop on an impossible owned-git-publication requirement.
	if runtimeTaskBypassesImplementationGateByStructuredContract(task) {
		return false
	}
	hasClaimEvidence := taskPointerValue(task.ClaimBranchID) != "" ||
		taskPointerValue(task.ClaimCheckoutID) != "" ||
		taskPointerValue(task.ClaimWriteScopeJSON) != ""
	if !hasClaimEvidence && runtimeProjectTaskLooksPatchQueueIntegrationConvergence(task) {
		return false
	}
	if runtimeProjectLaneRequiresImplementationGate(task.ProjectLane) {
		return true
	}
	return hasClaimEvidence
}

func runtimeProjectTaskLooksPatchQueueIntegrationConvergence(task WorkspaceTaskRecord) bool {
	if runtimeTaskLooksStrategicLeadRoleScopeRequest(task) || runtimeProjectClaimRepairTask(task) {
		return false
	}
	var requirements map[string]any
	if raw := strings.TrimSpace(task.TaskRequirementsJSON); raw != "" && raw != "{}" {
		_ = json.Unmarshal([]byte(raw), &requirements)
	}
	return projectTaskLooksCanonicalIntegrationConvergence(task.ProjectLane, task.Title, task.Description, task.Tags, requirements)
}

func projectPatchQueueIntegrationAssemblyEvidenceComplete(result StructuredTaskResult, trace *TaskRunTrace) bool {
	text := strings.ToLower(strings.Join([]string{
		result.Summary,
		result.Details,
		result.NextAction,
		result.OwnerAction,
		result.Materialize.DocKey,
		result.Materialize.DocTitle,
		result.Materialize.DocContent,
		blockedDetails(result.BlockedOn),
	}, "\n"))
	if text == "" && trace == nil {
		return false
	}
	// EP-03 (static-audit 2026-06-02): only defer on a *verdict-keyed* negative, never on
	// incidental narrative prose. An integrator that honestly narrates "validation was
	// previously not run; now it passes" must not be force-demoted by the bare phrase
	// "validation not run" while it also carries a real pass verdict/evidence.
	if containsAnySignal(text, []string{
		"full_product_verdict: fail",
		"full_product_verdict=fail",
		"full_product_verdict: failed",
		"full_product_verdict=failed",
		"full_product_verdict: missing",
		"full_product_verdict=missing",
		"full_product_verdict: not run",
		"full_product_verdict=not run",
		"validation_verdict: fail",
		"validation_verdict=fail",
		"build_or_test_command: not run",
		"build_or_test_command=not run",
		"test_command: not run",
		"test_command=not run",
	}) {
		return false
	}
	hasIntegrationReceipt := taskTraceHasSuccessfulTool(trace, "project_patch_queue_integrate") ||
		containsAnySignal(text, []string{
			"project_patch_queue_integrate",
			"project.patch_queue.integrated",
			"integration_receipt",
			"integration receipt",
			"canonical_target_head",
			"target_head_after",
			"remote_target_head_after",
		})
	// EP-02 (static-audit 2026-06-02): a verified exit-0 test/build run recorded in the trace
	// is durable evidence the assembled product passed — the shell tool sets IsError on any
	// non-zero exit, so a non-error shell receipt with a test-pass signal cannot be faked by
	// agent prose. Treat it as satisfying both the command-evidence and the pass-verdict
	// requirements, so an integrator that genuinely merged + ran `go test ./...` green is not
	// dead-ended for paraphrasing the literal `full_product_verdict: pass` token.
	verifiedTestPass := taskTraceHasVerifiedTestPass(trace)
	hasCommandEvidence := verifiedTestPass || containsAnySignal(text, []string{
		"build_or_test_command:",
		"build_or_test_command=",
		"test_command:",
		"test_command=",
		"smoke_command:",
		"smoke_command=",
		"go test",
		"go build",
		"npm test",
		"npm run test",
		"npm run build",
		"pnpm test",
		"pnpm build",
		"pytest",
		"cargo test",
		"smoke command:",
		"smoke command=",
		"smoke test:",
		"smoke test=",
		"browser smoke:",
		"browser smoke=",
	})
	hasFullProductPassVerdict := (hasIntegrationReceipt && verifiedTestPass) || containsAnySignal(text, []string{
		"full_product_verdict: pass",
		"full_product_verdict=pass",
		"full_product_verdict: passed",
		"full_product_verdict=passed",
	})
	return hasIntegrationReceipt && hasCommandEvidence && hasFullProductPassVerdict
}

// taskTraceHasVerifiedTestPass reports whether the trace contains a shell tool call that
// exited 0 (IsError=false — the shell tool flags any non-zero command exit as an error) and
// whose output carries a recognized test/build pass signal. This is durable, prose-proof
// evidence that the assembled product was actually built/tested green this cycle: a failing
// `go test` exits non-zero -> IsError=true -> excluded here, so a broken product cannot
// satisfy it. Used by the integration-assembly completion gate (EP-02).
func taskTraceHasVerifiedTestPass(trace *TaskRunTrace) bool {
	if trace == nil {
		return false
	}
	for _, receipt := range trace.ToolReceipts {
		if receipt.IsError {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(receipt.ToolName), "shell") {
			continue
		}
		lower := strings.ToLower(receipt.Output)
		// Explicit pass phrases (cargo/npm/jest-style + go's no-test packages).
		if containsAnySignal(lower, []string{
			"test result: ok",
			"all tests passed",
			"0 failures",
			"tests passed",
		}) {
			return true
		}
		// Go test pass lines look like "ok  \tpkg\t0.1s": a line starting with "ok" that
		// carries a tab-separated package. A failing `go test` exits non-zero, so the whole
		// receipt is IsError=true and never reaches here — only an all-green run matches.
		for _, line := range strings.Split(lower, "\n") {
			trimmed := strings.TrimSpace(line)
			if goTestReceiptLineLooksPassed(trimmed) {
				return true
			}
		}
	}
	return false
}

func goTestReceiptLineLooksPassed(trimmed string) bool {
	fields := strings.Fields(strings.TrimSpace(trimmed))
	if len(fields) < 2 {
		return false
	}
	switch fields[0] {
	case "ok":
		return true
	case "?":
		return strings.Contains(strings.ToLower(strings.TrimSpace(trimmed)), "[no test files]")
	default:
		return false
	}
}

func taskTraceHasSuccessfulTool(trace *TaskRunTrace, names ...string) bool {
	if trace == nil {
		return false
	}
	wanted := map[string]struct{}{}
	for _, name := range names {
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "" {
			wanted[name] = struct{}{}
		}
	}
	for _, toolName := range trace.SuccessfulToolCalls {
		normalized := strings.ToLower(strings.TrimSpace(toolName))
		if _, ok := wanted[normalized]; ok {
			return true
		}
		if idx := strings.Index(normalized, ":"); idx > 0 {
			if _, ok := wanted[normalized[:idx]]; ok {
				return true
			}
		}
	}
	return false
}

func selectProjectCompletionBranch(coordination ProjectCoordinationRecord, agentID string, task WorkspaceTaskRecord) (ProjectBranchRecord, bool) {
	agentID = strings.TrimSpace(agentID)
	claimBranchID := taskPointerValue(task.ClaimBranchID)
	if claimBranchID != "" {
		for _, branch := range coordination.Branches {
			if strings.TrimSpace(branch.BranchID) == claimBranchID && (strings.TrimSpace(branch.AgentID) == "" || strings.TrimSpace(branch.AgentID) == agentID) {
				return branch, true
			}
		}
	}
	taskID := strings.TrimSpace(task.TaskID)
	for _, branch := range coordination.Branches {
		if strings.TrimSpace(branch.AgentID) != "" && strings.TrimSpace(branch.AgentID) != agentID {
			continue
		}
		if strings.TrimSpace(branch.ActiveTaskID) == taskID || strings.TrimSpace(branch.ActiveClaimID) == taskID {
			return branch, true
		}
	}
	return ProjectBranchRecord{}, false
}

func selectProjectCompletionCheckout(coordination ProjectCoordinationRecord, branch ProjectBranchRecord, agentID string, task WorkspaceTaskRecord) (ProjectCheckoutRecord, bool) {
	agentID = strings.TrimSpace(agentID)
	claimCheckoutID := taskPointerValue(task.ClaimCheckoutID)
	if claimCheckoutID != "" {
		if checkout, ok := projectCompletionCheckoutByID(coordination.Checkouts, claimCheckoutID); ok &&
			projectCompletionCheckoutMatchesBranchTask(checkout, branch, agentID, task) {
			return checkout, true
		}
	}
	branchName := strings.TrimSpace(branch.BranchName)
	if branchName != "" {
		for _, checkout := range coordination.Checkouts {
			if !strings.EqualFold(strings.TrimSpace(checkout.BranchName), branchName) {
				continue
			}
			if projectCompletionCheckoutMatchesBranchTask(checkout, branch, agentID, task) {
				return checkout, true
			}
		}
	}
	if checkoutID := strings.TrimSpace(branch.CheckoutID); checkoutID != "" {
		if checkout, ok := projectCompletionCheckoutByID(coordination.Checkouts, checkoutID); ok &&
			projectCompletionCheckoutMatchesBranchTask(checkout, branch, agentID, task) {
			return checkout, true
		}
	}
	return ProjectCheckoutRecord{}, false
}

func projectCompletionCheckoutByID(checkouts []ProjectCheckoutRecord, checkoutID string) (ProjectCheckoutRecord, bool) {
	checkoutID = strings.TrimSpace(checkoutID)
	if checkoutID == "" {
		return ProjectCheckoutRecord{}, false
	}
	for _, checkout := range checkouts {
		if strings.TrimSpace(checkout.CheckoutID) == checkoutID {
			return checkout, true
		}
	}
	return ProjectCheckoutRecord{}, false
}

func projectCompletionCheckoutMatchesBranchTask(checkout ProjectCheckoutRecord, branch ProjectBranchRecord, agentID string, task WorkspaceTaskRecord) bool {
	if strings.TrimSpace(checkout.LocalPath) == "" {
		return false
	}
	if managerProjectCheckoutStatusTerminal(checkout.Status) {
		return false
	}
	if repoID := strings.TrimSpace(checkout.RepoID); repoID != "" && strings.TrimSpace(branch.RepoID) != "" && repoID != strings.TrimSpace(branch.RepoID) {
		return false
	}
	if checkoutAgent := strings.TrimSpace(checkout.AgentID); checkoutAgent != "" && strings.TrimSpace(agentID) != "" && checkoutAgent != strings.TrimSpace(agentID) {
		return false
	}
	if checkoutBranch := strings.TrimSpace(checkout.BranchName); checkoutBranch != "" && strings.TrimSpace(branch.BranchName) != "" && !strings.EqualFold(checkoutBranch, strings.TrimSpace(branch.BranchName)) {
		return false
	}
	taskID := strings.TrimSpace(task.TaskID)
	if activeTaskID := strings.TrimSpace(checkout.ActiveTaskID); activeTaskID != "" && taskID != "" && activeTaskID != taskID {
		return false
	}
	if activeClaimID := strings.TrimSpace(checkout.ActiveClaimID); activeClaimID != "" && taskID != "" && activeClaimID != taskID {
		return false
	}
	return true
}

func taskPointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

// projectImplementationUncommittedCommitDirective returns a deterministic, per-cycle
// directive telling an implementer to run project_branch_commit when its owned project
// checkout has uncommitted changes (G-SUB-1). The completion gate already enforces this
// for outcome=completed, but an agent that keeps yielding outcome=continue (write a
// file, build, repeat) is never told, on the continue path, that it has dirty product
// work to commit — so it writes/builds indefinitely without committing. This surfaces
// the same git fact as a continue-path nudge.
//
// To keep the per-cycle hot path cheap, the caller should only invoke this when the
// run may have touched project files (write_file or shell). Returns "" when there
// is no owned dirty checkout, the task is not a project-implementation task, or
// anything is unverifiable (advisory only, never blocks).
func (r *Runtime) projectImplementationUncommittedCommitDirective(ctx context.Context, task WorkspaceTaskRecord) string {
	if r == nil || r.client == nil {
		return ""
	}
	if !runtimeProjectCompletionEvidenceGateRequired(task) || strings.TrimSpace(task.ProjectID) == "" {
		return ""
	}
	coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, task.ProjectID)
	if err != nil {
		return ""
	}
	branch, ok := selectProjectCompletionBranch(coordination, r.cfg.AgentID, task)
	if !ok {
		return ""
	}
	checkout, ok := selectProjectCompletionCheckout(coordination, branch, r.cfg.AgentID, task)
	if !ok || strings.TrimSpace(checkout.LocalPath) == "" {
		return ""
	}
	localPath := strings.TrimSpace(checkout.LocalPath)
	if strings.TrimSpace(r.cfg.Workdir) != "" {
		if err := ensureProjectCheckoutMaterializePath(r.cfg.Workdir, localPath); err != nil {
			return ""
		}
	}
	if !isGitCheckout(ctx, localPath) {
		return ""
	}
	dirty, err := gitWorktreeDirty(ctx, localPath)
	if err != nil || !dirty {
		return ""
	}
	return fmt.Sprintf("UNCOMMITTED WORK: your owned project checkout %s has uncommitted changes on branch %s. This is self-actionable owned git publication work. Your next action must be project_branch_commit (push=true) to create a durable commit, then project_branch_review_ready — do not keep writing files, building, or posting status/docs while real product work sits uncommitted.", localPath, firstNonEmpty(branch.BranchID, "<owned-branch>"))
}

// projectImplementationPublicationDirective surfaces the deterministic NEXT publication step
// when an owned implementation branch is between commit and patch-queue submit, as a
// continue-path advisory. The completion gate (deferProjectImplementationCompletionUntilGitEvidenceCurrent)
// already encodes this chain but only fires on outcome=completed — so an agent exhibiting the
// "intent-without-action" stall (commits, then keeps yielding continue while polling/doc-writing)
// never gets nudged toward the next durable step. AG-01/AG-02 (static-audit 2026-06-02). Pure
// coordination data, no git I/O. Advisory only, never blocks. Returns "" when there is nothing
// to nudge (dirty checkout is owned by the commit directive; rejected heads are owned by revision).
func (r *Runtime) projectImplementationPublicationDirective(ctx context.Context, task WorkspaceTaskRecord) string {
	if r == nil || r.client == nil {
		return ""
	}
	if !runtimeProjectCompletionEvidenceGateRequired(task) || strings.TrimSpace(task.ProjectID) == "" {
		return ""
	}
	coordination, err := r.client.GetProjectCoordination(ctx, r.cfg.WorkspaceID, task.ProjectID)
	if err != nil {
		return ""
	}
	branch, ok := selectProjectCompletionBranch(coordination, r.cfg.AgentID, task)
	if !ok {
		return ""
	}
	status := strings.ToUpper(strings.TrimSpace(branch.Status))
	head := strings.TrimSpace(branch.HeadSHA)
	branchLabel := firstNonEmpty(strings.TrimSpace(branch.BranchID), "<owned-branch>")
	// AG-02: published READY_FOR_REVIEW but no patch-queue item for the head -> submit nudge.
	if status == "READY_FOR_REVIEW" {
		if head == "" {
			return ""
		}
		if decision, ok := latestProjectPatchQueueTerminalItemForBranchHead(coordination.PatchQueueItems, branch.BranchID, head); ok && projectPatchQueueNegativeTerminalState(decision.State) {
			return "" // a revision follow-up owns this head; do not nag submit
		}
		if projectPatchQueueItemExistsForBranchHead(coordination.PatchQueueItems, branch.BranchID, head) {
			return ""
		}
		return fmt.Sprintf("READY-FOR-REVIEW NOT SUBMITTED: your owned branch %s is READY_FOR_REVIEW at head %s but no patch queue item exists for that branch/head. The review packet document is not the queue handoff receipt. Your next action must be project_patch_queue_submit with the exact envelope below; do not keep polling the queue, writing docs, or posting status until the candidate is actually submitted.\n\n%s", branchLabel, head, projectPatchQueueSubmitArgsBlock(branch, r.cfg.AgentID))
	}
	// AG-01: committed (branch ACTIVE with a head) but not yet published for review -> review-ready nudge.
	if status == "ACTIVE" && head != "" {
		return fmt.Sprintf("COMMITTED BUT NOT REVIEW-READY: your owned branch %s is committed at head %s but is not READY_FOR_REVIEW. This is self-actionable owned git publication work. Your next action must be project_branch_review_ready for the current HEAD with verification evidence — do not keep writing files, polling, or posting status while a committed branch sits unpublished for review.", branchLabel, head)
	}
	return ""
}

func projectPatchQueueSubmitArgsBlock(branch ProjectBranchRecord, requiredAgentID string) string {
	var b strings.Builder
	b.WriteString("project_patch_queue_submit args:\n")
	writeArg := func(name, value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		b.WriteString("- " + name + ": " + value + "\n")
	}
	writeArg("project_id", branch.ProjectID)
	writeArg("repo_id", branch.RepoID)
	writeArg("branch_id", branch.BranchID)
	writeArg("branch_name", branch.BranchName)
	writeArg("review_doc_key", branch.ReviewDocKey)
	writeArg("head_sha", branch.HeadSHA)
	writeArg("base_ref", branch.BaseBranch)
	writeArg("base_sha", branch.BaseSHA)
	writeArg("pathset_json", branch.WriteScopeJSON)
	b.WriteString("- controlled_queue: true\n")
	b.WriteString("- repo_authority_mode: repoauthority_controlled_queue\n")
	writeArg("required_agent_id", requiredAgentID)
	return b.String()
}

func addPatchQueueSubmitEnvelopeRequirements(requirements map[string]any, branch ProjectBranchRecord) {
	if requirements == nil {
		return
	}
	add := func(name, value string) {
		value = strings.TrimSpace(value)
		if value != "" {
			requirements[name] = value
		}
	}
	add("project_id", branch.ProjectID)
	add("repo_id", branch.RepoID)
	add("branch_id", branch.BranchID)
	add("branch_name", branch.BranchName)
	add("review_doc_key", branch.ReviewDocKey)
	add("head_sha", branch.HeadSHA)
	add("base_ref", branch.BaseBranch)
	add("base_sha", branch.BaseSHA)
	add("pathset_json", branch.WriteScopeJSON)
}

// traceTouchedProjectFiles reports whether the run may have edited or tested project
// files this cycle (used to gate the cheap-but-not-free dirty-checkout probe).
func traceTouchedProjectFiles(trace *TaskRunTrace) bool {
	if trace == nil {
		return false
	}
	for _, toolName := range trace.SuccessfulToolCalls {
		switch strings.ToLower(strings.TrimSpace(toolName)) {
		case "write_file", "shell":
			return true
		}
	}
	return false
}

func demoteProjectImplementationCompletion(result StructuredTaskResult, detail string) StructuredTaskResult {
	detail = strings.TrimSpace(detail)
	if detail == "" {
		detail = "project implementation evidence is incomplete"
	}
	result.Outcome = "continue"
	result.Summary = "Completion deferred: project implementation evidence still needs the owned git publication step"
	result.Details = detail
	result.NextAction = "This is self-actionable, not an external blocker: call project_branch_commit with push=true for the owned checkout changes, then call project_branch_review_ready for the current HEAD, then complete the implementation lane."
	result.BlockedOn = nil
	result.Materialize = TaskMaterialization{}
	return result
}

func demoteProjectImplementationCompletionForPatchQueueSubmitHandoff(result StructuredTaskResult, detail string) StructuredTaskResult {
	result = demoteProjectImplementationCompletion(result, detail)
	result.Summary = "Completion deferred: READY_FOR_REVIEW branch still needs durable patch queue handoff"
	result.NextAction = "This is self-actionable, not an external blocker: call project_patch_queue_submit for the owned READY_FOR_REVIEW branch/head now. The review packet document and branch registry transition are not the patch queue receipt."
	return result
}

func demoteProjectPatchQueueIntegrationCompletion(result StructuredTaskResult) StructuredTaskResult {
	result.Outcome = "continue"
	result.Summary = "Completion deferred: integration still needs assembled full-product validation"
	result.Details = strings.TrimSpace(strings.Join([]string{
		strings.TrimSpace(result.Details),
		"Patch queue ACCEPTED is lane-level review evidence, not a full-product verdict. This integration lane must show the accepted branch was integrated into a canonical target head and must publish bounded build/test/smoke evidence plus full_product_verdict: pass for the assembled product before terminal completion.",
	}, "\n"))
	result.NextAction = "Finish the assembly gate now: run project_patch_queue_integrate if no integration receipt/canonical_target_head exists, then run a bounded build/test/smoke command on the integrated target and materialize exact command/status/head evidence with full_product_verdict: pass."
	result.BlockedOn = nil
	result.RequiresHuman = false
	result.Materialize = TaskMaterialization{}
	return result
}

func recoverableProjectImplementationFailure(result StructuredTaskResult, detail string) StructuredTaskResult {
	detail = strings.TrimSpace(detail)
	if detail == "" {
		detail = "recoverable project implementation work exists"
	}
	result.Outcome = "blocked"
	result.Summary = "Recoverable project work preserved after failed implementation cycle"
	result.Details = detail + "\nThe failed outcome is not allowed to release the task and abandon the active project branch while product work is still unpublished or unreviewed."
	result.NextAction = "Keep the owner on this lane: if the checkout is dirty, run project_branch_commit with push=true; then run project_branch_review_ready for the current HEAD. Route browser/smoke fallout as validation or patch-queue evidence after the branch is review-ready."
	result.BlockedOn = []BlockedRef{{
		Kind:   "recoverable_project_work",
		Detail: detail,
	}}
	result.RequiresHuman = false
	result.Materialize = TaskMaterialization{}
	return result
}

func recoverableProjectImplementationExecutionErrorResult(execErr error, detail string) StructuredTaskResult {
	detail = strings.TrimSpace(detail)
	if detail == "" {
		detail = "recoverable project implementation work exists"
	}
	errText := ""
	if execErr != nil {
		errText = strings.TrimSpace(execErr.Error())
	}
	result := StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Recoverable project work queued after execution error",
		Details:    detail,
		NextAction: "Resume the owned implementation lane now: inspect the checkout and task evidence, repair the product/code if needed, run bounded checks, call project_branch_commit with push=true, then project_branch_review_ready.",
	}
	if errText != "" {
		result.Details = detail + "\nExecution error was treated as recoverable because owned project work remains: " + errText
	}
	return result
}

func demoteProjectImplementationCompletionForPatchQueueDecision(result StructuredTaskResult, item ProjectPatchQueueItemRecord) StructuredTaskResult {
	state := strings.ToUpper(strings.TrimSpace(item.State))
	decisionSummary := strings.TrimSpace(item.DecisionSummary)
	if decisionSummary == "" {
		decisionSummary = "no decision summary recorded"
	}
	result.Outcome = "continue"
	result.Summary = "Completion deferred: current branch head has an unresolved patch queue decision"
	result.Details = fmt.Sprintf("patch queue item %s/%s is %s for branch_id=%s head_sha=%s: %s", strings.TrimSpace(item.QueueID), strings.TrimSpace(item.ItemID), firstNonEmpty(state, "UNKNOWN"), strings.TrimSpace(item.BranchID), strings.TrimSpace(item.HeadSHA), decisionSummary)
	if state == "REJECTED" {
		result.NextAction = "Treat the rejection as durable review state: fix the concrete decision summary in the owned checkout, create a new commit/head with project_branch_commit, publish project_branch_review_ready for that new head, then call project_patch_queue_submit with a fresh item_id or claim the existing patch queue revision follow-up."
	} else {
		result.NextAction = "Treat the blocked queue item as durable review state: address the decision summary with evidence or a revision, refresh project_branch_review_ready, then resolve through project_patch_queue_submit with a fresh item_id or the existing patch queue follow-up instead of relying on direct peer chat."
	}
	result.BlockedOn = nil
	result.Materialize = TaskMaterialization{}
	return result
}
