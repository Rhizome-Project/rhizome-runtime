package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/repoauthority"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

const repoMutationActuatorPollInterval = 5 * time.Second

type repoMutationActuatorRunResult struct {
	Applied         bool
	CandidateFound  bool
	BlockingReasons []string
	Activation      repoauthority.MutationActivationGateResult
	Result          repoauthority.MutationActuatorLiveResult
	StartEvent      sqlite.RuntimeEventRecord
	Event           sqlite.RuntimeEventRecord
}

type repoMutationTargetFilePlan struct {
	File       repoauthority.PatchMaterializedFile
	FullPath   string
	BeforeHash string
	Status     string
}

func startServeRepoMutationActuator(runCtx context.Context, store *sqlite.Store, readiness *ReadinessRegistry, supervisor *serveLoopSupervisor) {
	if !repoMutationLiveActuatorEnabled() {
		readiness.SetState(loopNameRepoMutationActuator, LoopDisabled)
		return
	}
	readiness.SetState(loopNameRepoMutationActuator, LoopRunning)
	supervisor.Go(func() {
		ticker := time.NewTicker(repoMutationActuatorPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				readiness.SetState(loopNameRepoMutationActuator, LoopStopped)
				return
			case <-ticker.C:
				result, err := runRepoMutationActuatorOnce(runCtx, store)
				if err != nil {
					log.Printf("[repo-mutation-actuator] error: %v", err)
					readiness.SetError(loopNameRepoMutationActuator, err)
					continue
				}
				readiness.SetState(loopNameRepoMutationActuator, LoopRunning)
				if result.Applied {
					log.Printf("[repo-mutation-actuator] materialized %s/%s digest=%s", result.Result.QueueID, result.Result.ItemID, result.Result.Digest)
				}
			}
		}
	})
}

func repoMutationLiveActuatorEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("RHIZOME_REPO_MUTATION_LIVE_ACTUATOR"))) {
	case "1", "true", "yes", "on", "enabled":
		return true
	default:
		return false
	}
}

func runRepoMutationActuatorOnce(ctx context.Context, store *sqlite.Store) (repoMutationActuatorRunResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if store == nil {
		return repoMutationActuatorRunResult{}, fmt.Errorf("store is required")
	}
	if !repoMutationLiveActuatorEnabled() {
		return repoMutationActuatorRunResult{}, nil
	}
	proof := store.ProjectPatchQueueDurabilityProof(ctx)
	candidate, ok, err := store.FirstProjectRepoMutationActivationCandidate(ctx)
	if err != nil {
		return repoMutationActuatorRunResult{}, err
	}
	if !ok {
		return repoMutationActuatorRunResult{}, nil
	}
	activation := collectRepoMutationActivationDiagnosticsForActuator(ctx, &proof, candidate, true)
	run := repoMutationActuatorRunResult{
		CandidateFound:  true,
		Activation:      activation,
		BlockingReasons: append([]string(nil), activation.BlockingReasons...),
	}
	if err := repoauthority.VerifyMutationActivationGateResult(activation); err != nil {
		return run, fmt.Errorf("verify mutation activation: %w", err)
	}
	if !activation.MutationAllowed {
		if repoMutationActuatorCanRecoverBlockedActivation(activation) {
			result, err := recoverRepoMutationMaterializationInTarget(ctx, candidate, activation)
			if err != nil {
				return run, err
			}
			_, event, err := store.RecordProjectPatchQueueActuatorResultWithEvent(ctx, sqlite.ProjectPatchQueueActuatorResultRecordInput{
				WorkspaceID: candidate.QueueItem.WorkspaceID,
				ProjectID:   candidate.QueueItem.ProjectID,
				QueueID:     candidate.QueueItem.QueueID,
				ItemID:      candidate.QueueItem.ItemID,
				Result:      result,
				ActorID:     sqlite.ProjectPatchQueueActuatorActorID,
				ActorType:   "system",
			})
			if err != nil {
				return run, err
			}
			run.Applied = true
			run.Result = result
			run.Event = event
			run.BlockingReasons = nil
		}
		return run, nil
	}
	startEvent, err := recordRepoMutationActuatorStartEvent(ctx, store, candidate, activation)
	if err != nil {
		return run, err
	}
	run.StartEvent = startEvent
	result, err := applyRepoMutationMaterializationToTarget(ctx, candidate, activation)
	if err != nil {
		return run, err
	}
	_, event, err := store.RecordProjectPatchQueueActuatorResultWithEvent(ctx, sqlite.ProjectPatchQueueActuatorResultRecordInput{
		WorkspaceID: candidate.QueueItem.WorkspaceID,
		ProjectID:   candidate.QueueItem.ProjectID,
		QueueID:     candidate.QueueItem.QueueID,
		ItemID:      candidate.QueueItem.ItemID,
		Result:      result,
		ActorID:     sqlite.ProjectPatchQueueActuatorActorID,
		ActorType:   "system",
	})
	if err != nil {
		return run, err
	}
	run.Applied = true
	run.Result = result
	run.Event = event
	run.BlockingReasons = nil
	return run, nil
}

func repoMutationActuatorCanRecoverBlockedActivation(activation repoauthority.MutationActivationGateResult) bool {
	if activation.MutationAllowed || activation.TargetWorktreeIdentity == nil || len(activation.BlockingReasons) == 0 {
		return false
	}
	for _, reason := range activation.BlockingReasons {
		if !strings.HasPrefix(strings.TrimSpace(reason), "live_actuator_target_identity:") {
			return false
		}
	}
	return repoMutationTargetDirtyOnlyRecoveryIdentity(activation)
}

func repoMutationTargetDirtyOnlyRecoveryIdentity(activation repoauthority.MutationActivationGateResult) bool {
	if activation.WorktreeIdentity == nil || activation.TargetWorktreeIdentity == nil {
		return false
	}
	source := *activation.WorktreeIdentity
	target := *activation.TargetWorktreeIdentity
	if !repoMutationWorktreeIdentityStrictlyClean(source) {
		return false
	}
	for _, value := range []string{
		target.RepoID,
		target.CheckoutID,
		target.BranchID,
		target.BranchName,
		target.MachineID,
		target.LocalPath,
		target.BaseSHA,
		target.HeadSHA,
		target.ObservedWorktreeRoot,
		target.ObservedBranchName,
		target.ObservedHeadSHA,
	} {
		if strings.TrimSpace(value) == "" {
			return false
		}
	}
	if !repoMutationCanonicalGitObjectID(target.BaseSHA) ||
		!repoMutationCanonicalGitObjectID(target.HeadSHA) ||
		!repoMutationCanonicalGitObjectID(target.ObservedHeadSHA) {
		return false
	}
	if strings.TrimSpace(target.ReadbackState) != "dirty" ||
		strings.TrimSpace(target.ObservedDirtyState) != "dirty" ||
		!strings.Contains(strings.ToLower(strings.TrimSpace(target.ReadbackError)), "uncommitted changes") {
		return false
	}
	if !pathsEqual(target.ObservedWorktreeRoot, target.LocalPath) ||
		strings.TrimSpace(target.ObservedBranchName) != strings.TrimSpace(target.BranchName) ||
		strings.TrimSpace(target.ObservedHeadSHA) != strings.TrimSpace(target.HeadSHA) {
		return false
	}
	if strings.TrimSpace(source.RepoID) != strings.TrimSpace(target.RepoID) ||
		strings.TrimSpace(source.CheckoutID) == strings.TrimSpace(target.CheckoutID) ||
		pathsEqual(source.LocalPath, target.LocalPath) ||
		strings.TrimSpace(source.BranchName) == strings.TrimSpace(target.BranchName) {
		return false
	}
	return strings.TrimSpace(target.BaseSHA) == strings.TrimSpace(source.BaseSHA) &&
		strings.TrimSpace(target.HeadSHA) == strings.TrimSpace(source.BaseSHA)
}

func repoMutationWorktreeIdentityStrictlyClean(evidence repoauthority.WorktreeIdentityEvidence) bool {
	for _, value := range []string{
		evidence.RepoID,
		evidence.CheckoutID,
		evidence.BranchID,
		evidence.BranchName,
		evidence.MachineID,
		evidence.LocalPath,
		evidence.BaseSHA,
		evidence.HeadSHA,
		evidence.ObservedWorktreeRoot,
		evidence.ObservedBranchName,
		evidence.ObservedHeadSHA,
	} {
		if strings.TrimSpace(value) == "" {
			return false
		}
	}
	return repoMutationCanonicalGitObjectID(evidence.BaseSHA) &&
		repoMutationCanonicalGitObjectID(evidence.HeadSHA) &&
		repoMutationCanonicalGitObjectID(evidence.ObservedHeadSHA) &&
		strings.TrimSpace(evidence.ReadbackState) == "ok" &&
		strings.TrimSpace(evidence.ObservedDirtyState) == "clean" &&
		pathsEqual(evidence.ObservedWorktreeRoot, evidence.LocalPath) &&
		strings.TrimSpace(evidence.ObservedBranchName) == strings.TrimSpace(evidence.BranchName) &&
		strings.TrimSpace(evidence.ObservedHeadSHA) == strings.TrimSpace(evidence.HeadSHA)
}

func repoMutationCanonicalGitObjectID(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, ch := range value {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func recordRepoMutationActuatorStartEvent(ctx context.Context, store *sqlite.Store, candidate sqlite.ProjectRepoMutationActivationCandidate, activation repoauthority.MutationActivationGateResult) (sqlite.RuntimeEventRecord, error) {
	item := candidate.QueueItem
	materializationPaths := make([]string, 0, len(item.Materialization.Files))
	for _, file := range item.Materialization.Files {
		materializationPaths = append(materializationPaths, strings.TrimSpace(file.Path))
	}
	sort.Strings(materializationPaths)
	payload := map[string]any{
		"schema":                                 "repo_mutation_actuator_started.v1",
		"workspace_id":                           item.WorkspaceID,
		"project_id":                             item.ProjectID,
		"repo_id":                                item.RepoID,
		"queue_id":                               item.QueueID,
		"item_id":                                item.ItemID,
		"target_checkout_id":                     candidate.TargetCheckout.CheckoutID,
		"target_branch_name":                     candidate.TargetCheckout.BranchName,
		"activation_digest":                      activation.Digest,
		"materialization_digest":                 item.MaterializationDigest,
		"materialization_authority_proof_digest": item.MaterializationAuthorityProofDigest,
		"file_count":                             len(materializationPaths),
		"paths":                                  materializationPaths,
	}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return sqlite.RuntimeEventRecord{}, fmt.Errorf("encode repo mutation actuator start payload: %w", err)
	}
	return store.RecordRuntimeEventWithLocalWorkspaceAuthority(ctx, item.WorkspaceID, sqlite.RuntimeEventInput{
		DedupKey: "project.patch_queue.actuator_started:" +
			strings.TrimSpace(item.WorkspaceID) + ":" +
			strings.TrimSpace(item.QueueID) + "/" + strings.TrimSpace(item.ItemID) + ":" +
			strings.TrimSpace(activation.Digest) + ":" +
			strings.TrimSpace(item.MaterializationDigest),
		WorkspaceID: item.WorkspaceID,
		EventType:   sqlite.ProjectPatchQueueActuatorStartedEventType,
		EntityType:  "project_patch_queue_item",
		EntityID:    item.QueueID + "/" + item.ItemID,
		ActorType:   "system",
		ActorID:     sqlite.ProjectPatchQueueActuatorActorID,
		PayloadJSON: string(rawPayload),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func collectRepoMutationActivationDiagnosticsForActuator(ctx context.Context, proof *sqlite.ProjectPatchQueueDurabilityProof, candidate sqlite.ProjectRepoMutationActivationCandidate, ok bool) repoauthority.MutationActivationGateResult {
	if !ok || !projectPatchQueueProofIsDurable(proof) {
		return collectRepoMutationActivationDiagnostics(proof)
	}
	liveVerifierEnabled, liveVerifierSource := repoMutationLiveVerifierState()
	return repoauthority.EvaluateMutationActivationGates(repoauthority.MutationActivationGateInput{
		AuthorityMode:               repoauthority.ModeControlledQueue,
		DirectMergeDisabled:         true,
		QueueDurable:                projectPatchQueueProofIsDurable(proof),
		ReviewerMeshMode:            repoauthority.MutationActivationReviewerMeshAdvisoryOnly,
		LiveMutationVerifierEnabled: liveVerifierEnabled,
		LiveMutationVerifierSource:  liveVerifierSource,
		LiveMutationActuatorEnabled: true,
		LiveMutationActuatorSource:  repoauthority.MutationActivationLiveActuatorSourceRuntime,
		Source:                      repoauthority.MutationActivationSourceDurableControlledQueueCandidate,
		Candidate:                   repoMutationCandidateSummary(candidate),
		WorktreeIdentity:            repoMutationWorktreeIdentityFromCandidate(ctx, candidate),
		TargetWorktreeIdentity:      repoMutationTargetWorktreeIdentityFromCandidate(ctx, candidate),
		Context:                     repoMutationContextFromCandidate(candidate),
		PatchQueueItem:              repoMutationPatchQueueItemFromCandidate(candidate),
		RollbackEvidence:            repoMutationRollbackEvidenceFromCandidate(candidate),
		ReviewerAdvisory:            repoMutationReviewerAdvisoryFromCandidate(candidate),
		OperatorEnablement:          repoMutationOperatorEnablementFromCandidate(candidate),
		PatchMaterialization:        repoMutationPatchMaterializationFromCandidate(candidate),
		PatchMaterializationProof:   repoMutationPatchMaterializationAuthorityProofFromCandidate(candidate),
	})
}

func applyRepoMutationMaterializationToTarget(ctx context.Context, candidate sqlite.ProjectRepoMutationActivationCandidate, activation repoauthority.MutationActivationGateResult) (repoauthority.MutationActuatorLiveResult, error) {
	return applyRepoMutationMaterializationToTargetMode(ctx, candidate, activation, false)
}

func recoverRepoMutationMaterializationInTarget(ctx context.Context, candidate sqlite.ProjectRepoMutationActivationCandidate, activation repoauthority.MutationActivationGateResult) (repoauthority.MutationActuatorLiveResult, error) {
	return applyRepoMutationMaterializationToTargetMode(ctx, candidate, activation, true)
}

func applyRepoMutationMaterializationToTargetMode(ctx context.Context, candidate sqlite.ProjectRepoMutationActivationCandidate, activation repoauthority.MutationActivationGateResult, allowDirtyRecovery bool) (repoauthority.MutationActuatorLiveResult, error) {
	if err := repoauthority.VerifyMutationActivationGateResult(activation); err != nil {
		return repoauthority.MutationActuatorLiveResult{}, err
	}
	if activation.TargetWorktreeIdentity == nil {
		return repoauthority.MutationActuatorLiveResult{}, fmt.Errorf("mutation activation is not ready for live actuator")
	}
	if !activation.MutationAllowed && (!allowDirtyRecovery || !repoMutationActuatorCanRecoverBlockedActivation(activation)) {
		return repoauthority.MutationActuatorLiveResult{}, fmt.Errorf("mutation activation is not ready for live actuator")
	}
	item := candidate.QueueItem
	materialization := item.Materialization
	appliedItem := repoMutationPatchQueueItemForMaterialization(candidate)
	if err := repoauthority.ValidatePatchMaterialization(materialization, appliedItem); err != nil {
		return repoauthority.MutationActuatorLiveResult{}, fmt.Errorf("validate materialization before actuator apply: %w", err)
	}
	if err := repoauthority.ValidatePatchMaterializationAuthorityProof(item.MaterializationAuthorityProof, materialization, appliedItem); err != nil {
		return repoauthority.MutationActuatorLiveResult{}, fmt.Errorf("validate materialization authority proof before actuator apply: %w", err)
	}
	target := *activation.TargetWorktreeIdentity
	beforeReadback := repoMutationReadGitWorktree(ctx, target.LocalPath, target.BranchName, target.HeadSHA)
	if beforeReadback.State != "ok" && !(allowDirtyRecovery && beforeReadback.State == "dirty") {
		return repoauthority.MutationActuatorLiveResult{}, fmt.Errorf("target worktree is not clean before actuator apply: %s: %s", beforeReadback.State, beforeReadback.Error)
	}
	plans := make([]repoMutationTargetFilePlan, 0, len(materialization.Files))
	for _, file := range materialization.Files {
		plan, err := planRepoMutationTargetFile(target.LocalPath, file)
		if err != nil {
			return repoauthority.MutationActuatorLiveResult{}, err
		}
		plans = append(plans, plan)
	}
	sort.Slice(plans, func(i, j int) bool {
		return plans[i].File.Path < plans[j].File.Path
	})
	expectedDirtyPaths := repoMutationExpectedFinalDirtyPaths(plans)
	if allowDirtyRecovery {
		beforeDirtyPaths, err := repoMutationTargetDirtyPaths(ctx, target.LocalPath)
		if err != nil {
			return repoauthority.MutationActuatorLiveResult{}, err
		}
		if !repoMutationPathSubset(beforeDirtyPaths, expectedDirtyPaths) {
			return repoauthority.MutationActuatorLiveResult{}, fmt.Errorf("target dirty pathset is outside actuator recovery scope: got %#v allowed %#v", beforeDirtyPaths, expectedDirtyPaths)
		}
	}
	for _, plan := range plans {
		if plan.Status != repoauthority.MutationActuatorLiveFileStatusApplied {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(plan.FullPath), 0o755); err != nil {
			return repoauthority.MutationActuatorLiveResult{}, fmt.Errorf("create parent for %s: %w", plan.File.Path, err)
		}
		if err := os.WriteFile(plan.FullPath, []byte(plan.File.Content), 0o644); err != nil {
			return repoauthority.MutationActuatorLiveResult{}, fmt.Errorf("write materialized file %s: %w", plan.File.Path, err)
		}
		afterHash, exists, err := repoMutationTargetFileHash(plan.FullPath)
		if err != nil {
			return repoauthority.MutationActuatorLiveResult{}, err
		}
		if !exists || afterHash != plan.File.ContentDigest {
			return repoauthority.MutationActuatorLiveResult{}, fmt.Errorf("post-apply digest mismatch for %s", plan.File.Path)
		}
	}
	afterHead, err := repoMutationGitOutput(ctx, target.LocalPath, "rev-parse", "HEAD")
	if err != nil {
		return repoauthority.MutationActuatorLiveResult{}, err
	}
	afterBranch, err := repoMutationGitOutput(ctx, target.LocalPath, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return repoauthority.MutationActuatorLiveResult{}, err
	}
	if strings.TrimSpace(afterHead) != strings.TrimSpace(target.HeadSHA) {
		return repoauthority.MutationActuatorLiveResult{}, fmt.Errorf("target HEAD changed during actuator apply")
	}
	if strings.TrimSpace(afterBranch) != strings.TrimSpace(target.BranchName) {
		return repoauthority.MutationActuatorLiveResult{}, fmt.Errorf("target branch changed during actuator apply")
	}
	dirtyPaths, err := repoMutationTargetDirtyPaths(ctx, target.LocalPath)
	if err != nil {
		return repoauthority.MutationActuatorLiveResult{}, err
	}
	if !sameStringSliceForRepoMutationActuator(dirtyPaths, expectedDirtyPaths) {
		return repoauthority.MutationActuatorLiveResult{}, fmt.Errorf("target dirty pathset mismatch after actuator apply: got %#v want %#v", dirtyPaths, expectedDirtyPaths)
	}
	targetDirtyState := "clean"
	if len(dirtyPaths) > 0 {
		targetDirtyState = "dirty"
	}
	files := make([]repoauthority.MutationActuatorLiveFileResult, 0, len(plans))
	mutationExecuted := false
	for _, plan := range plans {
		afterHash, exists, err := repoMutationTargetFileHash(plan.FullPath)
		if err != nil {
			return repoauthority.MutationActuatorLiveResult{}, err
		}
		if !exists || afterHash != plan.File.ContentDigest {
			return repoauthority.MutationActuatorLiveResult{}, fmt.Errorf("post-apply readback failed for %s", plan.File.Path)
		}
		if plan.Status == repoauthority.MutationActuatorLiveFileStatusApplied {
			mutationExecuted = true
		}
		files = append(files, repoauthority.MutationActuatorLiveFileResult{
			Path:          plan.File.Path,
			Status:        plan.Status,
			ChangeKind:    repoMutationMaterializedChangeKind(plan.File),
			BaseHash:      strings.TrimSpace(plan.File.BaseHash),
			BeforeHash:    plan.BeforeHash,
			CandidateHash: strings.TrimSpace(plan.File.CandidateHash),
			AfterHash:     afterHash,
			ContentDigest: strings.TrimSpace(plan.File.ContentDigest),
		})
	}
	return repoauthority.FinalizeMutationActuatorLiveResult(repoauthority.MutationActuatorLiveResult{
		WorkspaceID:                         item.WorkspaceID,
		ProjectID:                           item.ProjectID,
		RepoID:                              item.RepoID,
		QueueID:                             item.QueueID,
		ItemID:                              item.ItemID,
		TargetCheckoutID:                    target.CheckoutID,
		TargetBranchName:                    target.BranchName,
		TargetLocalPath:                     target.LocalPath,
		TargetHeadBefore:                    target.HeadSHA,
		TargetHeadAfter:                     strings.TrimSpace(afterHead),
		TargetDirtyStateAfter:               targetDirtyState,
		ActivationDigest:                    activation.Digest,
		MaterializationDigest:               item.MaterializationDigest,
		MaterializationAuthorityProofDigest: item.MaterializationAuthorityProofDigest,
		Files:                               files,
		MutationExecuted:                    mutationExecuted,
	}), nil
}

func planRepoMutationTargetFile(targetRoot string, file repoauthority.PatchMaterializedFile) (repoMutationTargetFilePlan, error) {
	fullPath, err := repoMutationTargetFilePath(targetRoot, file.Path)
	if err != nil {
		return repoMutationTargetFilePlan{}, err
	}
	beforeHash, exists, err := repoMutationTargetFileHash(fullPath)
	if err != nil {
		return repoMutationTargetFilePlan{}, err
	}
	changeKind := repoMutationMaterializedChangeKind(file)
	candidateHash := strings.TrimSpace(file.ContentDigest)
	if candidateHash == "" {
		candidateHash = strings.TrimSpace(file.CandidateHash)
	}
	plan := repoMutationTargetFilePlan{File: file, FullPath: fullPath, BeforeHash: beforeHash}
	switch changeKind {
	case repoauthority.CASPatchChangeAdd:
		switch {
		case !exists:
			plan.Status = repoauthority.MutationActuatorLiveFileStatusApplied
		case beforeHash == candidateHash:
			plan.Status = repoauthority.MutationActuatorLiveFileStatusAlreadyApplied
		default:
			return repoMutationTargetFilePlan{}, fmt.Errorf("add target already exists with different content: %s", file.Path)
		}
	case repoauthority.CASPatchChangeModify:
		switch {
		case beforeHash == candidateHash:
			plan.Status = repoauthority.MutationActuatorLiveFileStatusAlreadyApplied
		case beforeHash == strings.TrimSpace(file.BaseHash):
			plan.Status = repoauthority.MutationActuatorLiveFileStatusApplied
		default:
			return repoMutationTargetFilePlan{}, fmt.Errorf("modify target base hash mismatch for %s", file.Path)
		}
	default:
		return repoMutationTargetFilePlan{}, fmt.Errorf("change kind %q is outside live actuator add/modify scope", file.ChangeKind)
	}
	return plan, nil
}

func repoMutationExpectedFinalDirtyPaths(plans []repoMutationTargetFilePlan) []string {
	paths := make([]string, 0, len(plans))
	for _, plan := range plans {
		switch repoMutationMaterializedChangeKind(plan.File) {
		case repoauthority.CASPatchChangeAdd:
			paths = append(paths, plan.File.Path)
		case repoauthority.CASPatchChangeModify:
			if strings.TrimSpace(plan.File.CandidateHash) != strings.TrimSpace(plan.File.BaseHash) {
				paths = append(paths, plan.File.Path)
			}
		}
	}
	sort.Strings(paths)
	return paths
}

func repoMutationPathSubset(paths, allowed []string) bool {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, path := range allowed {
		allowedSet[strings.TrimSpace(path)] = struct{}{}
	}
	for _, path := range paths {
		if _, ok := allowedSet[strings.TrimSpace(path)]; !ok {
			return false
		}
	}
	return true
}

func repoMutationTargetDirtyPaths(ctx context.Context, targetRoot string) ([]string, error) {
	status, err := repoMutationGitRawOutput(ctx, targetRoot, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(status) == "" {
		return []string{}, nil
	}
	paths := make([]string, 0)
	seen := map[string]struct{}{}
	for _, rawLine := range strings.Split(status, "\n") {
		line := strings.TrimRight(rawLine, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		if len(line) < 4 {
			return nil, fmt.Errorf("cannot parse git status line %q", line)
		}
		path := strings.TrimSpace(line[3:])
		if strings.Contains(path, " -> ") {
			parts := strings.Split(path, " -> ")
			path = strings.TrimSpace(parts[len(parts)-1])
		}
		normalized, err := repoauthority.NormalizePath(strings.Trim(path, `"`))
		if err != nil {
			return nil, fmt.Errorf("git status path %q: %w", path, err)
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		paths = append(paths, normalized)
	}
	sort.Strings(paths)
	return paths, nil
}

func repoMutationGitRawOutput(ctx context.Context, localPath string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", localPath}, args...)...)
	raw, err := cmd.CombinedOutput()
	output := string(raw)
	if err != nil {
		message := strings.TrimSpace(output)
		if message == "" {
			message = err.Error()
		}
		return "", fmt.Errorf("git %s: %s", strings.Join(args, " "), message)
	}
	return output, nil
}

func sameStringSliceForRepoMutationActuator(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if strings.TrimSpace(left[i]) != strings.TrimSpace(right[i]) {
			return false
		}
	}
	return true
}

func repoMutationTargetFilePath(targetRoot string, relativePath string) (string, error) {
	normalized, err := repoauthority.NormalizePath(relativePath)
	if err != nil {
		return "", err
	}
	root, err := filepath.Abs(strings.TrimSpace(targetRoot))
	if err != nil {
		return "", err
	}
	fullPath, err := filepath.Abs(filepath.Join(root, filepath.FromSlash(normalized)))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(root, fullPath)
	if err != nil {
		return "", err
	}
	if rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", fmt.Errorf("materialized path escapes target root: %s", relativePath)
	}
	return fullPath, nil
}

func repoMutationTargetFileHash(fullPath string) (string, bool, error) {
	raw, err := os.ReadFile(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("read target file %s: %w", fullPath, err)
	}
	return repoauthority.PatchMaterializationContentDigest(string(raw)), true, nil
}

func repoMutationMaterializedChangeKind(file repoauthority.PatchMaterializedFile) string {
	if kind := strings.TrimSpace(file.ChangeKind); kind != "" {
		return kind
	}
	return repoauthority.CASPatchChangeModify
}

func repoMutationPatchQueueItemForMaterialization(candidate sqlite.ProjectRepoMutationActivationCandidate) repoauthority.PatchQueueItem {
	applied := repoMutationPatchQueueItemFromCandidate(candidate)
	raw := candidate.QueueItem
	applied.State = repoauthority.PatchQueueStateApplied
	applied.OperationID = firstNonEmpty(applied.OperationID, raw.OperationID)
	applied.OperationKind = firstNonEmpty(applied.OperationKind, raw.OperationKind)
	applied.CASResult = raw.CASResult
	applied.CASPatchDigest = firstNonEmpty(applied.CASPatchDigest, raw.CASPatchDigest)
	applied.CASEvaluationDigest = firstNonEmpty(applied.CASEvaluationDigest, raw.CASEvaluationDigest)
	applied.TestEvidence = raw.CASTestEvidence
	applied.TestEvidenceDigest = firstNonEmpty(applied.TestEvidenceDigest, raw.CASTestEvidenceDigest)
	applied.RollbackEvidence = raw.RollbackEvidence
	applied.RollbackEvidenceDigest = firstNonEmpty(applied.RollbackEvidenceDigest, raw.RollbackEvidenceDigest)
	applied.ReviewerAdvisory = raw.ReviewerAdvisory
	applied.ReviewerAdvisoryDigest = firstNonEmpty(applied.ReviewerAdvisoryDigest, raw.ReviewerAdvisoryDigest)
	applied.OperatorEnablement = raw.OperatorEnablement
	applied.OperatorEnablementDigest = firstNonEmpty(applied.OperatorEnablementDigest, raw.OperatorEnablementDigest)
	return applied
}
