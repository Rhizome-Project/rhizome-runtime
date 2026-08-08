package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/repoauthority"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRepoMutationActuatorAppliesMaterializationToTarget(t *testing.T) {
	gitFixture := newReadinessGitFixture(t)
	candidate, activation := newRepoMutationActuatorApplyFixture(t, gitFixture)

	result, err := applyRepoMutationMaterializationToTarget(t.Context(), candidate, activation)
	if err != nil {
		t.Fatalf("applyRepoMutationMaterializationToTarget: %v", err)
	}
	if err := repoauthority.VerifyMutationActuatorLiveResult(result); err != nil {
		t.Fatalf("VerifyMutationActuatorLiveResult: %v", err)
	}
	if !result.MutationExecuted || len(result.Files) != 1 || result.Files[0].Status != repoauthority.MutationActuatorLiveFileStatusApplied {
		t.Fatalf("expected one applied file mutation, got %+v", result)
	}
	raw, err := os.ReadFile(filepath.Join(gitFixture.TargetPath, "web-app.txt"))
	if err != nil {
		t.Fatalf("read target file: %v", err)
	}
	if string(raw) != "head\n" {
		t.Fatalf("target file content = %q, want head", raw)
	}
	if result.TargetHeadBefore != gitFixture.BaseSHA || result.TargetHeadAfter != gitFixture.BaseSHA {
		t.Fatalf("actuator must not move target HEAD, got before=%s after=%s base=%s", result.TargetHeadBefore, result.TargetHeadAfter, gitFixture.BaseSHA)
	}
	if branch := runReadinessGit(t, gitFixture.TargetPath, "rev-parse", "--abbrev-ref", "HEAD"); branch != "main" {
		t.Fatalf("actuator must not switch target branch, got %s", branch)
	}
	if status := runReadinessGit(t, gitFixture.TargetPath, "status", "--porcelain"); !strings.Contains(status, "web-app.txt") {
		t.Fatalf("expected target checkout to contain staged-for-review dirty file, got status %q", status)
	}
}

func TestRepoMutationActuatorRecoversAlreadyMaterializedTarget(t *testing.T) {
	gitFixture := newReadinessGitFixture(t)
	candidate, activation := newRepoMutationActuatorApplyFixture(t, gitFixture)

	if _, err := applyRepoMutationMaterializationToTarget(t.Context(), candidate, activation); err != nil {
		t.Fatalf("initial apply: %v", err)
	}
	recovered, err := recoverRepoMutationMaterializationInTarget(t.Context(), candidate, activation)
	if err != nil {
		t.Fatalf("recover already materialized target: %v", err)
	}
	if err := repoauthority.VerifyMutationActuatorLiveResult(recovered); err != nil {
		t.Fatalf("VerifyMutationActuatorLiveResult: %v", err)
	}
	if recovered.MutationExecuted {
		t.Fatalf("recovery should record already-applied evidence without another write: %+v", recovered)
	}
	if len(recovered.Files) != 1 || recovered.Files[0].Status != repoauthority.MutationActuatorLiveFileStatusAlreadyApplied {
		t.Fatalf("expected already_applied recovery evidence, got %+v", recovered.Files)
	}
}

func TestRepoMutationActuatorRecoveryRequiresDirtyOnlyTargetIdentity(t *testing.T) {
	gitFixture := newReadinessGitFixture(t)
	_, activation := newRepoMutationActuatorApplyFixture(t, gitFixture)
	dirtyOnly := activation
	dirtyOnly.MutationAllowed = false
	dirtyOnly.BlockingReasons = []string{"live_actuator_target_identity: git worktree has uncommitted changes"}
	target := *dirtyOnly.TargetWorktreeIdentity
	target.ReadbackState = "dirty"
	target.ReadbackError = "git worktree has uncommitted changes"
	target.ObservedDirtyState = "dirty"
	dirtyOnly.TargetWorktreeIdentity = &target
	if !repoMutationActuatorCanRecoverBlockedActivation(dirtyOnly) {
		t.Fatalf("expected dirty-only target identity to be recoverable")
	}

	aliased := dirtyOnly
	aliasedTarget := *aliased.TargetWorktreeIdentity
	aliasedTarget.LocalPath = aliased.WorktreeIdentity.LocalPath
	aliasedTarget.ObservedWorktreeRoot = aliased.WorktreeIdentity.ObservedWorktreeRoot
	aliased.TargetWorktreeIdentity = &aliasedTarget
	if repoMutationActuatorCanRecoverBlockedActivation(aliased) {
		t.Fatalf("target path alias must not be recoverable")
	}

	headDrift := dirtyOnly
	driftTarget := *headDrift.TargetWorktreeIdentity
	driftTarget.HeadSHA = dirtyOnly.WorktreeIdentity.HeadSHA
	driftTarget.ObservedHeadSHA = dirtyOnly.WorktreeIdentity.HeadSHA
	headDrift.TargetWorktreeIdentity = &driftTarget
	if repoMutationActuatorCanRecoverBlockedActivation(headDrift) {
		t.Fatalf("target head drift must not be recoverable")
	}
}

func newRepoMutationActuatorApplyFixture(t *testing.T, gitFixture readinessGitFixture) (sqlite.ProjectRepoMutationActivationCandidate, repoauthority.MutationActivationGateResult) {
	t.Helper()
	now := time.Date(2026, 4, 26, 2, 0, 0, 0, time.UTC)
	const (
		workspaceID = "ws-live-actuator"
		projectID   = "project-live-actuator"
		repoID      = "repo-main"
		queueID     = "queue-live-actuator"
		itemID      = "item-live-actuator"
		branchID    = "branch-live-actuator"
		reviewKey   = "project.project-live-actuator.branch.branch-live-actuator.review"
		agentID     = "worker-alpha"
	)
	baseRaw, err := os.ReadFile(filepath.Join(gitFixture.TargetPath, "web-app.txt"))
	if err != nil {
		t.Fatalf("read target base fixture file: %v", err)
	}
	baseContent := string(baseRaw)
	candidateContent := "head\n"
	baseHash := repoauthority.PatchMaterializationContentDigest(baseContent)
	candidateHash := repoauthority.PatchMaterializationContentDigest(candidateContent)
	authority := repoauthority.Context{
		Mode:        repoauthority.ModeControlledQueue,
		WorkspaceID: workspaceID,
		TaskID:      "task-live-actuator",
		SessionID:   "session-live-actuator",
		RunID:       "run-live-actuator",
		AgentID:     agentID,
		Principal:   repoauthority.PrincipalRef{Type: "agent", ID: agentID},
		CapabilitySnapshot: repoauthority.CapabilitySnapshotRef{
			ID:     "cap-live-actuator",
			Schema: "daemon_capability_snapshot.v1",
		},
		RepoRoot: gitFixture.Path,
		Base: repoauthority.BaseIdentity{
			Ref:      "main",
			TreeHash: "sha256:" + strings.Repeat("b", 64),
			FileHashes: map[string]string{
				"web-app.txt": baseHash,
			},
		},
		Pathset: []string{"web-app.txt"},
		Lease:   repoauthority.LeaseRef{ID: "lease-live-actuator", Term: 7},
		PatchQueue: repoauthority.PatchQueueRef{
			QueueID: queueID,
			ItemID:  itemID,
		},
		Operation: repoauthority.OperationRef{
			ID:   "op-live-actuator-apply",
			Kind: sqlite.ProjectPatchQueueOperationKindRepoPatchApply,
		},
	}
	patchQueueContext := authority.WithDefaults()
	patchQueueContext.Operation = repoauthority.OperationRef{}
	contextDigest, err := patchQueueContext.Digest()
	if err != nil {
		t.Fatalf("authority digest: %v", err)
	}
	cas := repoauthority.EvaluateCASPatchApply(repoauthority.CASPatchApplyInput{
		Context:           authority,
		CurrentFileHashes: map[string]string{"web-app.txt": baseHash},
		CandidateFileHashes: map[string]string{
			"web-app.txt": candidateHash,
		},
	})
	if cas.Status != repoauthority.CASPatchStatusApplied {
		t.Fatalf("CAS status = %q issues=%+v", cas.Status, cas.Issues)
	}
	testEvidence := repoauthority.PatchQueueTestEvidence{
		Schema:       repoauthority.PatchQueueTestEvidenceSchemaVersion,
		Name:         "live actuator unit",
		Command:      "go test ./cmd/rhizome -run TestRepoMutationActuatorAppliesMaterializationToTarget",
		Status:       repoauthority.PatchQueueTestStatusPassed,
		ExitCode:     0,
		OutputDigest: "sha256:" + strings.Repeat("7", 64),
	}
	appliedItem := repoauthority.PatchQueueItem{
		Schema:              repoauthority.PatchQueueItemSchemaVersion,
		ID:                  queueID + "/" + itemID,
		QueueID:             queueID,
		ItemID:              itemID,
		ReviewDocKey:        reviewKey,
		State:               repoauthority.PatchQueueStateApplied,
		Attempt:             1,
		MaxAttempts:         3,
		ContextDigest:       contextDigest,
		RepoLeaseID:         "lease-live-actuator",
		LeaseTerm:           7,
		Pathset:             []string{"web-app.txt"},
		WorkspaceID:         workspaceID,
		ProjectID:           projectID,
		TaskID:              "task-live-actuator",
		SessionID:           "session-live-actuator",
		RunID:               "run-live-actuator",
		AgentID:             agentID,
		PrincipalType:       "agent",
		PrincipalID:         agentID,
		BaseRef:             "main",
		BaseTreeHash:        "sha256:" + strings.Repeat("b", 64),
		CASResult:           cas,
		CASPatchDigest:      cas.PatchDigest,
		CASEvaluationDigest: repoauthority.PatchQueueCASEvaluationDigest(cas),
		TestEvidence:        testEvidence,
		TestEvidenceDigest:  repoauthority.PatchQueueTestEvidenceDigest(testEvidence),
		OperationID:         authority.Operation.ID,
		OperationKind:       authority.Operation.Kind,
	}
	rollback, err := repoauthority.NormalizePatchQueueRollbackEvidence(repoauthority.PatchQueueRollback{
		Reason:                     "prove rollback before live actuator unit",
		SourcePatchDigest:          cas.PatchDigest,
		VerificationCommand:        "go test ./cmd/rhizome",
		VerificationStatus:         repoauthority.PatchQueueTestStatusPassed,
		VerificationExitCode:       0,
		VerificationOutputDigest:   "sha256:" + strings.Repeat("8", 64),
		VerificationOutputSummary:  "ok",
		VerificationDurationMillis: 10,
		RollbackPaths: []repoauthority.PatchQueueRollbackPath{
			{Path: "web-app.txt", SourceBaseHash: baseHash, SourceAppliedHash: candidateHash, RollbackCandidateHash: baseHash},
		},
	}, appliedItem, repoauthority.OperationRef{ID: "op-live-actuator-rollback", Kind: sqlite.ProjectPatchQueueOperationKindRepoPatchApply}, now.Add(time.Second))
	if err != nil {
		t.Fatalf("NormalizePatchQueueRollbackEvidence: %v", err)
	}
	appliedItem.RollbackEvidence = rollback
	appliedItem.RollbackEvidenceDigest = repoauthority.PatchQueueRollbackEvidenceDigest(rollback)
	reviewer := repoauthority.PatchQueueReviewerAdvisory{
		Schema:                 repoauthority.PatchQueueReviewerAdvisorySchema,
		Mode:                   repoauthority.MutationActivationReviewerMeshAdvisoryOnly,
		Verdict:                repoauthority.PatchQueueReviewerAdvisoryVerdictReviewed,
		ReviewerID:             "reviewer-alpha",
		ReviewDocKey:           reviewKey,
		OperationID:            appliedItem.OperationID,
		OperationKind:          appliedItem.OperationKind,
		CASPatchDigest:         appliedItem.CASPatchDigest,
		CASEvaluationDigest:    appliedItem.CASEvaluationDigest,
		RollbackEvidenceDigest: appliedItem.RollbackEvidenceDigest,
		Summary:                "reviewed",
		RecordedAt:             now.Add(2 * time.Second).Format(time.RFC3339Nano),
	}
	appliedItem.ReviewerAdvisory = reviewer
	appliedItem.ReviewerAdvisoryDigest = repoauthority.PatchQueueReviewerAdvisoryDigest(reviewer)
	enablement := repoauthority.PatchQueueOperatorEnablement{
		Schema:                 repoauthority.PatchQueueOperatorEnablementSchema,
		Scope:                  repoauthority.MutationActivationOperatorEnablementScope,
		Enabled:                true,
		EnabledBy:              "operator-human",
		EnabledAt:              now.Add(3 * time.Second).Format(time.RFC3339Nano),
		Reason:                 "allow live actuator unit",
		WorkspaceID:            workspaceID,
		ProjectID:              projectID,
		QueueID:                queueID,
		ItemID:                 itemID,
		OperationID:            appliedItem.OperationID,
		CASPatchDigest:         appliedItem.CASPatchDigest,
		RollbackEvidenceDigest: appliedItem.RollbackEvidenceDigest,
		ReviewerAdvisoryDigest: appliedItem.ReviewerAdvisoryDigest,
	}
	appliedItem.OperatorEnablement = enablement
	appliedItem.OperatorEnablementDigest = repoauthority.PatchQueueOperatorEnablementDigest(enablement)
	materialization, err := repoauthority.NormalizePatchMaterialization(repoauthority.PatchMaterialization{
		RecordedBy: "integrator-alpha",
		Files: []repoauthority.PatchMaterializedFile{
			{Path: "web-app.txt", Content: candidateContent},
		},
	}, appliedItem, now.Add(4*time.Second))
	if err != nil {
		t.Fatalf("NormalizePatchMaterialization: %v", err)
	}
	proof, err := repoauthority.BuildPatchMaterializationAuthorityProof(materialization, appliedItem)
	if err != nil {
		t.Fatalf("BuildPatchMaterializationAuthorityProof: %v", err)
	}
	activationInput := repoauthority.MutationActivationGateInput{
		AuthorityMode:               repoauthority.ModeControlledQueue,
		DirectMergeDisabled:         true,
		QueueDurable:                true,
		ReviewerMeshMode:            repoauthority.MutationActivationReviewerMeshAdvisoryOnly,
		LiveMutationVerifierEnabled: true,
		LiveMutationVerifierSource:  repoauthority.MutationActivationLiveVerifierSourceEnv,
		LiveMutationActuatorEnabled: true,
		LiveMutationActuatorSource:  repoauthority.MutationActivationLiveActuatorSourceRuntime,
		Source:                      repoauthority.MutationActivationSourceDurableControlledQueueCandidate,
		Candidate: &repoauthority.MutationActivationCandidateSummary{
			WorkspaceID:      workspaceID,
			ProjectID:        projectID,
			RepoID:           repoID,
			QueueID:          queueID,
			ItemID:           itemID,
			BranchID:         branchID,
			BranchName:       gitFixture.BranchName,
			CheckoutID:       "checkout-worker",
			TargetCheckoutID: "checkout-integration",
			TargetBranchName: gitFixture.TargetBranchName,
			State:            sqlite.ProjectPatchQueueStateClaimed,
			BaseSHA:          gitFixture.BaseSHA,
			HeadSHA:          gitFixture.HeadSHA,
		},
		WorktreeIdentity: repoauthority.WorktreeIdentityEvidence{
			RepoID:               repoID,
			CheckoutID:           "checkout-worker",
			BranchID:             branchID,
			BranchName:           gitFixture.BranchName,
			MachineID:            "machine-test",
			LocalPath:            gitFixture.Path,
			BaseSHA:              gitFixture.BaseSHA,
			HeadSHA:              gitFixture.HeadSHA,
			ReadbackState:        "ok",
			ObservedWorktreeRoot: gitFixture.Path,
			ObservedBranchName:   gitFixture.BranchName,
			ObservedHeadSHA:      gitFixture.HeadSHA,
			ObservedDirtyState:   "clean",
		},
		TargetWorktreeIdentity: repoauthority.WorktreeIdentityEvidence{
			RepoID:               repoID,
			CheckoutID:           "checkout-integration",
			BranchID:             "checkout-integration",
			BranchName:           gitFixture.TargetBranchName,
			MachineID:            "machine-test",
			LocalPath:            gitFixture.TargetPath,
			BaseSHA:              gitFixture.BaseSHA,
			HeadSHA:              gitFixture.BaseSHA,
			ReadbackState:        "ok",
			ObservedWorktreeRoot: gitFixture.TargetPath,
			ObservedBranchName:   gitFixture.TargetBranchName,
			ObservedHeadSHA:      gitFixture.BaseSHA,
			ObservedDirtyState:   "clean",
		},
		Context:                   authority,
		PatchQueueItem:            appliedItem,
		RollbackEvidence:          rollback,
		ReviewerAdvisory:          reviewer,
		OperatorEnablement:        enablement,
		PatchMaterialization:      materialization,
		PatchMaterializationProof: proof,
	}
	activation := repoauthority.EvaluateMutationActivationGates(activationInput)
	if err := repoauthority.VerifyMutationActivationGateResult(activation); err != nil {
		t.Fatalf("VerifyMutationActivationGateResult: %v activation=%+v", err, activation)
	}
	if !activation.MutationAllowed {
		t.Fatalf("expected live activation to allow mutation, got %+v", activation.BlockingReasons)
	}
	item := sqlite.ProjectPatchQueueItemRecord{
		QueueID:                             queueID,
		ItemID:                              itemID,
		WorkspaceID:                         workspaceID,
		ProjectID:                           projectID,
		RepoID:                              repoID,
		BranchID:                            branchID,
		ReviewDocKey:                        reviewKey,
		RepoAuthorityMode:                   sqlite.ProjectPatchQueueAuthorityModeControlledQueue,
		State:                               sqlite.ProjectPatchQueueStateClaimed,
		Attempt:                             1,
		MaxAttempts:                         3,
		Pathset:                             []string{"web-app.txt"},
		BaseRef:                             "main",
		BaseSHA:                             gitFixture.BaseSHA,
		HeadSHA:                             gitFixture.HeadSHA,
		AgentID:                             agentID,
		PrincipalType:                       "agent",
		PrincipalID:                         agentID,
		CapabilitySnapshotID:                "cap-live-actuator",
		CapabilitySnapshotSchema:            "daemon_capability_snapshot.v1",
		RepoRoot:                            gitFixture.Path,
		BaseTreeHash:                        "sha256:" + strings.Repeat("b", 64),
		BaseFileHashes:                      map[string]string{"web-app.txt": baseHash},
		ContextDigest:                       contextDigest,
		RepoLeaseID:                         "lease-live-actuator",
		LeaseTerm:                           7,
		OperationID:                         appliedItem.OperationID,
		OperationKind:                       appliedItem.OperationKind,
		CASResult:                           cas,
		CASPatchDigest:                      cas.PatchDigest,
		CASEvaluationDigest:                 appliedItem.CASEvaluationDigest,
		CASTestEvidence:                     testEvidence,
		CASTestEvidenceDigest:               appliedItem.TestEvidenceDigest,
		Materialization:                     materialization,
		MaterializationDigest:               materialization.MaterializationDigest,
		MaterializationAuthorityProof:       proof,
		MaterializationAuthorityProofDigest: proof.AuthorityDigest,
		RollbackEvidence:                    rollback,
		RollbackEvidenceDigest:              appliedItem.RollbackEvidenceDigest,
		ReviewerAdvisory:                    reviewer,
		ReviewerAdvisoryDigest:              appliedItem.ReviewerAdvisoryDigest,
		OperatorEnablement:                  enablement,
		OperatorEnablementDigest:            appliedItem.OperatorEnablementDigest,
	}
	return sqlite.ProjectRepoMutationActivationCandidate{QueueItem: item}, activation
}
