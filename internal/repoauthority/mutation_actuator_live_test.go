package repoauthority

import (
	"strings"
	"testing"
)

func TestMutationActuatorLiveResultVerifiesAppliedModify(t *testing.T) {
	baseHash := PatchMaterializationContentDigest("base\n")
	candidateHash := PatchMaterializationContentDigest("candidate\n")
	result := FinalizeMutationActuatorLiveResult(MutationActuatorLiveResult{
		WorkspaceID:                         "ws-live",
		ProjectID:                           "project-live",
		RepoID:                              "repo-main",
		QueueID:                             "queue-live",
		ItemID:                              "item-live",
		TargetCheckoutID:                    "checkout-integration",
		TargetBranchName:                    "main",
		TargetLocalPath:                     "C:/fixtures/agents/integration/project",
		TargetHeadBefore:                    strings.Repeat("a", 40),
		TargetHeadAfter:                     strings.Repeat("a", 40),
		TargetDirtyStateAfter:               "dirty",
		ActivationDigest:                    "sha256:" + strings.Repeat("1", 64),
		MaterializationDigest:               "sha256:" + strings.Repeat("2", 64),
		MaterializationAuthorityProofDigest: "sha256:" + strings.Repeat("3", 64),
		Files: []MutationActuatorLiveFileResult{
			{
				Path:          "web/app.js",
				Status:        MutationActuatorLiveFileStatusApplied,
				ChangeKind:    CASPatchChangeModify,
				BaseHash:      baseHash,
				BeforeHash:    baseHash,
				CandidateHash: candidateHash,
				AfterHash:     candidateHash,
				ContentDigest: candidateHash,
			},
		},
		MutationExecuted: true,
	})
	if err := VerifyMutationActuatorLiveResult(result); err != nil {
		t.Fatalf("VerifyMutationActuatorLiveResult: %v", err)
	}
}

func TestMutationActuatorLiveResultRejectsHeadMovement(t *testing.T) {
	hash := PatchMaterializationContentDigest("candidate\n")
	result := FinalizeMutationActuatorLiveResult(MutationActuatorLiveResult{
		WorkspaceID:                         "ws-live",
		RepoID:                              "repo-main",
		QueueID:                             "queue-live",
		ItemID:                              "item-live",
		TargetCheckoutID:                    "checkout-integration",
		TargetBranchName:                    "main",
		TargetLocalPath:                     "C:/repo",
		TargetHeadBefore:                    strings.Repeat("a", 40),
		TargetHeadAfter:                     strings.Repeat("b", 40),
		ActivationDigest:                    "sha256:" + strings.Repeat("1", 64),
		MaterializationDigest:               "sha256:" + strings.Repeat("2", 64),
		MaterializationAuthorityProofDigest: "sha256:" + strings.Repeat("3", 64),
		Files: []MutationActuatorLiveFileResult{
			{
				Path:          "web/app.js",
				Status:        MutationActuatorLiveFileStatusAlreadyApplied,
				ChangeKind:    CASPatchChangeAdd,
				BeforeHash:    hash,
				CandidateHash: hash,
				AfterHash:     hash,
				ContentDigest: hash,
			},
		},
	})
	if err := VerifyMutationActuatorLiveResult(result); err == nil || !strings.Contains(err.Error(), "changed target HEAD") {
		t.Fatalf("expected head movement to fail, got %v", err)
	}
}

func TestMutationActuatorLiveResultRejectsNonCanonicalTargetHead(t *testing.T) {
	hash := PatchMaterializationContentDigest("candidate\n")
	result := FinalizeMutationActuatorLiveResult(MutationActuatorLiveResult{
		WorkspaceID:                         "ws-live",
		RepoID:                              "repo-main",
		QueueID:                             "queue-live",
		ItemID:                              "item-live",
		TargetCheckoutID:                    "checkout-integration",
		TargetBranchName:                    "main",
		TargetLocalPath:                     "C:/repo",
		TargetHeadBefore:                    "HEAD",
		TargetHeadAfter:                     "HEAD",
		ActivationDigest:                    "sha256:" + strings.Repeat("1", 64),
		MaterializationDigest:               "sha256:" + strings.Repeat("2", 64),
		MaterializationAuthorityProofDigest: "sha256:" + strings.Repeat("3", 64),
		Files: []MutationActuatorLiveFileResult{
			{
				Path:          "web/app.js",
				Status:        MutationActuatorLiveFileStatusAlreadyApplied,
				ChangeKind:    CASPatchChangeAdd,
				BeforeHash:    hash,
				CandidateHash: hash,
				AfterHash:     hash,
				ContentDigest: hash,
			},
		},
	})
	if err := VerifyMutationActuatorLiveResult(result); err == nil || !strings.Contains(err.Error(), "canonical git object id") {
		t.Fatalf("expected non-canonical target head to fail, got %v", err)
	}
}
