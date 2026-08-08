package repoauthority

import (
	"strings"
	"testing"
	"time"
)

func TestPatchMaterializationNormalizesAndValidatesContentBoundCAS(t *testing.T) {
	item, candidateContent := patchMaterializationTestItem(t, PatchMaterializationContentDigest("new\n"))

	materialization, err := NormalizePatchMaterialization(PatchMaterialization{
		RecordedBy: "integrator-agent",
		Files: []PatchMaterializedFile{
			{Path: "owned.go", Content: candidateContent},
		},
	}, item, time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("normalize materialization: %v", err)
	}
	if materialization.Schema != PatchMaterializationSchemaVersion ||
		materialization.WorkspaceID != item.WorkspaceID ||
		materialization.QueueID != item.QueueID ||
		materialization.ItemID != item.ItemID ||
		materialization.OperationID != item.OperationID ||
		materialization.CASPatchDigest != item.CASPatchDigest ||
		materialization.CASEvaluationDigest != item.CASEvaluationDigest {
		t.Fatalf("materialization did not inherit item identity: %+v", materialization)
	}
	if len(materialization.Files) != 1 ||
		materialization.Files[0].Path != "owned.go" ||
		materialization.Files[0].ContentEncoding != PatchMaterializationEncodingUTF8 ||
		materialization.Files[0].ContentDigest != PatchMaterializationContentDigest(candidateContent) ||
		materialization.Files[0].CandidateHash != materialization.Files[0].ContentDigest {
		t.Fatalf("materialized file did not normalize against content and CAS: %+v", materialization.Files)
	}
	if materialization.MaterializationDigest == "" || !strings.HasPrefix(materialization.MaterializationDigest, "sha256:") {
		t.Fatalf("expected materialization digest, got %q", materialization.MaterializationDigest)
	}
	if err := ValidatePatchMaterialization(materialization, item); err != nil {
		t.Fatalf("validate materialization: %v", err)
	}
}

func TestPatchMaterializationRejectsContentDigestDrift(t *testing.T) {
	item, candidateContent := patchMaterializationTestItem(t, PatchMaterializationContentDigest("new\n"))

	_, err := NormalizePatchMaterialization(PatchMaterialization{
		Files: []PatchMaterializedFile{
			{Path: "owned.go", Content: candidateContent, ContentDigest: "sha256:" + strings.Repeat("0", 64)},
		},
	}, item, time.Time{})
	if err == nil || !strings.Contains(err.Error(), "content_digest") {
		t.Fatalf("expected content digest drift to fail, got %v", err)
	}
}

func TestPatchMaterializationContentBoundsRejectOversizedInputs(t *testing.T) {
	item, _ := patchMaterializationTestItem(t, PatchMaterializationContentDigest("new\n"))

	_, err := NormalizePatchMaterialization(PatchMaterialization{
		Files: []PatchMaterializedFile{
			{Path: "owned.go", Content: strings.Repeat("x", int(PatchMaterializationMaxFileBytes)+1)},
		},
	}, item, time.Time{})
	if err == nil || !strings.Contains(err.Error(), "content policy") || !strings.Contains(err.Error(), "size") {
		t.Fatalf("expected oversized normalize to fail cheap content policy, got %v", err)
	}

	tooManyFiles := make([]PatchMaterializedFile, 0, PatchMaterializationMaxFiles+1)
	for i := 0; i <= PatchMaterializationMaxFiles; i++ {
		tooManyFiles = append(tooManyFiles, PatchMaterializedFile{Path: "bulk.txt", Content: "x"})
	}
	if err := ValidatePatchMaterializationContentBounds(PatchMaterialization{Files: tooManyFiles}); err == nil || !strings.Contains(err.Error(), "file count") {
		t.Fatalf("expected excessive file count to fail content policy, got %v", err)
	}

	exactUTF8Limit := strings.Repeat("é", int(PatchMaterializationMaxFileBytes/2))
	if got := int64(len([]byte(exactUTF8Limit))); got != PatchMaterializationMaxFileBytes {
		t.Fatalf("test fixture byte length = %d, want %d", got, PatchMaterializationMaxFileBytes)
	}
	if err := ValidatePatchMaterializationContentBounds(PatchMaterialization{
		Files: []PatchMaterializedFile{{Path: "utf8.txt", Content: exactUTF8Limit}},
	}); err != nil {
		t.Fatalf("expected exact UTF-8 byte boundary to pass, got %v", err)
	}
	if err := ValidatePatchMaterializationContentBounds(PatchMaterialization{
		Files: []PatchMaterializedFile{{Path: "utf8.txt", Content: exactUTF8Limit + "é"}},
	}); err == nil || !strings.Contains(err.Error(), "size") {
		t.Fatalf("expected UTF-8 byte overflow to fail content policy, got %v", err)
	}

	totalLimitChunk := strings.Repeat("y", int(PatchMaterializationMaxFileBytes)-1024)
	totalLimitFiles := make([]PatchMaterializedFile, 0, 5)
	for i := 0; i < 5; i++ {
		totalLimitFiles = append(totalLimitFiles, PatchMaterializedFile{Path: "total.txt", Content: totalLimitChunk})
	}
	if err := ValidatePatchMaterializationContentBounds(PatchMaterialization{Files: totalLimitFiles}); err == nil || !strings.Contains(err.Error(), "total size") {
		t.Fatalf("expected excessive total size to fail content policy, got %v", err)
	}
}

func TestPatchMaterializationRejectsMissingAndExtraPaths(t *testing.T) {
	item, candidateContent := patchMaterializationTestItem(t, PatchMaterializationContentDigest("new\n"))

	_, err := NormalizePatchMaterialization(PatchMaterialization{}, item, time.Time{})
	if err == nil || !strings.Contains(err.Error(), "file count") {
		t.Fatalf("expected missing file to fail, got %v", err)
	}
	_, err = NormalizePatchMaterialization(PatchMaterialization{
		Files: []PatchMaterializedFile{
			{Path: "owned.go", Content: candidateContent},
			{Path: "other.go", Content: "other\n"},
		},
	}, item, time.Time{})
	if err == nil || !strings.Contains(err.Error(), "file count") {
		t.Fatalf("expected extra file to fail, got %v", err)
	}
}

func TestPatchMaterializationRejectsCandidateHashDetachedFromContent(t *testing.T) {
	item, candidateContent := patchMaterializationTestItem(t, "sha256:"+strings.Repeat("a", 64))

	_, err := NormalizePatchMaterialization(PatchMaterialization{
		Files: []PatchMaterializedFile{
			{Path: "owned.go", Content: candidateContent},
		},
	}, item, time.Time{})
	if err == nil || !strings.Contains(err.Error(), "candidate_hash does not match content_digest") {
		t.Fatalf("expected detached candidate hash to fail, got %v", err)
	}
}

func TestPatchMaterializationAllowsConcreteCASPathsCoveredByScopedPathset(t *testing.T) {
	item, candidateContent := patchMaterializationScopedTestItem("web/**", "web/app.js")

	materialization, err := NormalizePatchMaterialization(PatchMaterialization{
		RecordedBy: "integrator-agent",
		Files: []PatchMaterializedFile{
			{Path: "web/app.js", Content: candidateContent},
		},
	}, item, time.Date(2026, 4, 26, 1, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("normalize scoped materialization: %v", err)
	}
	if len(materialization.Files) != 1 || materialization.Files[0].Path != "web/app.js" {
		t.Fatalf("expected concrete CAS path materialization, got %+v", materialization.Files)
	}
	if err := ValidatePatchMaterialization(materialization, item); err != nil {
		t.Fatalf("validate scoped materialization: %v", err)
	}
	if err := validatePatchMaterializationDiagnostic(patchMaterializationDiagnostic(materialization), item, patchMaterializationReadyWorktree(), materialization.MaterializationDigest); err != nil {
		t.Fatalf("validate scoped diagnostic materialization: %v", err)
	}
}

func TestPatchMaterializationAllowsAddedFileCASPath(t *testing.T) {
	candidateContent := "created\n"
	candidateHash := PatchMaterializationContentDigest(candidateContent)
	item := patchMaterializationScopedTestItemWithCASPath("web/**", CASPatchPathResult{
		Path:          "web/new.js",
		Status:        CASPatchStatusApplied,
		ChangeKind:    CASPatchChangeAdd,
		CandidateHash: candidateHash,
	}, candidateHash)

	materialization, err := NormalizePatchMaterialization(PatchMaterialization{
		RecordedBy: "integrator-agent",
		Files: []PatchMaterializedFile{
			{Path: "web/new.js", Content: candidateContent},
		},
	}, item, time.Date(2026, 4, 26, 1, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("normalize added materialization: %v", err)
	}
	if len(materialization.Files) != 1 {
		t.Fatalf("expected one materialized file, got %+v", materialization.Files)
	}
	file := materialization.Files[0]
	if file.Path != "web/new.js" || file.ChangeKind != CASPatchChangeAdd || file.BaseHash != "" || file.CandidateHash != candidateHash || file.ContentDigest != candidateHash {
		t.Fatalf("unexpected added materialization file: %+v", file)
	}
	proof, err := BuildPatchMaterializationAuthorityProof(materialization, item)
	if err != nil {
		t.Fatalf("authority proof for added materialization: %v", err)
	}
	if len(proof.Files) != 1 || proof.Files[0].ChangeKind != CASPatchChangeAdd || proof.Files[0].BaseHash != "" {
		t.Fatalf("unexpected added materialization authority proof: %+v", proof.Files)
	}
}

func TestPatchMaterializationRejectsCASPathOutsideScopedPathset(t *testing.T) {
	item, candidateContent := patchMaterializationScopedTestItem("api/**", "web/app.js")

	_, err := NormalizePatchMaterialization(PatchMaterialization{
		RecordedBy: "integrator-agent",
		Files: []PatchMaterializedFile{
			{Path: "web/app.js", Content: candidateContent},
		},
	}, item, time.Date(2026, 4, 26, 1, 0, 0, 0, time.UTC))
	if err == nil || !strings.Contains(err.Error(), "outside patch queue pathset") {
		t.Fatalf("expected scoped pathset coverage failure, got %v", err)
	}
}

func TestPatchMaterializationRequiresConcretePathsetEntriesInCAS(t *testing.T) {
	item, candidateContent := patchMaterializationScopedTestItem("cmd/app.go", "web/app.js")
	item.Pathset = []string{"cmd/app.go", "web/**"}

	_, err := NormalizePatchMaterialization(PatchMaterialization{
		RecordedBy: "integrator-agent",
		Files: []PatchMaterializedFile{
			{Path: "web/app.js", Content: candidateContent},
		},
	}, item, time.Date(2026, 4, 26, 1, 0, 0, 0, time.UTC))
	if err == nil || !strings.Contains(err.Error(), "missing concrete path") {
		t.Fatalf("expected missing concrete pathset entry failure, got %v", err)
	}
}

func patchMaterializationTestItem(t *testing.T, candidateHash string) (PatchQueueItem, string) {
	t.Helper()
	baseContent := "old\n"
	candidateContent := "new\n"
	baseHash := PatchMaterializationContentDigest(baseContent)
	ctx := Context{
		Mode:        ModeControlledQueue,
		WorkspaceID: "ws-materialization",
		TaskID:      "task-materialization",
		SessionID:   "session-materialization",
		RunID:       "run-materialization",
		AgentID:     "worker-agent",
		Principal:   PrincipalRef{Type: "agent", ID: "worker-agent"},
		CapabilitySnapshot: CapabilitySnapshotRef{
			ID:     "cap-materialization",
			Schema: "daemon_capability_snapshot.v1",
		},
		RepoRoot: "C:/fixtures/agents/worker-agent/materialization",
		Base: BaseIdentity{
			Ref:      "main",
			TreeHash: "sha256:" + strings.Repeat("b", 64),
			FileHashes: map[string]string{
				"owned.go": baseHash,
			},
		},
		Pathset: []string{"owned.go"},
		Lease:   LeaseRef{ID: "lease-materialization", Term: 3},
		PatchQueue: PatchQueueRef{
			QueueID: "queue-materialization",
			ItemID:  "item-materialization",
		},
		Operation: OperationRef{ID: "op-materialization-apply", Kind: "repo_patch_apply"},
	}
	contextDigest, err := ctx.Digest()
	if err != nil {
		t.Fatalf("context digest: %v", err)
	}
	cas := EvaluateCASPatchApply(CASPatchApplyInput{
		Context: ctx,
		CurrentFileHashes: map[string]string{
			"owned.go": baseHash,
		},
		CandidateFileHashes: map[string]string{
			"owned.go": candidateHash,
		},
	})
	if cas.Status != CASPatchStatusApplied {
		t.Fatalf("expected applied CAS fixture, got %+v", cas)
	}
	item := PatchQueueItem{
		Schema:              PatchQueueItemSchemaVersion,
		ID:                  "queue-materialization/item-materialization",
		QueueID:             "queue-materialization",
		ItemID:              "item-materialization",
		State:               PatchQueueStateApplied,
		Attempt:             1,
		MaxAttempts:         1,
		ContextDigest:       contextDigest,
		RepoLeaseID:         "lease-materialization",
		LeaseTerm:           3,
		Pathset:             []string{"owned.go"},
		WorkspaceID:         "ws-materialization",
		ProjectID:           "project-materialization",
		TaskID:              "task-materialization",
		SessionID:           "session-materialization",
		RunID:               "run-materialization",
		AgentID:             "worker-agent",
		PrincipalType:       "agent",
		PrincipalID:         "worker-agent",
		BaseRef:             "main",
		BaseTreeHash:        ctx.Base.TreeHash,
		CASResult:           cas,
		CASPatchDigest:      cas.PatchDigest,
		CASEvaluationDigest: PatchQueueCASEvaluationDigest(cas),
		OperationID:         "op-materialization-apply",
		OperationKind:       "repo_patch_apply",
		CreatedAt:           "2026-04-26T00:00:00Z",
		UpdatedAt:           "2026-04-26T00:00:00Z",
	}
	return item, candidateContent
}

func patchMaterializationScopedTestItemWithCASPath(scope string, casPath CASPatchPathResult, candidateHash string) PatchQueueItem {
	casPath.Status = patchMaterializationFirstNonEmpty(casPath.Status, CASPatchStatusApplied)
	cas := CASPatchApplyResult{
		Schema:        CASPatchApplySchemaVersion,
		Status:        CASPatchStatusApplied,
		PatchDigest:   digestCASPatchCandidates([]casPatchCandidateEntry{{path: casPath.Path, hash: candidateHash}}),
		ContextDigest: "sha256:" + strings.Repeat("c", 64),
		Paths:         []CASPatchPathResult{casPath},
	}
	return PatchQueueItem{
		Schema:              PatchQueueItemSchemaVersion,
		ID:                  "queue-materialization/item-materialization",
		QueueID:             "queue-materialization",
		ItemID:              "item-materialization",
		State:               PatchQueueStateApplied,
		Attempt:             1,
		MaxAttempts:         1,
		ContextDigest:       cas.ContextDigest,
		RepoLeaseID:         "lease-materialization",
		LeaseTerm:           3,
		Pathset:             []string{scope},
		WorkspaceID:         "ws-materialization",
		ProjectID:           "project-materialization",
		TaskID:              "task-materialization",
		SessionID:           "session-materialization",
		RunID:               "run-materialization",
		AgentID:             "worker-agent",
		PrincipalType:       "agent",
		PrincipalID:         "worker-agent",
		BaseRef:             "main",
		BaseTreeHash:        "sha256:" + strings.Repeat("b", 64),
		CASResult:           cas,
		CASPatchDigest:      cas.PatchDigest,
		CASEvaluationDigest: PatchQueueCASEvaluationDigest(cas),
		OperationID:         "op-materialization-apply",
		OperationKind:       "repo_patch_apply",
		CreatedAt:           "2026-04-26T00:00:00Z",
		UpdatedAt:           "2026-04-26T00:00:00Z",
	}
}

func patchMaterializationScopedTestItem(scope, casPath string) (PatchQueueItem, string) {
	baseContent := "old\n"
	candidateContent := "new\n"
	baseHash := PatchMaterializationContentDigest(baseContent)
	candidateHash := PatchMaterializationContentDigest(candidateContent)
	cas := CASPatchApplyResult{
		Schema:        CASPatchApplySchemaVersion,
		Status:        CASPatchStatusApplied,
		PatchDigest:   digestCASPatchCandidates([]casPatchCandidateEntry{{path: casPath, hash: candidateHash}}),
		ContextDigest: "sha256:" + strings.Repeat("c", 64),
		Paths: []CASPatchPathResult{
			{
				Path:          casPath,
				Status:        CASPatchStatusApplied,
				BaseHash:      baseHash,
				CurrentHash:   baseHash,
				CandidateHash: candidateHash,
			},
		},
	}
	return PatchQueueItem{
		Schema:              PatchQueueItemSchemaVersion,
		ID:                  "queue-materialization/item-materialization",
		QueueID:             "queue-materialization",
		ItemID:              "item-materialization",
		State:               PatchQueueStateApplied,
		Attempt:             1,
		MaxAttempts:         1,
		ContextDigest:       cas.ContextDigest,
		RepoLeaseID:         "lease-materialization",
		LeaseTerm:           3,
		Pathset:             []string{scope},
		WorkspaceID:         "ws-materialization",
		ProjectID:           "project-materialization",
		TaskID:              "task-materialization",
		SessionID:           "session-materialization",
		RunID:               "run-materialization",
		AgentID:             "worker-agent",
		PrincipalType:       "agent",
		PrincipalID:         "worker-agent",
		BaseRef:             "main",
		BaseTreeHash:        "sha256:" + strings.Repeat("b", 64),
		CASResult:           cas,
		CASPatchDigest:      cas.PatchDigest,
		CASEvaluationDigest: PatchQueueCASEvaluationDigest(cas),
		OperationID:         "op-materialization-apply",
		OperationKind:       "repo_patch_apply",
		CreatedAt:           "2026-04-26T00:00:00Z",
		UpdatedAt:           "2026-04-26T00:00:00Z",
	}, candidateContent
}

func patchMaterializationReadyWorktree() WorktreeIdentityEvidence {
	head := strings.Repeat("b", 40)
	return WorktreeIdentityEvidence{
		RepoID:               "repo-materialization",
		CheckoutID:           "checkout-materialization",
		BranchID:             "branch-materialization",
		BranchName:           "agent/integrator/materialization",
		MachineID:            "machine-materialization",
		LocalPath:            "C:/fixtures/agents/integrator/materialization",
		BaseSHA:              strings.Repeat("a", 40),
		HeadSHA:              head,
		ReadbackState:        "ok",
		ObservedWorktreeRoot: "C:/fixtures/agents/integrator/materialization",
		ObservedBranchName:   "agent/integrator/materialization",
		ObservedHeadSHA:      head,
		ObservedDirtyState:   "clean",
	}
}
