package repoauthority

import (
	"sort"
	"strings"
	"testing"
	"time"
)

func TestPatchQueueHappyPathApplied(t *testing.T) {
	leaseStore, queueStore, queueCtx, lease, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})

	proposed, err := queueStore.Propose(ProposePatchQueueItemInput{
		Context:    queueCtx,
		LeaseStore: leaseStore,
		Now:        now,
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if proposed.State != PatchQueueStateProposed {
		t.Fatalf("state = %q", proposed.State)
	}
	if proposed.RepoLeaseID != lease.ID || proposed.LeaseTerm != lease.Term {
		t.Fatalf("lease refs = %s/%d, want %s/%d", proposed.RepoLeaseID, proposed.LeaseTerm, lease.ID, lease.Term)
	}
	if proposed.ContextDigest == "" || proposed.ContextDigest != leaseContextDigestForPatchQueueTest(t, queueCtx) {
		t.Fatalf("context digest = %q", proposed.ContextDigest)
	}

	validating, err := queueStore.StartValidation(PatchQueueTransitionInput{
		Context: queueCtx,
		Now:     now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("StartValidation: %v", err)
	}
	if validating.State != PatchQueueStateValidating {
		t.Fatalf("state = %q, want validating", validating.State)
	}

	applyCtx := queueCtx
	applyCtx.Operation = OperationRef{ID: "op-b1-6-apply", Kind: "repo_patch_apply"}
	cas := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:           applyCtx,
		PatchID:           validating.ItemID,
		CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
		CandidateFileHashes: map[string]string{
			"owned.go": "sha256:new-owned",
		},
	})
	if cas.Status != CASPatchStatusApplied {
		t.Fatalf("CAS status = %q, issues=%+v", cas.Status, cas.Issues)
	}

	applied, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:    applyCtx,
		LeaseStore: leaseStore,
		CASResult:  cas,
		Now:        now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("CompleteValidation: %v", err)
	}
	if applied.State != PatchQueueStateApplied {
		t.Fatalf("state = %q, want applied", applied.State)
	}
	if applied.OperationID != "op-b1-6-apply" || applied.OperationKind != "repo_patch_apply" {
		t.Fatalf("operation refs = %q/%q", applied.OperationID, applied.OperationKind)
	}
	if applied.CASPatchDigest != cas.PatchDigest || applied.CASEvaluationDigest == "" {
		t.Fatalf("CAS digests not recorded: item=%+v cas=%+v", applied, cas)
	}
	if applied.CASResult.Status != CASPatchStatusApplied {
		t.Fatalf("CAS result not stored: %+v", applied.CASResult)
	}
}

func TestPatchQueueRejectsForgedAppliedCASEvidence(t *testing.T) {
	for _, tt := range []struct {
		name    string
		mutate  func(*CASPatchApplyResult)
		wantErr string
	}{
		{
			name: "path status conflict under applied result",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Paths[0].Status = CASPatchStatusConflict
				cas.Paths[0].CurrentHash = "sha256:drifted-current"
			},
			wantErr: "non-applied status",
		},
		{
			name: "path status failed under applied result",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Paths[0].Status = CASPatchStatusFailed
			},
			wantErr: "non-applied status",
		},
		{
			name: "applied path current hash drift",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Paths[0].CurrentHash = "sha256:drifted-current"
			},
			wantErr: "current hash must match base hash",
		},
		{
			name: "applied path missing candidate hash",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Paths[0].CandidateHash = ""
			},
			wantErr: "requires current and candidate hashes",
		},
		{
			name: "applied path base hash mismatch",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Paths[0].BaseHash = "sha256:forged-base"
			},
			wantErr: "base hash mismatch",
		},
		{
			name: "applied patch digest mismatch",
			mutate: func(cas *CASPatchApplyResult) {
				cas.PatchDigest = "sha256:wrong-patch-digest"
			},
			wantErr: "patch digest mismatch",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
			if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
				t.Fatalf("Propose: %v", err)
			}
			if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
				t.Fatalf("StartValidation: %v", err)
			}
			applyCtx := queueCtx
			applyCtx.Operation = OperationRef{ID: "op-b1-6-apply", Kind: "repo_patch_apply"}
			digest, err := applyCtx.Digest()
			if err != nil {
				t.Fatalf("Digest: %v", err)
			}
			baseHash := queueCtx.Base.FileHashes["owned.go"]
			cas := CASPatchApplyResult{
				Schema:        CASPatchApplySchemaVersion,
				Status:        CASPatchStatusApplied,
				ContextDigest: digest,
				PatchDigest:   patchQueueTestPatchDigest("owned.go", "sha256:new-owned"),
				Paths: []CASPatchPathResult{{
					Path:          "owned.go",
					Status:        CASPatchStatusApplied,
					BaseHash:      baseHash,
					CurrentHash:   baseHash,
					CandidateHash: "sha256:new-owned",
				}},
			}
			tt.mutate(&cas)

			_, err = queueStore.CompleteValidation(CompletePatchQueueValidationInput{
				Context:    applyCtx,
				LeaseStore: leaseStore,
				CASResult:  cas,
				Now:        now.Add(2 * time.Second),
			})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("CompleteValidation forged applied error = %v, want %q", err, tt.wantErr)
			}
			item, ok := queueStore.Get(queueCtx.PatchQueue.QueueID, queueCtx.PatchQueue.ItemID)
			if !ok || item.State != PatchQueueStateValidating {
				t.Fatalf("item after forged applied = %+v ok=%v, want validating", item, ok)
			}
		})
	}
}

func TestPatchQueueAppliedRejectsCASThatDoesNotCoverMutationPathset(t *testing.T) {
	leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go", "other.go"})
	if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
		t.Fatalf("StartValidation: %v", err)
	}
	applyCtx := queueCtx
	applyCtx.Operation = OperationRef{ID: "op-r4-5-apply", Kind: "repo_patch_apply"}
	cas := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:           applyCtx,
		CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
		CandidateFileHashes: map[string]string{
			"owned.go": "sha256:new-owned",
		},
	})
	if cas.Status != CASPatchStatusApplied {
		t.Fatalf("CAS status = %q, want applied before queue coverage validation", cas.Status)
	}

	_, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:    applyCtx,
		LeaseStore: leaseStore,
		CASResult:  cas,
		Now:        now.Add(2 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), `missing path "other.go"`) {
		t.Fatalf("CompleteValidation partial CAS error = %v, want pathset coverage reject", err)
	}
	item, ok := queueStore.Get(queueCtx.PatchQueue.QueueID, queueCtx.PatchQueue.ItemID)
	if !ok || item.State != PatchQueueStateValidating {
		t.Fatalf("item after partial CAS = %+v ok=%v, want validating", item, ok)
	}
}

func TestPatchQueueAppliedCASAndRollbackAllowConcretePathsCoveredByScopedPathset(t *testing.T) {
	now := time.Date(2026, 4, 21, 20, 45, 0, 0, time.UTC)
	preLease := patchQueuePreLeaseContext("agent-scoped", "task-scoped", "session-scoped", "run-scoped", "cap-scoped", []string{"web/**"})
	preLease.Base.FileHashes = map[string]string{
		"web/app.js": "sha256:base-web-app",
	}
	leaseStore := NewFileLeaseStore()
	lease, err := leaseStore.Acquire(AcquireFileLeaseInput{
		Context: preLease,
		TTL:     time.Minute,
		Now:     now,
	})
	if err != nil {
		t.Fatalf("Acquire scoped lease: %v", err)
	}
	queueStore := NewPatchQueueStore()
	queueCtx := preLease
	queueCtx.Lease = LeaseRef{ID: lease.ID, Term: lease.Term}
	queueCtx.PatchQueue = PatchQueueRef{QueueID: "patchq-scoped", ItemID: "patchitem-scoped"}
	if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
		t.Fatalf("Propose scoped queue item: %v", err)
	}
	if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
		t.Fatalf("StartValidation scoped queue item: %v", err)
	}
	applyCtx := queueCtx
	applyCtx.Operation = OperationRef{ID: "op-scoped-apply", Kind: "repo_patch_apply"}
	cas := EvaluateCASPatchApply(CASPatchApplyInput{
		Context: applyCtx,
		CurrentFileHashes: map[string]string{
			"web/app.js": "sha256:base-web-app",
		},
		CandidateFileHashes: map[string]string{
			"web/app.js": "sha256:new-web-app",
		},
	})
	if cas.Status != CASPatchStatusApplied {
		t.Fatalf("scoped CAS status = %q, issues=%+v", cas.Status, cas.Issues)
	}
	applied, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:    applyCtx,
		LeaseStore: leaseStore,
		CASResult:  cas,
		Now:        now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("CompleteValidation scoped CAS: %v", err)
	}
	if applied.State != PatchQueueStateApplied || len(applied.CASResult.Paths) != 1 || applied.CASResult.Paths[0].Path != "web/app.js" {
		t.Fatalf("unexpected scoped applied item: %+v", applied)
	}
	rollback, err := NormalizePatchQueueRollbackEvidence(PatchQueueRollback{
		Reason:                     "prove scoped rollback",
		VerificationCommand:        "go test ./internal/repoauthority",
		VerificationStatus:         PatchQueueTestStatusPassed,
		VerificationExitCode:       0,
		VerificationOutputDigest:   patchQueueTestDigest("f"),
		VerificationOutputSummary:  "ok",
		VerificationDurationMillis: 100,
		RollbackPaths: []PatchQueueRollbackPath{{
			Path: "web/app.js",
		}},
	}, applied, OperationRef{ID: "op-scoped-rollback", Kind: "repo_patch_apply"}, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("NormalizePatchQueueRollbackEvidence scoped: %v", err)
	}
	if len(rollback.RollbackPaths) != 1 || rollback.RollbackPaths[0].Path != "web/app.js" {
		t.Fatalf("unexpected scoped rollback paths: %+v", rollback.RollbackPaths)
	}
}

func TestPatchQueueBaseDriftConflictCarriesCASIssue(t *testing.T) {
	leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
	if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
		t.Fatalf("StartValidation: %v", err)
	}

	cas := EvaluateCASPatchApply(CASPatchApplyInput{
		Context: queueCtx,
		CurrentFileHashes: map[string]string{
			"owned.go": "sha256:drifted-current",
		},
		CandidateFileHashes: map[string]string{
			"owned.go": "sha256:new-owned",
		},
	})
	if cas.Status != CASPatchStatusConflict {
		t.Fatalf("CAS status = %q, want conflict", cas.Status)
	}

	conflict, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:   queueCtx,
		CASResult: cas,
		Now:       now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("CompleteValidation conflict: %v", err)
	}
	if conflict.State != PatchQueueStateConflict {
		t.Fatalf("state = %q, want conflict", conflict.State)
	}
	if len(conflict.ConflictIssues) != 1 || conflict.ConflictIssues[0].Kind != CASPatchIssueBaseDrift {
		t.Fatalf("conflict issues = %+v, want base drift", conflict.ConflictIssues)
	}
	if conflict.ConflictIssues[0].Path != "owned.go" {
		t.Fatalf("conflict path = %q", conflict.ConflictIssues[0].Path)
	}
}

func TestPatchQueueFailedValidationTestsMoveToTestConflict(t *testing.T) {
	leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
	if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
		t.Fatalf("StartValidation: %v", err)
	}
	cas := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:           queueCtx,
		CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
		CandidateFileHashes: map[string]string{
			"owned.go": "sha256:new-owned",
		},
	})
	if cas.Status != CASPatchStatusApplied {
		t.Fatalf("CAS status = %q, want applied", cas.Status)
	}

	testConflict, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:      queueCtx,
		CASResult:    cas,
		TestEvidence: patchQueueFailedTestEvidence(),
		Now:          now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("CompleteValidation test conflict: %v", err)
	}
	if testConflict.State != PatchQueueStateTestConflict {
		t.Fatalf("state = %q, want test_conflict", testConflict.State)
	}
	if testConflict.CASResult.Status != CASPatchStatusApplied {
		t.Fatalf("stored CAS status = %q, want applied", testConflict.CASResult.Status)
	}
	if testConflict.TestEvidence.Status != PatchQueueTestStatusFailed || testConflict.TestEvidence.Command == "" || testConflict.TestEvidence.OutputDigest == "" {
		t.Fatalf("test evidence not recorded: %+v", testConflict.TestEvidence)
	}
	if testConflict.TestEvidenceDigest == "" {
		t.Fatalf("test evidence digest is empty")
	}
	if testConflict.OperationID != "" || testConflict.OperationKind != "" {
		t.Fatalf("test_conflict must not record canonical operation refs: %+v", testConflict)
	}
}

func TestPatchQueueFailingTestEvidenceOverridesAppliedTerminal(t *testing.T) {
	leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
	if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
		t.Fatalf("StartValidation: %v", err)
	}
	applyCtx := queueCtx
	applyCtx.Operation = OperationRef{ID: "op-b3-3-apply", Kind: "repo_patch_apply"}
	cas := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:           applyCtx,
		CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
		CandidateFileHashes: map[string]string{
			"owned.go": "sha256:new-owned",
		},
	})
	if cas.Status != CASPatchStatusApplied {
		t.Fatalf("CAS status = %q, want applied", cas.Status)
	}

	item, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:      applyCtx,
		LeaseStore:   leaseStore,
		CASResult:    cas,
		TestEvidence: patchQueueFailedTestEvidence(),
		Now:          now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("CompleteValidation failing test evidence: %v", err)
	}
	if item.State != PatchQueueStateTestConflict {
		t.Fatalf("state = %q, want test_conflict not applied", item.State)
	}
	if item.OperationID != "" || item.OperationKind != "" {
		t.Fatalf("failing test evidence must block canonical operation binding: %+v", item)
	}
}

func TestPatchQueueRejectsMalformedTestConflictEvidence(t *testing.T) {
	for _, tt := range []struct {
		name    string
		mutate  func(*PatchQueueTestEvidence)
		wantErr string
	}{
		{
			name: "missing test name",
			mutate: func(evidence *PatchQueueTestEvidence) {
				evidence.Name = ""
			},
			wantErr: "test evidence name is required",
		},
		{
			name: "missing command",
			mutate: func(evidence *PatchQueueTestEvidence) {
				evidence.Command = ""
			},
			wantErr: "test evidence command is required",
		},
		{
			name: "missing output digest",
			mutate: func(evidence *PatchQueueTestEvidence) {
				evidence.OutputDigest = ""
			},
			wantErr: "test evidence output_digest is required",
		},
		{
			name: "malformed output digest",
			mutate: func(evidence *PatchQueueTestEvidence) {
				evidence.OutputDigest = "sha256:not-a-real-hash"
			},
			wantErr: "canonical sha256 digest",
		},
		{
			name: "unsupported status",
			mutate: func(evidence *PatchQueueTestEvidence) {
				evidence.Status = "flaky"
			},
			wantErr: "not supported",
		},
		{
			name: "failed test with zero exit code",
			mutate: func(evidence *PatchQueueTestEvidence) {
				evidence.ExitCode = 0
			},
			wantErr: "exit_code must be non-zero",
		},
		{
			name: "passed test with non-zero exit code",
			mutate: func(evidence *PatchQueueTestEvidence) {
				evidence.Status = PatchQueueTestStatusPassed
				evidence.ExitCode = 1
			},
			wantErr: "exit_code must be 0",
		},
		{
			name: "raw log summary too large",
			mutate: func(evidence *PatchQueueTestEvidence) {
				evidence.OutputSummary = strings.Repeat("x", patchQueueTestOutputSummaryMaxBytes+1)
			},
			wantErr: "output_summary exceeds",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
			if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
				t.Fatalf("Propose: %v", err)
			}
			if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
				t.Fatalf("StartValidation: %v", err)
			}
			cas := EvaluateCASPatchApply(CASPatchApplyInput{
				Context:           queueCtx,
				CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
				CandidateFileHashes: map[string]string{
					"owned.go": "sha256:new-owned",
				},
			})
			evidence := patchQueueFailedTestEvidence()
			tt.mutate(&evidence)

			_, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
				Context:      queueCtx,
				CASResult:    cas,
				TestEvidence: evidence,
				Now:          now.Add(2 * time.Second),
			})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("CompleteValidation malformed test evidence error = %v, want %q", err, tt.wantErr)
			}
			item, ok := queueStore.Get(queueCtx.PatchQueue.QueueID, queueCtx.PatchQueue.ItemID)
			if !ok || item.State != PatchQueueStateValidating {
				t.Fatalf("item after malformed test evidence = %+v ok=%v, want validating", item, ok)
			}
		})
	}
}

func TestPatchQueueRejectsTestEvidenceWithNonAppliedCAS(t *testing.T) {
	leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
	if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
		t.Fatalf("StartValidation: %v", err)
	}
	cas := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:           queueCtx,
		CurrentFileHashes: map[string]string{},
		CandidateFileHashes: map[string]string{
			"owned.go": "sha256:new-owned",
		},
	})
	if cas.Status != CASPatchStatusFailed {
		t.Fatalf("CAS status = %q, want failed", cas.Status)
	}

	_, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:      queueCtx,
		CASResult:    cas,
		TestEvidence: patchQueueFailedTestEvidence(),
		Now:          now.Add(2 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "requires applied CAS evidence") {
		t.Fatalf("CompleteValidation non-applied CAS with test evidence error = %v, want applied CAS evidence reject", err)
	}
	item, ok := queueStore.Get(queueCtx.PatchQueue.QueueID, queueCtx.PatchQueue.ItemID)
	if !ok || item.State != PatchQueueStateValidating {
		t.Fatalf("item after non-applied CAS test evidence = %+v ok=%v, want validating", item, ok)
	}
}

func TestPatchQueueRejectsForgedConflictCASEvidence(t *testing.T) {
	for _, tt := range []struct {
		name    string
		mutate  func(*CASPatchApplyResult)
		wantErr string
	}{
		{
			name: "outside issue path",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Issues[0].Path = "outside.go"
			},
			wantErr: "outside patch queue context",
		},
		{
			name: "unknown issue kind",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Issues[0].Kind = "unexpected_conflict"
			},
			wantErr: "not supported",
		},
		{
			name: "empty conflict issue path",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Issues[0].Path = ""
			},
			wantErr: "requires path",
		},
		{
			name: "missing base drift hashes",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Issues[0].ExpectedHash = ""
			},
			wantErr: "requires expected, actual and candidate hashes",
		},
		{
			name: "missing matching conflict path result",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Paths = nil
			},
			wantErr: "requires matching conflict path result",
		},
		{
			name: "unnormalized issue path",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Issues[0].Path = "./owned.go"
			},
			wantErr: "not normalized",
		},
		{
			name: "conflict without actual base drift",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Paths[0].CurrentHash = cas.Paths[0].BaseHash
				cas.Issues[0].ActualHash = cas.Paths[0].BaseHash
			},
			wantErr: "does not show base drift",
		},
		{
			name: "conflict issue hash mismatch",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Issues[0].ActualHash = "sha256:forged-actual"
			},
			wantErr: "hashes do not match conflict path result",
		},
		{
			name: "path result base hash mismatch",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Paths[0].BaseHash = "sha256:forged-base"
				cas.Issues[0].ExpectedHash = "sha256:forged-base"
			},
			wantErr: "base hash mismatch",
		},
		{
			name: "conflict patch digest mismatch",
			mutate: func(cas *CASPatchApplyResult) {
				cas.PatchDigest = "sha256:wrong-patch-digest"
			},
			wantErr: "patch digest mismatch",
		},
		{
			name: "failed path under conflict result",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Paths = append(cas.Paths, CASPatchPathResult{
					Path:          "other.go",
					Status:        CASPatchStatusFailed,
					BaseHash:      "sha256:base-other-go",
					CurrentHash:   "",
					CandidateHash: "sha256:new-other",
				})
				cas.PatchDigest = patchQueueTestPatchDigestFor([]casPatchCandidateEntry{
					{path: "owned.go", hash: "sha256:new-owned"},
					{path: "other.go", hash: "sha256:new-other"},
				})
			},
			wantErr: "cannot contain failed path result",
		},
		{
			name: "conflict path without issue",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Paths = append(cas.Paths, CASPatchPathResult{
					Path:          "other.go",
					Status:        CASPatchStatusConflict,
					BaseHash:      "sha256:base-other-go",
					CurrentHash:   "sha256:drifted-other",
					CandidateHash: "sha256:new-other",
				})
				cas.PatchDigest = patchQueueTestPatchDigestFor([]casPatchCandidateEntry{
					{path: "owned.go", hash: "sha256:new-owned"},
					{path: "other.go", hash: "sha256:new-other"},
				})
			},
			wantErr: "conflict path/result issue count mismatch",
		},
		{
			name: "duplicate conflict issue path",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Issues = append(cas.Issues, cas.Issues[0])
			},
			wantErr: "duplicate base_drift issue",
		},
		{
			name: "applied path under conflict hides drift",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Paths = append(cas.Paths, CASPatchPathResult{
					Path:          "other.go",
					Status:        CASPatchStatusApplied,
					BaseHash:      "sha256:base-other-go",
					CurrentHash:   "sha256:drifted-other",
					CandidateHash: "sha256:new-other",
				})
				cas.PatchDigest = patchQueueTestPatchDigestFor([]casPatchCandidateEntry{
					{path: "owned.go", hash: "sha256:new-owned"},
					{path: "other.go", hash: "sha256:new-other"},
				})
			},
			wantErr: "current hash must match base hash",
		},
		{
			name: "applied path under conflict missing current hash",
			mutate: func(cas *CASPatchApplyResult) {
				cas.Paths = append(cas.Paths, CASPatchPathResult{
					Path:          "other.go",
					Status:        CASPatchStatusApplied,
					BaseHash:      "sha256:base-other-go",
					CurrentHash:   "",
					CandidateHash: "sha256:new-other",
				})
				cas.PatchDigest = patchQueueTestPatchDigestFor([]casPatchCandidateEntry{
					{path: "owned.go", hash: "sha256:new-owned"},
					{path: "other.go", hash: "sha256:new-other"},
				})
			},
			wantErr: "requires current and candidate hashes",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go", "other.go"})
			if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
				t.Fatalf("Propose: %v", err)
			}
			if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
				t.Fatalf("StartValidation: %v", err)
			}
			digest, err := queueCtx.Digest()
			if err != nil {
				t.Fatalf("Digest: %v", err)
			}
			baseHash := queueCtx.Base.FileHashes["owned.go"]
			cas := CASPatchApplyResult{
				Schema:        CASPatchApplySchemaVersion,
				Status:        CASPatchStatusConflict,
				ContextDigest: digest,
				PatchDigest:   patchQueueTestPatchDigest("owned.go", "sha256:new-owned"),
				Paths: []CASPatchPathResult{{
					Path:          "owned.go",
					Status:        CASPatchStatusConflict,
					BaseHash:      baseHash,
					CurrentHash:   "sha256:drifted-current",
					CandidateHash: "sha256:new-owned",
				}},
				Issues: []CASPatchIssue{{
					Status:        CASPatchStatusConflict,
					Kind:          CASPatchIssueBaseDrift,
					Path:          "owned.go",
					Message:       "forged conflict",
					ExpectedHash:  baseHash,
					ActualHash:    "sha256:drifted-current",
					CandidateHash: "sha256:new-owned",
				}},
			}
			tt.mutate(&cas)

			_, err = queueStore.CompleteValidation(CompletePatchQueueValidationInput{
				Context:   queueCtx,
				CASResult: cas,
				Now:       now.Add(2 * time.Second),
			})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("CompleteValidation forged conflict error = %v, want %q", err, tt.wantErr)
			}
			item, ok := queueStore.Get(queueCtx.PatchQueue.QueueID, queueCtx.PatchQueue.ItemID)
			if !ok || item.State != PatchQueueStateValidating {
				t.Fatalf("item after forged conflict = %+v ok=%v, want validating", item, ok)
			}
		})
	}
}

func TestPatchQueueFailedCASMovesToFailed(t *testing.T) {
	leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
	if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
		t.Fatalf("StartValidation: %v", err)
	}
	cas := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:           queueCtx,
		CurrentFileHashes: map[string]string{},
		CandidateFileHashes: map[string]string{
			"owned.go": "sha256:new-owned",
		},
	})
	if cas.Status != CASPatchStatusFailed {
		t.Fatalf("CAS status = %q, want failed", cas.Status)
	}

	failed, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:   queueCtx,
		CASResult: cas,
		Now:       now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("CompleteValidation failed: %v", err)
	}
	if failed.State != PatchQueueStateFailed {
		t.Fatalf("state = %q, want failed", failed.State)
	}
	if failed.CASResult.Status != CASPatchStatusFailed {
		t.Fatalf("stored CAS status = %q", failed.CASResult.Status)
	}
}

func TestPatchQueueRecoveryDecisionSchedulesRetryThenDeadLetters(t *testing.T) {
	leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
	if _, err := queueStore.Propose(ProposePatchQueueItemInput{
		Context:     queueCtx,
		LeaseStore:  leaseStore,
		Now:         now,
		MaxAttempts: 2,
	}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
		t.Fatalf("StartValidation: %v", err)
	}
	cas := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:           queueCtx,
		CurrentFileHashes: map[string]string{},
		CandidateFileHashes: map[string]string{
			"owned.go": "sha256:new-owned",
		},
	})
	failed, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:   queueCtx,
		CASResult: cas,
		Now:       now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("CompleteValidation failed: %v", err)
	}
	if failed.State != PatchQueueStateFailed || failed.Attempt != 1 || failed.MaxAttempts != 2 {
		t.Fatalf("failed state/attempts = %q/%d/%d", failed.State, failed.Attempt, failed.MaxAttempts)
	}

	retryPending, err := queueStore.RecordRecoveryDecision(RecordPatchQueueRecoveryInput{
		Context:    queueCtx,
		Reason:     "transient current hash read failed",
		RetryDelay: 30 * time.Second,
		Now:        now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("RecordRecoveryDecision retry: %v", err)
	}
	if retryPending.State != PatchQueueStateRetryPending {
		t.Fatalf("state = %q, want retry_pending", retryPending.State)
	}
	if retryPending.NextRetryAt != formatLeaseTime(now.Add(33*time.Second)) {
		t.Fatalf("next_retry_at = %q", retryPending.NextRetryAt)
	}
	if len(retryPending.RecoveryDecisions) != 1 || retryPending.RecoveryDecisions[0].Decision != PatchQueueRecoveryDecisionRetry {
		t.Fatalf("recovery decisions = %+v", retryPending.RecoveryDecisions)
	}
	if retryPending.RecoveryDecisionDigest == "" {
		t.Fatalf("recovery decision digest is empty")
	}

	_, err = queueStore.StartRetryValidation(StartPatchQueueRetryValidationInput{
		Context:    queueCtx,
		LeaseStore: leaseStore,
		Now:        now.Add(20 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "not retryable until") {
		t.Fatalf("StartRetryValidation early error = %v, want retry backoff reject", err)
	}

	retrying, err := queueStore.StartRetryValidation(StartPatchQueueRetryValidationInput{
		Context:    queueCtx,
		LeaseStore: leaseStore,
		Now:        now.Add(40 * time.Second),
	})
	if err != nil {
		t.Fatalf("StartRetryValidation: %v", err)
	}
	if retrying.State != PatchQueueStateValidating || retrying.Attempt != 2 {
		t.Fatalf("retrying state/attempt = %q/%d", retrying.State, retrying.Attempt)
	}
	if retrying.NextRetryAt != "" || retrying.CASResult.Status != "" || retrying.CASEvaluationDigest != "" {
		t.Fatalf("retrying item carries stale attempt data: %+v", retrying)
	}

	cas = EvaluateCASPatchApply(CASPatchApplyInput{
		Context:           queueCtx,
		CurrentFileHashes: map[string]string{},
		CandidateFileHashes: map[string]string{
			"owned.go": "sha256:new-owned",
		},
	})
	if _, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:   queueCtx,
		CASResult: cas,
		Now:       now.Add(45 * time.Second),
	}); err != nil {
		t.Fatalf("CompleteValidation second failed: %v", err)
	}
	deadLetter, err := queueStore.RecordRecoveryDecision(RecordPatchQueueRecoveryInput{
		Context: queueCtx,
		Reason:  "second retry exhausted the bounded queue policy",
		Now:     now.Add(50 * time.Second),
	})
	if err != nil {
		t.Fatalf("RecordRecoveryDecision dead letter: %v", err)
	}
	if deadLetter.State != PatchQueueStateDeadLetter || deadLetter.DeadLetteredAt == "" || deadLetter.NextRetryAt != "" {
		t.Fatalf("dead letter state fields = %+v", deadLetter)
	}
	if len(deadLetter.RecoveryDecisions) != 2 || deadLetter.RecoveryDecisions[1].Decision != PatchQueueRecoveryDecisionDeadLetter {
		t.Fatalf("dead letter recovery decisions = %+v", deadLetter.RecoveryDecisions)
	}
}

func TestPatchQueueRollbackEvidenceMovesAppliedToRolledBack(t *testing.T) {
	leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Hour, []string{"owned.go"})
	if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
		t.Fatalf("StartValidation: %v", err)
	}
	applyCtx := queueCtx
	applyCtx.Operation = OperationRef{ID: "op-r4-5-apply", Kind: "repo_patch_apply"}
	cas := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:           applyCtx,
		CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
		CandidateFileHashes: map[string]string{
			"owned.go": "sha256:new-owned",
		},
	})
	applied, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:    applyCtx,
		LeaseStore: leaseStore,
		CASResult:  cas,
		Now:        now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("CompleteValidation applied: %v", err)
	}
	if applied.State != PatchQueueStateApplied || applied.OperationID != "op-r4-5-apply" {
		t.Fatalf("applied item = %+v", applied)
	}

	rollbackCtx := queueCtx
	rollbackCtx.Operation = OperationRef{ID: "op-r4-5-rollback", Kind: "repo_patch_apply"}
	rolledBack, err := queueStore.RecordRollback(RecordPatchQueueRollbackInput{
		Context:    rollbackCtx,
		LeaseStore: leaseStore,
		Evidence:   patchQueuePassedRollbackEvidenceForItem(applied),
		Now:        now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("RecordRollback: %v", err)
	}
	if rolledBack.State != PatchQueueStateRolledBack {
		t.Fatalf("state = %q, want rolled_back", rolledBack.State)
	}
	if rolledBack.OperationID != "op-r4-5-apply" || rolledBack.OperationKind != "repo_patch_apply" {
		t.Fatalf("source operation refs changed: %+v", rolledBack)
	}
	if rolledBack.RollbackEvidence.SourceOperationID != "op-r4-5-apply" ||
		rolledBack.RollbackEvidence.RollbackOperationID != "op-r4-5-rollback" ||
		rolledBack.RollbackEvidence.SourcePatchDigest != applied.CASPatchDigest ||
		rolledBack.RollbackEvidenceDigest == "" {
		t.Fatalf("rollback evidence not linked to source/apply: %+v", rolledBack.RollbackEvidence)
	}
}

func TestPatchQueueRejectsRollbackWithoutAppliedVerifiedEvidence(t *testing.T) {
	t.Run("test conflict was never canonically applied", func(t *testing.T) {
		leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Hour, []string{"owned.go"})
		if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
			t.Fatalf("StartValidation: %v", err)
		}
		cas := EvaluateCASPatchApply(CASPatchApplyInput{
			Context:           queueCtx,
			CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
			CandidateFileHashes: map[string]string{
				"owned.go": "sha256:new-owned",
			},
		})
		if _, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
			Context:      queueCtx,
			CASResult:    cas,
			TestEvidence: patchQueueFailedTestEvidence(),
			Now:          now.Add(2 * time.Second),
		}); err != nil {
			t.Fatalf("CompleteValidation test conflict: %v", err)
		}
		rollbackCtx := queueCtx
		rollbackCtx.Operation = OperationRef{ID: "op-r4-5-rollback", Kind: "repo_patch_apply"}
		_, err := queueStore.RecordRollback(RecordPatchQueueRollbackInput{
			Context:    rollbackCtx,
			LeaseStore: leaseStore,
			Evidence:   patchQueuePassedRollbackEvidence(),
			Now:        now.Add(3 * time.Second),
		})
		if err == nil || !strings.Contains(err.Error(), "illegal patch queue rollback from state test_conflict") {
			t.Fatalf("RecordRollback test_conflict error = %v, want illegal rollback", err)
		}
	})

	t.Run("rollback verification must pass", func(t *testing.T) {
		leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Hour, []string{"owned.go"})
		if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
			t.Fatalf("StartValidation: %v", err)
		}
		applyCtx := queueCtx
		applyCtx.Operation = OperationRef{ID: "op-r4-5-apply", Kind: "repo_patch_apply"}
		cas := EvaluateCASPatchApply(CASPatchApplyInput{
			Context:           applyCtx,
			CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
			CandidateFileHashes: map[string]string{
				"owned.go": "sha256:new-owned",
			},
		})
		if _, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
			Context:    applyCtx,
			LeaseStore: leaseStore,
			CASResult:  cas,
			Now:        now.Add(2 * time.Second),
		}); err != nil {
			t.Fatalf("CompleteValidation applied: %v", err)
		}
		rollbackCtx := queueCtx
		rollbackCtx.Operation = OperationRef{ID: "op-r4-5-rollback", Kind: "repo_patch_apply"}
		applied, ok := queueStore.Get(queueCtx.PatchQueue.QueueID, queueCtx.PatchQueue.ItemID)
		if !ok {
			t.Fatalf("applied item not found")
		}
		evidence := patchQueuePassedRollbackEvidenceForItem(applied)
		evidence.VerificationStatus = PatchQueueTestStatusFailed
		evidence.VerificationExitCode = 1
		_, err := queueStore.RecordRollback(RecordPatchQueueRollbackInput{
			Context:    rollbackCtx,
			LeaseStore: leaseStore,
			Evidence:   evidence,
			Now:        now.Add(3 * time.Second),
		})
		if err == nil || !strings.Contains(err.Error(), "verification_status must be") {
			t.Fatalf("RecordRollback failed verification error = %v, want verification reject", err)
		}
		item, ok := queueStore.Get(queueCtx.PatchQueue.QueueID, queueCtx.PatchQueue.ItemID)
		if !ok || item.State != PatchQueueStateApplied {
			t.Fatalf("item after rejected rollback = %+v ok=%v, want applied", item, ok)
		}
	})

	t.Run("rollback operation must be distinct from apply operation", func(t *testing.T) {
		leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Hour, []string{"owned.go"})
		if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
			t.Fatalf("StartValidation: %v", err)
		}
		applyCtx := queueCtx
		applyCtx.Operation = OperationRef{ID: "op-r4-5-apply", Kind: "repo_patch_apply"}
		cas := EvaluateCASPatchApply(CASPatchApplyInput{
			Context:           applyCtx,
			CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
			CandidateFileHashes: map[string]string{
				"owned.go": "sha256:new-owned",
			},
		})
		applied, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
			Context:    applyCtx,
			LeaseStore: leaseStore,
			CASResult:  cas,
			Now:        now.Add(2 * time.Second),
		})
		if err != nil {
			t.Fatalf("CompleteValidation applied: %v", err)
		}
		_, err = queueStore.RecordRollback(RecordPatchQueueRollbackInput{
			Context:    applyCtx,
			LeaseStore: leaseStore,
			Evidence:   patchQueuePassedRollbackEvidenceForItem(applied),
			Now:        now.Add(3 * time.Second),
		})
		if err == nil || !strings.Contains(err.Error(), "rollback operation must be distinct") {
			t.Fatalf("RecordRollback reused operation error = %v, want distinct operation reject", err)
		}
		item, ok := queueStore.Get(queueCtx.PatchQueue.QueueID, queueCtx.PatchQueue.ItemID)
		if !ok || item.State != PatchQueueStateApplied {
			t.Fatalf("item after rejected same-op rollback = %+v ok=%v, want applied", item, ok)
		}
	})

	t.Run("rollback patch digest must be derived from rollback paths", func(t *testing.T) {
		leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Hour, []string{"owned.go"})
		if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
			t.Fatalf("StartValidation: %v", err)
		}
		applyCtx := queueCtx
		applyCtx.Operation = OperationRef{ID: "op-r4-5-apply", Kind: "repo_patch_apply"}
		cas := EvaluateCASPatchApply(CASPatchApplyInput{
			Context:           applyCtx,
			CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
			CandidateFileHashes: map[string]string{
				"owned.go": "sha256:new-owned",
			},
		})
		applied, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
			Context:    applyCtx,
			LeaseStore: leaseStore,
			CASResult:  cas,
			Now:        now.Add(2 * time.Second),
		})
		if err != nil {
			t.Fatalf("CompleteValidation applied: %v", err)
		}
		rollbackCtx := queueCtx
		rollbackCtx.Operation = OperationRef{ID: "op-r4-5-rollback", Kind: "repo_patch_apply"}
		evidence := patchQueuePassedRollbackEvidenceForItem(applied)
		evidence.RollbackPatchDigest = patchQueueTestDigest("b")
		_, err = queueStore.RecordRollback(RecordPatchQueueRollbackInput{
			Context:    rollbackCtx,
			LeaseStore: leaseStore,
			Evidence:   evidence,
			Now:        now.Add(3 * time.Second),
		})
		if err == nil || !strings.Contains(err.Error(), "rollback_patch_digest mismatch") {
			t.Fatalf("RecordRollback unbound digest error = %v, want digest mismatch", err)
		}
		item, ok := queueStore.Get(queueCtx.PatchQueue.QueueID, queueCtx.PatchQueue.ItemID)
		if !ok || item.State != PatchQueueStateApplied {
			t.Fatalf("item after rejected digest rollback = %+v ok=%v, want applied", item, ok)
		}
	})
}

func TestPatchQueueDeadLetterAndRolledBackAreTerminal(t *testing.T) {
	t.Run("dead letter", func(t *testing.T) {
		leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
		if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
			t.Fatalf("StartValidation: %v", err)
		}
		cas := EvaluateCASPatchApply(CASPatchApplyInput{
			Context:           queueCtx,
			CurrentFileHashes: map[string]string{},
			CandidateFileHashes: map[string]string{
				"owned.go": "sha256:new-owned",
			},
		})
		if _, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
			Context:   queueCtx,
			CASResult: cas,
			Now:       now.Add(2 * time.Second),
		}); err != nil {
			t.Fatalf("CompleteValidation failed: %v", err)
		}
		deadLetter, err := queueStore.RecordRecoveryDecision(RecordPatchQueueRecoveryInput{
			Context: queueCtx,
			Reason:  "single attempt exhausted",
			Now:     now.Add(3 * time.Second),
		})
		if err != nil {
			t.Fatalf("RecordRecoveryDecision dead letter: %v", err)
		}
		if deadLetter.State != PatchQueueStateDeadLetter {
			t.Fatalf("state = %q, want dead_letter", deadLetter.State)
		}
		if _, err := queueStore.StartRetryValidation(StartPatchQueueRetryValidationInput{Context: queueCtx, LeaseStore: leaseStore, Now: now.Add(4 * time.Second)}); err == nil || !strings.Contains(err.Error(), "dead_letter -> validating") {
			t.Fatalf("StartRetryValidation dead_letter error = %v, want illegal transition", err)
		}
		if _, err := queueStore.Cancel(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(4 * time.Second)}); err == nil || !strings.Contains(err.Error(), "dead_letter -> canceled") {
			t.Fatalf("Cancel dead_letter error = %v, want illegal transition", err)
		}
		if _, err := queueStore.RecordRecoveryDecision(RecordPatchQueueRecoveryInput{Context: queueCtx, Reason: "retry dead letter", Now: now.Add(4 * time.Second)}); err == nil || !strings.Contains(err.Error(), "from state dead_letter") {
			t.Fatalf("RecordRecoveryDecision dead_letter error = %v, want illegal recovery", err)
		}
	})

	t.Run("rolled back", func(t *testing.T) {
		leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Hour, []string{"owned.go"})
		if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
			t.Fatalf("StartValidation: %v", err)
		}
		applyCtx := queueCtx
		applyCtx.Operation = OperationRef{ID: "op-r4-5-apply", Kind: "repo_patch_apply"}
		cas := EvaluateCASPatchApply(CASPatchApplyInput{
			Context:           applyCtx,
			CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
			CandidateFileHashes: map[string]string{
				"owned.go": "sha256:new-owned",
			},
		})
		applied, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
			Context:    applyCtx,
			LeaseStore: leaseStore,
			CASResult:  cas,
			Now:        now.Add(2 * time.Second),
		})
		if err != nil {
			t.Fatalf("CompleteValidation applied: %v", err)
		}
		rollbackCtx := queueCtx
		rollbackCtx.Operation = OperationRef{ID: "op-r4-5-rollback", Kind: "repo_patch_apply"}
		rolledBack, err := queueStore.RecordRollback(RecordPatchQueueRollbackInput{
			Context:    rollbackCtx,
			LeaseStore: leaseStore,
			Evidence:   patchQueuePassedRollbackEvidenceForItem(applied),
			Now:        now.Add(3 * time.Second),
		})
		if err != nil {
			t.Fatalf("RecordRollback: %v", err)
		}
		if rolledBack.State != PatchQueueStateRolledBack {
			t.Fatalf("state = %q, want rolled_back", rolledBack.State)
		}
		if _, err := queueStore.Cancel(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(4 * time.Second)}); err == nil || !strings.Contains(err.Error(), "rolled_back -> canceled") {
			t.Fatalf("Cancel rolled_back error = %v, want illegal transition", err)
		}
		if _, err := queueStore.RecordRecoveryDecision(RecordPatchQueueRecoveryInput{Context: queueCtx, Reason: "retry rollback", Now: now.Add(4 * time.Second)}); err == nil || !strings.Contains(err.Error(), "from state rolled_back") {
			t.Fatalf("RecordRecoveryDecision rolled_back error = %v, want illegal recovery", err)
		}
		if _, err := queueStore.RecordRollback(RecordPatchQueueRollbackInput{Context: rollbackCtx, LeaseStore: leaseStore, Evidence: patchQueuePassedRollbackEvidenceForItem(applied), Now: now.Add(4 * time.Second)}); err == nil || !strings.Contains(err.Error(), "rollback from state rolled_back") {
			t.Fatalf("RecordRollback rolled_back error = %v, want illegal rollback", err)
		}
	})
}

func TestPatchQueueRejectsMissingCASEvidence(t *testing.T) {
	leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
	if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
		t.Fatalf("StartValidation: %v", err)
	}

	_, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:   queueCtx,
		CASResult: CASPatchApplyResult{},
		Now:       now.Add(2 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "CAS evidence schema is required") {
		t.Fatalf("CompleteValidation missing CAS evidence error = %v, want schema required", err)
	}
	item, ok := queueStore.Get(queueCtx.PatchQueue.QueueID, queueCtx.PatchQueue.ItemID)
	if !ok || item.State != PatchQueueStateValidating {
		t.Fatalf("item after failed completion = %+v ok=%v, want validating", item, ok)
	}
}

func TestPatchQueueAppliedRequiresB17ReadyRefs(t *testing.T) {
	leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
	if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
		t.Fatalf("StartValidation: %v", err)
	}
	cas := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:           queueCtx,
		CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
		CandidateFileHashes: map[string]string{
			"owned.go": "sha256:new-owned",
		},
	})
	if cas.Status != CASPatchStatusApplied {
		t.Fatalf("CAS status = %q, want applied", cas.Status)
	}

	_, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:   queueCtx,
		CASResult: cas,
		Now:       now.Add(2 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "operation_id is required") {
		t.Fatalf("CompleteValidation without B1.7 refs error = %v, want operation_id required", err)
	}
}

func TestPatchQueueAppliedRequiresLiveLeaseAtTerminalApply(t *testing.T) {
	t.Run("expired lease", func(t *testing.T) {
		leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
		if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
			t.Fatalf("StartValidation: %v", err)
		}
		applyCtx := queueCtx
		applyCtx.Operation = OperationRef{ID: "op-b1-6-apply", Kind: "repo_patch_apply"}
		cas := EvaluateCASPatchApply(CASPatchApplyInput{
			Context:           applyCtx,
			CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
			CandidateFileHashes: map[string]string{
				"owned.go": "sha256:new-owned",
			},
		})

		_, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
			Context:    applyCtx,
			LeaseStore: leaseStore,
			CASResult:  cas,
			Now:        now.Add(time.Minute),
		})
		if err == nil || !strings.Contains(err.Error(), "expired") {
			t.Fatalf("CompleteValidation expired lease error = %v, want expired", err)
		}
		item, ok := queueStore.Get(queueCtx.PatchQueue.QueueID, queueCtx.PatchQueue.ItemID)
		if !ok || item.State != PatchQueueStateValidating {
			t.Fatalf("item after expired apply = %+v ok=%v, want validating", item, ok)
		}
	})

	t.Run("revoked lease", func(t *testing.T) {
		leaseStore, queueStore, queueCtx, lease, now := patchQueueFixture(t, time.Hour, []string{"owned.go"})
		if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
			t.Fatalf("StartValidation: %v", err)
		}
		if _, err := leaseStore.RevokeLeasesForHolder(RevokeFileLeasesInput{
			Holder: HolderForLease(lease),
			Now:    now.Add(2 * time.Second),
			Reason: "terminal apply test",
		}); err != nil {
			t.Fatalf("RevokeLeasesForHolder: %v", err)
		}
		applyCtx := queueCtx
		applyCtx.Operation = OperationRef{ID: "op-b1-6-apply", Kind: "repo_patch_apply"}
		cas := EvaluateCASPatchApply(CASPatchApplyInput{
			Context:           applyCtx,
			CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
			CandidateFileHashes: map[string]string{
				"owned.go": "sha256:new-owned",
			},
		})

		_, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
			Context:    applyCtx,
			LeaseStore: leaseStore,
			CASResult:  cas,
			Now:        now.Add(3 * time.Second),
		})
		if err == nil || !strings.Contains(err.Error(), "revoked") {
			t.Fatalf("CompleteValidation revoked lease error = %v, want revoked", err)
		}
		item, ok := queueStore.Get(queueCtx.PatchQueue.QueueID, queueCtx.PatchQueue.ItemID)
		if !ok || item.State != PatchQueueStateValidating {
			t.Fatalf("item after revoked apply = %+v ok=%v, want validating", item, ok)
		}
	})
}

func TestPatchQueueRejectsStaleLeaseAndContextMismatch(t *testing.T) {
	t.Run("stale lease term", func(t *testing.T) {
		leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
		queueCtx.Lease.Term++
		_, err := queueStore.Propose(ProposePatchQueueItemInput{
			Context:    queueCtx,
			LeaseStore: leaseStore,
			Now:        now,
		})
		if err == nil || !strings.Contains(err.Error(), "lease_term mismatch") {
			t.Fatalf("Propose stale lease error = %v, want lease term mismatch", err)
		}
	})

	t.Run("context mismatch", func(t *testing.T) {
		leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
		if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		stale := queueCtx
		stale.SessionID = "session-stale"
		_, err := queueStore.StartValidation(PatchQueueTransitionInput{
			Context: stale,
			Now:     now.Add(time.Second),
		})
		if err == nil || !strings.Contains(err.Error(), "context digest mismatch") {
			t.Fatalf("StartValidation stale context error = %v, want context digest mismatch", err)
		}
	})
}

func TestPatchQueueRejectsIllegalTransitions(t *testing.T) {
	t.Run("complete from proposed", func(t *testing.T) {
		leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
		if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		applyCtx := queueCtx
		applyCtx.Operation = OperationRef{ID: "op-b1-6-apply", Kind: "repo_patch_apply"}
		cas := EvaluateCASPatchApply(CASPatchApplyInput{
			Context:           applyCtx,
			CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
			CandidateFileHashes: map[string]string{
				"owned.go": "sha256:new-owned",
			},
		})
		_, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
			Context:    applyCtx,
			LeaseStore: leaseStore,
			CASResult:  cas,
			Now:        now.Add(time.Second),
		})
		if err == nil || !strings.Contains(err.Error(), "illegal patch queue transition proposed -> terminal") {
			t.Fatalf("CompleteValidation from proposed error = %v, want illegal transition", err)
		}
	})

	t.Run("start validation twice", func(t *testing.T) {
		leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
		if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
			t.Fatalf("StartValidation: %v", err)
		}
		_, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(2 * time.Second)})
		if err == nil || !strings.Contains(err.Error(), "illegal patch queue transition validating -> validating") {
			t.Fatalf("second StartValidation error = %v, want illegal transition", err)
		}
	})

	t.Run("cancel after applied", func(t *testing.T) {
		leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
		if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(time.Second)}); err != nil {
			t.Fatalf("StartValidation: %v", err)
		}
		applyCtx := queueCtx
		applyCtx.Operation = OperationRef{ID: "op-b1-6-apply", Kind: "repo_patch_apply"}
		cas := EvaluateCASPatchApply(CASPatchApplyInput{
			Context:           applyCtx,
			CurrentFileHashes: currentHashesForPatchQueueContext(queueCtx),
			CandidateFileHashes: map[string]string{
				"owned.go": "sha256:new-owned",
			},
		})
		if _, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{Context: applyCtx, LeaseStore: leaseStore, CASResult: cas, Now: now.Add(2 * time.Second)}); err != nil {
			t.Fatalf("CompleteValidation: %v", err)
		}
		_, err := queueStore.Cancel(PatchQueueTransitionInput{Context: queueCtx, Now: now.Add(3 * time.Second)})
		if err == nil || !strings.Contains(err.Error(), "illegal patch queue transition applied -> canceled") {
			t.Fatalf("Cancel applied error = %v, want illegal transition", err)
		}
	})
}

func TestPatchQueueCanCancelProposedItem(t *testing.T) {
	leaseStore, queueStore, queueCtx, _, now := patchQueueFixture(t, time.Minute, []string{"owned.go"})
	if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: queueCtx, LeaseStore: leaseStore, Now: now}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	canceled, err := queueStore.Cancel(PatchQueueTransitionInput{
		Context: queueCtx,
		Now:     now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if canceled.State != PatchQueueStateCanceled {
		t.Fatalf("state = %q, want canceled", canceled.State)
	}
	_, err = queueStore.StartValidation(PatchQueueTransitionInput{
		Context: queueCtx,
		Now:     now.Add(2 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "illegal patch queue transition canceled -> validating") {
		t.Fatalf("StartValidation after cancel error = %v, want illegal transition", err)
	}
}

func patchQueueFixture(t *testing.T, ttl time.Duration, pathset []string) (*FileLeaseStore, *PatchQueueStore, Context, FileLease, time.Time) {
	t.Helper()
	now := time.Date(2026, 4, 21, 20, 45, 0, 0, time.UTC)
	preLease := patchQueuePreLeaseContext("agent-b1-6", "task-b1-6", "session-b1-6", "run-b1-6", "cap-b1-6", pathset)
	leaseStore := NewFileLeaseStore()
	lease, err := leaseStore.Acquire(AcquireFileLeaseInput{
		Context: preLease,
		TTL:     ttl,
		Now:     now,
	})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	queueCtx := preLease
	queueCtx.Lease = LeaseRef{ID: lease.ID, Term: lease.Term}
	queueCtx.PatchQueue = PatchQueueRef{QueueID: "patchq-b1-6", ItemID: "patchitem-b1-6"}
	return leaseStore, NewPatchQueueStore(), queueCtx, lease, now
}

func patchQueuePreLeaseContext(agentID, taskID, sessionID, runID, capabilitySnapshotID string, pathset []string) Context {
	normalized, err := NormalizePathSet(pathset)
	if err != nil {
		panic(err)
	}
	hashes := make(map[string]string, len(normalized))
	for _, p := range normalized {
		hashes[p] = "sha256:base-" + strings.NewReplacer("/", "-", ".", "-").Replace(p)
	}
	return Context{
		Mode:        ModePatchOnlyTempRepo,
		WorkspaceID: "ws-b1-6",
		TaskID:      taskID,
		SessionID:   sessionID,
		RunID:       runID,
		AgentID:     agentID,
		Principal: PrincipalRef{
			Type: "agent",
			ID:   agentID,
		},
		CapabilitySnapshot: CapabilitySnapshotRef{
			ID:     capabilitySnapshotID,
			Schema: "runtime_capability_snapshot.v1",
		},
		RepoRoot: "C:/work/rhizome",
		Base: BaseIdentity{
			Ref:        "base-b1-6",
			TreeHash:   "sha256:base-tree-b1-6",
			FileHashes: hashes,
		},
		Pathset: normalized,
	}
}

func currentHashesForPatchQueueContext(ctx Context) map[string]string {
	current := make(map[string]string, len(ctx.Base.FileHashes))
	for path, hash := range ctx.Base.FileHashes {
		current[path] = hash
	}
	return current
}

func leaseContextDigestForPatchQueueTest(t *testing.T, ctx Context) string {
	t.Helper()
	digest, err := patchQueueContextDigest(ctx)
	if err != nil {
		t.Fatalf("patchQueueContextDigest: %v", err)
	}
	return digest
}

func patchQueueFailedTestEvidence() PatchQueueTestEvidence {
	return PatchQueueTestEvidence{
		Name:           "unit validation",
		Command:        "go test ./internal/repoauthority",
		Status:         PatchQueueTestStatusFailed,
		ExitCode:       1,
		OutputDigest:   "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		OutputSummary:  "FAIL: TestExample",
		DurationMillis: 1200,
	}
}

func patchQueuePassedRollbackEvidence() PatchQueueRollback {
	return PatchQueueRollback{
		Reason:                     "rollback verification passed",
		RollbackPatchDigest:        patchQueueTestDigest("d"),
		VerificationCommand:        "go test ./internal/repoauthority",
		VerificationStatus:         PatchQueueTestStatusPassed,
		VerificationExitCode:       0,
		VerificationOutputDigest:   patchQueueTestDigest("e"),
		VerificationOutputSummary:  "ok",
		VerificationDurationMillis: 100,
	}
}

func patchQueuePassedRollbackEvidenceForItem(item PatchQueueItem) PatchQueueRollback {
	paths := patchQueueRollbackPathsForItem(item)
	return PatchQueueRollback{
		Reason:                     "rollback verification passed",
		RollbackPatchDigest:        patchQueueRollbackDigestForPaths(paths),
		RollbackPaths:              paths,
		VerificationCommand:        "go test ./internal/repoauthority",
		VerificationStatus:         PatchQueueTestStatusPassed,
		VerificationExitCode:       0,
		VerificationOutputDigest:   patchQueueTestDigest("e"),
		VerificationOutputSummary:  "ok",
		VerificationDurationMillis: 100,
	}
}

func patchQueueRollbackPathsForItem(item PatchQueueItem) []PatchQueueRollbackPath {
	out := make([]PatchQueueRollbackPath, 0, len(item.CASResult.Paths))
	for _, pathResult := range item.CASResult.Paths {
		out = append(out, PatchQueueRollbackPath{
			Path:                  pathResult.Path,
			SourceBaseHash:        pathResult.BaseHash,
			SourceAppliedHash:     pathResult.CandidateHash,
			RollbackCandidateHash: pathResult.BaseHash,
		})
	}
	return out
}

func patchQueueRollbackDigestForPaths(paths []PatchQueueRollbackPath) string {
	candidates := make([]casPatchCandidateEntry, 0, len(paths))
	for _, path := range paths {
		candidates = append(candidates, casPatchCandidateEntry{
			path: path.Path,
			hash: path.RollbackCandidateHash,
		})
	}
	return patchQueueTestPatchDigestFor(candidates)
}

func patchQueueTestDigest(ch string) string {
	return "sha256:" + strings.Repeat(ch, 64)
}

func patchQueueTestPatchDigest(path, hash string) string {
	return patchQueueTestPatchDigestFor([]casPatchCandidateEntry{{path: path, hash: hash}})
}

func patchQueueTestPatchDigestFor(candidates []casPatchCandidateEntry) string {
	candidates = append([]casPatchCandidateEntry(nil), candidates...)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].path < candidates[j].path
	})
	return digestCASPatchCandidates(candidates)
}
