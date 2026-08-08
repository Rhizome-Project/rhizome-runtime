package repoauthority

import (
	"strings"
	"testing"
	"time"
)

func TestBaseDriftConflictModelCompatibleWithCASAndPatchQueueEvidence(t *testing.T) {
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
	item, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:   queueCtx,
		CASResult: cas,
		Now:       now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("CompleteValidation conflict: %v", err)
	}

	conflict, err := BuildBaseDriftConflict(BuildBaseDriftConflictInput{
		Context:        queueCtx,
		CASResult:      item.CASResult,
		PatchQueueItem: item,
	})
	if err != nil {
		t.Fatalf("BuildBaseDriftConflict: %v", err)
	}
	if conflict.Schema != ConflictModelSchemaVersion || conflict.Status != ConflictStatusConflict || conflict.Reason != ConflictReasonBaseDrift {
		t.Fatalf("unexpected conflict identity: %+v", conflict)
	}
	if conflict.Path != "owned.go" || conflict.ExpectedHash != queueCtx.Base.FileHashes["owned.go"] ||
		conflict.ActualHash != "sha256:drifted-current" || conflict.CandidateHash != "sha256:new-owned" {
		t.Fatalf("unexpected base_drift hashes: %+v", conflict)
	}
	if conflict.PatchQueueID != item.QueueID || conflict.PatchQueueItemID != item.ItemID || conflict.PatchQueueState != PatchQueueStateConflict {
		t.Fatalf("missing patch queue evidence: %+v", conflict)
	}
	if conflict.CASPatchDigest != item.CASPatchDigest || conflict.CASEvaluationDigest != item.CASEvaluationDigest {
		t.Fatalf("CAS digest evidence mismatch: conflict=%+v item=%+v", conflict, item)
	}
	if err := ValidateConflictModelResult(conflict); err != nil {
		t.Fatalf("ValidateConflictModelResult: %v", err)
	}
}

func TestBaseDriftConflictModelRejectsMalformedOrAmbiguousEvidence(t *testing.T) {
	t.Run("ambiguous path requires explicit path", func(t *testing.T) {
		leaseStore, queueStore, ctx, _, now := patchQueueFixture(t, time.Minute, []string{"a.go", "b.go"})
		if _, err := queueStore.Propose(ProposePatchQueueItemInput{Context: ctx, LeaseStore: leaseStore, Now: now}); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		if _, err := queueStore.StartValidation(PatchQueueTransitionInput{Context: ctx, Now: now.Add(time.Second)}); err != nil {
			t.Fatalf("StartValidation: %v", err)
		}
		cas := EvaluateCASPatchApply(CASPatchApplyInput{
			Context: ctx,
			CurrentFileHashes: map[string]string{
				"a.go": "sha256:drifted-a",
				"b.go": "sha256:drifted-b",
			},
			CandidateFileHashes: map[string]string{
				"a.go": "sha256:new-a",
				"b.go": "sha256:new-b",
			},
		})
		item, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
			Context:   ctx,
			CASResult: cas,
			Now:       now.Add(2 * time.Second),
		})
		if err != nil {
			t.Fatalf("CompleteValidation conflict: %v", err)
		}
		_, err = BuildBaseDriftConflict(BuildBaseDriftConflictInput{
			Context:        ctx,
			CASResult:      item.CASResult,
			PatchQueueItem: item,
		})
		if err == nil || !strings.Contains(err.Error(), "ambiguous") {
			t.Fatalf("BuildBaseDriftConflict ambiguous error = %v, want ambiguous", err)
		}
	})

	t.Run("missing patch queue item evidence fails closed", func(t *testing.T) {
		_, _, queueCtx, _, _ := patchQueueFixture(t, time.Minute, []string{"owned.go"})
		cas := EvaluateCASPatchApply(CASPatchApplyInput{
			Context: queueCtx,
			CurrentFileHashes: map[string]string{
				"owned.go": "sha256:drifted-current",
			},
			CandidateFileHashes: map[string]string{
				"owned.go": "sha256:new-owned",
			},
		})
		_, err := BuildBaseDriftConflict(BuildBaseDriftConflictInput{
			Context:   queueCtx,
			CASResult: cas,
			Path:      "owned.go",
		})
		if err == nil || !strings.Contains(err.Error(), "patch queue item evidence") {
			t.Fatalf("BuildBaseDriftConflict missing queue item error = %v, want queue evidence reject", err)
		}
	})

	t.Run("patch queue CAS digest evidence is required and bound", func(t *testing.T) {
		queueCtx, cas, item := baseDriftConflictFixture(t)
		for _, tt := range []struct {
			name    string
			mutate  func(*PatchQueueItem)
			wantErr string
		}{
			{
				name: "missing patch digest",
				mutate: func(item *PatchQueueItem) {
					item.CASPatchDigest = ""
				},
				wantErr: "CAS patch digest evidence",
			},
			{
				name: "mismatched patch digest",
				mutate: func(item *PatchQueueItem) {
					item.CASPatchDigest = "sha256:wrong-patch"
				},
				wantErr: "CAS patch digest mismatch",
			},
			{
				name: "missing evaluation digest",
				mutate: func(item *PatchQueueItem) {
					item.CASEvaluationDigest = ""
				},
				wantErr: "CAS evaluation digest evidence",
			},
			{
				name: "mismatched evaluation digest",
				mutate: func(item *PatchQueueItem) {
					item.CASEvaluationDigest = "sha256:wrong-eval"
				},
				wantErr: "CAS evaluation digest mismatch",
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				badItem := item
				tt.mutate(&badItem)
				_, err := BuildBaseDriftConflict(BuildBaseDriftConflictInput{
					Context:        queueCtx,
					CASResult:      cas,
					PatchQueueItem: badItem,
					Path:           "owned.go",
				})
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("BuildBaseDriftConflict digest evidence error = %v, want %q", err, tt.wantErr)
				}
			})
		}
	})

	t.Run("missing base drift hash fails closed", func(t *testing.T) {
		_, _, queueCtx, _, _ := patchQueueFixture(t, time.Minute, []string{"owned.go"})
		cas := EvaluateCASPatchApply(CASPatchApplyInput{
			Context: queueCtx,
			CurrentFileHashes: map[string]string{
				"owned.go": "sha256:drifted-current",
			},
			CandidateFileHashes: map[string]string{
				"owned.go": "sha256:new-owned",
			},
		})
		cas.Issues[0].ActualHash = ""
		_, err := BuildBaseDriftConflict(BuildBaseDriftConflictInput{
			Context:   queueCtx,
			CASResult: cas,
			Path:      "owned.go",
		})
		if err == nil || !strings.Contains(err.Error(), "requires expected, actual and candidate hashes") {
			t.Fatalf("BuildBaseDriftConflict missing hash error = %v, want hash reject", err)
		}
	})

	t.Run("validator rejects forged equal hashes", func(t *testing.T) {
		result := ConflictModelResult{
			Schema:              ConflictModelSchemaVersion,
			ConflictID:          "conflict_forged",
			Status:              ConflictStatusConflict,
			Reason:              ConflictReasonBaseDrift,
			Path:                "owned.go",
			WorkspaceID:         "ws-b4",
			ContextDigest:       "sha256:ctx",
			RepoLeaseID:         "repolease:v1:b4:1",
			LeaseTerm:           1,
			PatchQueueID:        "patchq-b4",
			PatchQueueItemID:    "patchitem-b4",
			PatchQueueState:     PatchQueueStateConflict,
			ExpectedHash:        "sha256:same",
			ActualHash:          "sha256:same",
			CandidateHash:       "sha256:new",
			CASPatchDigest:      "sha256:patch",
			CASEvaluationDigest: "sha256:eval",
			CASIssueKind:        CASPatchIssueBaseDrift,
		}
		err := ValidateConflictModelResult(result)
		if err == nil || !strings.Contains(err.Error(), "must differ") {
			t.Fatalf("ValidateConflictModelResult equal hashes error = %v, want differ reject", err)
		}
	})
}

func TestPathConflictModelRepresentsSamePathConflict(t *testing.T) {
	left := pathConflictItem("patchq-b4", "patchitem-left", "agent-left", "task-left", "owned.go")
	right := pathConflictItem("patchq-b4", "patchitem-right", "agent-right", "task-right", "owned.go")

	conflict, err := BuildPathConflict(BuildPathConflictInput{
		Path:  "owned.go",
		Left:  left,
		Right: right,
	})
	if err != nil {
		t.Fatalf("BuildPathConflict: %v", err)
	}
	if conflict.Status != ConflictStatusConflict || conflict.Reason != ConflictReasonPathConflict || conflict.Path != "owned.go" {
		t.Fatalf("unexpected path conflict: %+v", conflict)
	}
	if len(conflict.Contenders) != 2 {
		t.Fatalf("contenders = %+v, want two", conflict.Contenders)
	}
	if conflict.Contenders[0].ItemID != "patchitem-left" || conflict.Contenders[1].ItemID != "patchitem-right" {
		t.Fatalf("contenders not stable/sorted: %+v", conflict.Contenders)
	}
	if err := ValidateConflictModelResult(conflict); err != nil {
		t.Fatalf("ValidateConflictModelResult: %v", err)
	}
}

func TestPathConflictModelAcceptsScopedPatchQueuePathsets(t *testing.T) {
	left := pathConflictItem("patchq-b4", "patchitem-left", "agent-left", "task-left", "internal/eval/**")
	right := pathConflictItem("patchq-b4", "patchitem-right", "agent-right", "task-right", "internal/eval/**")
	conflict, err := BuildPathConflict(BuildPathConflictInput{
		Path:  "internal/eval/parser_test.go",
		Left:  left,
		Right: right,
	})
	if err != nil {
		t.Fatalf("BuildPathConflict with scoped pathsets: %v", err)
	}
	if conflict.Path != "internal/eval/parser_test.go" || len(conflict.Contenders) != 2 {
		t.Fatalf("unexpected scoped path conflict: %+v", conflict)
	}
}

func TestSemanticConflictModelRepresentsDisjointSemanticCollision(t *testing.T) {
	store, left, right := semanticConflictFixture()

	conflict, err := BuildSemanticConflict(BuildSemanticConflictInput{
		Kind:            SemanticConflictKindAPIContract,
		Subject:         "api.user.create",
		EvidenceSummary: "API response shape and test fixture default user disagree even though pathsets are disjoint.",
		Left:            left,
		Right:           right,
		PatchQueueStore: store,
	})
	if err != nil {
		t.Fatalf("BuildSemanticConflict: %v", err)
	}
	if conflict.Schema != ConflictModelSchemaVersion || conflict.Status != ConflictStatusConflict || conflict.Reason != ConflictReasonSemantic {
		t.Fatalf("unexpected semantic conflict identity: %+v", conflict)
	}
	if conflict.Path != "api/client.go" || len(conflict.Paths) != 2 || conflict.Paths[0] != "api/client.go" || conflict.Paths[1] != "tests/fixtures/user.json" {
		t.Fatalf("unexpected semantic paths: %+v", conflict)
	}
	if conflict.SemanticKind != SemanticConflictKindAPIContract || conflict.SemanticSubject != "api.user.create" {
		t.Fatalf("unexpected semantic evidence: %+v", conflict)
	}
	if conflict.SemanticEvidenceDigest != digestSemanticConflictEvidence(conflict) {
		t.Fatalf("semantic evidence digest = %q, want %q", conflict.SemanticEvidenceDigest, digestSemanticConflictEvidence(conflict))
	}
	if len(conflict.SemanticContenders) != 2 {
		t.Fatalf("semantic contenders = %+v, want two", conflict.SemanticContenders)
	}
	if conflict.SemanticContenders[0].ItemID != "patchitem-api" || conflict.SemanticContenders[1].ItemID != "patchitem-fixture" {
		t.Fatalf("semantic contenders are not stable/sorted: %+v", conflict.SemanticContenders)
	}
	if conflict.SemanticContenders[0].CapabilitySnapshotID == "" || conflict.SemanticContenders[1].PrincipalID == "" {
		t.Fatalf("semantic contenders missing owner/capability evidence: %+v", conflict.SemanticContenders)
	}
	if conflict.ConflictID != conflictModelID(conflict) {
		t.Fatalf("conflict id = %q, want %q", conflict.ConflictID, conflictModelID(conflict))
	}
	if err := ValidateConflictModelResultWithPatchQueueStore(conflict, store); err != nil {
		t.Fatalf("ValidateConflictModelResultWithPatchQueueStore: %v", err)
	}
	if err := ValidateConflictModelResult(conflict); err == nil || !strings.Contains(err.Error(), "requires patch queue store validation") {
		t.Fatalf("ValidateConflictModelResult without patch queue store error = %v, want patch queue store requirement", err)
	}
}

func TestSemanticConflictModelRejectsMalformedEvidence(t *testing.T) {
	store, validLeft, validRight := semanticConflictFixture()

	for _, tt := range []struct {
		name    string
		input   BuildSemanticConflictInput
		wantErr string
	}{
		{
			name: "unsupported semantic kind",
			input: BuildSemanticConflictInput{
				Kind:            "mystery_collision",
				Subject:         "api.user.create",
				EvidenceSummary: "summary",
				Left:            validLeft,
				Right:           validRight,
			},
			wantErr: "semantic_kind",
		},
		{
			name: "missing subject",
			input: BuildSemanticConflictInput{
				Kind:            SemanticConflictKindAPIContract,
				EvidenceSummary: "summary",
				Left:            validLeft,
				Right:           validRight,
			},
			wantErr: "semantic_subject",
		},
		{
			name: "missing summary",
			input: BuildSemanticConflictInput{
				Kind:    SemanticConflictKindAPIContract,
				Subject: "api.user.create",
				Left:    validLeft,
				Right:   validRight,
			},
			wantErr: "semantic_evidence_summary",
		},
		{
			name: "summary too large",
			input: BuildSemanticConflictInput{
				Kind:            SemanticConflictKindAPIContract,
				Subject:         "api.user.create",
				EvidenceSummary: strings.Repeat("x", SemanticConflictEvidenceSummaryMaxBytes+1),
				Left:            validLeft,
				Right:           validRight,
			},
			wantErr: "exceeds",
		},
		{
			name: "same item is ambiguous",
			input: BuildSemanticConflictInput{
				Kind:            SemanticConflictKindAPIContract,
				Subject:         "api.user.create",
				EvidenceSummary: "summary",
				Left:            validLeft,
				Right:           validLeft,
			},
			wantErr: "different patch queue items",
		},
		{
			name: "workspace mismatch",
			input: BuildSemanticConflictInput{
				Kind:            SemanticConflictKindAPIContract,
				Subject:         "api.user.create",
				EvidenceSummary: "summary",
				Left:            validLeft,
				Right: func() PatchQueueItem {
					item := validRight
					item.WorkspaceID = "ws-other"
					return item
				}(),
			},
			wantErr: "same workspace",
		},
		{
			name: "base identity mismatch",
			input: BuildSemanticConflictInput{
				Kind:            SemanticConflictKindAPIContract,
				Subject:         "api.user.create",
				EvidenceSummary: "summary",
				Left:            validLeft,
				Right: func() PatchQueueItem {
					item := validRight
					item.BaseTreeHash = "sha256:other-base"
					return item
				}(),
			},
			wantErr: "same base identity",
		},
		{
			name: "same path is path conflict not semantic",
			input: BuildSemanticConflictInput{
				Kind:            SemanticConflictKindAPIContract,
				Subject:         "api.user.create",
				EvidenceSummary: "summary",
				Left:            validLeft,
				Right:           pathConflictItem("patchq-b4", "patchitem-other-api", "agent-other", "task-other", "api/client.go"),
			},
			wantErr: "use path_conflict",
		},
		{
			name: "terminal contender rejected",
			input: BuildSemanticConflictInput{
				Kind:            SemanticConflictKindAPIContract,
				Subject:         "api.user.create",
				EvidenceSummary: "summary",
				Left: func() PatchQueueItem {
					item := validLeft
					item.State = PatchQueueStateApplied
					return item
				}(),
				Right: validRight,
			},
			wantErr: "active semantic_conflict contender state",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			input := tt.input
			if input.PatchQueueStore == nil {
				input.PatchQueueStore = store
			}
			_, err := BuildSemanticConflict(input)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("BuildSemanticConflict error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestSemanticConflictModelValidatorRejectsForgedEvidence(t *testing.T) {
	store, left, right := semanticConflictFixture()
	conflict, err := BuildSemanticConflict(BuildSemanticConflictInput{
		Kind:            SemanticConflictKindTestFixture,
		Subject:         "fixture.default_user",
		EvidenceSummary: "Fixture expects legacy user shape while API patch creates the new shape.",
		Left:            left,
		Right:           right,
		PatchQueueStore: store,
	})
	if err != nil {
		t.Fatalf("BuildSemanticConflict: %v", err)
	}

	for _, tt := range []struct {
		name        string
		mutate      func(*ConflictModelResult)
		recomputeID bool
		wantErr     string
	}{
		{
			name: "conflict id mismatch",
			mutate: func(result *ConflictModelResult) {
				result.ConflictID = "conflict_forged"
			},
			wantErr: "conflict_id mismatch",
		},
		{
			name: "missing semantic digest",
			mutate: func(result *ConflictModelResult) {
				result.SemanticEvidenceDigest = ""
			},
			recomputeID: true,
			wantErr:     "evidence digest",
		},
		{
			name: "mismatched semantic digest",
			mutate: func(result *ConflictModelResult) {
				result.SemanticEvidenceSummary = "forged summary"
			},
			recomputeID: true,
			wantErr:     "evidence digest mismatch",
		},
		{
			name: "path not in paths",
			mutate: func(result *ConflictModelResult) {
				result.Path = "other.go"
			},
			recomputeID: true,
			wantErr:     "must be included",
		},
		{
			name: "contender missing capability",
			mutate: func(result *ConflictModelResult) {
				result.SemanticContenders[0].CapabilitySnapshotID = ""
				result.SemanticEvidenceDigest = digestSemanticConflictEvidence(*result)
			},
			recomputeID: true,
			wantErr:     "capability_snapshot_id",
		},
		{
			name: "contender pathset not canonical",
			mutate: func(result *ConflictModelResult) {
				result.SemanticContenders[0].Pathset = []string{"z.go", "api/client.go"}
				result.SemanticEvidenceDigest = digestSemanticConflictEvidence(*result)
			},
			recomputeID: true,
			wantErr:     "pathset is not canonical",
		},
		{
			name: "overlapping contender pathsets",
			mutate: func(result *ConflictModelResult) {
				result.SemanticContenders[1].Pathset = []string{"api/client.go"}
				result.Paths = []string{"api/client.go"}
				result.SemanticEvidenceDigest = digestSemanticConflictEvidence(*result)
			},
			recomputeID: true,
			wantErr:     "use path_conflict",
		},
		{
			name: "contender base identity forged",
			mutate: func(result *ConflictModelResult) {
				result.SemanticContenders[1].BaseTreeHash = "sha256:other-base"
				result.SemanticEvidenceDigest = digestSemanticConflictEvidence(*result)
			},
			recomputeID: true,
			wantErr:     "same base identity",
		},
		{
			name: "contender base identity self-consistently forged",
			mutate: func(result *ConflictModelResult) {
				for i := range result.SemanticContenders {
					result.SemanticContenders[i].BaseRef = "refs/heads/forged-base"
					result.SemanticContenders[i].BaseTreeHash = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
				}
				result.SemanticEvidenceDigest = digestSemanticConflictEvidence(*result)
			},
			recomputeID: true,
			wantErr:     "contender store mismatch",
		},
		{
			name: "path conflict contenders mixed in",
			mutate: func(result *ConflictModelResult) {
				result.Contenders = []ConflictContender{pathConflictContender(left), pathConflictContender(right)}
			},
			recomputeID: true,
			wantErr:     "path_conflict contenders",
		},
		{
			name: "base drift evidence mixed in",
			mutate: func(result *ConflictModelResult) {
				result.CASIssueKind = CASPatchIssueBaseDrift
			},
			recomputeID: true,
			wantErr:     "base_drift evidence",
		},
		{
			name: "CAS evidence mixed in",
			mutate: func(result *ConflictModelResult) {
				result.CASStatus = CASPatchStatusApplied
				result.CASPatchDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				result.CASEvaluationDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			},
			recomputeID: true,
			wantErr:     "base_drift evidence",
		},
		{
			name: "top level contender evidence mixed in",
			mutate: func(result *ConflictModelResult) {
				result.TaskID = "task-top-level-forged"
				result.SessionID = "session-top-level-forged"
				result.RunID = "run-top-level-forged"
				result.AgentID = "agent-top-level-forged"
				result.PrincipalType = "agent"
				result.PrincipalID = "principal-top-level-forged"
				result.CapabilitySnapshotID = "caps-top-level-forged"
				result.CapabilitySnapshotSchema = "capability_snapshot.v1"
				result.ContextDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
				result.RepoLeaseID = "lease-top-level-forged"
				result.LeaseTerm = 99
				result.PatchQueueID = "patchq-top-level-forged"
				result.PatchQueueItemID = "patchitem-top-level-forged"
				result.PatchQueueState = PatchQueueStateValidating
				result.BaseRef = "refs/heads/forged"
				result.BaseTreeHash = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
			},
			recomputeID: true,
			wantErr:     "top-level contender evidence",
		},
		{
			name: "test evidence mixed in",
			mutate: func(result *ConflictModelResult) {
				evidence := patchQueueFailedTestEvidence()
				result.TestEvidence = &evidence
				result.TestEvidenceDigest = digestPatchQueueTestEvidence(evidence)
			},
			recomputeID: true,
			wantErr:     "test_conflict evidence",
		},
		{
			name: "operation evidence mixed in",
			mutate: func(result *ConflictModelResult) {
				result.OperationID = "op-forged"
			},
			recomputeID: true,
			wantErr:     "mutation operation evidence",
		},
		{
			name: "late stale write evidence mixed in",
			mutate: func(result *ConflictModelResult) {
				contender := result.SemanticContenders[0]
				attemptedAt := time.Date(2026, 4, 22, 2, 30, 0, 0, time.UTC)
				holder := FileLeaseHolder{
					WorkspaceID:          contender.WorkspaceID,
					TaskID:               contender.TaskID,
					SessionID:            contender.SessionID,
					RunID:                contender.RunID,
					AgentID:              contender.AgentID,
					Principal:            PrincipalRef{Type: contender.PrincipalType, ID: contender.PrincipalID},
					CapabilitySnapshotID: contender.CapabilitySnapshotID,
					ContextDigest:        contender.ContextDigest,
				}
				lease := FileLease{
					Schema:               FileLeaseSchemaVersion,
					ID:                   contender.RepoLeaseID,
					Term:                 contender.LeaseTerm,
					Status:               FileLeaseStatusActive,
					WorkspaceID:          contender.WorkspaceID,
					TaskID:               contender.TaskID,
					SessionID:            contender.SessionID,
					RunID:                contender.RunID,
					AgentID:              contender.AgentID,
					Principal:            holder.Principal,
					CapabilitySnapshotID: contender.CapabilitySnapshotID,
					RepoRoot:             "C:/repo",
					Pathset:              append([]string(nil), contender.Pathset...),
					ContextDigest:        contender.ContextDigest,
					AcquiredAt:           formatLeaseTime(attemptedAt.Add(-time.Minute)),
					UpdatedAt:            formatLeaseTime(attemptedAt.Add(-time.Second)),
					ExpiresAt:            formatLeaseTime(attemptedAt.Add(time.Minute)),
				}
				result.AttemptedAt = formatLeaseTime(attemptedAt)
				result.RejectionKind = LateStaleWriteRejectionStaleHolder
				result.RejectionMessage = "lease context session mismatch: got \"session-a\" want \"session-b\""
				result.AttemptedHolder = &holder
				result.AttemptedHolderDigest = digestFileLeaseHolder(holder)
				result.ObservedLease = &lease
				result.ObservedLeaseDigest = digestFileLease(lease)
				result.ObservedLeaseStatus = lease.Status
				result.ObservedLeaseTerm = lease.Term
				result.ObservedHolder = &holder
				result.ObservedHolderDigest = digestFileLeaseHolder(holder)
			},
			recomputeID: true,
			wantErr:     "late_stale_write evidence",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			forged := conflict
			forged.SemanticContenders = append([]SemanticConflictContender(nil), conflict.SemanticContenders...)
			tt.mutate(&forged)
			if tt.recomputeID {
				forged.ConflictID = conflictModelID(forged)
			}
			err := ValidateConflictModelResultWithPatchQueueStore(forged, store)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateConflictModelResultWithPatchQueueStore forged semantic conflict error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func semanticConflictFixture() (*PatchQueueStore, PatchQueueItem, PatchQueueItem) {
	left := pathConflictItem("patchq-b4", "patchitem-api", "agent-api", "task-api", "api/client.go")
	right := pathConflictItem("patchq-b4", "patchitem-fixture", "agent-fixture", "task-fixture", "tests/fixtures/user.json")
	store := NewPatchQueueStore()
	store.items[patchQueueKey(left.QueueID, left.ItemID)] = clonePatchQueueItem(left)
	store.items[patchQueueKey(right.QueueID, right.ItemID)] = clonePatchQueueItem(right)
	return store, left, right
}

func TestTestConflictModelCompatibleWithPatchQueueEvidence(t *testing.T) {
	queueCtx, item := testConflictFixture(t)

	conflict, err := BuildTestConflict(BuildTestConflictInput{
		Context:        queueCtx,
		PatchQueueItem: item,
	})
	if err != nil {
		t.Fatalf("BuildTestConflict: %v", err)
	}
	if conflict.Schema != ConflictModelSchemaVersion || conflict.Status != ConflictStatusConflict || conflict.Reason != ConflictReasonTestConflict {
		t.Fatalf("unexpected test conflict identity: %+v", conflict)
	}
	if conflict.Path != "owned.go" || len(conflict.Paths) != 1 || conflict.Paths[0] != "owned.go" {
		t.Fatalf("unexpected test conflict paths: %+v", conflict)
	}
	if conflict.PatchQueueID != item.QueueID || conflict.PatchQueueItemID != item.ItemID || conflict.PatchQueueState != PatchQueueStateTestConflict {
		t.Fatalf("missing patch queue evidence: %+v", conflict)
	}
	if conflict.CASStatus != CASPatchStatusApplied || conflict.CASPatchDigest != item.CASPatchDigest || conflict.CASEvaluationDigest != item.CASEvaluationDigest {
		t.Fatalf("missing applied CAS evidence: conflict=%+v item=%+v", conflict, item)
	}
	if conflict.TestEvidence == nil || conflict.TestEvidence.Status != PatchQueueTestStatusFailed || conflict.TestEvidenceDigest != item.TestEvidenceDigest {
		t.Fatalf("missing failed test evidence: conflict=%+v item=%+v", conflict, item)
	}
	if conflict.OperationID != "" || conflict.OperationKind != "" {
		t.Fatalf("test conflict must not carry operation refs: %+v", conflict)
	}
	if conflict.ConflictID != conflictModelID(conflict) {
		t.Fatalf("conflict id = %q, want stable digest %q", conflict.ConflictID, conflictModelID(conflict))
	}
	if err := ValidateConflictModelResult(conflict); err != nil {
		t.Fatalf("ValidateConflictModelResult: %v", err)
	}
}

func TestTestConflictModelRejectsMalformedEvidence(t *testing.T) {
	t.Run("wrong queue state", func(t *testing.T) {
		queueCtx, item := testConflictFixture(t)
		item.State = PatchQueueStateValidating
		_, err := BuildTestConflict(BuildTestConflictInput{Context: queueCtx, PatchQueueItem: item})
		if err == nil || !strings.Contains(err.Error(), "patch queue state") {
			t.Fatalf("BuildTestConflict wrong state error = %v, want state reject", err)
		}
	})

	t.Run("passing test evidence", func(t *testing.T) {
		queueCtx, item := testConflictFixture(t)
		item.TestEvidence.Status = PatchQueueTestStatusPassed
		item.TestEvidence.ExitCode = 0
		item.TestEvidenceDigest = digestPatchQueueTestEvidence(item.TestEvidence)
		_, err := BuildTestConflict(BuildTestConflictInput{Context: queueCtx, PatchQueueItem: item})
		if err == nil || !strings.Contains(err.Error(), "failed test evidence") {
			t.Fatalf("BuildTestConflict passing evidence error = %v, want failed evidence reject", err)
		}
	})

	t.Run("missing test evidence", func(t *testing.T) {
		queueCtx, item := testConflictFixture(t)
		item.TestEvidence = PatchQueueTestEvidence{}
		item.TestEvidenceDigest = ""
		_, err := BuildTestConflict(BuildTestConflictInput{Context: queueCtx, PatchQueueItem: item})
		if err == nil || !strings.Contains(err.Error(), "test evidence") {
			t.Fatalf("BuildTestConflict missing evidence error = %v, want evidence reject", err)
		}
	})

	t.Run("missing test evidence digest", func(t *testing.T) {
		queueCtx, item := testConflictFixture(t)
		item.TestEvidenceDigest = ""
		_, err := BuildTestConflict(BuildTestConflictInput{Context: queueCtx, PatchQueueItem: item})
		if err == nil || !strings.Contains(err.Error(), "test evidence digest") {
			t.Fatalf("BuildTestConflict missing evidence digest error = %v, want digest reject", err)
		}
	})

	t.Run("mismatched test evidence digest", func(t *testing.T) {
		queueCtx, item := testConflictFixture(t)
		item.TestEvidenceDigest = "sha256:wrong-test-evidence"
		_, err := BuildTestConflict(BuildTestConflictInput{Context: queueCtx, PatchQueueItem: item})
		if err == nil || !strings.Contains(err.Error(), "test evidence digest mismatch") {
			t.Fatalf("BuildTestConflict digest mismatch error = %v, want digest reject", err)
		}
	})

	t.Run("stale lease context mismatch", func(t *testing.T) {
		queueCtx, item := testConflictFixture(t)
		item.LeaseTerm++
		_, err := BuildTestConflict(BuildTestConflictInput{Context: queueCtx, PatchQueueItem: item})
		if err == nil || !strings.Contains(err.Error(), "lease_term mismatch") {
			t.Fatalf("BuildTestConflict stale lease error = %v, want lease mismatch", err)
		}
	})

	t.Run("operation refs on item are rejected", func(t *testing.T) {
		queueCtx, item := testConflictFixture(t)
		item.OperationID = "op-forged"
		item.OperationKind = "repo_patch_apply"
		_, err := BuildTestConflict(BuildTestConflictInput{Context: queueCtx, PatchQueueItem: item})
		if err == nil || !strings.Contains(err.Error(), "operation refs") {
			t.Fatalf("BuildTestConflict operation refs error = %v, want operation reject", err)
		}
	})

	t.Run("whitespace operation refs on item are rejected", func(t *testing.T) {
		queueCtx, item := testConflictFixture(t)
		item.OperationID = " "
		_, err := BuildTestConflict(BuildTestConflictInput{Context: queueCtx, PatchQueueItem: item})
		if err == nil || !strings.Contains(err.Error(), "operation refs") {
			t.Fatalf("BuildTestConflict whitespace operation refs error = %v, want operation reject", err)
		}
	})

	t.Run("operation refs on context are rejected", func(t *testing.T) {
		queueCtx, item := testConflictFixture(t)
		queueCtx.Operation = OperationRef{ID: "op-forged", Kind: "repo_patch_apply"}
		_, err := BuildTestConflict(BuildTestConflictInput{Context: queueCtx, PatchQueueItem: item})
		if err == nil || !strings.Contains(err.Error(), "operation refs") {
			t.Fatalf("BuildTestConflict context operation refs error = %v, want operation reject", err)
		}
	})

	t.Run("whitespace operation refs on context are rejected", func(t *testing.T) {
		queueCtx, item := testConflictFixture(t)
		queueCtx.Operation = OperationRef{Kind: " "}
		_, err := BuildTestConflict(BuildTestConflictInput{Context: queueCtx, PatchQueueItem: item})
		if err == nil || !strings.Contains(err.Error(), "operation refs") {
			t.Fatalf("BuildTestConflict whitespace context operation refs error = %v, want operation reject", err)
		}
	})

	t.Run("non-applied CAS result is rejected", func(t *testing.T) {
		queueCtx, item := testConflictFixture(t)
		item.CASResult.Status = CASPatchStatusFailed
		item.CASEvaluationDigest = digestCASPatchApplyResult(item.CASResult)
		_, err := BuildTestConflict(BuildTestConflictInput{Context: queueCtx, PatchQueueItem: item})
		if err == nil || !strings.Contains(err.Error(), "applied CAS result") {
			t.Fatalf("BuildTestConflict non-applied CAS error = %v, want applied reject", err)
		}
	})
}

func TestTestConflictModelValidatorRejectsForgedEvidence(t *testing.T) {
	queueCtx, item := testConflictFixture(t)
	conflict, err := BuildTestConflict(BuildTestConflictInput{Context: queueCtx, PatchQueueItem: item})
	if err != nil {
		t.Fatalf("BuildTestConflict: %v", err)
	}

	for _, tt := range []struct {
		name    string
		mutate  func(*ConflictModelResult)
		wantErr string
	}{
		{
			name: "missing task id",
			mutate: func(result *ConflictModelResult) {
				result.TaskID = ""
			},
			wantErr: "task_id",
		},
		{
			name: "missing session id",
			mutate: func(result *ConflictModelResult) {
				result.SessionID = ""
			},
			wantErr: "session_id",
		},
		{
			name: "missing run id",
			mutate: func(result *ConflictModelResult) {
				result.RunID = ""
			},
			wantErr: "run_id",
		},
		{
			name: "missing agent id",
			mutate: func(result *ConflictModelResult) {
				result.AgentID = ""
			},
			wantErr: "agent_id",
		},
		{
			name: "missing principal type",
			mutate: func(result *ConflictModelResult) {
				result.PrincipalType = ""
			},
			wantErr: "principal_type",
		},
		{
			name: "missing principal id",
			mutate: func(result *ConflictModelResult) {
				result.PrincipalID = ""
			},
			wantErr: "principal_id",
		},
		{
			name: "missing capability snapshot id",
			mutate: func(result *ConflictModelResult) {
				result.CapabilitySnapshotID = ""
			},
			wantErr: "capability_snapshot_id",
		},
		{
			name: "missing capability snapshot schema",
			mutate: func(result *ConflictModelResult) {
				result.CapabilitySnapshotSchema = ""
			},
			wantErr: "capability_snapshot_schema",
		},
		{
			name: "forged conflict id",
			mutate: func(result *ConflictModelResult) {
				result.ConflictID = "conflict_forged"
			},
			wantErr: "conflict_id mismatch",
		},
		{
			name: "whitespace padded conflict id",
			mutate: func(result *ConflictModelResult) {
				result.ConflictID = " " + result.ConflictID + " "
			},
			wantErr: "conflict_id mismatch",
		},
		{
			name: "whitespace padded schema",
			mutate: func(result *ConflictModelResult) {
				result.Schema = " " + result.Schema + " "
				result.ConflictID = conflictModelID(*result)
			},
			wantErr: "conflict schema",
		},
		{
			name: "whitespace padded status",
			mutate: func(result *ConflictModelResult) {
				result.Status = " " + result.Status + " "
				result.ConflictID = conflictModelID(*result)
			},
			wantErr: "conflict status",
		},
		{
			name: "whitespace padded reason",
			mutate: func(result *ConflictModelResult) {
				result.Reason = " " + result.Reason + " "
				result.ConflictID = conflictModelID(*result)
			},
			wantErr: "conflict reason",
		},
		{
			name: "whitespace padded workspace id",
			mutate: func(result *ConflictModelResult) {
				result.WorkspaceID = " " + result.WorkspaceID + " "
				result.ConflictID = conflictModelID(*result)
			},
			wantErr: "workspace_id is not canonical",
		},
		{
			name: "wrong queue state",
			mutate: func(result *ConflictModelResult) {
				result.PatchQueueState = PatchQueueStateApplied
			},
			wantErr: "patch_queue_state",
		},
		{
			name: "passing test evidence",
			mutate: func(result *ConflictModelResult) {
				evidence := *result.TestEvidence
				evidence.Status = PatchQueueTestStatusPassed
				evidence.ExitCode = 0
				result.TestEvidence = &evidence
				result.TestEvidenceDigest = digestPatchQueueTestEvidence(evidence)
			},
			wantErr: "failed test evidence",
		},
		{
			name: "missing test evidence",
			mutate: func(result *ConflictModelResult) {
				result.TestEvidence = nil
			},
			wantErr: "test evidence is required",
		},
		{
			name: "missing test evidence digest",
			mutate: func(result *ConflictModelResult) {
				result.TestEvidenceDigest = ""
			},
			wantErr: "test_evidence_digest",
		},
		{
			name: "whitespace padded test evidence schema with recomputed conflict id",
			mutate: func(result *ConflictModelResult) {
				evidence := *result.TestEvidence
				evidence.Schema = " " + evidence.Schema + " "
				result.TestEvidence = &evidence
				result.ConflictID = conflictModelID(*result)
			},
			wantErr: "test evidence is not canonical",
		},
		{
			name: "whitespace padded test evidence command with recomputed conflict id",
			mutate: func(result *ConflictModelResult) {
				evidence := *result.TestEvidence
				evidence.Command = " " + evidence.Command + " "
				result.TestEvidence = &evidence
				result.ConflictID = conflictModelID(*result)
			},
			wantErr: "test evidence is not canonical",
		},
		{
			name: "whitespace padded test evidence status with recomputed conflict id",
			mutate: func(result *ConflictModelResult) {
				evidence := *result.TestEvidence
				evidence.Status = " " + evidence.Status + " "
				result.TestEvidence = &evidence
				result.ConflictID = conflictModelID(*result)
			},
			wantErr: "test evidence is not canonical",
		},
		{
			name: "whitespace padded test evidence output digest with recomputed conflict id",
			mutate: func(result *ConflictModelResult) {
				evidence := *result.TestEvidence
				evidence.OutputDigest = " " + evidence.OutputDigest + " "
				result.TestEvidence = &evidence
				result.ConflictID = conflictModelID(*result)
			},
			wantErr: "test evidence is not canonical",
		},
		{
			name: "mismatched test evidence digest",
			mutate: func(result *ConflictModelResult) {
				result.TestEvidenceDigest = "sha256:wrong"
			},
			wantErr: "test evidence digest mismatch",
		},
		{
			name: "operation refs present",
			mutate: func(result *ConflictModelResult) {
				result.OperationID = "op-forged"
			},
			wantErr: "operation refs",
		},
		{
			name: "whitespace operation id present with recomputed conflict id",
			mutate: func(result *ConflictModelResult) {
				result.OperationID = " "
				result.ConflictID = conflictModelID(*result)
			},
			wantErr: "operation refs",
		},
		{
			name: "whitespace operation kind present with recomputed conflict id",
			mutate: func(result *ConflictModelResult) {
				result.OperationKind = " "
				result.ConflictID = conflictModelID(*result)
			},
			wantErr: "operation refs",
		},
		{
			name: "non-applied CAS status",
			mutate: func(result *ConflictModelResult) {
				result.CASStatus = CASPatchStatusFailed
			},
			wantErr: "cas_status",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			forged := conflict
			tt.mutate(&forged)
			err := ValidateConflictModelResult(forged)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateConflictModelResult forged evidence error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestLateStaleWriteConflictModelRecordsRejectedMutation(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*FileLeaseStore, *Context, FileLease, time.Time)
		now        func(time.Time) time.Time
		wantKind   string
		wantStatus string
	}{
		{
			name: "expired lease",
			now: func(now time.Time) time.Time {
				return now.Add(time.Minute)
			},
			wantKind:   LateStaleWriteRejectionExpiredLease,
			wantStatus: FileLeaseStatusExpired,
		},
		{
			name: "revoked lease",
			mutate: func(store *FileLeaseStore, _ *Context, lease FileLease, now time.Time) {
				if _, err := store.RevokeLeasesForHolder(RevokeFileLeasesInput{
					Holder: HolderForLease(lease),
					Now:    now.Add(time.Second),
					Reason: "B4.5 stale write test",
				}); err != nil {
					t.Fatalf("RevokeLeasesForHolder: %v", err)
				}
			},
			now: func(now time.Time) time.Time {
				return now.Add(2 * time.Second)
			},
			wantKind:   LateStaleWriteRejectionRevokedLease,
			wantStatus: FileLeaseStatusRevoked,
		},
		{
			name: "stale lease term",
			mutate: func(_ *FileLeaseStore, ctx *Context, lease FileLease, _ time.Time) {
				ctx.Lease.Term = lease.Term + 1
			},
			now: func(now time.Time) time.Time {
				return now.Add(time.Second)
			},
			wantKind:   LateStaleWriteRejectionStaleLeaseTerm,
			wantStatus: FileLeaseStatusActive,
		},
		{
			name: "stale holder",
			mutate: func(_ *FileLeaseStore, ctx *Context, _ FileLease, _ time.Time) {
				ctx.SessionID = "session-stale-b4-5"
			},
			now: func(now time.Time) time.Time {
				return now.Add(time.Second)
			},
			wantKind:   LateStaleWriteRejectionStaleHolder,
			wantStatus: FileLeaseStatusActive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, ctx, lease, now := bindingFixture(t, time.Minute)
			if tt.mutate != nil {
				tt.mutate(store, &ctx, lease, now)
			}
			attemptedAt := tt.now(now)

			conflict, err := BuildLateStaleWriteConflict(BuildLateStaleWriteConflictInput{
				Context:       ctx,
				LeaseStore:    store,
				MutationPaths: []string{"owned.go"},
				Now:           attemptedAt,
			})
			if err != nil {
				t.Fatalf("BuildLateStaleWriteConflict: %v", err)
			}
			if conflict.Schema != ConflictModelSchemaVersion || conflict.Status != ConflictStatusConflict || conflict.Reason != ConflictReasonLateStaleWrite {
				t.Fatalf("unexpected conflict identity: %+v", conflict)
			}
			if conflict.Path != "owned.go" || len(conflict.MutationPaths) != 1 || conflict.MutationPaths[0] != "owned.go" {
				t.Fatalf("unexpected mutation paths: %+v", conflict)
			}
			if conflict.RepoLeaseID != ctx.Lease.ID || conflict.LeaseTerm != ctx.Lease.Term || conflict.ObservedLeaseTerm != lease.Term {
				t.Fatalf("lease evidence mismatch: conflict=%+v ctx=%+v lease=%+v", conflict, ctx.Lease, lease)
			}
			if conflict.OperationID != ctx.Operation.ID || conflict.OperationKind != ctx.Operation.Kind {
				t.Fatalf("operation evidence mismatch: %+v", conflict)
			}
			if conflict.PatchQueueID != ctx.PatchQueue.QueueID || conflict.PatchQueueItemID != ctx.PatchQueue.ItemID {
				t.Fatalf("patch queue evidence mismatch: %+v", conflict)
			}
			if conflict.RejectionKind != tt.wantKind || conflict.ObservedLeaseStatus != tt.wantStatus || conflict.RejectionMessage == "" {
				t.Fatalf("rejection evidence mismatch: %+v", conflict)
			}
			if conflict.AttemptedHolder == nil || conflict.AttemptedHolder.SessionID != ctx.SessionID {
				t.Fatalf("attempted holder evidence mismatch: %+v", conflict.AttemptedHolder)
			}
			if conflict.ObservedHolder == nil || conflict.ObservedHolder.SessionID != lease.SessionID {
				t.Fatalf("observed holder evidence mismatch: %+v", conflict.ObservedHolder)
			}
			if conflict.AttemptedHolderDigest != digestFileLeaseHolder(*conflict.AttemptedHolder) ||
				conflict.ObservedHolderDigest != digestFileLeaseHolder(*conflict.ObservedHolder) {
				t.Fatalf("holder digest evidence mismatch: %+v", conflict)
			}
			if conflict.ObservedLease == nil ||
				conflict.ObservedLeaseDigest != digestFileLease(*conflict.ObservedLease) ||
				!sameFileLeaseHolder(HolderForLease(*conflict.ObservedLease), *conflict.ObservedHolder) {
				t.Fatalf("observed lease evidence mismatch: %+v", conflict)
			}
			if conflict.ConflictID != conflictModelID(conflict) {
				t.Fatalf("conflict id = %q, want %q", conflict.ConflictID, conflictModelID(conflict))
			}
			if err := ValidateConflictModelResultWithLeaseStore(conflict, store); err != nil {
				t.Fatalf("ValidateConflictModelResultWithLeaseStore: %v", err)
			}
			if err := ValidateConflictModelResult(conflict); err == nil || !strings.Contains(err.Error(), "requires lease store validation") {
				t.Fatalf("ValidateConflictModelResult without lease store error = %v, want lease store requirement", err)
			}
		})
	}
}

func TestLateStaleWriteConflictRejectsNonStaleAttempts(t *testing.T) {
	t.Run("accepted mutation is not a conflict", func(t *testing.T) {
		store, ctx, _, now := bindingFixture(t, time.Minute)
		_, err := BuildLateStaleWriteConflict(BuildLateStaleWriteConflictInput{
			Context:       ctx,
			LeaseStore:    store,
			MutationPaths: []string{"owned.go"},
			Now:           now.Add(time.Second),
		})
		if err == nil || !strings.Contains(err.Error(), "requires rejected mutation operation binding") {
			t.Fatalf("BuildLateStaleWriteConflict accepted mutation error = %v, want rejected binding", err)
		}
	})

	t.Run("path outside lease is not stale", func(t *testing.T) {
		store, ctx, _, now := bindingFixture(t, time.Minute)
		_, err := BuildLateStaleWriteConflict(BuildLateStaleWriteConflictInput{
			Context:       ctx,
			LeaseStore:    store,
			MutationPaths: []string{"other.go"},
			Now:           now.Add(time.Second),
		})
		if err == nil || !strings.Contains(err.Error(), "requires stale lease rejection") {
			t.Fatalf("BuildLateStaleWriteConflict outside path error = %v, want non-stale reject", err)
		}
	})

	t.Run("missing lease store", func(t *testing.T) {
		_, ctx, _, now := bindingFixture(t, time.Minute)
		_, err := BuildLateStaleWriteConflict(BuildLateStaleWriteConflictInput{
			Context:       ctx,
			MutationPaths: []string{"owned.go"},
			Now:           now.Add(time.Second),
		})
		if err == nil || !strings.Contains(err.Error(), "file lease store") {
			t.Fatalf("BuildLateStaleWriteConflict missing store error = %v, want store reject", err)
		}
	})

	t.Run("missing operation id", func(t *testing.T) {
		store, ctx, _, now := bindingFixture(t, time.Minute)
		ctx.Operation.ID = ""
		_, err := BuildLateStaleWriteConflict(BuildLateStaleWriteConflictInput{
			Context:       ctx,
			LeaseStore:    store,
			MutationPaths: []string{"owned.go"},
			Now:           now.Add(time.Second),
		})
		if err == nil || !strings.Contains(err.Error(), "operation_id") {
			t.Fatalf("BuildLateStaleWriteConflict missing operation error = %v, want operation reject", err)
		}
	})

	t.Run("unknown lease id", func(t *testing.T) {
		store, ctx, _, now := bindingFixture(t, time.Minute)
		ctx.Lease.ID = "repolease:v1:missing"
		_, err := BuildLateStaleWriteConflict(BuildLateStaleWriteConflictInput{
			Context:       ctx,
			LeaseStore:    store,
			MutationPaths: []string{"owned.go"},
			Now:           now.Add(time.Second),
		})
		if err == nil || !strings.Contains(err.Error(), "existing lease evidence") {
			t.Fatalf("BuildLateStaleWriteConflict missing lease error = %v, want lease evidence reject", err)
		}
	})
}

func TestLateStaleWriteConflictValidatorRejectsForgedEvidence(t *testing.T) {
	store, ctx, _, now := bindingFixture(t, time.Minute)
	conflict, err := BuildLateStaleWriteConflict(BuildLateStaleWriteConflictInput{
		Context:       ctx,
		LeaseStore:    store,
		MutationPaths: []string{"owned.go"},
		Now:           now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("BuildLateStaleWriteConflict: %v", err)
	}

	for _, tt := range []struct {
		name        string
		mutate      func(*ConflictModelResult)
		recomputeID bool
		wantErr     string
	}{
		{
			name: "conflict id mismatch",
			mutate: func(result *ConflictModelResult) {
				result.ConflictID = "conflict_forged"
			},
			wantErr: "conflict_id mismatch",
		},
		{
			name: "unknown rejection kind",
			mutate: func(result *ConflictModelResult) {
				result.RejectionKind = "maybe_stale"
			},
			recomputeID: true,
			wantErr:     "rejection_kind",
		},
		{
			name: "missing attempted holder",
			mutate: func(result *ConflictModelResult) {
				result.AttemptedHolder = nil
			},
			recomputeID: true,
			wantErr:     "attempted_holder",
		},
		{
			name: "missing observed holder",
			mutate: func(result *ConflictModelResult) {
				result.ObservedHolder = nil
			},
			recomputeID: true,
			wantErr:     "observed_holder",
		},
		{
			name: "missing observed lease",
			mutate: func(result *ConflictModelResult) {
				result.ObservedLease = nil
			},
			recomputeID: true,
			wantErr:     "observed_lease",
		},
		{
			name: "non canonical observed holder with recomputed conflict id",
			mutate: func(result *ConflictModelResult) {
				observed := *result.ObservedHolder
				observed.SessionID = " " + observed.SessionID + " "
				result.ObservedHolder = &observed
			},
			recomputeID: true,
			wantErr:     "observed_holder",
		},
		{
			name: "attempted holder context digest mismatch",
			mutate: func(result *ConflictModelResult) {
				attempted := *result.AttemptedHolder
				attempted.ContextDigest = "sha256:other-context"
				result.AttemptedHolder = &attempted
				result.AttemptedHolderDigest = digestFileLeaseHolder(attempted)
			},
			recomputeID: true,
			wantErr:     "attempted_holder context digest mismatch",
		},
		{
			name: "attempted holder digest mismatch",
			mutate: func(result *ConflictModelResult) {
				attempted := *result.AttemptedHolder
				attempted.AgentID = "agent-forged"
				result.AttemptedHolder = &attempted
			},
			recomputeID: true,
			wantErr:     "attempted_holder_digest mismatch",
		},
		{
			name: "invalid operation kind",
			mutate: func(result *ConflictModelResult) {
				result.OperationKind = "read_only_probe"
			},
			recomputeID: true,
			wantErr:     "operation_kind",
		},
		{
			name: "missing mutation paths",
			mutate: func(result *ConflictModelResult) {
				result.MutationPaths = nil
			},
			recomputeID: true,
			wantErr:     "mutation paths",
		},
		{
			name: "stale term kind without term mismatch",
			mutate: func(result *ConflictModelResult) {
				result.RejectionKind = LateStaleWriteRejectionStaleLeaseTerm
				result.ObservedLeaseTerm = result.LeaseTerm
				result.RejectionMessage = "lease.term mismatch: got 1 want 1"
			},
			recomputeID: true,
			wantErr:     "stale_lease_term evidence",
		},
		{
			name: "stale term kind with different holder",
			mutate: func(result *ConflictModelResult) {
				result.RejectionKind = LateStaleWriteRejectionStaleLeaseTerm
				setObservedLeaseStatusTerm(result, FileLeaseStatusActive, result.LeaseTerm+1)
				result.RejectionMessage = "lease_term mismatch: got 2 want 1"
				observed := *result.ObservedHolder
				observed.SessionID = "session-other"
				setObservedLeaseHolderEvidence(result, observed)
			},
			recomputeID: true,
			wantErr:     "observed_lease id binding mismatch",
		},
		{
			name: "stale term kind with terminal lease status",
			mutate: func(result *ConflictModelResult) {
				result.RejectionKind = LateStaleWriteRejectionStaleLeaseTerm
				setObservedLeaseStatusTerm(result, FileLeaseStatusRevoked, result.LeaseTerm+1)
				result.RejectionMessage = "lease_term mismatch: got 2 want 1"
			},
			recomputeID: true,
			wantErr:     "observed_lease id binding mismatch",
		},
		{
			name: "expired kind without expired status",
			mutate: func(result *ConflictModelResult) {
				setObservedLeaseStatusTerm(result, FileLeaseStatusActive, result.ObservedLeaseTerm)
			},
			recomputeID: true,
			wantErr:     "expired_lease evidence",
		},
		{
			name: "expired kind with different observed holder",
			mutate: func(result *ConflictModelResult) {
				observed := *result.ObservedHolder
				observed.AgentID = "agent-other"
				setObservedLeaseHolderEvidence(result, observed)
			},
			recomputeID: true,
			wantErr:     "observed_lease id binding mismatch",
		},
		{
			name: "active stale holder with already expired observed lease",
			mutate: func(result *ConflictModelResult) {
				result.RejectionKind = LateStaleWriteRejectionStaleHolder
				setObservedLeaseStatusTerm(result, FileLeaseStatusActive, result.ObservedLeaseTerm)
				attemptedAt := mustParseConflictTimestamp(result.AttemptedAt)
				lease := *result.ObservedLease
				lease.SessionID = "session-expired-active"
				lease.ExpiresAt = formatLeaseTime(attemptedAt.Add(-time.Nanosecond))
				result.ObservedLease = &lease
				result.ObservedLeaseDigest = digestFileLease(lease)
				observed := HolderForLease(lease)
				result.ObservedHolder = &observed
				result.ObservedHolderDigest = digestFileLeaseHolder(observed)
				result.RejectionMessage = "lease context session mismatch: got " + quoteForConflictTest(result.AttemptedHolder.SessionID) + " want " + quoteForConflictTest(observed.SessionID)
			},
			recomputeID: true,
			wantErr:     "active lease expired at attempted_at",
		},
		{
			name: "expired lease with future expiry",
			mutate: func(result *ConflictModelResult) {
				attemptedAt := mustParseConflictTimestamp(result.AttemptedAt)
				lease := *result.ObservedLease
				lease.ExpiresAt = formatLeaseTime(attemptedAt.Add(time.Hour))
				result.ObservedLease = &lease
				result.ObservedLeaseDigest = digestFileLease(lease)
			},
			recomputeID: true,
			wantErr:     "expired lease timing",
		},
		{
			name: "stale holder kind with terminal lease status",
			mutate: func(result *ConflictModelResult) {
				result.RejectionKind = LateStaleWriteRejectionStaleHolder
				setObservedLeaseStatusTerm(result, FileLeaseStatusRevoked, result.ObservedLeaseTerm)
				result.RejectionMessage = "lease holder mismatch: got stale holder"
				observed := *result.ObservedHolder
				observed.SessionID = "session-other"
				setObservedLeaseHolderEvidence(result, observed)
			},
			recomputeID: true,
			wantErr:     "stale_holder evidence",
		},
		{
			name: "stale holder observed holder outside workspace scope",
			mutate: func(result *ConflictModelResult) {
				result.RejectionKind = LateStaleWriteRejectionStaleHolder
				setObservedLeaseStatusTerm(result, FileLeaseStatusActive, result.ObservedLeaseTerm)
				result.RejectionMessage = "lease holder mismatch: forged observed holder"
				observed := *result.ObservedHolder
				observed.WorkspaceID = "workspace-other"
				observed.TaskID = "task-other"
				setObservedLeaseHolderEvidence(result, observed)
			},
			recomputeID: true,
			wantErr:     "observed_holder workspace mismatch",
		},
		{
			name: "stale holder observed holder identity forged with matching holder digest",
			mutate: func(result *ConflictModelResult) {
				result.RejectionKind = LateStaleWriteRejectionStaleHolder
				setObservedLeaseStatusTerm(result, FileLeaseStatusActive, result.ObservedLeaseTerm)
				result.RejectionMessage = "lease holder mismatch: forged observed identity"
				observed := *result.ObservedHolder
				observed.SessionID = "session-forged-observed"
				observed.RunID = "run-forged-observed"
				observed.AgentID = "agent-forged-observed"
				result.ObservedHolder = &observed
				result.ObservedHolderDigest = digestFileLeaseHolder(observed)
			},
			recomputeID: true,
			wantErr:     "observed_lease holder mismatch",
		},
		{
			name: "stale holder observed lease snapshot forged with matching nested digests",
			mutate: func(result *ConflictModelResult) {
				result.RejectionKind = LateStaleWriteRejectionStaleHolder
				setObservedLeaseStatusTerm(result, FileLeaseStatusActive, result.ObservedLeaseTerm)
				result.RejectionMessage = "lease holder mismatch: forged observed lease snapshot"
				lease := *result.ObservedLease
				lease.SessionID = "session-forged-observed"
				lease.RunID = "run-forged-observed"
				lease.AgentID = "agent-forged-observed"
				result.ObservedLease = &lease
				result.ObservedLeaseDigest = digestFileLease(lease)
				observed := HolderForLease(lease)
				result.ObservedHolder = &observed
				result.ObservedHolderDigest = digestFileLeaseHolder(observed)
			},
			recomputeID: true,
			wantErr:     "observed_lease id binding mismatch",
		},
		{
			name: "stale holder observed lease session forged with generic message",
			mutate: func(result *ConflictModelResult) {
				result.RejectionKind = LateStaleWriteRejectionStaleHolder
				setObservedLeaseStatusTerm(result, FileLeaseStatusActive, result.ObservedLeaseTerm)
				result.RejectionMessage = "lease holder mismatch: forged observed lease snapshot"
				lease := *result.ObservedLease
				lease.SessionID = "session-forged-observed"
				result.ObservedLease = &lease
				result.ObservedLeaseDigest = digestFileLease(lease)
				observed := HolderForLease(lease)
				result.ObservedHolder = &observed
				result.ObservedHolderDigest = digestFileLeaseHolder(observed)
			},
			recomputeID: true,
			wantErr:     "stale_holder rejection message",
		},
		{
			name: "stale holder observed lease session forged with exact message",
			mutate: func(result *ConflictModelResult) {
				result.RejectionKind = LateStaleWriteRejectionStaleHolder
				setObservedLeaseStatusTerm(result, FileLeaseStatusActive, result.ObservedLeaseTerm)
				lease := *result.ObservedLease
				lease.SessionID = "session-forged-observed"
				result.ObservedLease = &lease
				result.ObservedLeaseDigest = digestFileLease(lease)
				observed := HolderForLease(lease)
				result.ObservedHolder = &observed
				result.ObservedHolderDigest = digestFileLeaseHolder(observed)
				result.RejectionMessage = "lease context session mismatch: got " + quoteForConflictTest(result.AttemptedHolder.SessionID) + " want " + quoteForConflictTest(observed.SessionID)
			},
			recomputeID: true,
			wantErr:     "observed_lease store mismatch",
		},
		{
			name: "stale holder observed lease snapshot forged with rebuilt lease id",
			mutate: func(result *ConflictModelResult) {
				result.RejectionKind = LateStaleWriteRejectionStaleHolder
				setObservedLeaseStatusTerm(result, FileLeaseStatusActive, result.ObservedLeaseTerm)
				lease := *result.ObservedLease
				lease.SessionID = "session-forged-observed"
				lease.RunID = "run-forged-observed"
				lease.AgentID = "agent-forged-observed"
				lease.ID = buildFileLeaseID(Context{
					WorkspaceID: lease.WorkspaceID,
					TaskID:      lease.TaskID,
					AgentID:     lease.AgentID,
				}, lease.Term, lease.ContextDigest)
				result.RepoLeaseID = lease.ID
				result.ObservedLease = &lease
				result.ObservedLeaseDigest = digestFileLease(lease)
				observed := HolderForLease(lease)
				result.ObservedHolder = &observed
				result.ObservedHolderDigest = digestFileLeaseHolder(observed)
				result.RejectionMessage = "lease context session mismatch: got " + quoteForConflictTest(result.AttemptedHolder.SessionID) + " want " + quoteForConflictTest(observed.SessionID)
			},
			recomputeID: true,
			wantErr:     "observed_lease store lookup",
		},
		{
			name: "stale holder observed holder identity forged without digest update",
			mutate: func(result *ConflictModelResult) {
				result.RejectionKind = LateStaleWriteRejectionStaleHolder
				setObservedLeaseStatusTerm(result, FileLeaseStatusActive, result.ObservedLeaseTerm)
				result.RejectionMessage = "lease holder mismatch: forged observed identity"
				observed := *result.ObservedHolder
				observed.SessionID = "session-forged-observed"
				observed.RunID = "run-forged-observed"
				observed.AgentID = "agent-forged-observed"
				result.ObservedHolder = &observed
			},
			recomputeID: true,
			wantErr:     "observed_holder_digest mismatch",
		},
		{
			name: "invalid attempted at timestamp",
			mutate: func(result *ConflictModelResult) {
				result.AttemptedAt = "not-a-time"
			},
			recomputeID: true,
			wantErr:     "attempted_at",
		},
		{
			name: "CAS evidence must not appear",
			mutate: func(result *ConflictModelResult) {
				result.CASStatus = CASPatchStatusApplied
			},
			recomputeID: true,
			wantErr:     "CAS conflict evidence",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			forged := conflict
			tt.mutate(&forged)
			if tt.recomputeID {
				forged.ConflictID = conflictModelID(forged)
			}
			err := ValidateConflictModelResultWithLeaseStore(forged, store)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateConflictModelResultWithLeaseStore forged late stale write error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func setObservedLeaseHolderEvidence(result *ConflictModelResult, holder FileLeaseHolder) {
	result.ObservedHolder = &holder
	result.ObservedHolderDigest = digestFileLeaseHolder(holder)
	if result.ObservedLease != nil {
		lease := *result.ObservedLease
		lease.WorkspaceID = holder.WorkspaceID
		lease.TaskID = holder.TaskID
		lease.SessionID = holder.SessionID
		lease.RunID = holder.RunID
		lease.AgentID = holder.AgentID
		lease.Principal = holder.Principal
		lease.CapabilitySnapshotID = holder.CapabilitySnapshotID
		lease.ContextDigest = holder.ContextDigest
		result.ObservedLease = &lease
		result.ObservedLeaseDigest = digestFileLease(lease)
	}
}

func setObservedLeaseStatusTerm(result *ConflictModelResult, status string, term int64) {
	result.ObservedLeaseStatus = status
	result.ObservedLeaseTerm = term
	if result.ObservedLease != nil {
		lease := *result.ObservedLease
		lease.Status = status
		lease.Term = term
		attemptedAt := mustParseConflictTimestamp(result.AttemptedAt)
		expiresAt := mustParseConflictTimestamp(lease.ExpiresAt)
		if status == FileLeaseStatusActive && !attemptedAt.Before(expiresAt) {
			lease.ExpiresAt = formatLeaseTime(attemptedAt.Add(time.Minute))
		}
		if (status == FileLeaseStatusReleased || status == FileLeaseStatusRevoked) && !mustParseConflictTimestamp(lease.UpdatedAt).Before(expiresAt) {
			lease.ExpiresAt = formatLeaseTime(attemptedAt.Add(time.Minute))
		}
		result.ObservedLease = &lease
		result.ObservedLeaseDigest = digestFileLease(lease)
	}
}

func mustParseConflictTimestamp(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return parsed
}

func quoteForConflictTest(value string) string {
	return `"` + value + `"`
}

func TestConflictModelIDMatchesRepoAcceptanceHarnessFixtures(t *testing.T) {
	t.Run("path conflict", func(t *testing.T) {
		result := ConflictModelResult{
			Schema:      ConflictModelSchemaVersion,
			Status:      ConflictStatusConflict,
			Reason:      ConflictReasonPathConflict,
			Path:        "src/shared.py",
			WorkspaceID: "workspace-b6",
			Contenders: []ConflictContender{
				repoAcceptanceHarnessContender("patch-a", "agent-a", PatchQueueStateProposed),
				repoAcceptanceHarnessContender("patch-b", "agent-b", PatchQueueStateValidating),
			},
		}
		if got, want := conflictModelID(result), "conflict_b4e1203f1f77118a"; got != want {
			t.Fatalf("conflictModelID(path_conflict) = %q, want %q", got, want)
		}
	})

	t.Run("path conflict with html escaped path", func(t *testing.T) {
		result := ConflictModelResult{
			Schema:      ConflictModelSchemaVersion,
			Status:      ConflictStatusConflict,
			Reason:      ConflictReasonPathConflict,
			Path:        "src/a&b.py",
			WorkspaceID: "workspace-b6",
			Contenders: []ConflictContender{
				repoAcceptanceHarnessContender("patch-a", "agent-a", PatchQueueStateProposed),
				repoAcceptanceHarnessContender("patch-b", "agent-b", PatchQueueStateValidating),
			},
		}
		if got, want := conflictModelID(result), "conflict_ec03f27d40ad5cac"; got != want {
			t.Fatalf("conflictModelID(path_conflict html path) = %q, want %q", got, want)
		}
	})

	t.Run("path conflict with utf8 path", func(t *testing.T) {
		result := ConflictModelResult{
			Schema:      ConflictModelSchemaVersion,
			Status:      ConflictStatusConflict,
			Reason:      ConflictReasonPathConflict,
			Path:        "src/\u00e9.py",
			WorkspaceID: "workspace-b6",
			Contenders: []ConflictContender{
				repoAcceptanceHarnessContender("patch-a", "agent-a", PatchQueueStateProposed),
				repoAcceptanceHarnessContender("patch-b", "agent-b", PatchQueueStateValidating),
			},
		}
		if got, want := conflictModelID(result), "conflict_d00bfcdb155b7dc0"; got != want {
			t.Fatalf("conflictModelID(path_conflict utf8 path) = %q, want %q", got, want)
		}
	})

	t.Run("base drift", func(t *testing.T) {
		result := ConflictModelResult{
			Schema:              ConflictModelSchemaVersion,
			Status:              ConflictStatusConflict,
			Reason:              ConflictReasonBaseDrift,
			Path:                "src/drift.py",
			WorkspaceID:         "workspace-b6",
			TaskID:              "task-patch-drift",
			SessionID:           "session-patch-drift",
			RunID:               "run-patch-drift",
			AgentID:             "agent-drift",
			PrincipalID:         "principal-agent-drift",
			ContextDigest:       "context-patch-drift",
			RepoLeaseID:         "lease-patch-drift",
			LeaseTerm:           1,
			PatchQueueID:        "queue-b6",
			PatchQueueItemID:    "patch-drift",
			PatchQueueState:     PatchQueueStateConflict,
			BaseRef:             "main@base-b6",
			BaseTreeHash:        "tree-b6",
			ExpectedHash:        "hash-base",
			ActualHash:          "hash-drifted",
			CandidateHash:       "hash-candidate",
			CASPatchDigest:      "patch-digest-patch-drift",
			CASEvaluationDigest: "eval-digest-patch-drift",
			CASIssueKind:        CASPatchIssueBaseDrift,
		}
		if got, want := conflictModelID(result), "conflict_7d3af7dd6050af9a"; got != want {
			t.Fatalf("conflictModelID(base_drift) = %q, want %q", got, want)
		}
	})

	t.Run("base drift with utf8 path", func(t *testing.T) {
		result := ConflictModelResult{
			Schema:              ConflictModelSchemaVersion,
			Status:              ConflictStatusConflict,
			Reason:              ConflictReasonBaseDrift,
			Path:                "src/\u00e9.py",
			WorkspaceID:         "workspace-b6",
			TaskID:              "task-patch-drift",
			SessionID:           "session-patch-drift",
			RunID:               "run-patch-drift",
			AgentID:             "agent-drift",
			PrincipalID:         "principal-agent-drift",
			ContextDigest:       "context-patch-drift",
			RepoLeaseID:         "lease-patch-drift",
			LeaseTerm:           1,
			PatchQueueID:        "queue-b6",
			PatchQueueItemID:    "patch-drift",
			PatchQueueState:     PatchQueueStateConflict,
			BaseRef:             "main@base-b6",
			BaseTreeHash:        "tree-b6",
			ExpectedHash:        "hash-base",
			ActualHash:          "hash-drifted",
			CandidateHash:       "hash-candidate",
			CASPatchDigest:      "patch-digest-patch-drift",
			CASEvaluationDigest: "eval-digest-patch-drift",
			CASIssueKind:        CASPatchIssueBaseDrift,
		}
		if got, want := conflictModelID(result), "conflict_4145acfdad63a5c2"; got != want {
			t.Fatalf("conflictModelID(base_drift utf8 path) = %q, want %q", got, want)
		}
	})

	t.Run("base drift with html escaped path", func(t *testing.T) {
		result := ConflictModelResult{
			Schema:              ConflictModelSchemaVersion,
			Status:              ConflictStatusConflict,
			Reason:              ConflictReasonBaseDrift,
			Path:                "src/a&b.py",
			WorkspaceID:         "workspace-b6",
			TaskID:              "task-patch-drift",
			SessionID:           "session-patch-drift",
			RunID:               "run-patch-drift",
			AgentID:             "agent-drift",
			PrincipalID:         "principal-agent-drift",
			ContextDigest:       "context-patch-drift",
			RepoLeaseID:         "lease-patch-drift",
			LeaseTerm:           1,
			PatchQueueID:        "queue-b6",
			PatchQueueItemID:    "patch-drift",
			PatchQueueState:     PatchQueueStateConflict,
			BaseRef:             "main@base-b6",
			BaseTreeHash:        "tree-b6",
			ExpectedHash:        "hash-base",
			ActualHash:          "hash-drifted",
			CandidateHash:       "hash-candidate",
			CASPatchDigest:      "patch-digest-patch-drift",
			CASEvaluationDigest: "eval-digest-patch-drift",
			CASIssueKind:        CASPatchIssueBaseDrift,
		}
		if got, want := conflictModelID(result), "conflict_cff2e2cfb83ab6a5"; got != want {
			t.Fatalf("conflictModelID(base_drift html path) = %q, want %q", got, want)
		}
	})
}

func TestPathConflictModelRejectsMalformedOrAmbiguousEvidence(t *testing.T) {
	t.Run("same item is ambiguous", func(t *testing.T) {
		item := pathConflictItem("patchq-b4", "patchitem-left", "agent-left", "task-left", "owned.go")
		_, err := BuildPathConflict(BuildPathConflictInput{Path: "owned.go", Left: item, Right: item})
		if err == nil || !strings.Contains(err.Error(), "different patch queue items") {
			t.Fatalf("BuildPathConflict same item error = %v, want different item reject", err)
		}
	})

	t.Run("path must be covered by both contenders", func(t *testing.T) {
		left := pathConflictItem("patchq-b4", "patchitem-left", "agent-left", "task-left", "owned.go")
		right := pathConflictItem("patchq-b4", "patchitem-right", "agent-right", "task-right", "other.go")
		_, err := BuildPathConflict(BuildPathConflictInput{Path: "owned.go", Left: left, Right: right})
		if err == nil || !strings.Contains(err.Error(), "does not cover path") {
			t.Fatalf("BuildPathConflict path coverage error = %v, want coverage reject", err)
		}
	})

	t.Run("workspace mismatch fails closed", func(t *testing.T) {
		left := pathConflictItem("patchq-b4", "patchitem-left", "agent-left", "task-left", "owned.go")
		right := pathConflictItem("patchq-b4", "patchitem-right", "agent-right", "task-right", "owned.go")
		right.WorkspaceID = "ws-other"
		_, err := BuildPathConflict(BuildPathConflictInput{Path: "owned.go", Left: left, Right: right})
		if err == nil || !strings.Contains(err.Error(), "same workspace") {
			t.Fatalf("BuildPathConflict workspace error = %v, want workspace reject", err)
		}
	})

	t.Run("terminal and unknown contender states are rejected", func(t *testing.T) {
		for _, state := range []string{
			PatchQueueStateApplied,
			PatchQueueStateFailed,
			PatchQueueStateCanceled,
			PatchQueueStateConflict,
			PatchQueueStateTestConflict,
			"surprise_state",
		} {
			t.Run(state, func(t *testing.T) {
				left := pathConflictItem("patchq-b4", "patchitem-left", "agent-left", "task-left", "owned.go")
				right := pathConflictItem("patchq-b4", "patchitem-right", "agent-right", "task-right", "owned.go")
				left.State = state
				_, err := BuildPathConflict(BuildPathConflictInput{Path: "owned.go", Left: left, Right: right})
				if err == nil || !strings.Contains(err.Error(), "active path_conflict contender state") {
					t.Fatalf("BuildPathConflict state %q error = %v, want active-state reject", state, err)
				}
			})
		}
	})

	t.Run("validator rejects missing contender", func(t *testing.T) {
		result := ConflictModelResult{
			Schema:      ConflictModelSchemaVersion,
			ConflictID:  "conflict_forged",
			Status:      ConflictStatusConflict,
			Reason:      ConflictReasonPathConflict,
			Path:        "owned.go",
			WorkspaceID: "ws-b4",
			Contenders:  []ConflictContender{pathConflictContender(pathConflictItem("patchq-b4", "patchitem-left", "agent-left", "task-left", "owned.go"))},
		}
		err := ValidateConflictModelResult(result)
		if err == nil || !strings.Contains(err.Error(), "exactly two contenders") {
			t.Fatalf("ValidateConflictModelResult missing contender error = %v, want count reject", err)
		}
	})
}

func pathConflictItem(queueID, itemID, agentID, taskID, path string) PatchQueueItem {
	return PatchQueueItem{
		Schema:                   PatchQueueItemSchemaVersion,
		ID:                       "patchqitem:v1:" + itemID,
		QueueID:                  queueID,
		ItemID:                   itemID,
		State:                    PatchQueueStateValidating,
		ContextDigest:            "sha256:ctx-" + itemID,
		RepoLeaseID:              "repolease:v1:" + itemID,
		LeaseTerm:                10,
		Pathset:                  []string{path},
		WorkspaceID:              "ws-b4",
		TaskID:                   taskID,
		SessionID:                "session-" + itemID,
		RunID:                    "run-" + itemID,
		AgentID:                  agentID,
		PrincipalType:            "agent",
		PrincipalID:              agentID,
		CapabilitySnapshotID:     "cap-" + itemID,
		CapabilitySnapshotSchema: "runtime_capability_snapshot.v1",
		BaseRef:                  "base-b4",
		BaseTreeHash:             "sha256:base-tree-b4",
		CASPatchDigest:           "sha256:patch-" + itemID,
		CASEvaluationDigest:      "sha256:cas-" + itemID,
	}
}

func baseDriftConflictFixture(t *testing.T) (Context, CASPatchApplyResult, PatchQueueItem) {
	t.Helper()
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
	item, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:   queueCtx,
		CASResult: cas,
		Now:       now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("CompleteValidation conflict: %v", err)
	}
	return queueCtx, item.CASResult, item
}

func testConflictFixture(t *testing.T) (Context, PatchQueueItem) {
	t.Helper()
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
	item, err := queueStore.CompleteValidation(CompletePatchQueueValidationInput{
		Context:      queueCtx,
		CASResult:    cas,
		TestEvidence: patchQueueFailedTestEvidence(),
		Now:          now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("CompleteValidation test_conflict: %v", err)
	}
	if item.State != PatchQueueStateTestConflict {
		t.Fatalf("state = %q, want test_conflict", item.State)
	}
	return queueCtx, item
}

func pathConflictContender(item PatchQueueItem) ConflictContender {
	return ConflictContender{
		WorkspaceID:         item.WorkspaceID,
		QueueID:             item.QueueID,
		ItemID:              item.ItemID,
		State:               item.State,
		ContextDigest:       item.ContextDigest,
		RepoLeaseID:         item.RepoLeaseID,
		LeaseTerm:           item.LeaseTerm,
		TaskID:              item.TaskID,
		SessionID:           item.SessionID,
		RunID:               item.RunID,
		AgentID:             item.AgentID,
		BaseRef:             item.BaseRef,
		BaseTreeHash:        item.BaseTreeHash,
		CASPatchDigest:      item.CASPatchDigest,
		CASEvaluationDigest: item.CASEvaluationDigest,
	}
}

func repoAcceptanceHarnessContender(itemID, agentID, state string) ConflictContender {
	return ConflictContender{
		WorkspaceID:         "workspace-b6",
		QueueID:             "queue-b6",
		ItemID:              itemID,
		State:               state,
		ContextDigest:       "context-" + itemID,
		RepoLeaseID:         "lease-" + itemID,
		LeaseTerm:           1,
		TaskID:              "task-" + itemID,
		SessionID:           "session-" + itemID,
		RunID:               "run-" + itemID,
		AgentID:             agentID,
		BaseRef:             "main@base-b6",
		BaseTreeHash:        "tree-b6",
		CASPatchDigest:      "patch-digest-" + itemID,
		CASEvaluationDigest: "eval-digest-" + itemID,
	}
}
