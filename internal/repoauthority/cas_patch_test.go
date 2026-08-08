package repoauthority

import (
	"strings"
	"testing"
)

func TestCASPatchApplyAppliedDeterministic(t *testing.T) {
	ctx := casPatchTestContext([]string{"a.go", "dir/b.go"})
	current := map[string]string{
		"a.go":     ctx.Base.FileHashes["a.go"],
		"dir/b.go": ctx.Base.FileHashes["dir/b.go"],
	}
	candidate := map[string]string{
		"dir/b.go": "sha256:new-b",
		"a.go":     "sha256:new-a",
	}

	first := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:             ctx,
		PatchID:             "patch-b1-5",
		CurrentFileHashes:   current,
		CandidateFileHashes: candidate,
	})
	second := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:             ctx,
		PatchID:             "patch-b1-5",
		CurrentFileHashes:   current,
		CandidateFileHashes: candidate,
	})

	if first.Status != CASPatchStatusApplied {
		t.Fatalf("status = %q, issues=%+v", first.Status, first.Issues)
	}
	if len(first.Paths) != 2 || first.Paths[0].Path != "a.go" || first.Paths[1].Path != "dir/b.go" {
		t.Fatalf("paths are not deterministic/sorted: %+v", first.Paths)
	}
	if first.PatchDigest == "" || first.PatchDigest != second.PatchDigest {
		t.Fatalf("patch digest not deterministic: %q vs %q", first.PatchDigest, second.PatchDigest)
	}
	if first.ContextDigest == "" || first.ContextDigest != second.ContextDigest {
		t.Fatalf("context digest not deterministic: %q vs %q", first.ContextDigest, second.ContextDigest)
	}
	if len(first.Issues) != 0 {
		t.Fatalf("applied result has issues: %+v", first.Issues)
	}
}

func TestCASPatchApplyBaseDriftProducesExplicitConflict(t *testing.T) {
	ctx := casPatchTestContext([]string{"owned.go"})
	result := EvaluateCASPatchApply(CASPatchApplyInput{
		Context: ctx,
		CurrentFileHashes: map[string]string{
			"owned.go": "sha256:drifted-current",
		},
		CandidateFileHashes: map[string]string{
			"owned.go": "sha256:new-owned",
		},
	})

	if result.Status != CASPatchStatusConflict {
		t.Fatalf("status = %q, want conflict; result=%+v", result.Status, result)
	}
	if len(result.Paths) != 1 || result.Paths[0].Status != CASPatchStatusConflict {
		t.Fatalf("path result = %+v, want conflict", result.Paths)
	}
	if !hasCASPatchIssue(result, CASPatchIssueBaseDrift, "owned.go") {
		t.Fatalf("missing base_drift issue in %+v", result.Issues)
	}
	issue := result.Issues[0]
	if issue.ExpectedHash != ctx.Base.FileHashes["owned.go"] || issue.ActualHash != "sha256:drifted-current" || issue.CandidateHash != "sha256:new-owned" {
		t.Fatalf("unexpected base drift issue hashes: %+v", issue)
	}
}

func TestCASPatchApplyMissingCurrentHashFailsClosed(t *testing.T) {
	ctx := casPatchTestContext([]string{"owned.go"})
	result := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:           ctx,
		CurrentFileHashes: map[string]string{},
		CandidateFileHashes: map[string]string{
			"owned.go": "sha256:new-owned",
		},
	})

	if result.Status != CASPatchStatusFailed {
		t.Fatalf("status = %q, want failed; result=%+v", result.Status, result)
	}
	if !hasCASPatchIssue(result, CASPatchIssueCurrentHashMissing, "owned.go") {
		t.Fatalf("missing current hash issue in %+v", result.Issues)
	}
}

func TestCASPatchApplyMissingBaseHashFailsClosed(t *testing.T) {
	ctx := casPatchTestContext([]string{"owned.go"})
	delete(ctx.Base.FileHashes, "owned.go")
	result := EvaluateCASPatchApply(CASPatchApplyInput{
		Context: ctx,
		CurrentFileHashes: map[string]string{
			"owned.go": "sha256:base-owned-go",
		},
		CandidateFileHashes: map[string]string{
			"owned.go": "sha256:new-owned",
		},
	})

	if result.Status != CASPatchStatusFailed {
		t.Fatalf("status = %q, want failed; result=%+v", result.Status, result)
	}
	if !hasCASPatchIssue(result, CASPatchIssueBaseHashMissing, "owned.go") {
		t.Fatalf("missing base hash issue in %+v", result.Issues)
	}
}

func TestCASPatchApplyPathOutsideContextFailsClosed(t *testing.T) {
	ctx := casPatchTestContext([]string{"owned.go"})
	result := EvaluateCASPatchApply(CASPatchApplyInput{
		Context: ctx,
		CurrentFileHashes: map[string]string{
			"outside.go": "sha256:base-outside",
		},
		CandidateFileHashes: map[string]string{
			"outside.go": "sha256:new-outside",
		},
	})

	if result.Status != CASPatchStatusFailed {
		t.Fatalf("status = %q, want failed; result=%+v", result.Status, result)
	}
	if !hasCASPatchIssue(result, CASPatchIssuePathOutsideContext, "outside.go") {
		t.Fatalf("missing outside context issue in %+v", result.Issues)
	}
}

func TestCASPatchApplyAcceptsConcreteCandidateCoveredByScopedPathset(t *testing.T) {
	ctx := casPatchTestContext([]string{"web/**"})
	ctx.Base.FileHashes = map[string]string{
		"web/app.js": "sha256:base-web-app",
	}
	result := EvaluateCASPatchApply(CASPatchApplyInput{
		Context: ctx,
		CurrentFileHashes: map[string]string{
			"web/app.js": "sha256:base-web-app",
		},
		CandidateFileHashes: map[string]string{
			"web/app.js": "sha256:new-web-app",
		},
	})

	if result.Status != CASPatchStatusApplied {
		t.Fatalf("status = %q, want applied for concrete path under scope; result=%+v", result.Status, result)
	}
	if len(result.Paths) != 1 || result.Paths[0].Path != "web/app.js" {
		t.Fatalf("unexpected scoped CAS paths: %+v", result.Paths)
	}

	result = EvaluateCASPatchApply(CASPatchApplyInput{
		Context: ctx,
		CurrentFileHashes: map[string]string{
			"api/server.go": "sha256:base-api",
		},
		CandidateFileHashes: map[string]string{
			"api/server.go": "sha256:new-api",
		},
	})
	if result.Status != CASPatchStatusFailed || !hasCASPatchIssue(result, CASPatchIssuePathOutsideContext, "api/server.go") {
		t.Fatalf("expected outside-scope candidate to fail closed, got %+v", result)
	}
}

func TestCASPatchApplyAcceptsAddedFileUnderScopedPathset(t *testing.T) {
	ctx := casPatchTestContext([]string{"web/**"})
	ctx.Base.FileHashes = map[string]string{}

	result := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:           ctx,
		CurrentFileHashes: map[string]string{},
		CandidateFileHashes: map[string]string{
			"web/new.js": "sha256:new-web-file",
		},
	})

	if result.Status != CASPatchStatusApplied {
		t.Fatalf("status = %q, want applied for added file; result=%+v", result.Status, result)
	}
	if len(result.Paths) != 1 {
		t.Fatalf("paths = %+v, want one path", result.Paths)
	}
	path := result.Paths[0]
	if path.Path != "web/new.js" || path.ChangeKind != CASPatchChangeAdd || path.BaseHash != "" || path.CurrentHash != "" || path.CandidateHash != "sha256:new-web-file" {
		t.Fatalf("unexpected add CAS path: %+v", path)
	}
	if err := validateCASEvidence(result, ctx); err != nil {
		t.Fatalf("validate add CAS evidence: %v", err)
	}
}

func TestCASPatchApplyAcceptsAddedFileUnderExactPathset(t *testing.T) {
	ctx := casPatchTestContext([]string{"web/new.js"})
	ctx.Base.FileHashes = map[string]string{}

	result := EvaluateCASPatchApply(CASPatchApplyInput{
		Context:           ctx,
		CurrentFileHashes: map[string]string{},
		CandidateFileHashes: map[string]string{
			"web/new.js": "sha256:new-web-file",
		},
	})

	if result.Status != CASPatchStatusApplied {
		t.Fatalf("status = %q, want applied for exact added file; result=%+v", result.Status, result)
	}
	if len(result.Paths) != 1 || result.Paths[0].ChangeKind != CASPatchChangeAdd || result.Paths[0].BaseHash != "" || result.Paths[0].CurrentHash != "" {
		t.Fatalf("unexpected exact add CAS path: %+v", result.Paths)
	}
	if err := validateCASEvidence(result, ctx); err != nil {
		t.Fatalf("validate exact add CAS evidence: %v", err)
	}
}

func TestCASPatchApplyRejectsMissingOrInvalidCandidateHashes(t *testing.T) {
	tests := []struct {
		name      string
		candidate map[string]string
		wantKind  string
		wantPath  string
	}{
		{
			name:      "missing candidate map",
			candidate: nil,
			wantKind:  CASPatchIssueCandidateHashesRequired,
		},
		{
			name:      "empty candidate hash",
			candidate: map[string]string{"owned.go": " "},
			wantKind:  CASPatchIssueCandidateHashMissing,
			wantPath:  "owned.go",
		},
		{
			name:      "traversal candidate path",
			candidate: map[string]string{"../owned.go": "sha256:new"},
			wantKind:  CASPatchIssueCandidatePathInvalid,
			wantPath:  "../owned.go",
		},
		{
			name:      "unstable candidate path",
			candidate: map[string]string{"./owned.go": "sha256:new"},
			wantKind:  CASPatchIssueCandidatePathUnstable,
			wantPath:  "./owned.go",
		},
		{
			name:      "absolute candidate path",
			candidate: map[string]string{"C:\\tmp\\owned.go": "sha256:new"},
			wantKind:  CASPatchIssueCandidatePathInvalid,
			wantPath:  "C:\\tmp\\owned.go",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := casPatchTestContext([]string{"owned.go"})
			result := EvaluateCASPatchApply(CASPatchApplyInput{
				Context: ctx,
				CurrentFileHashes: map[string]string{
					"owned.go": ctx.Base.FileHashes["owned.go"],
				},
				CandidateFileHashes: tt.candidate,
			})
			if result.Status != CASPatchStatusFailed {
				t.Fatalf("status = %q, want failed; result=%+v", result.Status, result)
			}
			if !hasCASPatchIssue(result, tt.wantKind, tt.wantPath) {
				t.Fatalf("missing issue kind=%q path=%q in %+v", tt.wantKind, tt.wantPath, result.Issues)
			}
		})
	}
}

func TestCASPatchApplyFailureTrumpsConflict(t *testing.T) {
	ctx := casPatchTestContext([]string{"a.go", "b.go"})
	result := EvaluateCASPatchApply(CASPatchApplyInput{
		Context: ctx,
		CurrentFileHashes: map[string]string{
			"a.go": "sha256:drifted-a",
		},
		CandidateFileHashes: map[string]string{
			"a.go": "sha256:new-a",
			"b.go": "sha256:new-b",
		},
	})

	if result.Status != CASPatchStatusFailed {
		t.Fatalf("status = %q, want failed because missing current hash must trump conflict; result=%+v", result.Status, result)
	}
	if !hasCASPatchIssue(result, CASPatchIssueBaseDrift, "a.go") {
		t.Fatalf("missing base drift issue in %+v", result.Issues)
	}
	if !hasCASPatchIssue(result, CASPatchIssueCurrentHashMissing, "b.go") {
		t.Fatalf("missing current hash issue in %+v", result.Issues)
	}
}

func casPatchTestContext(pathset []string) Context {
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
		WorkspaceID: "ws-b1-5",
		TaskID:      "task-b1-5",
		SessionID:   "session-b1-5",
		RunID:       "run-b1-5",
		AgentID:     "agent-b1-5",
		Principal: PrincipalRef{
			Type: "agent",
			ID:   "agent-b1-5",
		},
		CapabilitySnapshot: CapabilitySnapshotRef{
			ID:     "cap-b1-5",
			Schema: "runtime_capability_snapshot.v1",
		},
		RepoRoot: "C:/work/rhizome",
		Base: BaseIdentity{
			Ref:        "base-b1-5",
			TreeHash:   "sha256:base-tree-b1-5",
			FileHashes: hashes,
		},
		Pathset: normalized,
	}
}

func hasCASPatchIssue(result CASPatchApplyResult, kind, path string) bool {
	for _, issue := range result.Issues {
		if issue.Kind == kind && issue.Path == path {
			return true
		}
	}
	return false
}
