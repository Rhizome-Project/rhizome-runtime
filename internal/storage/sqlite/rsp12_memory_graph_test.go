package sqlite

import (
	"context"
	"testing"
)

func TestRSP12MemoryGraphTypeSemantics(t *testing.T) {
	t.Parallel()

	t.Run("workspace memory canonical types", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name      string
			record    WorkspaceMemoryRecord
			wantType  string
			wantLayer string
			wantPred  string
			wantMod   string
		}{
			{
				name:      "procedure",
				record:    WorkspaceMemoryRecord{MemoryType: "PROCEDURE"},
				wantType:  "PROCEDURE",
				wantLayer: "PROCEDURAL",
				wantPred:  "prescribes",
				wantMod:   "inferred",
			},
			{
				name:      "anti procedure",
				record:    WorkspaceMemoryRecord{MemoryType: "ANTI_PROCEDURE"},
				wantType:  "ANTI_PROCEDURE",
				wantLayer: "PROCEDURAL",
				wantPred:  "proscribes",
				wantMod:   "inferred",
			},
			{
				name:      "self model",
				record:    WorkspaceMemoryRecord{MemoryType: "SELF_MODEL"},
				wantType:  "SELF_MODEL",
				wantLayer: "IDENTITY",
				wantPred:  "models",
				wantMod:   "inferred",
			},
			{
				name:      "goal commitment",
				record:    WorkspaceMemoryRecord{MemoryType: "GOAL_COMMITMENT"},
				wantType:  "GOAL_COMMITMENT",
				wantLayer: "IDENTITY",
				wantPred:  "commits_to",
				wantMod:   "constrained",
			},
			{
				name:      "alternative branch",
				record:    WorkspaceMemoryRecord{MemoryType: "ALTERNATIVE_BRANCH"},
				wantType:  "ALTERNATIVE_BRANCH",
				wantLayer: "SEMANTIC",
				wantPred:  "branches",
				wantMod:   "proposed",
			},
			{
				name:      "unknown falls back to episode pack",
				record:    WorkspaceMemoryRecord{MemoryType: "mystery"},
				wantType:  "EPISODE_PACK",
				wantLayer: "EPISODIC",
				wantPred:  "summarizes",
				wantMod:   "observed",
			},
		}

		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				gotType := canonicalMemoryTypeFromWorkspaceMemory(tc.record)
				if gotType != tc.wantType {
					t.Fatalf("canonicalMemoryTypeFromWorkspaceMemory() = %q, want %q", gotType, tc.wantType)
				}
				if gotLayer := memoryGraphLayerForType(gotType); gotLayer != tc.wantLayer {
					t.Fatalf("memoryGraphLayerForType(%q) = %q, want %q", gotType, gotLayer, tc.wantLayer)
				}
				if gotPred := memoryGraphPredicateForType(gotType); gotPred != tc.wantPred {
					t.Fatalf("memoryGraphPredicateForType(%q) = %q, want %q", gotType, gotPred, tc.wantPred)
				}
				if gotMod := memoryGraphModalityForType(gotType); gotMod != tc.wantMod {
					t.Fatalf("memoryGraphModalityForType(%q) = %q, want %q", gotType, gotMod, tc.wantMod)
				}
			})
		}
	})

	t.Run("knowledge claim canonical types", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name      string
			record    KnowledgeClaimRecord
			wantType  string
			wantLayer string
			wantPred  string
			wantMod   string
		}{
			{
				name:      "procedure",
				record:    KnowledgeClaimRecord{ClaimType: "PROCEDURE"},
				wantType:  "PROCEDURE",
				wantLayer: "PROCEDURAL",
				wantPred:  "prescribes",
				wantMod:   "inferred",
			},
			{
				name:      "anti procedure",
				record:    KnowledgeClaimRecord{ClaimType: "ANTI_PROCEDURE"},
				wantType:  "ANTI_PROCEDURE",
				wantLayer: "PROCEDURAL",
				wantPred:  "proscribes",
				wantMod:   "inferred",
			},
			{
				name:      "self model",
				record:    KnowledgeClaimRecord{ClaimType: "SELF_MODEL"},
				wantType:  "SELF_MODEL",
				wantLayer: "IDENTITY",
				wantPred:  "models",
				wantMod:   "inferred",
			},
			{
				name:      "goal commitment",
				record:    KnowledgeClaimRecord{ClaimType: "GOAL_COMMITMENT"},
				wantType:  "GOAL_COMMITMENT",
				wantLayer: "IDENTITY",
				wantPred:  "commits_to",
				wantMod:   "constrained",
			},
			{
				name:      "alternative branch",
				record:    KnowledgeClaimRecord{ClaimType: "ALTERNATIVE_BRANCH"},
				wantType:  "ALTERNATIVE_BRANCH",
				wantLayer: "SEMANTIC",
				wantPred:  "branches",
				wantMod:   "proposed",
			},
			{
				name:      "unknown falls back to fact",
				record:    KnowledgeClaimRecord{ClaimType: "mystery"},
				wantType:  "FACT",
				wantLayer: "SEMANTIC",
				wantPred:  "states",
				wantMod:   "inferred",
			},
		}

		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				gotType := canonicalMemoryTypeFromKnowledgeClaim(tc.record)
				if gotType != tc.wantType {
					t.Fatalf("canonicalMemoryTypeFromKnowledgeClaim() = %q, want %q", gotType, tc.wantType)
				}
				if gotLayer := memoryGraphLayerForType(gotType); gotLayer != tc.wantLayer {
					t.Fatalf("memoryGraphLayerForType(%q) = %q, want %q", gotType, gotLayer, tc.wantLayer)
				}
				if gotPred := memoryGraphPredicateForType(gotType); gotPred != tc.wantPred {
					t.Fatalf("memoryGraphPredicateForType(%q) = %q, want %q", gotType, gotPred, tc.wantPred)
				}
				if gotMod := memoryGraphClaimModality(tc.record, gotType); gotMod != tc.wantMod {
					t.Fatalf("memoryGraphClaimModality(%+v, %q) = %q, want %q", tc.record, gotType, gotMod, tc.wantMod)
				}
			})
		}
	})
}

func TestRSP12MemoryGraphGroundingNormalizationAndAliasTargets(t *testing.T) {
	t.Parallel()

	normCases := []struct {
		name string
		in   string
		want string
	}{
		{name: "workspace doc alias", in: "document", want: "workspace_doc"},
		{name: "artifact alias", in: "artifact", want: "artifact_ref"},
		{name: "workspace artifact alias", in: "workspace_artifact", want: "artifact_ref"},
		{name: "custom kind is preserved", in: "custom_kind", want: "custom_kind"},
	}
	for _, tc := range normCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := normalizeMemoryGraphGroundingSourceKind(tc.in); got != tc.want {
				t.Fatalf("normalizeMemoryGraphGroundingSourceKind(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}

	docKeys := extractMemoryGroundingDocKeys("workspace_doc", "workspace_doc:ws-alpha/runbook#root")
	if len(docKeys) != 1 || docKeys[0] != "runbook" {
		t.Fatalf("unexpected workspace doc keys: %+v", docKeys)
	}

	artifactRefs := extractMemoryGroundingArtifactRefs("artifact_ref", "artifact:ws-alpha/diagram#root")
	if len(artifactRefs) != 1 || artifactRefs[0] != "diagram" {
		t.Fatalf("unexpected artifact refs: %+v", artifactRefs)
	}

	workspaceID := "ws-rsp12-alias"
	rootRef := buildWorkspaceDocSegmentRef(workspaceID, "runbook", "root")
	sourceKind, sourceRef, ok := memoryRootSegmentAliasTarget(workspaceID, "segment_ref", rootRef)
	if !ok || sourceKind != "workspace_doc" || sourceRef != "runbook" {
		t.Fatalf("unexpected root segment alias target: kind=%q ref=%q ok=%v", sourceKind, sourceRef, ok)
	}

	aliasIndex := memoryGraphRootSegmentAliasIndex(workspaceID, []MemoryGraphNodeVersionRecord{
		{RefKind: "workspace_doc", RefID: "runbook"},
		{RefKind: "segment_ref", RefID: rootRef},
	})
	if aliasIndex == nil {
		t.Fatalf("expected alias index to contain the root segment alias")
	}
	if _, ok := aliasIndex[memoryGraphVersionKey("segment_ref", rootRef)]; !ok {
		t.Fatalf("expected root segment alias to be indexed, got %+v", aliasIndex)
	}
	if idx := memoryGraphRootSegmentAliasIndex(workspaceID, []MemoryGraphNodeVersionRecord{
		{RefKind: "segment_ref", RefID: rootRef},
	}); idx != nil {
		t.Fatalf("expected alias index to stay nil without a source ref, got %+v", idx)
	}
}

func TestRSP12MemoryGraphDriftReportAndSearchSurfaceStaleness(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-rsp12-drift"
		docKey      = "rsp-runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "RSP 1.2 Drift",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "RSP Runbook",
		Content:     "# RSP Runbook\nInitial canonical guidance.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}

	node, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Canonical replay note",
		Body:        "Canonical replay note stays grounded in the runbook.",
		Summary:     "Canonical replay note.",
		SourceKind:  "workspace_doc",
		SourceID:    docKey,
		Importance:  0.8,
		Confidence:  0.9,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	before, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "canonical replay",
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search before update: %v", err)
	}
	if len(before.Hits) != 1 {
		t.Fatalf("expected one hit before update, got %+v", before.Hits)
	}
	if before.Hits[0].DriftState != "CURRENT" {
		t.Fatalf("expected current hit before update, got %+v", before.Hits[0])
	}

	rootRef := buildWorkspaceDocSegmentRef(workspaceID, docKey, "root")
	report, err := store.buildMemoryGraphDriftReport(ctx, workspaceID, []MemoryGraphNodeVersionRecord{
		{
			MemoryID:     "memnode:rsp12-manual",
			WorkspaceID:  workspaceID,
			RefKind:      "workspace_doc",
			RefID:        docKey,
			VersionToken: docV1.SHA,
			Weight:       0.8,
		},
		{
			MemoryID:     "memnode:rsp12-manual",
			WorkspaceID:  workspaceID,
			RefKind:      "segment_ref",
			RefID:        rootRef,
			VersionToken: memoryGraphSegmentVersionToken(rootRef, docV1.SHA),
			Weight:       0.3,
		},
	})
	if err != nil {
		t.Fatalf("build drift report: %v", err)
	}
	if report.Status != "CURRENT" || report.ComparedRefCount != 1 || report.DriftedRefCount != 0 || report.Drift != 0 {
		t.Fatalf("expected alias-aware current report, got %+v", report)
	}
	if item := findMemoryGraphVersionStatus(report.Items, "workspace_doc", docKey); item == nil || item.State != "CURRENT" {
		t.Fatalf("expected current workspace doc item, got %+v", report.Items)
	}
	if item := findMemoryGraphVersionStatus(report.Items, "segment_ref", rootRef); item == nil || item.State != "ALIASED_SOURCE" {
		t.Fatalf("expected root segment alias to be suppressed, got %+v", report.Items)
	}

	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "RSP Runbook",
		Content:     "# RSP Runbook\nRevised canonical guidance.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	after, err := store.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
		WorkspaceID: workspaceID,
		Query:       "canonical replay",
		OriginKind:  "workspace_memory",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search after update: %v", err)
	}
	if len(after.Hits) != 1 {
		t.Fatalf("expected one hit after update, got %+v", after.Hits)
	}
	hit := after.Hits[0]
	if hit.MemoryID != "memnode:workspace_memory:"+node.MemoryID {
		t.Fatalf("unexpected hit after update: %+v", hit)
	}
	if hit.DriftState != "STALE" {
		t.Fatalf("expected stale hit after doc update, got %+v", hit)
	}
	if hit.DriftScore <= 0 {
		t.Fatalf("expected positive drift score after doc update, got %+v", hit)
	}
	if hit.Snippet == "" || !hasString(hit.RefKinds, "workspace_doc") || !hasString(hit.RefKinds, "segment_ref") {
		t.Fatalf("expected grounded hit diagnostics, got %+v", hit)
	}
}

func findMemoryGraphVersionStatus(items []MemoryGraphVersionStatus, refKind, refID string) *MemoryGraphVersionStatus {
	for idx := range items {
		if items[idx].RefKind == refKind && items[idx].RefID == refID {
			return &items[idx]
		}
	}
	return nil
}

func hasString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
