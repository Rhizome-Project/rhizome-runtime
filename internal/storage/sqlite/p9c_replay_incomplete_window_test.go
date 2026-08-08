package sqlite

import (
	"context"
	"slices"
	"testing"
	"time"
)

func TestRuntimeReplayBoundedWindowKeepsCausalityAndControlTruthPartial(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	const (
		workspaceID = "ws-p9c-replay-incomplete"
		clusterID   = "cluster-p9c-replay-incomplete"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P9C Replay Incomplete Window",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	now := time.Now().UTC()
	filter := UnifiedControlReportFilter{WorkspaceID: workspaceID, ProtoClusterID: clusterID}

	parent, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:     "rtev-p9c-parent",
		WorkspaceID: workspaceID,
		EventType:   "test.parent",
		EntityType:  "test_entity",
		EntityID:    "parent",
		ActorType:   "system",
		ActorID:     "tester",
		PayloadJSON: `{"kind":"parent"}`,
		CreatedAt:   now.Add(-6 * time.Minute).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("record parent event: %v", err)
	}

	live, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Epoch:             1,
		TTLSeconds:        600,
		ControlMode:       "coherence",
		CandidateMode:     "throughput",
		CandidateControls: p5cSuggestedControls(6, "candidate-live"),
		AdvisoryControls:  p5cSuggestedControls(5, "advisory-live"),
		EffectiveControls: p5cSuggestedControls(4, "effective-live"),
		ResolvedFrom:      "proto_cluster",
		MatchScore:        92,
		BasisSummary:      "live effective controls",
		GeneratedAt:       now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
		ActorID:           "operator.live",
	})
	if err != nil {
		t.Fatalf("persist live effective controls: %v", err)
	}

	if _, err := store.RecordUnifiedControlSnapshot(ctx, UnifiedControlReport{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Resolved:          true,
		ResolvedFrom:      "proto_cluster",
		AdvisoryOnly:      false,
		ControlMode:       live.ControlMode,
		CandidateMode:     live.CandidateMode,
		CandidateControls: live.CandidateControls,
		AdvisoryControls:  live.AdvisoryControls,
		EffectiveControls: live.EffectiveControls,
		EffectiveControlsAudit: &UnifiedControlEffectiveControlsAudit{
			Found:        true,
			Live:         true,
			ScopeSource:  "proto_cluster",
			Epoch:        live.Epoch,
			TTLSeconds:   live.TTLSeconds,
			ExpiresAt:    live.ExpiresAt,
			GeneratedAt:  live.GeneratedAt,
			ActorID:      live.ActorID,
			ResolvedFrom: live.ResolvedFrom,
			MatchScore:   live.MatchScore,
			BasisSummary: live.BasisSummary,
		},
		GeneratedAt: now.Add(-4 * time.Minute).Format(time.RFC3339Nano),
		Summary:     "live effective snapshot",
	}, filter, UnifiedControlSnapshotInput{ActorID: "auditor"}); err != nil {
		t.Fatalf("record effective snapshot: %v", err)
	}

	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:        "rtev-p9c-child",
		WorkspaceID:    workspaceID,
		EventType:      "test.child",
		EntityType:     "test_entity",
		EntityID:       "child",
		ActorType:      "system",
		ActorID:        "tester",
		ParentRefsJSON: `["` + parent.EventID + `"]`,
		PayloadJSON:    `{"kind":"child"}`,
		CreatedAt:      now.Add(-3 * time.Minute).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record child event: %v", err)
	}

	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:     "rtev-p9c-filler",
		WorkspaceID: workspaceID,
		EventType:   "test.filler",
		EntityType:  "test_entity",
		EntityID:    "filler",
		ActorType:   "system",
		ActorID:     "tester",
		PayloadJSON: `{"kind":"filler"}`,
		CreatedAt:   now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record filler event: %v", err)
	}

	if _, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Epoch:             2,
		TTLSeconds:        1,
		ControlMode:       live.ControlMode,
		CandidateMode:     live.CandidateMode,
		CandidateControls: p5cSuggestedControls(7, "candidate-expired"),
		AdvisoryControls:  p5cSuggestedControls(6, "advisory-expired"),
		EffectiveControls: p5cSuggestedControls(3, "effective-expired"),
		ResolvedFrom:      "proto_cluster",
		MatchScore:        77,
		BasisSummary:      "expired effective controls",
		GeneratedAt:       now.Add(-90 * time.Minute).Format(time.RFC3339Nano),
		ActorID:           "operator.expired",
	}); err != nil {
		t.Fatalf("persist expired effective controls: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       4,
	})
	if err != nil {
		t.Fatalf("replay bounded runtime journal: %v", err)
	}

	if !report.Truncated || !report.WindowIncomplete {
		t.Fatalf("expected bounded replay window to stay explicitly incomplete, got %+v", report)
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected bounded replay window to degrade verdict to warn, got %+v", report.Evaluation)
	}
	if report.Scope.Authoritative || report.Scope.IntegrityBand != "PARTIAL" {
		t.Fatalf("expected bounded replay scope to remain partial, got %+v", report.Scope)
	}
	if len(report.MissingParentRefs) != 1 || report.MissingParentRefs[0] != parent.EventID {
		t.Fatalf("expected missing parent ref for truncated child lineage, got %+v", report.MissingParentRefs)
	}
	if !slices.Contains(report.Scope.Reasons, "truncated_window") || !slices.Contains(report.Scope.Reasons, "missing_parent_refs") {
		t.Fatalf("expected bounded replay scope reasons to include truncation and missing parents, got %+v", report.Scope)
	}
	if len(report.UnifiedSnapshots) != 1 {
		t.Fatalf("expected effective snapshot to remain visible in bounded replay, got %+v", report.UnifiedSnapshots)
	}

	p5cRequireReplayFinding(t, report, "replay_scope_partial")
	p5cRequireReplayFinding(t, report, "unified_control_effective_snapshot_rollback_trace_window_incomplete")
	if p5cHasReplayFindingCode(report, "unified_control_effective_snapshot_missing_rollback_trace") {
		t.Fatalf("did not expect false missing rollback trace under incomplete window, got %+v", report.Evaluation.Findings)
	}
}
