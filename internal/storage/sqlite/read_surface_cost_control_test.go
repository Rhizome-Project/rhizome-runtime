package sqlite

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestReadSurfacePolicyLargeSyntheticInputsStayBounded(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		got  ReadSurfacePolicy
		want ReadSurfaceBudget
	}{
		{
			name: "runtime replay",
			got: runtimeReplayReadSurfacePolicy(RuntimeReplayFilter{
				Limit:            5000,
				ExcludeSynthetic: true,
			}),
			want: ReadSurfaceBudget{ReplayEvents: readSurfaceReplayLimitMax},
		},
		{
			name: "instrumentation report",
			got: instrumentationReadSurfacePolicy(InstrumentationReportFilter{
				Limit:        5000,
				ClusterLimit: 5000,
			}),
			want: ReadSurfaceBudget{
				ReplayEvents: readSurfaceReplayLimitMax,
				ClusterItems: readSurfaceClusterLimitMax,
			},
		},
		{
			name: "unified control",
			got: unifiedControlReadSurfacePolicy(UnifiedControlReportFilter{
				FrontierLimit: 5000,
			}),
			want: ReadSurfaceBudget{FrontierItems: readSurfaceFrontierLimitMax},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.got.Budget != tc.want {
				t.Fatalf("budget mismatch: got %+v want %+v", tc.got.Budget, tc.want)
			}
			if len(tc.got.Shedding) == 0 {
				t.Fatalf("expected shedding strategy for %s", tc.name)
			}
		})
	}
}

func TestReadSurfaceBudgetControlReportFanout(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-read-surface-fanout"
	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a", "agent-b")
	ensureTensionOverlayTables(t, ctx, store)

	baseAt := time.Now().UTC().Add(-200 * time.Minute)
	for i := 0; i < 200; i++ {
		clusterID := fmt.Sprintf("task:%s/task-%03d", workspaceID, i)
		now := baseAt.Add(-time.Duration(i) * time.Minute).Format(time.RFC3339Nano)
		insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
			TensionID:       fmt.Sprintf("tension:%s/%03d", workspaceID, i),
			WorkspaceID:     workspaceID,
			ProtoClusterID:  clusterID,
			TensionType:     "bottleneck",
			LifecycleState:  tensionLifecycleActive,
			ReviewStatus:    tensionReviewConfirmed,
			Title:           "Synthetic load cluster",
			Summary:         "Synthetic fan-out cluster used to verify read-surface trimming",
			AnchorKind:      "claim_subject",
			AnchorRef:       fmt.Sprintf("claim-%03d", i),
			TaskIDs:         []string{fmt.Sprintf("task-%03d", i)},
			AgentIDs:        []string{"agent-a", "agent-b"},
			BaseScore:       50,
			SurfaceScore:    50 + (i % 10),
			EvidenceCount:   1,
			LastSeenEventID: fmt.Sprintf("event-%03d", i),
			LastSeenAt:      now,
			CreatedAt:       now,
			UpdatedAt:       now,
		})
	}

	report, err := store.BuildControlReport(ctx, ControlReportFilter{
		WorkspaceID: workspaceID,
		Limit:       1000,
	})
	if err != nil {
		t.Fatalf("build control report: %v", err)
	}
	if report.Filter.Limit != readSurfaceReportLimitMax {
		t.Fatalf("expected control report limit to clamp to %d, got %d", readSurfaceReportLimitMax, report.Filter.Limit)
	}
	if report.Workspace.TotalClusters != 200 {
		t.Fatalf("expected 200 synthetic clusters to be analyzed, got %d", report.Workspace.TotalClusters)
	}
	if len(report.Clusters) != readSurfaceReportLimitMax {
		t.Fatalf("expected returned cluster slice to be capped at %d, got %d", readSurfaceReportLimitMax, len(report.Clusters))
	}
	if report.Workspace.HighestPressureClusterID == "" {
		t.Fatalf("expected highest pressure cluster to be tracked, got %+v", report.Workspace)
	}
}
