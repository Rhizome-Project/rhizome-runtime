package sqlite

import "testing"

func TestReadSurfacePolicyProfiles(t *testing.T) {
	tests := []struct {
		name string
		got  ReadSurfacePolicy
		want ReadSurfacePolicy
	}{
		{
			name: "runtime_replay",
			got: runtimeReplayReadSurfacePolicy(RuntimeReplayFilter{
				Limit:            900,
				ExcludeSynthetic: true,
			}),
			want: ReadSurfacePolicy{
				ConsumerClass:   readSurfaceConsumerReplay,
				Materialization: readSurfaceMaterializationLiveWindow,
				Budget:          ReadSurfaceBudget{ReplayEvents: readSurfaceReplayLimitMax},
				Shedding:        []string{readSurfaceSheddingTruncateTail, readSurfaceSheddingExcludeSynthetic},
			},
		},
		{
			name: "instrumentation_report",
			got: instrumentationReadSurfacePolicy(InstrumentationReportFilter{
				Limit:        900,
				ClusterLimit: 900,
			}),
			want: ReadSurfacePolicy{
				ConsumerClass:   readSurfaceConsumerDashboard,
				Materialization: readSurfaceMaterializationDerivedSnapshot,
				Budget: ReadSurfaceBudget{
					ReplayEvents: readSurfaceReplayLimitMax,
					ClusterItems: readSurfaceClusterLimitMax,
				},
				Shedding: []string{readSurfaceSheddingTruncateTail, readSurfaceSheddingCapClusters, readSurfaceSheddingExcludeSynthetic},
			},
		},
		{
			name: "control_report",
			got:  controlReadSurfacePolicy(ControlReportFilter{}, controlReadClusterWindow),
			want: ReadSurfacePolicy{
				ConsumerClass:   readSurfaceConsumerOperator,
				Materialization: readSurfaceMaterializationDerivedSnapshot,
				Budget: ReadSurfaceBudget{
					ReplayEvents: readSurfaceReplayLimitMax,
					ClusterItems: controlReadClusterWindow,
					TensionItems: controlReadTensionWindow,
				},
				Shedding: []string{readSurfaceSheddingTruncateTail, readSurfaceSheddingCapClusters},
			},
		},
		{
			name: "corridor_readiness",
			got:  corridorReadinessPolicy(CorridorReadinessFilter{}, corridorReadClusterWindow),
			want: ReadSurfacePolicy{
				ConsumerClass:   readSurfaceConsumerObserver,
				Materialization: readSurfaceMaterializationDerivedSnapshot,
				Budget: ReadSurfaceBudget{
					ReplayEvents: readSurfaceReplayLimitMax,
					ClusterItems: corridorReadClusterWindow,
				},
				Shedding: []string{readSurfaceSheddingTruncateTail, readSurfaceSheddingCapClusters},
			},
		},
		{
			name: "cluster_control_state",
			got: clusterControlStatePolicy(ClusterControlStateFilter{
				ProtoClusterID: "cluster-1",
				Limit:          900,
			}),
			want: ReadSurfacePolicy{
				ConsumerClass:   readSurfaceConsumerOperator,
				Materialization: readSurfaceMaterializationBasisSnapshot,
				Budget: ReadSurfaceBudget{
					ReplayEvents: clusterControlTickLimit("cluster-1"),
					ClusterItems: readSurfaceReportLimitMax,
				},
				Shedding: []string{readSurfaceSheddingTruncateTail, readSurfaceSheddingCapClusters},
			},
		},
		{
			name: "unified_control",
			got: unifiedControlReadSurfacePolicy(UnifiedControlReportFilter{
				FrontierLimit: 900,
			}),
			want: ReadSurfacePolicy{
				ConsumerClass:   readSurfaceConsumerOperator,
				Materialization: readSurfaceMaterializationFrontierCapped,
				Budget:          ReadSurfaceBudget{FrontierItems: readSurfaceFrontierLimitMax},
				Shedding:        []string{readSurfaceSheddingCapFrontier, readSurfaceSheddingSuppressVerboseDetails},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.got.ConsumerClass != tc.want.ConsumerClass {
				t.Fatalf("consumer class mismatch: got %q want %q", tc.got.ConsumerClass, tc.want.ConsumerClass)
			}
			if tc.got.Materialization != tc.want.Materialization {
				t.Fatalf("materialization mismatch: got %q want %q", tc.got.Materialization, tc.want.Materialization)
			}
			if tc.got.Budget != tc.want.Budget {
				t.Fatalf("budget mismatch: got %+v want %+v", tc.got.Budget, tc.want.Budget)
			}
			if len(tc.got.Shedding) != len(tc.want.Shedding) {
				t.Fatalf("shedding length mismatch: got %+v want %+v", tc.got.Shedding, tc.want.Shedding)
			}
			for i := range tc.want.Shedding {
				if tc.got.Shedding[i] != tc.want.Shedding[i] {
					t.Fatalf("shedding mismatch at %d: got %q want %q", i, tc.got.Shedding[i], tc.want.Shedding[i])
				}
			}
			if len(tc.want.Notes) > 0 && len(tc.got.Notes) == 0 {
				t.Fatalf("expected notes to be present")
			}
		})
	}
}
