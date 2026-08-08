package sqlite

import "testing"

func TestNormalizeReadSurfaceBudgets(t *testing.T) {
	t.Parallel()

	t.Run("runtime replay caps oversized limits", func(t *testing.T) {
		t.Parallel()
		filter := normalizeRuntimeReplayFilter(RuntimeReplayFilter{
			WorkspaceID: " ws ",
			AgentID:     " agent ",
			SessionID:   " sess ",
			TaskID:      " task ",
			Limit:       5000,
		})
		if filter.Limit != readSurfaceReplayLimitMax {
			t.Fatalf("expected replay limit cap %d, got %d", readSurfaceReplayLimitMax, filter.Limit)
		}
		if filter.WorkspaceID != "ws" || filter.AgentID != "agent" || filter.SessionID != "sess" || filter.TaskID != "task" {
			t.Fatalf("expected trimmed replay filter, got %+v", filter)
		}
	})

	t.Run("instrumentation caps cluster and replay windows", func(t *testing.T) {
		t.Parallel()
		filter := normalizeInstrumentationReportFilter(InstrumentationReportFilter{
			WorkspaceID:  " ws ",
			AgentID:      " agent ",
			SessionID:    " sess ",
			TaskID:       " task ",
			Limit:        5000,
			ClusterLimit: 5000,
		})
		if filter.Limit != readSurfaceReplayLimitMax {
			t.Fatalf("expected instrumentation replay limit cap %d, got %d", readSurfaceReplayLimitMax, filter.Limit)
		}
		if filter.ClusterLimit != readSurfaceClusterLimitMax {
			t.Fatalf("expected instrumentation cluster limit cap %d, got %d", readSurfaceClusterLimitMax, filter.ClusterLimit)
		}
	})

	t.Run("control report limit is bounded", func(t *testing.T) {
		t.Parallel()
		filter := normalizeControlReportFilter(ControlReportFilter{
			WorkspaceID: "ws",
			Limit:       999,
		})
		if filter.Limit != readSurfaceReportLimitMax {
			t.Fatalf("expected control report cap %d, got %d", readSurfaceReportLimitMax, filter.Limit)
		}
	})

	t.Run("corridor readiness limit is bounded", func(t *testing.T) {
		t.Parallel()
		filter := normalizeCorridorReadinessFilter(CorridorReadinessFilter{
			WorkspaceID: "ws",
			Limit:       999,
		})
		if filter.Limit != readSurfaceReportLimitMax {
			t.Fatalf("expected corridor readiness cap %d, got %d", readSurfaceReportLimitMax, filter.Limit)
		}
	})

	t.Run("cluster control state limit is bounded", func(t *testing.T) {
		t.Parallel()
		filter := normalizeClusterControlStateFilter(ClusterControlStateFilter{
			WorkspaceID: "ws",
			Limit:       999,
		})
		if filter.Limit != readSurfaceReportLimitMax {
			t.Fatalf("expected cluster control state cap %d, got %d", readSurfaceReportLimitMax, filter.Limit)
		}
	})

	t.Run("unified control frontier limit is bounded", func(t *testing.T) {
		t.Parallel()
		filter := normalizeUnifiedControlReportFilter(UnifiedControlReportFilter{
			WorkspaceID:   "ws",
			FrontierLimit: 999,
		})
		if filter.FrontierLimit != readSurfaceFrontierLimitMax {
			t.Fatalf("expected unified control frontier cap %d, got %d", readSurfaceFrontierLimitMax, filter.FrontierLimit)
		}
	})
}
