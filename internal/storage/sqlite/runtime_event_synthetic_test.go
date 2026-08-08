package sqlite

import "testing"

func TestSyntheticOperationalEventExcludesFutureCorridorSnapshots(t *testing.T) {
	t.Parallel()

	cases := []RuntimeEventRecord{
		{EventType: "cluster.corridor_readiness_snapshot", EntityType: "instrumentation_corridor"},
		{EventType: "cluster.corridor_fit_snapshot", EntityType: "instrumentation_corridor_fit"},
		{EventType: "cluster.corridor_boundary_snapshot", EntityType: "instrumentation_corridor_boundary"},
		{EventType: "cluster.corridor_violation_snapshot", EntityType: "instrumentation_corridor_violation"},
		{EventType: "cluster.corridor_boundary_snapshot", EntityType: "instrumentation_corridor_custom"},
		{EventType: "memory.residency_reported", EntityType: "memory_residency"},
		{EventType: "memory.metrics_reported", EntityType: "memory_metrics"},
		{EventType: "memory.invalidation_enqueued", EntityType: "memory_invalidation"},
		{EventType: "memory.invalidation_delivered", EntityType: "memory_invalidation"},
		{EventType: "memory.invalidation_failed", EntityType: "memory_invalidation"},
		{EventType: "memory.invalidation_dead_lettered", EntityType: "memory_invalidation"},
		{EventType: "memory.invalidation_requeued", EntityType: "memory_invalidation"},
		{EventType: "memory.invalidation_acked", EntityType: "memory_invalidation"},
		{EventType: "memory.coherence_snapshot", EntityType: "memory_coherence"},
		{EventType: "rsp.belief_snapshot", EntityType: "rsp_belief"},
		{EventType: "memory.promotion_enqueued", EntityType: "memory_promotion"},
		{EventType: "memory.promotion_resolved", EntityType: "memory_promotion"},
	}
	for _, event := range cases {
		if !isSyntheticOperationalEvent(event) {
			t.Fatalf("expected synthetic exclusion for %+v", event)
		}
	}
}

func TestSyntheticOperationalEventExcludesFutureCorridorEntityTypes(t *testing.T) {
	t.Parallel()

	cases := []RuntimeEventRecord{
		{EventType: "cluster.runtime_annotation", EntityType: "instrumentation_corridor_boundary"},
		{EventType: "cluster.runtime_annotation", EntityType: "instrumentation_corridor_violation"},
	}
	for _, event := range cases {
		if !isSyntheticOperationalEvent(event) {
			t.Fatalf("expected synthetic exclusion for corridor entity type %+v", event)
		}
	}
}
