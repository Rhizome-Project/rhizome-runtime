package sqlite

import "strings"

// Synthetic operator snapshots and derived overlays must not feed back into
// instrumentation, corridor-fit, or tension extraction as operational evidence.
func isSyntheticOperationalEvent(event RuntimeEventRecord) bool {
	eventType := strings.ToLower(strings.TrimSpace(event.EventType))
	entityType := strings.ToLower(strings.TrimSpace(event.EntityType))
	switch {
	case entityType == "tension",
		entityType == "instrumentation_control",
		entityType == "instrumentation_unified_control",
		entityType == "instrumentation_control_state",
		entityType == "rsp_belief",
		entityType == "rsp_anomaly",
		entityType == "rsp_state",
		entityType == "rsp_forecast",
		entityType == "rsp_risk",
		entityType == "memory_residency",
		entityType == "memory_invalidation",
		entityType == "memory_coherence",
		entityType == "memory_metrics",
		entityType == "memory_promotion",
		entityType == "instrumentation_corridor",
		entityType == "instrumentation_corridor_fit",
		entityType == "instrumentation_corridor_policy",
		strings.HasPrefix(entityType, "instrumentation_corridor_"):
		return true
	}
	if strings.HasPrefix(eventType, "tension.") {
		return true
	}
	if isSyntheticCorridorSnapshotEventType(eventType) {
		return true
	}
	switch eventType {
	case "cluster.metric_snapshot",
		"cluster.control_advisory_snapshot",
		"cluster.unified_control_advisory_snapshot",
		"cluster.unified_control_effective_snapshot",
		"cluster.control_state_ticked",
		"cluster.control_state_stabilized",
		"cluster.control_mode_transitioned",
		"cluster.control_state_snapshot",
		"cluster.corridor_readiness_snapshot",
		"cluster.corridor_fit_snapshot",
		"cluster.corridor_policy_ticked",
		"cluster.corridor_policy_transitioned",
		"cluster.corridor_policy_snapshot",
		"memory.residency_reported",
		"memory.metrics_reported",
		"memory.invalidation_enqueued",
		"memory.invalidation_delivered",
		"memory.invalidation_failed",
		"memory.invalidation_dead_lettered",
		"memory.invalidation_requeued",
		"memory.invalidation_acked",
		"memory.coherence_snapshot",
		"rsp.belief_snapshot",
		"rsp.anomaly_snapshot",
		"rsp.state_snapshot",
		"rsp.risk_snapshot",
		"memory.promotion_enqueued",
		"memory.promotion_resolved",
		"controlplane.snapshot":
		return true
	default:
		return false
	}
}

func isSyntheticCorridorSnapshotEventType(eventType string) bool {
	eventType = strings.ToLower(strings.TrimSpace(eventType))
	return strings.HasPrefix(eventType, "cluster.corridor_") && strings.HasSuffix(eventType, "_snapshot")
}
