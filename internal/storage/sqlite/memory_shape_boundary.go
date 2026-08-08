package sqlite

import "strings"

const (
	memoryShapeCanonicalRetained            = "retain_current_canonical_shape"
	memoryShapeSurfaceAuthorityCompatOnly   = "compatibility_only"
	memoryShapeSurfaceRoleDerivedProjection = "derived_compatibility_projection"
)

type MemoryShapeBoundaryContract struct {
	CanonicalShape            string `json:"canonical_shape"`
	SurfaceAuthority          string `json:"surface_authority"`
	SurfaceRole               string `json:"surface_role"`
	ProjectionCoverage        string `json:"projection_coverage,omitempty"`
	ProjectionLagState        string `json:"projection_lag_state,omitempty"`
	ProjectionLagMessage      string `json:"projection_lag_message,omitempty"`
	ProjectionPendingCount    int    `json:"projection_pending_count,omitempty"`
	ProjectionProcessingCount int    `json:"projection_processing_count,omitempty"`
	ProjectionFailedCount     int    `json:"projection_failed_count,omitempty"`
}

func memoryGraphBoundaryContract() MemoryShapeBoundaryContract {
	return MemoryShapeBoundaryContract{
		CanonicalShape:       memoryShapeCanonicalRetained,
		SurfaceAuthority:     memoryShapeSurfaceAuthorityCompatOnly,
		SurfaceRole:          memoryShapeSurfaceRoleDerivedProjection,
		ProjectionCoverage:   "CURRENT",
		ProjectionLagState:   "ok",
		ProjectionLagMessage: "projection surface is current",
	}
}

func DefaultMemoryShapeBoundaryContract() MemoryShapeBoundaryContract {
	return memoryGraphBoundaryContract()
}

func memoryGraphBoundaryContractWithProjectionLag(snapshot MemoryProjectionLagSnapshot) MemoryShapeBoundaryContract {
	contract := memoryGraphBoundaryContract()
	state := strings.TrimSpace(snapshot.State)
	if state == "" || strings.EqualFold(state, "ok") {
		return contract
	}
	contract.ProjectionCoverage = "PARTIAL"
	contract.ProjectionLagState = state
	contract.ProjectionLagMessage = strings.TrimSpace(snapshot.Message)
	contract.ProjectionPendingCount = snapshot.PendingCount
	contract.ProjectionProcessingCount = snapshot.ProcessingCount
	contract.ProjectionFailedCount = snapshot.FailedCount
	return contract
}

func memoryGraphCanonicalAuthority(originKind string) string {
	switch strings.ToLower(strings.TrimSpace(originKind)) {
	case "workspace_memory":
		return "workspace_memory"
	case "knowledge_claim":
		return "knowledge_claim"
	case "episode_pack":
		return "episode_pack"
	default:
		return "external_or_unknown_origin"
	}
}

func applyMemoryGraphBoundary(record *MemoryGraphNodeRecord) {
	if record == nil {
		return
	}
	record.CanonicalAuthority = memoryGraphCanonicalAuthority(record.OriginKind)
	record.SurfaceAuthority = memoryShapeSurfaceAuthorityCompatOnly
	record.SurfaceRole = memoryShapeSurfaceRoleDerivedProjection
	record.CompatibilityOnly = true
}
