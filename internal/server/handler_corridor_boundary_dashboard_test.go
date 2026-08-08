package server

import (
	"strings"
	"testing"
)

func TestDashboardIncludesCorridorBoundarySurface(t *testing.T) {
	required := []string{
		"Corridor Boundary / Violations",
		`id="corridor-fit-state"`,
		`id="corridor-fit-summary"`,
		"Corridor boundary and violation approximation will appear once the corridor-fit read-side report loads.",
		"function focusCorridorFitSurface(",
		"function openCorridorBoundarySurface(",
		"function renderCorridorFitState(",
		"function corridorFitViolationGaps(",
		"function corridorFitDominantGap(",
		"function corridorFitBasisSummary(",
		"Selected Boundary Approximation",
		"Boundary Basis",
		"Dominant Violation",
		"Out-of-Range Metrics",
		"Open Boundary Surface",
		"workspace.instrumentation.corridor.fit.report",
		"workspace.instrumentation.corridor.fit.cluster",
		"workspace.instrumentation.corridor.fit.snapshot",
		"showProtoClusterDetail(",
		"openTensionsForProtoCluster(",
		"showRuntimeEventDetail(",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard corridor boundary surface is missing %s", needle)
		}
	}
}

func TestDashboardCorridorBoundarySurfaceKeepsReadOnlyWording(t *testing.T) {
	required := "Read-only corridor-boundary and violation approximation over catalog-range checks, proxy metric vectors, and confirmed tensions. It stays operator-facing and does not assign corridors or carry policy authority."
	if !strings.Contains(dashboardHTML, required) {
		t.Fatalf("dashboard corridor boundary surface should keep explicit read-only wording")
	}
	for _, forbidden := range []string{
		"apply corridor fit",
		"automatic corridor writes",
		"policy engine",
		"corridor actuation",
	} {
		if strings.Contains(strings.ToLower(dashboardHTML), strings.ToLower(forbidden)) {
			t.Fatalf("dashboard corridor boundary surface leaked active wording via %s", forbidden)
		}
	}
}
