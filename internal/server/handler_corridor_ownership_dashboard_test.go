package server

import (
	"strings"
	"testing"
)

func TestDashboardIncludesCorridorOwnershipSurface(t *testing.T) {
	required := []string{
		"Corridor Ownership / Basis",
		`id="corridor-ownership-state"`,
		`id="corridor-ownership-summary"`,
		`id="corridor-ownership-snapshot-btn"`,
		"Cluster-level corridor ownership and basis will appear once the ownership read-side report loads.",
		"corridorOwnershipReportCache",
		"corridorOwnershipDetailCache",
		"corridorOwnershipSnapshotEventCache",
		"function corridorOwnershipParams(",
		"function findCorridorOwnershipCluster(",
		"function corridorOwnershipStateColor(",
		"function corridorOwnershipBasisFreshness(",
		"function syncCorridorOwnershipSnapshotFromRuntimeEvents(",
		"function renderCorridorOwnershipState(",
		"async function loadCorridorOwnership(",
		"async function showCorridorOwnershipClusterDetail(",
		"async function createCorridorOwnershipSnapshot(",
		"function openCorridorOwnershipSurface(",
		"workspace.instrumentation.corridor.ownership.report",
		"workspace.instrumentation.corridor.ownership.cluster",
		"workspace.instrumentation.corridor.ownership.snapshot",
		"cluster.corridor_ownership_snapshot",
		"Selected Ownership Basis",
		"Ownership State",
		"Basis Task Class",
		"Basis Source",
		"Basis Freshness",
		"Active Steward Leases",
		"Current Steward Lease",
		"No active steward lease is currently visible for this cluster.",
		"Read-only lease visibility for overlapping coordination and stewarded recovery; this does not grant write authority by itself.",
		"Steward Agent",
		"Active Leases",
		"Owner Task Anchors",
		"Supporting Tasks",
		"Conflicting Tasks",
		"Task Basis Inputs",
		"Corridor Ownership Snapshot Summary",
		"Open Latest Ownership Snapshot",
		"Read-only cluster-level basis layer between task-first corridor authority and downstream corridor fit / boundary diagnostics.",
		"Open Ownership Surface",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard corridor ownership surface is missing %s", needle)
		}
	}
}

func TestDashboardCorridorOwnershipKeepsReadOnlyWording(t *testing.T) {
	required := "It keeps a single visible basis for operator inspection only and does not assign a corridor or apply policy."
	if !strings.Contains(dashboardHTML, required) {
		t.Fatalf("dashboard corridor ownership surface should keep explicit read-only wording")
	}
	for _, forbidden := range []string{
		"policy authority for corridor ownership",
		"assign ownership automatically",
		"apply ownership policy",
		"workspace.policy.corridor.ownership",
	} {
		if strings.Contains(strings.ToLower(dashboardHTML), strings.ToLower(forbidden)) {
			t.Fatalf("dashboard corridor ownership surface leaked active wording via %s", forbidden)
		}
	}
}
