package server

import (
	"strings"
	"testing"
)

func TestDashboardInstrumentationIncludesRoleLockSurfaces(t *testing.T) {
	required := []string{
		"Role-Lock Peak",
		"role-lock ",
		"Role-Lock Index",
		"Role-Lock Components",
		"Steward HHI",
		"Accepted Builder HHI",
		"Default Reviewer HHI",
		"Motif Reuse HHI",
		"Missing Components",
		"Highest read-side anti-lock-in estimate in ",
		"Read-side anti-lock-in estimate from steward, accepted builder, and blocking reviewer concentration. Missing components stay visible rather than inferred.",
		"This is operator-facing evidence only; it does not assign roles, leases, or write policy.",
		"Component not yet surfaced in read-side metrics.",
		"No missing role-lock components are currently visible.",
		"function instrumentationRoleLock(",
		"function instrumentationRoleLockSummary(",
		"function instrumentationRoleLockPeak(",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard instrumentation role-lock surface is missing %s", needle)
		}
	}
}

func TestDashboardInstrumentationRoleLockKeepsReadSideWording(t *testing.T) {
	for _, forbidden := range []string{
		"apply role-lock policy",
		"assign roles automatically",
		"grant lease authority",
		"workspace.policy.role_lock",
	} {
		if strings.Contains(strings.ToLower(dashboardHTML), strings.ToLower(forbidden)) {
			t.Fatalf("dashboard instrumentation role-lock surface leaked active wording via %s", forbidden)
		}
	}
}
