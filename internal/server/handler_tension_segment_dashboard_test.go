package server

import (
	"strings"
	"testing"
)

func TestDashboardTensionDetailIncludesSegmentAndConstraintSections(t *testing.T) {
	required := []string{
		"['Segments', tension.segment_refs]",
		"['Constraints', tension.constraint_refs]",
		"['Members', tension.members]",
		"['Blocked By', tension.blocked_by_tension_ids]",
		"['Blocks', tension.blocks_tension_ids]",
		"['Dependencies', detail.dependencies, 'blocked by']",
		"['Dependents', detail.dependents, 'blocks']",
		"Frontier Capacity",
		"Free Agents",
		"Base Importance",
		"Visibility Score",
		"Surfaced Priority",
		"renderSegmentBadgeRow('Document Segments'",
		"renderSegmentBadgeRow('Artifact Segments'",
		"function corridorSegmentEntries(",
		"Related Segments",
		"Document segments and task-first corridor authority stay read-only operator evidence only.",
		"Artifact/doc segments stay read-only evidence anchors, separate from corridor readiness/fit.",
		"Read-only artifact/doc segment context for operator inspection only.",
		"Open Corridor Surface",
		"Open Control Scaffold",
		"Runtime Event",
		"Open Doc",
		"showDoc(",
		"showRuntimeEventDetail(",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard tension detail is missing %s", needle)
		}
	}
}

func TestDashboardSegmentSurfaceKeepsReadOnlyOperatorFraming(t *testing.T) {
	for _, forbidden := range []string{
		"segment policy",
		"apply segment",
		"segment authority",
		"artifact segment writes",
	} {
		if strings.Contains(strings.ToLower(dashboardHTML), strings.ToLower(forbidden)) {
			t.Fatalf("dashboard segment surface leaked active policy wording via %s", forbidden)
		}
	}
}
