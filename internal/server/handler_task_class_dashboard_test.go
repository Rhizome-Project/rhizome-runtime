package server

import (
	"strings"
	"testing"
)

func TestDashboardIncludesTaskClassOperatorSurface(t *testing.T) {
	required := []string{
		`id="ct-task-class"`,
		"Task Class (optional)",
		"operator-authored if set, otherwise derived read-side only",
		"task_class: taskClass || undefined",
		"document.getElementById('ct-task-class').value = ''",
		"Task Class Evidence",
		"Task Class Source",
		"Task-First Corridor Authority",
		"Authority Basis Freshness",
		"No task-class evidence yet",
		"class '+esc(String(taskClass).toLowerCase())+' | source '+esc(String(taskClassSource).toLowerCase())+'",
		"function corridorTaskClassValue(",
		"function corridorTaskClassSource(",
		"function corridorAuthorityApproximation(",
		"function corridorAuthorityBasisFreshnessApproximation(",
		"function taskClaimLifecycleEvents(",
		"function taskBlockingActions(",
		"function taskClaimLifecycleTone(",
		"function showTaskDetail(",
		"Claim Lock History",
		"Operator-facing lifecycle of claim state and blocking human actions; read-side evidence only.",
		"Lifecycle Events",
		"No claim lifecycle runtime events are currently visible for this task.",
		"Blocking Actions",
		"No blocking actions are currently attached to this task.",
		"findLinkedRebaseQueueForAction(action.action_id)",
		"rebase_workflow_state",
		"Rebase workflow:",
		"Start Rebase</button>",
		"Pause Rebase</button>",
		"Latest Runtime Event",
		"Open Runtime Event",
		"Open Action",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard task-class surface is missing %s", needle)
		}
	}
}

func TestDashboardTaskClassSurfaceKeepsOperatorFraming(t *testing.T) {
	for _, forbidden := range []string{
		"task-class authority",
		"apply task class",
		"automatic task classification writes",
		"task claim policy authority",
		"automatic claim lock writes",
	} {
		if strings.Contains(strings.ToLower(dashboardHTML), strings.ToLower(forbidden)) {
			t.Fatalf("dashboard task-class surface leaked active policy wording via %s", forbidden)
		}
	}
}

func TestDashboardTaskDetailKeepsCurrentClaimStateContract(t *testing.T) {
	required := []string{
		"Claim note:",
		"Assigned to",
		"Task-First Corridor Authority",
		"Authority Basis Freshness",
		"Task Graph Visualization",
		"showTaskDetail(",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard task detail surface is missing %s", needle)
		}
	}
}
