package server

import (
	"strings"
	"testing"
)

func TestDashboardOperatorQueueSurfacesVisibleLockEvidence(t *testing.T) {
	required := []string{
		"function queueTaskLockEvidence(",
		"function queueLifecycleEvidence(",
		"task lock ",
		"keep session active",
		"allow session to end",
		"last escalated ",
		"Visible Lock Evidence",
		"return '<div class=\"action-card",
		"queueLifecycleEvidence(item, authority)",
		"(_cachedTasks || []).find(candidate => candidate.task_id === item.task_id)",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard operator queue surface is missing %s", needle)
		}
	}
}

func TestDashboardOperatorQueueKeepsReadSideWording(t *testing.T) {
	for _, forbidden := range []string{
		"apply queue lease policy",
		"automatic queue lock writes",
		"grant queue authority",
		"workspace.policy.queue",
	} {
		if strings.Contains(strings.ToLower(dashboardHTML), strings.ToLower(forbidden)) {
			t.Fatalf("dashboard operator queue surface leaked active wording via %s", forbidden)
		}
	}
}

func TestDashboardOperatorQueueSurfacesRebaseFollowupContext(t *testing.T) {
	required := []string{
		"function queueRebaseFollowupPayload(",
		"function queueRebaseFollowupBadges(",
		"function queueRebaseFollowupSummary(",
		"function findLinkedRebaseQueueForAction(",
		"tension_rebase_followup:",
		"repair_tension_id",
		"fork_tension_id",
		"rebase_plan_class",
		"conflict_safe_class",
		"rebase_workflow_state",
		"rebase_workflow_step",
		"action_id",
		"action_status",
		"action_started_by",
		"action_paused_by",
		"queueRebaseFollowupSummary(linkedRebaseQueue)",
		"queueRebaseFollowupBadges(linkedRebaseQueue)",
		"Rebase workflow:",
		"started by ",
		"Paused By",
		"queueRebaseFollowupBadges(item)",
		"queueRebaseFollowupSummary(item)",
		"Rebase Follow-Up",
		"Linked Rebase Workflow",
		"Workflow State",
		"Workflow Step",
		"Linked Action",
		"Action Status",
		"Start Rebase Work",
		"Start Rebase</button>",
		"Pause Rebase</button>",
		"action.start",
		"action.started",
		"action.pause",
		"action.paused",
		"Fork Tension",
		"Repair Tension",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard rebase follow-up surface is missing %s", needle)
		}
	}
}

func TestDashboardOperatorQueueResolveAndEscalateCarryRevisionToken(t *testing.T) {
	required := []string{
		"workspace.ops.upsert",
		"workspace.ops.escalate",
		"workspace.ops.resolve",
		"current_updated_at: String(item.updated_at || '').trim()",
		"current_updated_at: existing ? String(existing.updated_at || '').trim() : undefined",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard operator queue action flow is missing %s", needle)
		}
	}
}
