package main

import (
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestCollectServiceHealthPayloadExposesStructuredStuckAgentHealth(t *testing.T) {
	stuckHealth := sqlite.StuckAgentSnapshot{
		State:                     "degraded",
		Message:                   "stuck agent risk detected: dead_heartbeats=1, stale_activity=1",
		ReferenceAt:               "2026-04-21T12:00:00Z",
		ActiveSessionCount:        2,
		DeadHeartbeatSessionCount: 1,
		StaleActivitySessionCount: 1,
		OfflineAgentSessionCount:  1,
		StartupGraceSessionCount:  0,
	}

	payload := collectServiceHealthPayloadWithAuthorityAndReviewerScarcityHealthAndStuckAgentHealth(
		appConfigForTest(),
		nil,
		sqlite.MemoryProjectionLagSnapshot{State: "unsupported", Message: "not collected"},
		DiagnosticSignal{State: "unsupported", Message: "operator queue lag not collected"},
		DiagnosticSignal{State: "unsupported", Message: "reviewer scarcity not collected"},
		sqlite.ReviewerScarcityHealthSnapshot{},
		DiagnosticSignal{State: "degraded", Message: stuckHealth.Message},
		&stuckHealth,
		sqlite.AuthorityNodeDiagnostics{},
		sqlite.AuthorityLeaseDiagnostics{},
	)

	if payload.StuckAgentsHealth == nil {
		t.Fatal("expected structured stuck-agent health payload to be exposed")
	}
	if payload.StuckAgentsHealth.DeadHeartbeatSessionCount != 1 || payload.StuckAgentsHealth.StaleActivitySessionCount != 1 {
		t.Fatalf("unexpected structured stuck-agent payload: %+v", payload.StuckAgentsHealth)
	}
	if payload.Extended.StuckAgents.State != "degraded" {
		t.Fatalf("expected extended stuck-agents signal to remain degraded, got %+v", payload.Extended.StuckAgents)
	}
}

func appConfigForTest() app.Config {
	return app.Config{}
}
