package sqlite

import (
	"context"
	"strings"
	"testing"
)

func TestSnapshotRSPStateReportRequiresStateShadowCapability(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenario(t, ctx, store, "rsp-state-policy")
	if _, err := store.PutCapabilityPolicy(ctx, CapabilityPolicyInput{
		WorkspaceID: scenario.workspaceID,
		SubjectType: "workspace",
		SubjectID:   scenario.workspaceID,
		Capability:  rspCapabilityStateShadow,
		ToolID:      "*",
		Effect:      "DENY",
		Reason:      "disable state shadow for policy test",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("disable state shadow capability: %v", err)
	}

	_, err := store.SnapshotRSPStateReport(ctx, RSPStateReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err == nil || !IsRSPRolloutDisabledError(err) {
		t.Fatalf("expected rollout policy error for state snapshot, got %v", err)
	}
}

func TestSnapshotRSPBeliefReportRequiresBeliefLiveCapability(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedBlockedTensionScenario(t, ctx, store, "rsp-belief-policy")

	_, err := store.SnapshotRSPBeliefReport(ctx, RSPBeliefReportFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
	})
	if err == nil || !IsRSPRolloutDisabledError(err) {
		t.Fatalf("expected rollout policy error for belief snapshot, got %v", err)
	}
}

func TestSnapshotRSPForecastReportRequiresForecastShadowCapability(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationLocusHealthyScenario(t, ctx, store, "rsp-forecast-policy")

	_, err := store.SnapshotRSPForecastReport(ctx, RSPForecastReportFilter{
		WorkspaceID:   scenario.workspaceID,
		AgentID:       scenario.agentID,
		TaskID:        scenario.taskID,
		SessionID:     scenario.sessionID,
		DocKeys:       []string{scenario.docKey},
		ArtifactRefs:  []string{scenario.artifactRef},
		FrontierLimit: 2,
	})
	if err == nil || !IsRSPRolloutDisabledError(err) {
		t.Fatalf("expected rollout policy error for forecast snapshot, got %v", err)
	}
}

func TestRSPFirehoseKeepsMotifHistoryScopedPerWorkspace(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()
	for _, workspaceID := range []string{"ws-firehose-a", "ws-firehose-b"} {
		if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
			WorkspaceID: workspaceID,
			Title:       workspaceID,
			CreatedBy:   "tester",
		}); err != nil {
			t.Fatalf("create workspace %s: %v", workspaceID, err)
		}
		if err := store.RegisterAgent(ctx, AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     "shared-agent",
			OwnerUserID: "tester",
			DisplayName: "shared-agent",
		}); err != nil {
			t.Fatalf("register shared agent in %s: %v", workspaceID, err)
		}
	}

	fh := NewRSPFirehose(store)
	fh.processMotifsAndAgentState(ctx, RuntimeEventRecord{
		EventID:     "evt-a1",
		WorkspaceID: "ws-firehose-a",
		AgentID:     "shared-agent",
		EntityID:    "entity-1",
		EventType:   "artifact.patch",
	})
	fh.processMotifsAndAgentState(ctx, RuntimeEventRecord{
		EventID:     "evt-a2",
		WorkspaceID: "ws-firehose-a",
		AgentID:     "shared-agent",
		EntityID:    "entity-1",
		EventType:   "verifier.fail",
	})
	fh.processMotifsAndAgentState(ctx, RuntimeEventRecord{
		EventID:     "evt-b1",
		WorkspaceID: "ws-firehose-b",
		AgentID:     "shared-agent",
		EntityID:    "entity-1",
		EventType:   "artifact.patch",
	})

	eventsB, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: "ws-firehose-b",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace-b runtime events: %v", err)
	}
	if len(eventsB) != 0 {
		t.Fatalf("expected no cross-workspace anomaly events, got %+v", eventsB)
	}
	if ctxB, ok := fh.agentContext[rspFirehoseAgentContextKey("ws-firehose-b", "shared-agent")]; !ok {
		t.Fatalf("expected workspace-b agent context to exist")
	} else if len(ctxB.LastEntityEvents[rspFirehoseEntityContextKey("ws-firehose-b", "entity-1")]) != 1 {
		t.Fatalf("expected independent workspace-b motif window, got %+v", ctxB.LastEntityEvents)
	}
	if ctxA, ok := fh.agentContext[rspFirehoseAgentContextKey("ws-firehose-a", "shared-agent")]; !ok {
		t.Fatalf("expected workspace-a agent context to exist")
	} else if len(ctxA.LastEntityEvents[rspFirehoseEntityContextKey("ws-firehose-a", "entity-1")]) != 2 {
		t.Fatalf("expected workspace-a motif window to remain isolated, got %+v", ctxA.LastEntityEvents)
	}
}

func TestIsRSPRolloutDisabledErrorDetectsPolicyMessage(t *testing.T) {
	t.Parallel()

	if !IsRSPRolloutDisabledError(assertError("rsp.state.shadow disabled by rollout policy")) {
		t.Fatal("expected rollout policy helper to match storage error")
	}
	if IsRSPRolloutDisabledError(assertError("something else")) {
		t.Fatal("expected rollout policy helper to ignore unrelated errors")
	}
}

type rspSnapshotPolicyError string

func (e rspSnapshotPolicyError) Error() string {
	return string(e)
}

func assertError(message string) error {
	return rspSnapshotPolicyError(strings.TrimSpace(message))
}
