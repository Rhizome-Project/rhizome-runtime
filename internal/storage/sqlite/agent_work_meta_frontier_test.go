package sqlite

import (
	"context"
	"testing"
	"time"
)

func TestGetAgentWorkNextAdvisoryIncludesEmergentMetaTensionFrontier(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-work-meta-frontier"
		agentID     = "agent-a"
		taskID      = "task-meta-frontier"
		clusterID   = "task:ws-agent-work-meta-frontier/task-meta-frontier"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, agentID)
	ensureTensionOverlayTables(t, ctx, store)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-agent-work-meta-frontier")

	now := time.Now().UTC().Format(time.RFC3339Nano)
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "tension:task:" + workspaceID + "/" + taskID,
		WorkspaceID:    workspaceID,
		ProtoClusterID: clusterID,
		TensionType:    "gap",
		LifecycleState: tensionLifecycleActive,
		ReviewStatus:   tensionReviewPending,
		Title:          "Atomic task tension",
		AnchorKind:     "task",
		AnchorRef:      taskID,
		TaskIDs:        []string{taskID},
		BaseScore:      55,
		SurfaceScore:   72,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:      "ten_meta_agent_work_frontier",
		WorkspaceID:    workspaceID,
		ProtoClusterID: clusterID,
		TensionType:    "meta-tension",
		LifecycleState: tensionLifecycleEmergent,
		ReviewStatus:   tensionReviewPending,
		Title:          "Emergent meta tension",
		AnchorKind:     "scc_condensation",
		AnchorRef:      "2_members",
		TaskIDs:        []string{taskID},
		BaseScore:      85,
		SurfaceScore:   77,
		CreatedAt:      now,
		UpdatedAt:      now,
	})

	result, err := store.GetAgentWorkNext(ctx, AgentWorkNextFilter{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		IncludePacket:   true,
		IncludeAdvisory: true,
		FrontierLimit:   2,
		CandidateTaskID: taskID,
		Trigger:         "inbound_message",
	})
	if err != nil {
		t.Fatalf("get agent work next: %v", err)
	}
	if result.Packet == nil || result.Packet.Advisory == nil {
		t.Fatalf("expected advisory packet, got %+v", result)
	}
	if len(result.Packet.Advisory.Frontier) == 0 {
		t.Fatalf("expected advisory frontier, got %+v", result.Packet.Advisory)
	}
	first := result.Packet.Advisory.Frontier[0]
	if first.TensionID != "ten_meta_agent_work_frontier" {
		t.Fatalf("expected emergent meta tension to lead advisory frontier, got %+v", result.Packet.Advisory.Frontier)
	}
	if first.Kind != "meta" || first.SurfacedPriority <= 0 {
		t.Fatalf("expected structural meta frontier fields, got %+v", first)
	}
}
