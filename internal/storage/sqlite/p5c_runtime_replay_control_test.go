package sqlite

import (
	"context"
	"testing"
)

func TestRuntimeReplayIncludesCanonicalControlTruth(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	const workspaceID = "ws-p5c-replay-control"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P5C Replay Control",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	firstPolicy, firstEvent, err := store.PutCapabilityPolicyWithEvent(ctx, CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "agent",
		SubjectID:   "agent-a",
		Capability:  "tool.call",
		ToolID:      "dangerous-tool",
		Effect:      "DENY",
		Reason:      "first policy",
		CreatedBy:   "operator-a",
	})
	if err != nil {
		t.Fatalf("put first capability policy: %v", err)
	}
	secondPolicy, secondEvent, err := store.PutCapabilityPolicyWithEvent(ctx, CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "agent",
		SubjectID:   "agent-a",
		Capability:  "tool.call",
		ToolID:      "dangerous-tool",
		Effect:      "REQUIRE_APPROVAL",
		Reason:      "second policy",
		CreatedBy:   "operator-b",
	})
	if err != nil {
		t.Fatalf("put second capability policy: %v", err)
	}
	if secondPolicy.PolicyID != firstPolicy.PolicyID {
		t.Fatalf("expected stable policy id, first=%+v second=%+v", firstPolicy, secondPolicy)
	}

	parent, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:     "rtev-p5c-parent",
		WorkspaceID: workspaceID,
		EventType:   "legacy.signal",
		EntityType:  "legacy_event",
		EntityID:    "parent",
		ActorType:   "system",
		ActorID:     "tester",
		PayloadJSON: `{"message":"parent"}`,
		CreatedAt:   "2026-04-08T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("record parent event: %v", err)
	}
	insertTensionRecordFixture(t, ctx, store, tensionRecordFixture{
		TensionID:       "tension-a",
		WorkspaceID:     workspaceID,
		ProtoClusterID:  "task:" + workspaceID + "/task-a",
		TensionType:     "bottleneck",
		LifecycleState:  tensionLifecycleActive,
		ReviewStatus:    tensionReviewConfirmed,
		Title:           "Replay control exclusion fixture",
		Summary:         "Ensures exclusion replay can materialize inline control truth",
		AnchorKind:      "entity_id",
		AnchorRef:       "task-a",
		TaskIDs:         []string{"task-a"},
		BaseScore:       1,
		SurfaceScore:    1,
		EvidenceCount:   1,
		LastSeenEventID: parent.EventID,
		LastSeenAt:      "2026-04-08T00:00:00Z",
		ConfirmedBy:     "tester",
		CreatedAt:       "2026-04-08T00:00:00Z",
		UpdatedAt:       "2026-04-08T00:00:00Z",
	})
	setWorkspaceControlEpochAnchor(t, store, workspaceID, "2026-04-08T00:00:00Z")

	refresh, _, err := store.RequestControlCommandWithEvent(ctx, ControlCommandInput{
		WorkspaceID: workspaceID,
		CommandType: ControlCommandRefreshKernel,
		AgentID:     "agent-a",
		Reason:      "refresh bounded kernel",
		RequestedBy: "operator-c",
		ParentRefs:  []string{parent.EventID},
	})
	if err != nil {
		t.Fatalf("request refresh control command: %v", err)
	}
	exclude, _, err := store.RequestControlCommandWithEvent(ctx, ControlCommandInput{
		WorkspaceID: workspaceID,
		CommandType: ControlCommandExcludeAgentTension,
		TensionID:   "tension-a",
		AgentID:     "agent-a",
		TTLSeconds:  300,
		Reason:      "mute thrash loop",
		RequestedBy: "rsp.motif_detector",
		ActorType:   "system",
	})
	if err != nil {
		t.Fatalf("request exclusion control command: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}

	if len(report.CapabilityPolicies) != 1 {
		t.Fatalf("expected one replayed capability policy, got %+v", report.CapabilityPolicies)
	}
	policy := report.CapabilityPolicies[0]
	if policy.PolicyID != firstPolicy.PolicyID || policy.Effect != "REQUIRE_APPROVAL" || policy.Reason != "second policy" || policy.CreatedBy != "operator-b" {
		t.Fatalf("expected latest capability policy state, got %+v", policy)
	}
	if policy.CreatedAt != firstEvent.CreatedAt || policy.UpdatedAt != secondEvent.CreatedAt {
		t.Fatalf("expected policy timestamps from first and latest event, got %+v first=%+v second=%+v", policy, firstEvent, secondEvent)
	}

	if len(report.ControlCommands) != 2 {
		t.Fatalf("expected two replayed control commands, got %+v", report.ControlCommands)
	}
	commands := map[string]ControlCommandRecord{}
	for _, command := range report.ControlCommands {
		commands[command.CommandType] = command
	}
	refreshReplay, ok := commands[ControlCommandRefreshKernel]
	if !ok {
		t.Fatalf("missing refresh control command in %+v", report.ControlCommands)
	}
	if refreshReplay.CommandID != refresh.CommandID || refreshReplay.RequestedBy != "operator-c" || refreshReplay.AppliedInline {
		t.Fatalf("unexpected refresh replay %+v", refreshReplay)
	}
	if refreshReplay.Ownership.ActuatorOwner != "RMP" || refreshReplay.Ownership.RSP != controlOwnershipRoleAdvisoryOnly {
		t.Fatalf("expected refresh replay to reconstruct ownership matrix, got %+v", refreshReplay.Ownership)
	}
	if len(refreshReplay.ParentRefs) != 1 || refreshReplay.ParentRefs[0] != parent.EventID {
		t.Fatalf("expected parent refs to round-trip for refresh command, got %+v", refreshReplay.ParentRefs)
	}
	excludeReplay, ok := commands[ControlCommandExcludeAgentTension]
	if !ok {
		t.Fatalf("missing exclusion control command in %+v", report.ControlCommands)
	}
	if excludeReplay.CommandID != exclude.CommandID || !excludeReplay.AppliedInline || excludeReplay.ExpiresAt == "" {
		t.Fatalf("unexpected exclusion replay %+v", excludeReplay)
	}
	if excludeReplay.Ownership.ActuatorOwner != "RRP" || excludeReplay.Ownership.RSP != controlOwnershipRoleSignalSource {
		t.Fatalf("expected exclusion replay to reconstruct ownership matrix, got %+v", excludeReplay.Ownership)
	}
}

func TestRuntimeReplayRejectsMalformedControlTruthPayloads(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	const workspaceID = "ws-p5c-replay-control-malformed"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P5C Replay Control Malformed",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:     "rtev-p5c-bad-policy",
		WorkspaceID: workspaceID,
		EventType:   "capability_policy.put",
		EntityType:  "capability_policy",
		EntityID:    "policy-bad",
		ActorType:   "operator",
		ActorID:     "operator-a",
		PayloadJSON: `{"effect":"DENY"}`,
		CreatedAt:   "2026-04-08T01:00:00Z",
	}); err != nil {
		t.Fatalf("record malformed capability policy event: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:     "rtev-p5c-bad-command",
		WorkspaceID: workspaceID,
		EventType:   controlCommandEventType,
		EntityType:  controlCommandEntityType,
		EntityID:    "cmd-bad",
		ActorType:   "operator",
		ActorID:     "operator-b",
		PayloadJSON: `{"requested_by":"operator-b"}`,
		CreatedAt:   "2026-04-08T01:01:00Z",
	}); err != nil {
		t.Fatalf("record malformed control command event: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}

	if len(report.CapabilityPolicies) != 0 {
		t.Fatalf("did not expect malformed capability policy payload to fabricate state, got %+v", report.CapabilityPolicies)
	}
	if len(report.ControlCommands) != 0 {
		t.Fatalf("did not expect malformed control command payload to fabricate state, got %+v", report.ControlCommands)
	}

	found := map[string]bool{}
	for _, finding := range report.Evaluation.Findings {
		if finding.Code == "malformed_event_payload" {
			found[finding.SourceEventID] = true
		}
	}
	for _, eventID := range []string{"rtev-p5c-bad-policy", "rtev-p5c-bad-command"} {
		if !found[eventID] {
			t.Fatalf("expected malformed_event_payload finding for %s, got %+v", eventID, report.Evaluation.Findings)
		}
	}
}
