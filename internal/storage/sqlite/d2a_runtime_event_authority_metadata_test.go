package sqlite_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestCapabilityPolicyRuntimeEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-policy-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Policy Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	_, event, err := store.PutCapabilityPolicyWithEvent(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: workspaceID,
		SubjectType: "workspace",
		SubjectID:   workspaceID,
		Capability:  "rsp.governed_hints.live",
		ToolID:      "*",
		Effect:      "ALLOW",
		Reason:      "stamp canonical journal append with authority provenance",
		CreatedBy:   "operator-a",
	})
	if err != nil {
		t.Fatalf("put capability policy with event: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "capability_policy.put",
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one capability policy event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestControlCommandRuntimeEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-command-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Control Command Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	_, event, err := store.RequestControlCommandWithEvent(ctx, sqlite.ControlCommandInput{
		WorkspaceID: workspaceID,
		CommandType: sqlite.ControlCommandRefreshKernel,
		AgentID:     "agent-refresh",
		Reason:      "stamp canonical journal append with authority provenance",
		RequestedBy: "operator-a",
	})
	if err != nil {
		t.Fatalf("request control command with event: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "control.command.requested",
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one control command event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestOperatorQueueRuntimeEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-queue-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Queue Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	_, event, err := store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID: workspaceID,
		QueueKey:    "d2a-authority-metadata",
		QueueType:   "REVIEW",
		Title:       "Queue append should carry authority metadata",
		Summary:     "tests queue path",
		SourceKind:  "test",
		SourceID:    "source-a",
	})
	if err != nil {
		t.Fatalf("upsert operator queue item with event: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.created",
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one operator queue event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestOperatorQueueRuntimeEventHelperCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-queue-runtime-helper-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Queue Runtime Helper Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	result, err := store.UpsertOperatorQueueItemWithRuntimeEvent(ctx,
		sqlite.OperatorQueueUpsertInput{
			WorkspaceID: workspaceID,
			QueueKey:    "d2a-authority-runtime-helper",
			QueueType:   "FOLLOW_UP",
			Title:       "Queue runtime helper should carry authority metadata",
			Summary:     "tests runtime helper path",
			SourceKind:  "test",
			SourceID:    "source-b",
		},
		sqlite.RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "operator_queue.audit",
			EntityType:  "operator_queue",
			EntityID:    "d2a-authority-runtime-helper",
			ActorType:   "operator",
			ActorID:     "operator-a",
			PayloadJSON: `{"kind":"audit"}`,
		},
	)
	if err != nil {
		t.Fatalf("upsert operator queue item with runtime event: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, result.QueueEvent.Event, authority)
	assertRuntimeEventAuthorityMetadata(t, result.RuntimeEvent, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.audit",
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one helper runtime event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestInstrumentationSnapshotRuntimeEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationScenario(t, ctx, store, "ws-d2a-instrumentation-snapshot-authority-metadata", "task-d2a-instrumentation-snapshot")
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, scenario.workspaceID)

	report, err := store.BuildInstrumentationReport(ctx, sqlite.InstrumentationReportFilter{
		WorkspaceID:  scenario.workspaceID,
		TaskID:       scenario.taskID,
		Limit:        200,
		ClusterLimit: 5,
	})
	if err != nil {
		t.Fatalf("build instrumentation report: %v", err)
	}
	event, err := store.RecordInstrumentationMetricSnapshot(ctx, report, sqlite.InstrumentationSnapshotInput{
		ActorID: "dashboard",
		Limit:   2,
	})
	if err != nil {
		t.Fatalf("record instrumentation metric snapshot: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.metric_snapshot",
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list instrumentation snapshot runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one instrumentation snapshot runtime event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestControlSnapshotRuntimeEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	scenario := seedInstrumentationScenario(t, ctx, store, "ws-d2a-control-snapshot-authority-metadata", "task-d2a-control-snapshot")
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, scenario.workspaceID)

	refresh, err := store.RefreshTensions(ctx, sqlite.TensionRefreshInput{
		WorkspaceID: scenario.workspaceID,
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("refresh tensions: %v", err)
	}
	if refresh.CreatedCount == 0 {
		t.Fatalf("expected tension refresh to create at least one tension, got %+v", refresh)
	}
	items, err := store.ListTensions(ctx, sqlite.TensionFilter{
		WorkspaceID: scenario.workspaceID,
		TaskID:      scenario.taskID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list tensions: %v", err)
	}
	var primary sqlite.TensionRecord
	found := false
	for _, item := range items {
		if item.TensionType == "bottleneck" {
			primary = item
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected bottleneck tension, got %+v", items)
	}
	if _, err := store.ConfirmTension(ctx, sqlite.TensionMutationInput{
		WorkspaceID: scenario.workspaceID,
		TensionID:   primary.TensionID,
		ActorID:     "dashboard",
		Reason:      "confirm for authority metadata snapshot",
	}); err != nil {
		t.Fatalf("confirm tension: %v", err)
	}

	report, err := store.BuildControlReport(ctx, sqlite.ControlReportFilter{
		WorkspaceID: scenario.workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("build control report: %v", err)
	}
	event, err := store.RecordControlSignalSnapshot(ctx, report, sqlite.ControlSnapshotInput{
		ActorID: "dashboard",
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("record control signal snapshot: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: scenario.workspaceID,
		EventType:   "cluster.control_advisory_snapshot",
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list control snapshot runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one control snapshot runtime event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestWorkspaceMemoryLifecycleRuntimeEventsCarryAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-memory-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Workspace Memory Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	record, recordedEvent, _, err := store.RecordWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "NOTE",
		Title:       "Authority-backed workspace memory",
		Body:        "workspace_memory lifecycle events should carry authority metadata",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("record workspace memory with effects: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, recordedEvent, authority)

	archived, archivedEvent, _, err := store.ArchiveWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		ArchivedBy:  "operator-a",
		Reason:      "authority-backed archive",
	})
	if err != nil {
		t.Fatalf("archive workspace memory with effects: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, archivedEvent, authority)

	restored, restoredEvent, _, err := store.RestoreWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryRestoreInput{
		WorkspaceID: workspaceID,
		MemoryID:    archived.MemoryID,
		RestoredBy:  "operator-a",
	})
	if err != nil {
		t.Fatalf("restore workspace memory with effects: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, restoredEvent, authority)

	recordedEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    restored.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace_memory.recorded events: %v", err)
	}
	if len(recordedEvents) == 0 {
		t.Fatal("expected at least one workspace_memory.recorded event")
	}
	assertRuntimeEventAuthorityMetadata(t, recordedEvents[0], authority)

	archivedEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.archived",
		EntityType:  "workspace_memory",
		EntityID:    restored.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace_memory.archived events: %v", err)
	}
	if len(archivedEvents) != 1 {
		t.Fatalf("expected one workspace_memory.archived event, got %d", len(archivedEvents))
	}
	assertRuntimeEventAuthorityMetadata(t, archivedEvents[0], authority)

	restoredEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.restored",
		EntityType:  "workspace_memory",
		EntityID:    restored.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace_memory.restored events: %v", err)
	}
	if len(restoredEvents) != 1 {
		t.Fatalf("expected one workspace_memory.restored event, got %d", len(restoredEvents))
	}
	assertRuntimeEventAuthorityMetadata(t, restoredEvents[0], authority)
}

func TestRMPRunBatchedPruningArchivedRuntimeEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-rmp-pruning-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A RMP Pruning Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	record, _, _, err := store.RecordWorkspaceMemoryWithEffects(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "LESSON",
		Title:       "Prunable authority-backed workspace memory",
		Body:        "RMP pruning archive events should carry authority metadata.",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("record workspace memory with effects: %v", err)
	}
	seedPastGcWorkspaceMemoryForPruningAuthorityTest(t, ctx, store, workspaceID, record.MemoryID)

	pruned, err := store.RMPRunBatchedPruning(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("run batched pruning: %v", err)
	}
	if len(pruned) != 1 {
		t.Fatalf("expected one pruned memory node, got %+v", pruned)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.archived",
		EntityType:  "workspace_memory",
		EntityID:    record.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace_memory.archived events after pruning: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one workspace_memory.archived event after pruning, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestAgentUpdatePostedCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-agent-update-authority-metadata"
		agentID     = "agent-d2a-agent-update-authority-metadata"
		updateID    = "update-d2a-agent-update-authority-metadata"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Agent Update Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "tests",
		DisplayName: "Agent Update Metadata",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	event, err := store.RecordAgentUpdateWithEvent(ctx, sqlite.AgentUpdateInput{
		UpdateID:      updateID,
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		UpdateType:    "progress",
		Summary:       "authority metadata should stamp agent_update.posted",
		PayloadJSON:   `{"kind":"progress"}`,
		RequiresHuman: true,
	})
	if err != nil {
		t.Fatalf("record agent update with event: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_update.posted",
		EntityType:  "agent_update",
		EntityID:    updateID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one agent_update.posted event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestAgentSessionLifecycleCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-session-authority-metadata"
		sourceAgent = "agent-d2a-session-source"
		targetAgent = "agent-d2a-session-target"
		sessionID   = "sess-d2a-session-source"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Session Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agentID := range []string{sourceAgent, targetAgent} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "tests",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	startState, startEvent, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     sourceAgent,
		Summary:     "authority metadata should stamp session start",
		HandoffTo:   targetAgent,
	})
	if err != nil {
		t.Fatalf("record session start with event: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, startEvent, authority)

	statusState, statusEvent, err := store.RecordAgentSessionCoordinationWithEvent(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStatus,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     sourceAgent,
		Summary:     "authority metadata should stamp session status",
		HandoffTo:   targetAgent,
	})
	if err != nil {
		t.Fatalf("record session status with event: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, statusEvent, authority)

	takeover, takeoverEvent, err := store.TakeOverAgentSessionWithEvent(ctx, sqlite.AgentSessionTakeoverInput{
		WorkspaceID:     workspaceID,
		SessionID:       sessionID,
		TakeoverAgentID: targetAgent,
		Summary:         "authority metadata should stamp session takeover",
	})
	if err != nil {
		t.Fatalf("take over agent session with event: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, takeoverEvent, authority)

	startEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventStart,
		EntityType:  "agent_session",
		EntityID:    startState.SessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list session.start events: %v", err)
	}
	if len(startEvents) == 0 {
		t.Fatal("expected at least one session.start event")
	}
	assertRuntimeEventAuthorityMetadata(t, startEvents[0], authority)

	statusEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventStatus,
		EntityType:  "agent_session",
		EntityID:    statusState.SessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list session.status events: %v", err)
	}
	if len(statusEvents) != 1 {
		t.Fatalf("expected one session.status event, got %d", len(statusEvents))
	}
	assertRuntimeEventAuthorityMetadata(t, statusEvents[0], authority)

	endEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventEnd,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list session.end events: %v", err)
	}
	if len(endEvents) != 1 {
		t.Fatalf("expected one session.end event, got %d", len(endEvents))
	}
	assertRuntimeEventAuthorityMetadata(t, endEvents[0], authority)

	successorStartEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   model.SessionEventStart,
		EntityType:  "agent_session",
		EntityID:    takeover.SuccessorState.SessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list successor session.start events: %v", err)
	}
	if len(successorStartEvents) != 1 {
		t.Fatalf("expected one successor session.start event, got %d", len(successorStartEvents))
	}
	assertRuntimeEventAuthorityMetadata(t, successorStartEvents[0], authority)

	takeoverEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "session.takeover",
		EntityType:  "agent_session",
		EntityID:    sessionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list session.takeover events: %v", err)
	}
	if len(takeoverEvents) != 1 {
		t.Fatalf("expected one session.takeover event, got %d", len(takeoverEvents))
	}
	assertRuntimeEventAuthorityMetadata(t, takeoverEvents[0], authority)
}

func TestCreateHumanActionWithQueueEffectsCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-action-create-authority-metadata"
		taskID      = "task-d2a-action-create-authority-metadata"
		agentID     = "agent-d2a-action-create-authority-metadata"
	)

	authority := seedD1CActionAuthorityFixture(t, ctx, store, workspaceID, taskID, agentID)

	result, err := store.CreateHumanActionWithQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Authority-backed action.create",
		Description: "action.created should carry authority metadata",
		Blocking:    true,
	}, nil, nil)
	if err != nil {
		t.Fatalf("create human action with queue effects: %v", err)
	}
	if result.ActionQueue == nil {
		t.Fatal("expected action queue event")
	}
	if result.ActionEvent == nil {
		t.Fatal("expected action runtime event")
	}

	assertRuntimeEventAuthorityMetadata(t, result.ActionQueue.Event, authority)
	assertRuntimeEventAuthorityMetadata(t, *result.ActionEvent, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.created",
		EntityType:  "human_action",
		EntityID:    result.Action.ActionID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one action.created event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestResolveHumanActionWithQueueEffectsCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-action-resolve-authority-metadata"
		taskID      = "task-d2a-action-resolve-authority-metadata"
		agentID     = "agent-d2a-action-resolve-authority-metadata"
	)

	authority := seedD1CActionAuthorityFixture(t, ctx, store, workspaceID, taskID, agentID)
	created, err := store.CreateHumanActionWithQueueEffects(ctx, sqlite.HumanActionInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		AssignedTo:  "reviewer-a",
		Title:       "Authority-backed action.resolve",
		Description: "action.resolved should carry authority metadata",
		Blocking:    true,
	}, nil, nil)
	if err != nil {
		t.Fatalf("seed human action with queue effects: %v", err)
	}
	if created.ActionQueue == nil {
		t.Fatal("expected seeded action queue event")
	}

	resolved, err := store.ResolveHumanActionWithQueueEffects(
		ctx,
		created.Action.ActionID,
		"COMPLETED",
		"done",
		"reviewer-a",
		&sqlite.OperatorQueueResolveInput{
			WorkspaceID:             workspaceID,
			QueueID:                 created.ActionQueue.Record.QueueID,
			Status:                  "RESOLVED",
			ResolvedBy:              "reviewer-a",
			Resolution:              "done",
			Summary:                 created.ActionQueue.Record.Summary,
			Details:                 created.ActionQueue.Record.Details,
			PayloadJSON:             created.ActionQueue.Record.PayloadJSON,
			RequireCurrentUpdatedAt: created.ActionQueue.Record.UpdatedAt,
		},
		nil,
		nil,
		&sqlite.RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "action.resolved",
			EntityType:  "human_action",
			EntityID:    created.Action.ActionID,
			ActorType:   "operator",
			ActorID:     "reviewer-a",
			AgentID:     agentID,
			TaskID:      taskID,
			PayloadJSON: `{"resolution":"COMPLETED"}`,
		},
		created.Action,
	)
	if err != nil {
		t.Fatalf("resolve human action with queue effects: %v", err)
	}
	if resolved.ActionQueue == nil {
		t.Fatal("expected resolved action queue event")
	}
	if resolved.ActionEvent == nil {
		t.Fatal("expected resolved action runtime event")
	}

	assertRuntimeEventAuthorityMetadata(t, resolved.ActionQueue.Event, authority)
	assertRuntimeEventAuthorityMetadata(t, *resolved.ActionEvent, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    created.Action.ActionID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one action.resolved event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestRequestKnowledgeClaimReviewWithEffectsCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-knowledge-claim-review-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Knowledge Claim Review Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	claim, _, _, err := store.RecordKnowledgeClaimWithEffects(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "LESSON",
		Subject:     "Authority-backed review request",
		Body:        "A fenced lifecycle event should carry authority metadata.",
		Summary:     "review request authority metadata",
		SourceKind:  "manual",
		SourceID:    "operator-a",
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}

	_, queue, claimEvent, queueEvent, _, err := store.RequestKnowledgeClaimReviewWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "operator-a",
		Reason:      "requires operator review",
		ReviewDueAt: "2026-04-12T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("request knowledge claim review with effects: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, claimEvent, authority)
	assertRuntimeEventAuthorityMetadata(t, queueEvent, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_requested",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one knowledge_claim.review_requested event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)

	queueEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list queue runtime events: %v", err)
	}
	if len(queueEvents) == 0 {
		t.Fatal("expected at least one queue runtime event")
	}
	assertRuntimeEventAuthorityMetadata(t, queueEvents[0], authority)
}

func TestEscalateKnowledgeClaimReviewWithEffectsCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-knowledge-claim-escalate-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Knowledge Claim Escalation Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	claim, _, _, err := store.RecordKnowledgeClaimWithEffects(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "LESSON",
		Subject:     "Authority-backed review escalation",
		Body:        "A fenced escalation event should carry authority metadata.",
		Summary:     "review escalation authority metadata",
		SourceKind:  "manual",
		SourceID:    "operator-a",
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}
	if _, _, _, _, _, err := store.RequestKnowledgeClaimReviewWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "operator-a",
		Reason:      "prepare escalation",
	}); err != nil {
		t.Fatalf("request review before escalation: %v", err)
	}

	escalation, claimEvent, queueEvent, _, err := store.EscalateKnowledgeClaimReviewWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "operator-b",
		Reason:      "review overdue",
		AssignedTo:  "reviewer-b",
		Urgency:     "HIGH",
		ReviewDueAt: "2026-04-13T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("escalate knowledge claim review with effects: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, claimEvent, authority)
	assertRuntimeEventAuthorityMetadata(t, queueEvent, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.review_escalated",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one knowledge_claim.review_escalated event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)

	queueEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    escalation.Queue.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list queue runtime events: %v", err)
	}
	if len(queueEvents) == 0 {
		t.Fatal("expected queue runtime events for escalation")
	}
	assertRuntimeEventAuthorityMetadata(t, queueEvents[0], authority)
}

func TestArchiveKnowledgeClaimWithEffectsCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-knowledge-claim-archive-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Knowledge Claim Archive Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	claim, _, _, err := store.RecordKnowledgeClaimWithEffects(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "LESSON",
		Subject:     "Authority-backed claim archive",
		Body:        "A fenced archive event should carry authority metadata.",
		Summary:     "archive authority metadata",
		SourceKind:  "manual",
		SourceID:    "operator-a",
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}
	if _, _, _, _, _, err := store.RequestKnowledgeClaimReviewWithEffects(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "operator-a",
		Reason:      "prepare archive",
	}); err != nil {
		t.Fatalf("request review before archive: %v", err)
	}

	_, queue, claimEvent, queueEvent, _, err := store.ArchiveKnowledgeClaimWithEffects(ctx, sqlite.KnowledgeClaimArchiveInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ArchivedBy:  "operator-a",
		Reason:      "superseded externally",
	})
	if err != nil {
		t.Fatalf("archive knowledge claim with effects: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, claimEvent, authority)
	assertRuntimeEventAuthorityMetadata(t, queueEvent, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.archived",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one knowledge_claim.archived event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)

	queueEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list queue runtime events: %v", err)
	}
	if len(queueEvents) == 0 {
		t.Fatal("expected queue runtime events for archived claim")
	}
	assertRuntimeEventAuthorityMetadata(t, queueEvents[0], authority)
}

func TestUpsertExecutionRunWithEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-execution-run-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Execution Run Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	run, event, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-d2a-authority-metadata",
		Title:       "Authority-backed execution run",
		Summary:     "execution_run.written should carry authority metadata",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("upsert execution run with event: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    run.RunID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one execution_run.written event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestUpsertExecutionRunCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-execution-run-helper-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Execution Run Helper Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	run, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-d2a-helper-authority-metadata",
		Title:       "Authority-backed execution run helper",
		Summary:     "generic execution_run.written should carry authority metadata",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("upsert execution run: %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_run.written",
		EntityType:  "execution_run",
		EntityID:    run.RunID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one execution_run.written event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestRecordExecutionStepWithEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-execution-step-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Execution Step Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	run, _, err := store.UpsertExecutionRunWithEvent(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-d2a-step-authority-metadata",
		Title:       "Authority-backed execution run for step",
		Summary:     "seed run for execution step authority metadata",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("seed execution run with event: %v", err)
	}

	step, event, err := store.RecordExecutionStepWithEvent(ctx, sqlite.ExecutionStepInput{
		WorkspaceID: workspaceID,
		RunID:       run.RunID,
		StepID:      "step-d2a-authority-metadata",
		Phase:       "EXECUTE",
		Title:       "Authority-backed execution step",
		Summary:     "execution_step.written should carry authority metadata",
		Status:      "ACTIVE",
		SortOrder:   10,
	})
	if err != nil {
		t.Fatalf("record execution step with event: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		EntityID:    step.StepID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one execution_step.written event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestRecordExecutionStepCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-execution-step-helper-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Execution Step Helper Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	run, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		WorkspaceID: workspaceID,
		RunID:       "run-d2a-step-helper-authority-metadata",
		Title:       "Authority-backed execution run helper for step",
		Summary:     "seed run for generic execution step authority metadata",
		Status:      "ACTIVE",
	})
	if err != nil {
		t.Fatalf("seed execution run: %v", err)
	}

	step, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		WorkspaceID: workspaceID,
		RunID:       run.RunID,
		StepID:      "step-d2a-helper-authority-metadata",
		Phase:       "EXECUTE",
		Title:       "Authority-backed execution step helper",
		Summary:     "generic execution_step.written should carry authority metadata",
		Status:      "ACTIVE",
		SortOrder:   10,
	})
	if err != nil {
		t.Fatalf("record execution step: %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "execution_step.written",
		EntityType:  "execution_step",
		EntityID:    step.StepID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one execution_step.written event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestRecordKnowledgeClaimWithAuthorityEffectsCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-knowledge-claim-write-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Knowledge Claim Write Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	claim, event, _, err := store.RecordKnowledgeClaimWithAuthorityEffects(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "LESSON",
		Subject:     "Authority-backed claim write",
		Body:        "The public authority-backed claim write path should stamp knowledge_claim.written.",
		Summary:     "claim write authority metadata",
		SourceKind:  "manual",
		SourceID:    "operator-a",
	})
	if err != nil {
		t.Fatalf("record knowledge claim with authority effects: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    claim.ClaimID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one knowledge_claim.written event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestPersistEffectiveControlsCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-effective-controls-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Effective Controls Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	record, err := store.PersistEffectiveControls(ctx, sqlite.EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    "cluster-alpha",
		Epoch:             7,
		TTLSeconds:        300,
		CandidateControls: sampleSuggestedControls(4, "throughput"),
		AdvisoryControls:  sampleSuggestedControls(3, "review"),
		EffectiveControls: sampleSuggestedControls(2, "safety"),
		GeneratedAt:       "2026-04-10T15:00:00Z",
		ActorID:           "operator-a",
	})
	if err != nil {
		t.Fatalf("persist effective controls: %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "effective_controls.persisted",
		EntityType:  "effective_controls",
		EntityID:    "proto_cluster:" + record.ProtoClusterID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one effective_controls.persisted event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestEnqueueMemoryPromotionWithEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-memory-promotion-enqueue-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Memory Promotion Enqueue Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	record, event, err := store.EnqueueMemoryPromotionWithEvent(ctx, sqlite.MemoryPromotionEnqueueInput{
		WorkspaceID: workspaceID,
		Candidate: sqlite.MemoryPromotionCandidate{
			MemoryType: "LESSON",
			Body:       "A fenced memory promotion enqueue should stamp authority metadata.",
			SourceKind: "manual",
			SourceID:   "operator-a",
		},
		BasisDigest: "basis-digest-d2a-memory-promotion-enqueue",
		ProposedBy:  "operator-a",
	})
	if err != nil {
		t.Fatalf("enqueue memory promotion with event: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.promotion_enqueued",
		EntityType:  "memory_promotion",
		EntityID:    record.PromotionID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list memory promotion enqueue events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one memory.promotion_enqueued event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)

	queue, err := store.GetOperatorQueueItem(ctx, workspaceID, "", "memory_promotion:"+record.PromotionID+":review")
	if err != nil {
		t.Fatalf("get mirrored operator queue item: %v", err)
	}
	queueEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.created",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list operator queue created events: %v", err)
	}
	if len(queueEvents) != 1 {
		t.Fatalf("expected one mirrored operator_queue.created event, got %d", len(queueEvents))
	}
	assertRuntimeEventAuthorityMetadata(t, queueEvents[0], authority)
}

func TestResolveMemoryPromotionCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-memory-promotion-resolve-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Memory Promotion Resolve Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	record, _, err := store.EnqueueMemoryPromotionWithEvent(ctx, sqlite.MemoryPromotionEnqueueInput{
		WorkspaceID: workspaceID,
		Candidate: sqlite.MemoryPromotionCandidate{
			MemoryType: "LESSON",
			Body:       "A fenced memory promotion resolve should stamp authority metadata.",
			SourceKind: "manual",
			SourceID:   "operator-a",
		},
		BasisDigest: "basis-digest-d2a-memory-promotion-resolve",
		ProposedBy:  "operator-a",
	})
	if err != nil {
		t.Fatalf("enqueue memory promotion: %v", err)
	}

	resolved, err := store.ResolveMemoryPromotion(ctx, sqlite.MemoryPromotionResolveInput{
		WorkspaceID: workspaceID,
		PromotionID: record.PromotionID,
		Resolution:  "REJECTED",
		ResolvedBy:  "operator-b",
	})
	if err != nil {
		t.Fatalf("resolve memory promotion: %v", err)
	}
	if resolved.Event == nil {
		t.Fatal("expected resolved promotion runtime event")
	}
	assertRuntimeEventAuthorityMetadata(t, *resolved.Event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.promotion_resolved",
		EntityType:  "memory_promotion",
		EntityID:    record.PromotionID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list memory promotion resolved events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one memory.promotion_resolved event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)

	queue, err := store.GetOperatorQueueItem(ctx, workspaceID, "", "memory_promotion:"+record.PromotionID+":review")
	if err != nil {
		t.Fatalf("get mirrored operator queue item after resolve: %v", err)
	}
	queueEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("list operator queue resolved events: %v", err)
	}
	if len(queueEvents) != 1 {
		t.Fatalf("expected one mirrored operator_queue.resolved event, got %d", len(queueEvents))
	}
	assertRuntimeEventAuthorityMetadata(t, queueEvents[0], authority)
}

func TestWorkspaceDocLifecycleCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-workspace-doc-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Workspace Doc Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	upserted, err := store.UpsertWorkspaceDocWithEvent(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "runbook",
		Title:       "Runbook",
		Content:     "Workspace doc lifecycle events should stamp authority metadata.",
		UpdatedBy:   "tests",
	})
	if err != nil {
		t.Fatalf("upsert workspace doc with event: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, upserted, authority)

	archived, err := store.ArchiveWorkspaceDocWithEvent(ctx, workspaceID, "runbook", "tests")
	if err != nil {
		t.Fatalf("archive workspace doc with event: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, archived, authority)

	deleted, err := store.DeleteWorkspaceDocWithEvent(ctx, workspaceID, "runbook", "tests")
	if err != nil {
		t.Fatalf("delete workspace doc with event: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, deleted, authority)

	for _, tc := range []struct {
		eventType string
	}{
		{eventType: "workspace_doc.upserted"},
		{eventType: "workspace_doc.archived"},
		{eventType: "workspace_doc.deleted"},
	} {
		events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   tc.eventType,
			EntityType:  "workspace_doc",
			EntityID:    "runbook",
			Limit:       5,
		})
		if err != nil {
			t.Fatalf("list %s runtime events: %v", tc.eventType, err)
		}
		if len(events) != 1 {
			t.Fatalf("expected one %s event, got %d", tc.eventType, len(events))
		}
		assertRuntimeEventAuthorityMetadata(t, events[0], authority)
	}
}

func TestWorkspaceArtifactLifecycleCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-workspace-artifact-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Workspace Artifact Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	record, created, _, err := store.RecordWorkspaceArtifactWithEffects(ctx, sqlite.WorkspaceArtifactInput{
		ArtifactID:   "artifact-d2a-authority-metadata",
		WorkspaceID:  workspaceID,
		Title:        "Authority-backed workspace artifact",
		ArtifactRef:  "artifact://authority-metadata",
		Kind:         "reference",
		ContentType:  "text/plain",
		CreatedBy:    "tests",
		MetadataJSON: `{"origin":"d2a"}`,
	})
	if err != nil {
		t.Fatalf("record workspace artifact with effects: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, created, authority)

	persisted, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_artifact.created",
		EntityType:  "workspace_artifact",
		EntityID:    record.ArtifactID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace_artifact.created events: %v", err)
	}
	if len(persisted) == 0 {
		t.Fatal("expected workspace_artifact.created event")
	}
	assertRuntimeEventAuthorityMetadata(t, persisted[0], authority)
}

func TestAgentMessageRuntimeEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-message-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Message Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerMessagingAuthorityTestAgents(t, ctx, store, workspaceID, "agent-a", "agent-b")
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	messageID, event, err := store.SendMessageWithAuthorityEvent(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Channel:     "default",
		Content:     "authority-backed agent message",
	})
	if err != nil {
		t.Fatalf("send message with authority event: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		EntityID:    messageID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list agent_message.sent events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one agent_message.sent event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestSendMessageCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-message-helper-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Message Helper Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerMessagingAuthorityTestAgents(t, ctx, store, workspaceID, "agent-a", "agent-b")
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	messageID, err := store.SendMessage(ctx, sqlite.MessageSendInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Content:     "generic helper message",
	})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_message.sent",
		EntityType:  "agent_message",
		EntityID:    messageID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list agent_message.sent events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one agent_message.sent event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestAgentRequestRuntimeEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-agent-request-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Agent Request Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerMessagingAuthorityTestAgents(t, ctx, store, workspaceID, "agent-a", "agent-b")
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, event, err := store.CreateAgentRequestWithAuthorityEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "review",
		Payload:     `{"kind":"authority"}`,
	})
	if err != nil {
		t.Fatalf("create agent request with authority event: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_request.sent",
		EntityType:  "agent_request",
		EntityID:    requestID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list agent_request.sent events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one agent_request.sent event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestCreateAgentRequestCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-agent-request-helper-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Agent Request Helper Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerMessagingAuthorityTestAgents(t, ctx, store, workspaceID, "agent-a", "agent-b")
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, err := store.CreateAgentRequest(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "review",
		Payload:     `{"kind":"generic-helper"}`,
	})
	if err != nil {
		t.Fatalf("create agent request: %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_request.sent",
		EntityType:  "agent_request",
		EntityID:    requestID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list agent_request.sent events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one agent_request.sent event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestAgentResponseRuntimeEventCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-agent-response-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Agent Response Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerMessagingAuthorityTestAgents(t, ctx, store, workspaceID, "agent-a", "agent-b")
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, _, err := store.CreateAgentRequestWithEvent(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "review",
		Payload:     `{"kind":"response"}`,
	})
	if err != nil {
		t.Fatalf("seed agent request: %v", err)
	}
	event, err := store.RespondAgentRequestWithAuthorityEvent(ctx, requestID, `{"status":"ok"}`)
	if err != nil {
		t.Fatalf("respond agent request with authority event: %v", err)
	}
	assertRuntimeEventAuthorityMetadata(t, event, authority)

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_response.recorded",
		EntityType:  "agent_request",
		EntityID:    requestID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list agent_response.recorded events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one agent_response.recorded event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func TestRespondAgentRequestCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-d2a-agent-response-helper-authority-metadata"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Agent Response Helper Authority Metadata",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	registerMessagingAuthorityTestAgents(t, ctx, store, workspaceID, "agent-a", "agent-b")
	authority := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	requestID, err := store.CreateAgentRequest(ctx, sqlite.AgentRequestInput{
		WorkspaceID: workspaceID,
		FromAgentID: "agent-a",
		ToAgentID:   "agent-b",
		Method:      "review",
		Payload:     `{"kind":"helper-response"}`,
	})
	if err != nil {
		t.Fatalf("seed agent request: %v", err)
	}
	if err := store.RespondAgentRequest(ctx, requestID, `{"status":"ok"}`); err != nil {
		t.Fatalf("respond agent request: %v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_response.recorded",
		EntityType:  "agent_request",
		EntityID:    requestID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list agent_response.recorded events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one agent_response.recorded event, got %d", len(events))
	}
	assertRuntimeEventAuthorityMetadata(t, events[0], authority)
}

func assertRuntimeEventAuthorityMetadata(t *testing.T, event sqlite.RuntimeEventRecord, authority sqlite.WorkspaceAuthorityRecord) {
	t.Helper()

	if event.AuthorityHolderNodeID != authority.HolderAuthorityNodeID {
		t.Fatalf("expected authority holder %q, got %q", authority.HolderAuthorityNodeID, event.AuthorityHolderNodeID)
	}
	if event.AuthorityTerm != authority.Term {
		t.Fatalf("expected authority term %d, got %d", authority.Term, event.AuthorityTerm)
	}
	expectedFingerprint := testAuthorityLeaseTokenFingerprint(authority.LeaseToken)
	if event.AuthorityLeaseTokenFingerprint != expectedFingerprint {
		t.Fatalf("expected authority lease fingerprint %q, got %q", expectedFingerprint, event.AuthorityLeaseTokenFingerprint)
	}
}

func testAuthorityLeaseTokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	encoded := hex.EncodeToString(sum[:])
	if len(encoded) > 16 {
		encoded = encoded[:16]
	}
	return "sha256:" + encoded
}
