package sqlite

import (
	"context"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestInstrumentationEvidenceForEventExtractsDocArtifactAndUpdateTypes(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-instrumentation-evidence-types"
		taskID      = "task-instrumentation-evidence-types"
		docKey      = "runbook"
		artifactID  = "artifact-evidence-types"
		artifactRef = "artifact://evidence-types"
		updateAgent = "agent-update"
		sourceAgent = "agent-source"
		targetAgent = "agent-target"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, updateAgent, sourceAgent, targetAgent)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-evidence-types")

	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "entity-type extraction doc",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, WorkspaceArtifactInput{
		ArtifactID:  artifactID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Title:       "Evidence Artifact",
		ArtifactRef: artifactRef,
		Kind:        "log",
		ContentType: "text/plain",
		CreatedBy:   sourceAgent,
	}); err != nil {
		t.Fatalf("create workspace artifact: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     updateAgent,
		UpdateType:  "status",
		Summary:     "linked update",
		PayloadJSON: `{"task_ids":["` + taskID + `"],"doc_keys":["` + docKey + `"],"artifacts":[{"ref":"` + artifactRef + `"}]}`,
	}); err != nil {
		t.Fatalf("record agent update: %v", err)
	}
	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, sourceAgent)
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-source",
		AgentID:     sourceAgent,
		TaskID:      taskID,
		Summary:     "source session",
		OwnerScope:  "task/session",
		HandoffTo:   targetAgent,
		RelatedDocKeys: []string{
			docKey,
		},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: artifactRef},
		},
	}); err != nil {
		t.Fatalf("record source session: %v", err)
	}
	if _, err := store.TakeOverAgentSession(ctx, AgentSessionTakeoverInput{
		WorkspaceID:        workspaceID,
		SessionID:          "sess-source",
		SuccessorSessionID: "sess-target",
		TakeoverAgentID:    targetAgent,
		Summary:            "handoff to target agent",
		SuccessorSummary:   "target resumes ownership",
	}); err != nil {
		t.Fatalf("take over agent session: %v", err)
	}

	docEvent := requireInstrumentationInternalEvent(t, ctx, store, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_doc.upserted",
		EntityType:  "workspace_doc",
		EntityID:    docKey,
		Limit:       1,
	})
	docEvidence, err := store.instrumentationEvidenceForEvent(ctx, workspaceID, docEvent, map[string]TaskHydrationBundle{}, map[string]instrumentationAgentUpdateLinks{})
	if err != nil {
		t.Fatalf("doc evidence: %v", err)
	}
	if len(docEvidence.docKeys) != 1 || docEvidence.docKeys[0] != docKey {
		t.Fatalf("expected workspace_doc evidence to keep doc key, got %+v", docEvidence)
	}

	artifactEvent := requireInstrumentationInternalEvent(t, ctx, store, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_artifact.created",
		EntityType:  "workspace_artifact",
		EntityID:    artifactID,
		Limit:       1,
	})
	artifactEvidence, err := store.instrumentationEvidenceForEvent(ctx, workspaceID, artifactEvent, map[string]TaskHydrationBundle{}, map[string]instrumentationAgentUpdateLinks{})
	if err != nil {
		t.Fatalf("artifact evidence: %v", err)
	}
	if !equalStringSlices(artifactEvidence.taskIDs, []string{taskID}) || !equalStringSlices(artifactEvidence.artifactRefs, []string{artifactRef}) {
		t.Fatalf("expected workspace_artifact evidence to include task/artifact refs, got %+v", artifactEvidence)
	}

	updateEvent := requireInstrumentationInternalEvent(t, ctx, store, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_update.posted",
		EntityType:  "agent_update",
		Limit:       1,
	})
	updateEvidence, err := store.instrumentationEvidenceForEvent(ctx, workspaceID, updateEvent, map[string]TaskHydrationBundle{}, map[string]instrumentationAgentUpdateLinks{})
	if err != nil {
		t.Fatalf("agent update evidence: %v", err)
	}
	if !equalStringSlices(updateEvidence.taskIDs, []string{taskID}) || !equalStringSlices(updateEvidence.docKeys, []string{docKey}) || !equalStringSlices(updateEvidence.artifactRefs, []string{artifactRef}) {
		t.Fatalf("expected agent_update evidence to resolve task/doc/artifact refs, got %+v", updateEvidence)
	}

}

func TestInstrumentationEvidenceForTakeoverIncludesSuccessorSession(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-instrumentation-evidence-takeover"
		taskID      = "task-instrumentation-evidence-takeover"
		docKey      = "runbook"
		artifactRef = "artifact://evidence-takeover"
		sourceAgent = "agent-source"
		targetAgent = "agent-target"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, sourceAgent, targetAgent)
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-evidence-takeover")

	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "takeover extraction doc",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	claimInternalTaskForSessionStart(t, ctx, store, workspaceID, taskID, sourceAgent)
	if _, err := store.RecordAgentSessionCoordination(ctx, AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-source",
		AgentID:     sourceAgent,
		TaskID:      taskID,
		Summary:     "source session",
		OwnerScope:  "task/session",
		HandoffTo:   targetAgent,
		RelatedDocKeys: []string{
			docKey,
		},
		RelatedArtifactRefs: []model.AgentUpdateArtifactRef{
			{Ref: artifactRef},
		},
	}); err != nil {
		t.Fatalf("record source session: %v", err)
	}
	if _, err := store.TakeOverAgentSession(ctx, AgentSessionTakeoverInput{
		WorkspaceID:        workspaceID,
		SessionID:          "sess-source",
		SuccessorSessionID: "sess-target",
		TakeoverAgentID:    targetAgent,
		Summary:            "handoff to target agent",
		SuccessorSummary:   "target resumes ownership",
	}); err != nil {
		t.Fatalf("take over agent session: %v", err)
	}

	takeoverEvent := requireInstrumentationInternalEvent(t, ctx, store, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "session.takeover",
		EntityType:  "agent_session",
		EntityID:    "sess-source",
		Limit:       1,
	})
	takeoverEvidence, err := store.instrumentationEvidenceForEvent(ctx, workspaceID, takeoverEvent, map[string]TaskHydrationBundle{}, map[string]instrumentationAgentUpdateLinks{})
	if err != nil {
		t.Fatalf("takeover evidence: %v", err)
	}
	if !equalStringSlices(takeoverEvidence.taskIDs, []string{taskID}) {
		t.Fatalf("expected takeover evidence to keep task id, got %+v", takeoverEvidence)
	}
	if !equalStringSlices(takeoverEvidence.sessionIDs, []string{"sess-source", "sess-target"}) {
		t.Fatalf("expected takeover evidence to include both sessions, got %+v", takeoverEvidence)
	}
	if !equalStringSlices(takeoverEvidence.docKeys, []string{docKey}) {
		t.Fatalf("expected takeover evidence to include related docs, got %+v", takeoverEvidence)
	}
}

func TestInstrumentationEvidenceForEventDeduplicatesAcrossPayloadAndHydration(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-instrumentation-evidence-dedupe"
		taskID      = "task-instrumentation-evidence-dedupe"
		docKey      = "runbook"
		artifactID  = "artifact-evidence-dedupe"
		artifactRef = "artifact://evidence-dedupe"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-evidence-dedupe")

	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "dedupe doc",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	if err := store.CreateWorkspaceArtifact(ctx, WorkspaceArtifactInput{
		ArtifactID:  artifactID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		Title:       "Dedupe Artifact",
		ArtifactRef: artifactRef,
		Kind:        "log",
		ContentType: "text/plain",
		CreatedBy:   "agent-a",
	}); err != nil {
		t.Fatalf("create workspace artifact: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		UpdateType:  "status",
		Summary:     "dedupe update",
		PayloadJSON: `{"task_ids":["` + taskID + `","` + taskID + `"],"doc_keys":["` + docKey + `","` + docKey + `"],"artifacts":[{"ref":"` + artifactRef + `"},{"ref":"` + artifactRef + `"}]}`,
	}); err != nil {
		t.Fatalf("record agent update: %v", err)
	}

	updateEvent := requireInstrumentationInternalEvent(t, ctx, store, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_update.posted",
		EntityType:  "agent_update",
		Limit:       1,
	})
	evidence, err := store.instrumentationEvidenceForEvent(ctx, workspaceID, updateEvent, map[string]TaskHydrationBundle{}, map[string]instrumentationAgentUpdateLinks{})
	if err != nil {
		t.Fatalf("resolve instrumentation evidence: %v", err)
	}
	if !equalStringSlices(evidence.taskIDs, []string{taskID}) {
		t.Fatalf("expected deduped task ids, got %+v", evidence.taskIDs)
	}
	if !equalStringSlices(evidence.docKeys, []string{docKey}) {
		t.Fatalf("expected deduped doc keys, got %+v", evidence.docKeys)
	}
	if !equalStringSlices(evidence.artifactRefs, []string{artifactRef}) {
		t.Fatalf("expected deduped artifact refs, got %+v", evidence.artifactRefs)
	}
}

func TestInstrumentationEvidenceSkipsInvalidTaskIDsFromUpdatePayload(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID   = "ws-instrumentation-evidence-invalid-task"
		taskID        = "task-instrumentation-valid"
		invalidTaskID = "task-instrumentation-valid.cycle_status"
	)

	setupInstrumentationInternalWorkspace(t, ctx, store, workspaceID, "agent-a")
	createInstrumentationInternalTask(t, ctx, store, workspaceID, taskID, "node-valid")

	if err := store.RecordAgentUpdate(ctx, AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     "agent-a",
		UpdateType:  "coordination",
		Summary:     "public peer response evidence",
		PayloadJSON: `{"task_ids":["` + taskID + `","` + invalidTaskID + `"],"doc_key":"task.` + taskID + `.cycle_status"}`,
	}); err != nil {
		t.Fatalf("record agent update: %v", err)
	}

	updateEvent := requireInstrumentationInternalEvent(t, ctx, store, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "agent_update.posted",
		EntityType:  "agent_update",
		Limit:       1,
	})
	evidence, err := store.instrumentationEvidenceForEvent(ctx, workspaceID, updateEvent, map[string]TaskHydrationBundle{}, map[string]instrumentationAgentUpdateLinks{})
	if err != nil {
		t.Fatalf("resolve instrumentation evidence: %v", err)
	}
	if !equalStringSlices(evidence.taskIDs, []string{taskID}) {
		t.Fatalf("expected invalid task id to be skipped, got %+v", evidence.taskIDs)
	}
}

func setupInstrumentationInternalWorkspace(t *testing.T, ctx context.Context, store *Store, workspaceID string, agentIDs ...string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Instrumentation Internal",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
}

func createInstrumentationInternalTask(t *testing.T, ctx context.Context, store *Store, workspaceID, taskID, nodeID string) {
	t.Helper()

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: nodeID, Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
}

func requireInstrumentationInternalEvent(t *testing.T, ctx context.Context, store *Store, filter RuntimeEventFilter) RuntimeEventRecord {
	t.Helper()

	events, err := store.ListRuntimeEvents(ctx, filter)
	if err != nil {
		t.Fatalf("list runtime events %+v: %v", filter, err)
	}
	if len(events) == 0 {
		t.Fatalf("expected runtime event for filter %+v", filter)
	}
	return events[0]
}
