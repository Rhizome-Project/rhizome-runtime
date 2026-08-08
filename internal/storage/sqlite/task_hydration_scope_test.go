package sqlite_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestGetTaskHydrationBundleIncludeAllDocsFiltersStaleWorkspaceDocNoise(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-hydration-scope"
		taskID      = "task-task-hydration-scope"
		agentID     = "agent-task-hydration-scope"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Task Hydration Scope",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Hydration Scope Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}

	docs := []sqlite.WorkspaceDocInput{
		{
			WorkspaceID: workspaceID,
			DocKey:      "task." + taskID,
			Title:       "Task doc",
			Content:     "Task-scoped working note",
			UpdatedBy:   "developer",
		},
		{
			WorkspaceID: workspaceID,
			DocKey:      "tooling",
			Title:       "Tooling",
			Content:     "Current tool policy",
			UpdatedBy:   "developer",
		},
		{
			WorkspaceID: workspaceID,
			DocKey:      "agent." + agentID + ".current_context",
			Title:       "Current Context",
			Content:     "# Agent Current Context\n\n- agent_id: " + agentID + "\n- session_id: sess-1\n- task_id: " + taskID + "\n- task_title: scoped\n- outcome: active\n- summary: correct task\n",
			UpdatedBy:   agentID,
		},
		{
			WorkspaceID: workspaceID,
			DocKey:      "agent." + agentID + ".claimed_work",
			Title:       "Claimed Work",
			Content:     "# Claimed Work Ledger\n\n- agent_id: " + agentID + "\n- workspace_id: " + workspaceID + "\n\n## Active Claim\n- task_id: " + taskID + "\n- session_id: sess-1\n- state: ACTIVE\n",
			UpdatedBy:   agentID,
		},
		{
			WorkspaceID: workspaceID,
			DocKey:      "agent." + agentID + ".current_context_stale",
			Title:       "Ignored",
			Content:     "should not match suffix parser",
			UpdatedBy:   agentID,
		},
		{
			WorkspaceID: workspaceID,
			DocKey:      "agent.other-agent.current_context",
			Title:       "Foreign Context",
			Content:     "# Agent Current Context\n\n- agent_id: other-agent\n- session_id: sess-foreign\n- task_id: " + taskID + "\n- summary: foreign context\n",
			UpdatedBy:   "other-agent",
		},
		{
			WorkspaceID: workspaceID,
			DocKey:      "agent." + agentID + ".current_context",
			Title:       "Current Context",
			Content:     "# Agent Current Context\n\n- agent_id: " + agentID + "\n- session_id: sess-1\n- task_id: " + taskID + "\n- task_title: scoped\n- outcome: active\n- summary: correct task\n",
			UpdatedBy:   agentID,
		},
		{
			WorkspaceID: workspaceID,
			DocKey:      "agent.topology.additional_agents.v1",
			Title:       "Additional Agents",
			Content:     "Historical topology memo",
			UpdatedBy:   "observer",
		},
		{
			WorkspaceID: workspaceID,
			DocKey:      "agent." + agentID + ".claimed_work_old",
			Title:       "Ignored claimed work variant",
			Content:     "not a recognized agent hydration doc key",
			UpdatedBy:   agentID,
		},
	}
	for _, doc := range docs {
		if err := store.UpsertWorkspaceDoc(ctx, doc); err != nil {
			t.Fatalf("upsert doc %s: %v", doc.DocKey, err)
		}
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "agent." + agentID + ".current_context",
		Title:       "Current Context",
		Content:     "# Agent Current Context\n\n- agent_id: " + agentID + "\n- session_id: sess-1\n- task_id: " + taskID + "\n- task_title: scoped\n- outcome: active\n- summary: correct task\n",
		UpdatedBy:   agentID,
	}); err != nil {
		t.Fatalf("upsert current context: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "agent." + agentID + ".claimed_work",
		Title:       "Claimed Work",
		Content:     "# Claimed Work Ledger\n\n- agent_id: " + agentID + "\n- workspace_id: " + workspaceID + "\n\n## Active Claim\n- task_id: " + taskID + "\n- session_id: sess-1\n- state: ACTIVE\n",
		UpdatedBy:   agentID,
	}); err != nil {
		t.Fatalf("upsert claimed work: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "agent." + agentID + ".current_context",
		Title:       "Current Context",
		Content:     "# Agent Current Context\n\n- agent_id: " + agentID + "\n- session_id: sess-1\n- task_id: " + taskID + "\n- task_title: scoped\n- outcome: active\n- summary: correct task\n",
		UpdatedBy:   agentID,
	}); err != nil {
		t.Fatalf("re-upsert current context: %v", err)
	}

	// Replace the active current_context with a stale-other-task variant and then backfill the correct
	// claimed_work doc so we can prove includeAll filtering uses task scope, not just the doc key prefix.
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "agent." + agentID + ".current_context",
		Title:       "Current Context",
		Content:     "# Agent Current Context\n\n- agent_id: " + agentID + "\n- session_id: sess-stale\n- task_id: task-other\n- task_title: stale\n- outcome: blocked\n- summary: stale task\n",
		UpdatedBy:   agentID,
	}); err != nil {
		t.Fatalf("upsert stale current context: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "agent." + agentID + ".claimed_work",
		Title:       "Claimed Work",
		Content:     "# Claimed Work Ledger\n\n- agent_id: " + agentID + "\n- workspace_id: " + workspaceID + "\n\n## Active Claim\n- task_id: " + taskID + "\n- session_id: sess-1\n- state: ACTIVE\n",
		UpdatedBy:   agentID,
	}); err != nil {
		t.Fatalf("re-upsert claimed work: %v", err)
	}

	bundle, err := store.GetTaskHydrationBundle(ctx, sqlite.TaskHydrationFilter{
		TaskID:         taskID,
		WorkspaceID:    workspaceID,
		IncludeAllDocs: true,
	})
	if err != nil {
		t.Fatalf("GetTaskHydrationBundle(includeAll): %v", err)
	}

	got := map[string]bool{}
	for _, doc := range bundle.Docs {
		got[doc.DocKey] = true
	}
	if !got["task."+taskID] || !got["tooling"] || !got["agent."+agentID+".claimed_work"] {
		t.Fatalf("expected scoped hydration docs to survive filtering, got %+v", bundle.Docs)
	}
	if got["agent.topology.additional_agents.v1"] {
		t.Fatalf("expected historical topology doc to be filtered from includeAll hydration, got %+v", bundle.Docs)
	}
	if got["agent.other-agent.current_context"] {
		t.Fatalf("expected foreign agent current_context doc to be filtered from includeAll hydration, got %+v", bundle.Docs)
	}
	if got["agent."+agentID+".current_context"] {
		t.Fatalf("expected stale same-agent current_context for another task to be filtered from includeAll hydration, got %+v", bundle.Docs)
	}
}

func TestGetTaskHydrationBundleIncludesTaskRequirementSourceDocs(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-hydration-source-docs"
		taskID      = "task-task-hydration-source-docs"
		sourceDoc   = "run.clearpress.operator-spec"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Task Hydration Source Docs",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}}})
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:               taskID,
		OwnerUserID:          "developer",
		Priority:             "high",
		TaskKind:             "COORDINATION",
		TaskRequirementsJSON: `{"schema":"task_requirements.v1","operator_spec_doc_key":"run.clearpress.operator-spec"}`,
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      sourceDoc,
		Title:       "Operator Spec",
		Content:     "Full source product brief.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert source doc: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "unrelated.doc",
		Title:       "Noise",
		Content:     "Should stay out of scoped hydration.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert noise doc: %v", err)
	}

	bundle, err := store.GetTaskHydrationBundle(ctx, sqlite.TaskHydrationFilter{
		WorkspaceID:    workspaceID,
		TaskID:         taskID,
		IncludeAllDocs: true,
	})
	if err != nil {
		t.Fatalf("GetTaskHydrationBundle: %v", err)
	}
	if !hydrationBundleHasDoc(bundle, sourceDoc) {
		t.Fatalf("expected source doc %s in hydration docs: %+v", sourceDoc, bundle.Docs)
	}
	if hydrationBundleHasDoc(bundle, "unrelated.doc") {
		t.Fatalf("unrelated doc leaked into scoped hydration: %+v", bundle.Docs)
	}
}

func TestGetTaskHydrationBundleExplicitDocKeysCanStillFetchTopologyDoc(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-hydration-explicit-doc"
		taskID      = "task-task-hydration-explicit-doc"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Task Hydration Explicit Doc",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      "agent.topology.additional_agents.v1",
		Title:       "Additional Agents",
		Content:     "Historical topology memo",
		UpdatedBy:   "observer",
	}); err != nil {
		t.Fatalf("upsert topology doc: %v", err)
	}

	bundle, err := store.GetTaskHydrationBundle(ctx, sqlite.TaskHydrationFilter{
		TaskID:           taskID,
		WorkspaceID:      workspaceID,
		DocKeys:          []string{"agent.topology.additional_agents.v1"},
		IncludeAllDocs:   false,
		UpdatesLimit:     1,
		ArtifactLimit:    1,
		RelatedTaskLimit: 1,
	})
	if err != nil {
		t.Fatalf("GetTaskHydrationBundle(explicit topology doc): %v", err)
	}
	if len(bundle.Docs) != 1 || bundle.Docs[0].DocKey != "agent.topology.additional_agents.v1" {
		t.Fatalf("expected explicit topology doc request to bypass includeAll filtering, got %+v", bundle.Docs)
	}
}

func TestGetTaskHydrationBundleFiltersForeignProjectTaskUpdates(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-hydration-project-updates"
		agentID     = "agent-task-hydration-project-updates"
		currentTask = "task-current-project-hydration"
		foreignTask = "task-foreign-project-hydration"
		currentProj = "project-current-hydration"
		foreignProj = "project-foreign-hydration"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Task Hydration Project Updates",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Hydration Project Updates Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	for _, project := range []struct {
		id    string
		title string
	}{
		{currentProj, "Current Hydration Project"},
		{foreignProj, "Foreign Hydration Project"},
	} {
		if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
			WorkspaceID: workspaceID,
			ProjectID:   project.id,
			Title:       project.title,
			CreatedBy:   "developer",
		}); err != nil {
			t.Fatalf("create project %s: %v", project.id, err)
		}
	}
	for _, taskID := range []string{currentTask, foreignTask} {
		createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      taskID,
			LinkedBy:    "developer",
		}); err != nil {
			t.Fatalf("attach task %s: %v", taskID, err)
		}
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, projectID := range []string{currentProj, foreignProj} {
		if _, _, err := store.ClaimProjectStrategicLeadWithEvent(ctx, sqlite.ProjectLeadClaimInput{
			WorkspaceID:           workspaceID,
			ProjectID:             projectID,
			AgentID:               agentID,
			ActorID:               agentID,
			ActorType:             "agent",
			LeaseSeconds:          900,
			Summary:               "test project lead for hydration task field setup",
			PromptContextEnvelope: sqlite.BuildProjectPromptContextEnvelope("project.lead.claim", "server_rpc", workspaceID, "agent", agentID),
			PromptContextSurface:  "project.lead.claim",
		}); err != nil {
			t.Fatalf("claim project lead for %s: %v", projectID, err)
		}
	}
	if _, err := store.UpdateTaskProjectFields(ctx, sqlite.TaskProjectFieldsUpdateInput{
		WorkspaceID: workspaceID,
		TaskID:      currentTask,
		ActorID:     agentID,
		ProjectID:   stringPtrForHydrationScopeTest(currentProj),
		ProjectLane: stringPtrForHydrationScopeTest("strategy"),
	}); err != nil {
		t.Fatalf("set current project fields: %v", err)
	}
	if _, err := store.UpdateTaskProjectFields(ctx, sqlite.TaskProjectFieldsUpdateInput{
		WorkspaceID: workspaceID,
		TaskID:      foreignTask,
		ActorID:     agentID,
		ProjectID:   stringPtrForHydrationScopeTest(foreignProj),
		ProjectLane: stringPtrForHydrationScopeTest("implementation"),
	}); err != nil {
		t.Fatalf("set foreign project fields: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		UpdateType:  "coordination",
		Summary:     "clean update for " + currentTask,
		PayloadJSON: `{"task_ids":["` + currentTask + `"],"doc_key":"task.` + currentTask + `.plan"}`,
	}); err != nil {
		t.Fatalf("record clean update: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		UpdateType:  "coordination",
		Summary:     "contaminated update for " + currentTask + " and " + foreignTask,
		PayloadJSON: `{"task_ids":["` + currentTask + `","` + foreignTask + `"],"doc_key":"task.` + foreignTask + `.agent_response"}`,
	}); err != nil {
		t.Fatalf("record contaminated update: %v", err)
	}

	bundle, err := store.GetTaskHydrationBundle(ctx, sqlite.TaskHydrationFilter{
		TaskID:       currentTask,
		WorkspaceID:  workspaceID,
		UpdatesLimit: 10,
	})
	if err != nil {
		t.Fatalf("GetTaskHydrationBundle: %v", err)
	}
	if len(bundle.Updates) != 1 {
		t.Fatalf("expected only clean update, got %+v", bundle.Updates)
	}
	if got := bundle.Updates[0].Summary; got != "clean update for "+currentTask {
		t.Fatalf("unexpected update summary %q", got)
	}
}

func TestGetTaskHydrationBundleExposesSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-hydration-side-effects"
		agentID     = "agent-task-hydration-side-effects"
		taskID      = "task-hydration-side-effects"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Task Hydration Side Effects",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Hydration Side Effects Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		UpdateType:  "coordination",
		Summary:     "side effect pending classification for " + taskID,
		PayloadJSON: hydrationSideEffectPayloadForTest(taskID),
	}); err != nil {
		t.Fatalf("record side-effect update: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		UpdateType:  "coordination",
		Summary:     "legacy plain text update for " + taskID,
		PayloadJSON: "plain legacy payload for " + taskID,
	}); err != nil {
		t.Fatalf("record legacy update: %v", err)
	}

	bundle, err := store.GetTaskHydrationBundle(ctx, sqlite.TaskHydrationFilter{
		TaskID:       taskID,
		WorkspaceID:  workspaceID,
		UpdatesLimit: 10,
	})
	if err != nil {
		t.Fatalf("GetTaskHydrationBundle: %v", err)
	}
	if len(bundle.Updates) != 2 {
		t.Fatalf("expected two related updates, got %+v", bundle.Updates)
	}
	if len(bundle.SideEffects) != 1 {
		t.Fatalf("expected one hydrated side effect, got %+v", bundle.SideEffects)
	}
	effect := bundle.SideEffects[0]
	if effect.SideEffectRef != "side-effect:"+taskID || effect.IntegrationStatus != "pending_classification" {
		t.Fatalf("unexpected hydrated side effect %+v", effect)
	}
}

func TestGetTaskHydrationBundleSuppressesResolvedSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-hydration-resolved-side-effects"
		agentID     = "agent-task-hydration-resolved-side-effects"
		taskID      = "task-hydration-resolved-side-effects"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Resolved Side Effects",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Resolved Side Effects Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		UpdateType:  "blocked",
		Summary:     "side effect pending classification for " + taskID,
		PayloadJSON: hydrationSideEffectPayloadForTest(taskID),
	}); err != nil {
		t.Fatalf("record pending side-effect update: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		UpdateType:  "side_effect_resolution",
		Summary:     "side effect split_tension for side-effect:" + taskID + " on " + taskID,
		PayloadJSON: hydrationResolvedSideEffectPayloadForTest(taskID),
	}); err != nil {
		t.Fatalf("record resolved side-effect update: %v", err)
	}

	bundle, err := store.GetTaskHydrationBundle(ctx, sqlite.TaskHydrationFilter{
		TaskID:       taskID,
		WorkspaceID:  workspaceID,
		UpdatesLimit: 10,
	})
	if err != nil {
		t.Fatalf("GetTaskHydrationBundle: %v", err)
	}
	if len(bundle.Updates) != 2 {
		t.Fatalf("expected both related updates to remain visible, got %+v", bundle.Updates)
	}
	if len(bundle.SideEffects) != 0 {
		t.Fatalf("expected resolved side effect to suppress stale pending entry, got %+v", bundle.SideEffects)
	}
}

// TestGetTaskHydrationBundleSuppressesFastPathResolvedSideEffects is the TE-51
// hydration guard: after a deterministic fast-path resolution records a
// side_effect_resolution update (decision=reroute_to_active_lane,
// source_kind=core:side_effect_resolution, integration_status=verification_requested),
// task hydration must reflect the resolved state -- the stale pending side effect
// for the same ref must not reappear. The resolution update is recorded AFTER the
// pending update, and listTaskRelatedUpdates returns newest-first, so the
// resolution is seen first and suppresses the older pending entry.
func TestGetTaskHydrationBundleSuppressesFastPathResolvedSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-hydration-fastpath-resolved"
		agentID     = "agent-task-hydration-fastpath-resolved"
		taskID      = "task-hydration-fastpath-resolved"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Fast Path Resolved Side Effects",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Fast Path Resolved Side Effects Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		UpdateType:  "blocked",
		Summary:     "side effect pending classification for " + taskID,
		PayloadJSON: hydrationSideEffectPayloadForTest(taskID),
	}); err != nil {
		t.Fatalf("record pending side-effect update: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		UpdateType:  "side_effect_resolution",
		Summary:     "fast-path reroute_to_active_lane for side-effect:" + taskID + " on " + taskID,
		PayloadJSON: hydrationFastPathResolvedSideEffectPayloadForTest(taskID),
	}); err != nil {
		t.Fatalf("record fast-path resolution update: %v", err)
	}

	bundle, err := store.GetTaskHydrationBundle(ctx, sqlite.TaskHydrationFilter{
		TaskID:       taskID,
		WorkspaceID:  workspaceID,
		UpdatesLimit: 10,
	})
	if err != nil {
		t.Fatalf("GetTaskHydrationBundle: %v", err)
	}
	if len(bundle.Updates) != 2 {
		t.Fatalf("expected both updates visible, got %+v", bundle.Updates)
	}
	if len(bundle.SideEffects) != 0 {
		t.Fatalf("expected fast-path resolution to suppress stale pending side effect, got %+v", bundle.SideEffects)
	}
}

// TestGetTaskHydrationBundleKeepsPendingWhenFastPathResolutionCoversDifferentRef
// is the negative guard: a fast-path resolution that resolves a DIFFERENT ref must
// not suppress an unrelated still-pending side effect (no false suppression).
func TestGetTaskHydrationBundleKeepsPendingWhenFastPathResolutionCoversDifferentRef(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-task-hydration-fastpath-otherref"
		agentID     = "agent-task-hydration-fastpath-otherref"
		taskID      = "task-hydration-fastpath-otherref"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Fast Path Other Ref Side Effects",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Fast Path Other Ref Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	createSingleNodeTask(t, ctx, store, taskID, "node-"+taskID)
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	// Pending side effect keyed off the task ref.
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		UpdateType:  "blocked",
		Summary:     "side effect pending classification for " + taskID,
		PayloadJSON: hydrationSideEffectPayloadForTest(taskID),
	}); err != nil {
		t.Fatalf("record pending side-effect update: %v", err)
	}
	// Fast-path resolution that covers a DIFFERENT ref (still mentions taskID in
	// the payload so the LIKE join keeps it in scope) must not suppress the pending one.
	otherResolution := strings.Replace(
		hydrationFastPathResolvedSideEffectPayloadForTest(taskID),
		`"side_effect_ref":"side-effect:`+taskID+`"`,
		`"side_effect_ref":"side-effect:other-`+taskID+`"`,
		1,
	)
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		UpdateType:  "side_effect_resolution",
		Summary:     "fast-path reroute_to_active_lane for a different ref on " + taskID,
		PayloadJSON: otherResolution,
	}); err != nil {
		t.Fatalf("record other-ref resolution update: %v", err)
	}

	bundle, err := store.GetTaskHydrationBundle(ctx, sqlite.TaskHydrationFilter{
		TaskID:       taskID,
		WorkspaceID:  workspaceID,
		UpdatesLimit: 10,
	})
	if err != nil {
		t.Fatalf("GetTaskHydrationBundle: %v", err)
	}
	if len(bundle.SideEffects) != 1 {
		t.Fatalf("expected the still-pending side effect to survive an unrelated resolution, got %+v", bundle.SideEffects)
	}
	if bundle.SideEffects[0].SideEffectRef != "side-effect:"+taskID {
		t.Fatalf("unexpected surviving side effect %+v", bundle.SideEffects[0])
	}
}

func TestRecordAgentUpdateRejectsMalformedSideEffects(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-agent-update-malformed-side-effects"
		agentID     = "agent-update-malformed-side-effects"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Malformed Side Effects",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Malformed Side Effects Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		UpdateType:  "coordination",
		Summary:     "malformed side effects should reject",
		PayloadJSON: `{"status":"blocked","side_effects":[{"actor":"agent-alpha","operation":"change"}]}`,
	})
	if err == nil {
		t.Fatal("expected malformed side-effect update to be rejected")
	}
	if !strings.Contains(err.Error(), "side_effects.schema must be artifact_bound_side_effect.v1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func hydrationSideEffectPayloadForTest(taskID string) string {
	return `{"task_ids":["` + taskID + `"],"side_effects":[{"schema":"artifact_bound_side_effect.v1","side_effect_ref":"side-effect:` + taskID + `","actor":"agent-alpha","lane_ref":"lane:` + taskID + `","tension_ref":"` + taskID + `","artifact_ref":"artifact:repo","region_ref":"region:editor","operation":"change","source_kind":"adapter:git","boundary_ref":"boundary:editor","boundary_relation":"out_of_boundary","materialization_state":"present_unintegrated","integration_intent":"foundation_effect","integration_status":"pending_classification","decision":"none_pending","justification":"classification required","derived_region_refs":["region:editor"]}]}`
}

func hydrationResolvedSideEffectPayloadForTest(taskID string) string {
	return `{"task_ids":["` + taskID + `"],"side_effects":[{"schema":"artifact_bound_side_effect.v1","side_effect_ref":"side-effect:` + taskID + `","actor":"agent-alpha","lane_ref":"lane:` + taskID + `","tension_ref":"` + taskID + `","artifact_ref":"artifact:repo","region_ref":"region:editor","operation":"classify","source_kind":"core:side_effect_resolution","boundary_ref":"boundary:editor","boundary_relation":"adjacent_foundation_candidate","materialization_state":"present_unintegrated","integration_intent":"repair","integration_status":"split_requested","decision":"split_tension","justification":"split into foundation lane","derived_region_refs":["region:editor"]}]}`
}

// hydrationFastPathResolvedSideEffectPayloadForTest mirrors the side_effect_resolution
// update payload that the agent deterministic fast path emits for the in-scope
// reroute_to_active_lane decision (operation=classify, source_kind=core:side_effect_resolution,
// boundary_relation=ambiguous, integration_intent=repair, integration_status=verification_requested).
func hydrationFastPathResolvedSideEffectPayloadForTest(taskID string) string {
	return `{"task_ids":["` + taskID + `"],"side_effects":[{"schema":"artifact_bound_side_effect.v1","side_effect_ref":"side-effect:` + taskID + `","actor":"agent-alpha","lane_ref":"lane:` + taskID + `","tension_ref":"` + taskID + `","artifact_ref":"artifact:repo","region_ref":"region:editor","operation":"classify","source_kind":"core:side_effect_resolution","boundary_ref":"boundary:editor","boundary_relation":"ambiguous","materialization_state":"present_unintegrated","integration_intent":"repair","integration_status":"verification_requested","decision":"reroute_to_active_lane","justification":"deterministic in-scope reroute","derived_region_refs":["region:editor"]}]}`
}

func stringPtrForHydrationScopeTest(value string) *string {
	return &value
}
