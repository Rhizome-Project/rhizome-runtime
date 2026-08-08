package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/bridgepolicy"
	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceClaimConflict(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-conflict",
		Title:       "Conflict Test",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-conflict")

	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: "ws-conflict",
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-1", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      "task-claim-conflict",
		OwnerUserID: "developer",
		Priority:    "normal",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-conflict",
		TaskID:      "task-claim-conflict",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}

	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-conflict",
		TaskID:      "task-claim-conflict",
		AgentID:     "agent-a",
		Summary:     "working on it",
	}); err != nil {
		t.Fatalf("claim task by agent-a: %v", err)
	}

	err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-conflict",
		TaskID:      "task-claim-conflict",
		AgentID:     "agent-b",
		Summary:     "trying to steal claim",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimConflict) {
		t.Fatalf("expected ErrTaskClaimConflict, got %v", err)
	}
}

func TestWorkspaceClaimAllowsRoleRoutedIntegrationDespiteTaskOwner(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const (
		workspaceID  = "ws-agent-owner-claim-gate"
		taskID       = "task-epsilon-owned-integration-continuation"
		revisionID   = "task-zeta-owned-revision-continuation"
		toolOnlyID   = "task-zeta-owned-tool-only-integration-continuation"
		humanTaskID  = "task-human-owned-ordinary"
		ownerID      = "epsilon"
		integratorID = "zeta"
		otherID      = "alpha"
		leadID       = "beta"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Agent Owner Claim Gate",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, agentID := range []string{ownerID, integratorID, otherID, leadID} {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	createProjectForGitTest(t, ctx, store, workspaceID, "project-owner-claim-gate", leadID)
	claimProjectLeadForGitTest(t, ctx, store, workspaceID, "project-owner-claim-gate", leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, "project-owner-claim-gate", ownerID, sqlite.ProjectRoleIntegrator, leadID)
	assignAgentWorkProjectRole(t, ctx, store, workspaceID, "project-owner-claim-gate", integratorID, sqlite.ProjectRoleIntegrator, leadID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, spec := range []struct {
		taskID       string
		owner        string
		projectID    string
		lane         string
		requirements string
	}{
		{
			taskID:    taskID,
			owner:     ownerID,
			projectID: "project-owner-claim-gate",
			lane:      "integration",
			requirements: `{
				"schema":"task_requirements.v1",
				"patch_queue_task_kind":"integration",
				"required_project_role":"INTEGRATOR",
				"required_tool":"project_patch_queue_integrate"
			}`,
		},
		{
			taskID:    revisionID,
			owner:     integratorID,
			projectID: "project-owner-claim-gate",
			lane:      "implementation",
			requirements: `{
				"schema":"task_requirements.v1",
				"patch_queue_task_kind":"revision",
				"required_tool":"project_patch_queue_revise"
			}`,
		},
		{
			taskID:    toolOnlyID,
			owner:     integratorID,
			projectID: "project-owner-claim-gate",
			lane:      "integration",
			requirements: `{
				"schema":"task_requirements.v1",
				"patch_queue_task_kind":"integration",
				"required_tool":"project_patch_queue_integrate"
			}`,
		},
		{taskID: humanTaskID, owner: "developer"},
	} {
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			WorkspaceID:          workspaceID,
			TaskID:               spec.taskID,
			OwnerUserID:          spec.owner,
			Priority:             "high",
			Title:                spec.taskID,
			Description:          "Claim admission must distinguish owner-routed continuations from role-routed integration.",
			TaskKind:             "COORDINATION",
			TaskTemplate:         "integration",
			ProjectID:            spec.projectID,
			ProjectLane:          spec.lane,
			TaskRequirementsJSON: spec.requirements,
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", spec.taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: workspaceID,
			TaskID:      spec.taskID,
			LinkedBy:    "developer",
		}); err != nil {
			t.Fatalf("attach task %s: %v", spec.taskID, err)
		}
	}

	err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     otherID,
		Summary:     "non-integrator direct claim should be rejected",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "INTEGRATOR") {
		t.Fatalf("expected non-integrator claim to fail on role gate, got %v", err)
	}
	err = store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      revisionID,
		AgentID:     otherID,
		Summary:     "non-owner revision continuation claim should still be rejected",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "assigned to agent zeta") {
		t.Fatalf("expected non-integration continuation to remain owner-routed, got %v", err)
	}
	err = store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      toolOnlyID,
		AgentID:     otherID,
		Summary:     "tool-only integration continuation must not bypass owner route",
	})
	if !errors.Is(err, sqlite.ErrTaskClaimAdmissionInvalid) || !strings.Contains(err.Error(), "assigned to agent zeta") {
		t.Fatalf("expected tool-only integration continuation to remain owner-routed, got %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     integratorID,
		Summary:     "active integrator can claim role-routed integration task",
	}); err != nil {
		t.Fatalf("active integrator claim should succeed despite owner_user_id %s: %v", ownerID, err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      humanTaskID,
		AgentID:     otherID,
		Summary:     "human-owned task remains claimable by agents",
	}); err != nil {
		t.Fatalf("human-owned task should remain claimable by agent: %v", err)
	}
}

func TestWorkspaceSnapshotIncludesDocsTasksAndUpdates(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-snapshot",
		Title:       "Snapshot Test",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, "ws-snapshot")
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: "ws-snapshot",
		DocKey:      "current_context",
		Title:       "Current Context",
		Content:     "We are coordinating agents.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID:  "ws-snapshot",
		AgentID:      "agent-snapshot",
		OwnerUserID:  "developer",
		DisplayName:  "Snapshot Agent",
		Capabilities: []string{"coordination", "docs"},
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID:   "ws-snapshot",
		AgentID:       "agent-snapshot",
		UpdateType:    "blocker",
		Summary:       "Need human auth",
		RequiresHuman: true,
		PayloadJSON:   `{"reason":"auth"}`,
	}); err != nil {
		t.Fatalf("record update: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-1", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      "task-snapshot",
		OwnerUserID: "developer",
		Priority:    "high",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: "ws-snapshot",
		TaskID:      "task-snapshot",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID:  "ws-snapshot",
		ToolID:       "tg-bot",
		DisplayName:  "Telegram Bot",
		Description:  "Human-facing channel",
		OwnerUserID:  "developer",
		OwnerAgentID: "agent-snapshot",
		Kind:         "BOT",
		Status:       "PLANNED",
		AccessLevel:  "HUMAN_GATED",
		Capabilities: []string{"notify", "human-ping"},
	}); err != nil {
		t.Fatalf("register workspace tool: %v", err)
	}
	updates, err := store.ListWorkspaceArtifacts(ctx, sqlite.WorkspaceArtifactFilter{
		WorkspaceID: "ws-snapshot",
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list workspace artifacts before create: %v", err)
	}
	if len(updates) != 0 {
		t.Fatalf("expected no artifacts before create, got %+v", updates)
	}
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		WorkspaceID: "ws-snapshot",
		TaskID:      "task-snapshot",
		UpdateID:    snapshotUpdateID(t, store, ctx, "ws-snapshot"),
		Title:       "Telegram Bot Spec",
		ArtifactRef: "docs/telegram-bot.md",
		Kind:        "document",
		ContentType: "text/markdown",
		CreatedBy:   "agent-snapshot",
	}); err != nil {
		t.Fatalf("create workspace artifact: %v", err)
	}

	snapshot, err := store.GetWorkspaceSnapshot(ctx, "ws-snapshot", 10)
	if err != nil {
		t.Fatalf("get workspace snapshot: %v", err)
	}
	if snapshot.Workspace.WorkspaceID != "ws-snapshot" {
		t.Fatalf("expected ws-snapshot, got %q", snapshot.Workspace.WorkspaceID)
	}
	if len(snapshot.Docs) != 1 || snapshot.Docs[0].DocKey != "current_context" {
		t.Fatalf("expected current_context doc, got %+v", snapshot.Docs)
	}
	if len(snapshot.Agents) != 1 || snapshot.Agents[0].AgentID != "agent-snapshot" {
		t.Fatalf("expected agent-snapshot, got %+v", snapshot.Agents)
	}
	if len(snapshot.Tools) != 1 || snapshot.Tools[0].ToolID != "tg-bot" {
		t.Fatalf("expected tg-bot in snapshot tools, got %+v", snapshot.Tools)
	}
	if len(snapshot.Tasks) != 1 || snapshot.Tasks[0].TaskID != "task-snapshot" {
		t.Fatalf("expected task-snapshot, got %+v", snapshot.Tasks)
	}
	if len(snapshot.RecentArtifacts) != 1 || snapshot.RecentArtifacts[0].Title != "Telegram Bot Spec" {
		t.Fatalf("expected recent artifact in snapshot, got %+v", snapshot.RecentArtifacts)
	}
	if len(snapshot.RecentUpdates) != 1 || !snapshot.RecentUpdates[0].RequiresHuman {
		t.Fatalf("expected requires-human update, got %+v", snapshot.RecentUpdates)
	}
}

func TestRegisterWorkspaceToolRejectsRemovedLegacyProvider(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-removed-tool",
		Title:       "Removed Tool",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-removed-tool")

	err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID: "ws-removed-tool",
		ToolID:      sqlite.RemovedLegacyProviderToolID,
		DisplayName: "Removed Provider",
		OwnerUserID: "developer",
		Kind:        "BRIDGE",
		Status:      "ACTIVE",
		AccessLevel: "WORKSPACE",
	})
	if err == nil {
		t.Fatal("expected removed legacy provider registration to fail")
	}
	if !strings.Contains(err.Error(), "has been removed from Rhizome") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWorkspaceToolRemoveIsIdempotentAndAudited(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-tools",
		Title:       "Tool Removal Test",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-tools")
	if err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID: "ws-tools",
		ToolID:      "tg-bot",
		DisplayName: "Telegram Bot",
		Description: "Human-facing channel",
		OwnerUserID: "developer",
		Kind:        "BOT",
		Status:      "ACTIVE",
		AccessLevel: "WORKSPACE",
	}); err != nil {
		t.Fatalf("register workspace tool: %v", err)
	}

	existed, err := store.RemoveWorkspaceTool(ctx, sqlite.WorkspaceToolRemoveInput{
		WorkspaceID: "ws-tools",
		ToolID:      "tg-bot",
		RemovedBy:   "developer",
	})
	if err != nil {
		t.Fatalf("remove workspace tool: %v", err)
	}
	if !existed {
		t.Fatalf("expected first remove to report existed=true")
	}
	if _, err := store.GetWorkspaceTool(ctx, "ws-tools", "tg-bot"); !errors.Is(err, sqlite.ErrToolNotFound) {
		t.Fatalf("expected tool to be gone, got %v", err)
	}

	events, err := store.ListAuditEvents(ctx, sqlite.AuditEventFilter{
		EventType: "workspace_tool_removed",
		EntityID:  "ws-tools/tg-bot",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one removal audit event, got %+v", events)
	}

	var payload struct {
		RemovedBy string `json:"removed_by"`
		Tool      struct {
			ToolID      string `json:"tool_id"`
			DisplayName string `json:"display_name"`
			Status      string `json:"status"`
		} `json:"tool"`
	}
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode audit payload: %v", err)
	}
	if payload.RemovedBy != "developer" {
		t.Fatalf("expected removed_by developer, got %+v", payload)
	}
	if payload.Tool.ToolID != "tg-bot" || payload.Tool.DisplayName != "Telegram Bot" || payload.Tool.Status != "ACTIVE" {
		t.Fatalf("expected tool snapshot in audit payload, got %+v", payload.Tool)
	}

	existed, err = store.RemoveWorkspaceTool(ctx, sqlite.WorkspaceToolRemoveInput{
		WorkspaceID: "ws-tools",
		ToolID:      "tg-bot",
		RemovedBy:   "developer",
	})
	if err != nil {
		t.Fatalf("second remove workspace tool: %v", err)
	}
	if existed {
		t.Fatalf("expected second remove to report existed=false")
	}

	events, err = store.ListAuditEvents(ctx, sqlite.AuditEventFilter{
		EventType: "workspace_tool_removed",
		EntityID:  "ws-tools/tg-bot",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list audit events after second remove: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected idempotent remove to avoid duplicate audit events, got %+v", events)
	}
}

func TestWorkspaceToolPolicyEnvelopeSurfacedFromManifest(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-tool-policy",
		Title:       "Tool Policy",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-tool-policy")

	manifest, err := json.Marshal(map[string]any{
		"provider": "external-provider",
		"policy_envelope": bridgepolicy.BuildEnvelope(
			"external-provider",
			bridgepolicy.TierCodeExec,
			bridgepolicy.PostureLegacyUnsupported,
			[]bridgepolicy.SurfacePolicy{
				bridgepolicy.NewLegacyPolicy("external-provider/local-provider", bridgepolicy.TierCodeExec, "local CLI"),
				bridgepolicy.NewLegacyPolicy("external-provider/signed-session-shortcut", bridgepolicy.TierHighRisk, "signed-session shortcut"),
			},
			"legacy bridge",
		),
	})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}

	if err := store.RegisterWorkspaceTool(ctx, sqlite.WorkspaceToolInput{
		WorkspaceID:  "ws-tool-policy",
		ToolID:       "dangerous-provider",
		DisplayName:  "Dangerous Provider",
		Description:  "Legacy bridge",
		OwnerUserID:  "developer",
		Kind:         "BRIDGE",
		Status:       "ACTIVE",
		AccessLevel:  "AGENT_ONLY",
		Capabilities: []string{"bridge.high_risk", "bridge.policy_enveloped"},
		ManifestJSON: string(manifest),
	}); err != nil {
		t.Fatalf("register workspace tool: %v", err)
	}

	record, err := store.GetWorkspaceTool(ctx, "ws-tool-policy", "dangerous-provider")
	if err != nil {
		t.Fatalf("get workspace tool: %v", err)
	}
	if record.PolicyEnvelope == nil || !record.PolicyEnvelope.HighRisk || !record.PolicyEnvelope.OperatorControlRequired {
		t.Fatalf("expected parsed high-risk policy envelope, got %+v", record.PolicyEnvelope)
	}

	items, err := store.ListWorkspaceTools(ctx, sqlite.WorkspaceToolFilter{WorkspaceID: "ws-tool-policy"})
	if err != nil {
		t.Fatalf("list workspace tools: %v", err)
	}
	if len(items) != 1 || items[0].PolicyEnvelope == nil || items[0].PolicyEnvelope.Surface != "external-provider" {
		t.Fatalf("expected list workspace tools to surface policy envelope, got %+v", items)
	}
}

func TestCoordinationTaskExcludedFromExecutableNodesAndClosable(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-coordination-task-closable"

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Coordination Task Closable",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimExternalWorkspaceAuthority(t, ctx, store, workspaceID)

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:       "task-coordination",
		OwnerUserID:  "developer",
		Priority:     "normal",
		Title:        "Backlog item",
		TaskKind:     "COORDINATION",
		TaskTemplate: "research",
	}, graph); err != nil {
		t.Fatalf("create coordination task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      "task-coordination",
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach coordination task: %v", err)
	}

	executable, err := store.ListExecutableNodes(ctx, 10)
	if err != nil {
		t.Fatalf("list executable nodes: %v", err)
	}
	if len(executable) != 0 {
		t.Fatalf("expected coordination task to be excluded from executable nodes, got %+v", executable)
	}

	if err := store.CloseTask(ctx, sqlite.TaskCloseInput{
		WorkspaceID: workspaceID,
		TaskID:      "task-coordination",
		ActorID:     "agent-snapshot",
		Resolution:  "RESOLVED",
		Reason:      "planning complete",
	}); err != nil {
		t.Fatalf("close coordination task: %v", err)
	}

	status, err := store.GetTaskStatus(ctx, workspaceID, "task-coordination")
	if err != nil {
		t.Fatalf("get coordination task status: %v", err)
	}
	if status.Status != "RESOLVED" {
		t.Fatalf("expected coordination task resolved, got %s", status.Status)
	}
	if status.TaskKind != "COORDINATION" {
		t.Fatalf("expected coordination task kind, got %s", status.TaskKind)
	}
	if len(status.Nodes) != 1 || status.Nodes[0].Status != "RESOLVED" {
		t.Fatalf("expected coordination node resolved, got %+v", status.Nodes)
	}
}

func TestWorkspaceDocRevisionsSearchAndTaskLinks(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-links",
		Title:       "Links Test",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, "ws-links")
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: "ws-links",
		DocKey:      "tooling",
		Title:       "Tooling",
		Content:     "Telegram tool registry draft v1",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: "ws-links",
		DocKey:      "tooling",
		Title:       "Tooling",
		Content:     "Telegram tool registry draft v2",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, taskID := range []string{"task-a", "task-b"} {
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			TaskID:       taskID,
			OwnerUserID:  "developer",
			Priority:     "normal",
			Title:        taskID,
			TaskKind:     "COORDINATION",
			TaskTemplate: "integration",
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: "ws-links",
			TaskID:      taskID,
			LinkedBy:    "developer",
		}); err != nil {
			t.Fatalf("attach task %s: %v", taskID, err)
		}
	}
	if err := store.AddWorkspaceTaskLink(ctx, sqlite.WorkspaceTaskLinkInput{
		WorkspaceID: "ws-links",
		FromTaskID:  "task-a",
		ToTaskID:    "task-b",
		LinkType:    "BLOCKS",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("add workspace task link: %v", err)
	}

	revisions, err := store.ListWorkspaceDocRevisions(ctx, "ws-links", "tooling", 10)
	if err != nil {
		t.Fatalf("list doc revisions: %v", err)
	}
	if len(revisions) != 2 {
		t.Fatalf("expected 2 revisions, got %+v", revisions)
	}

	links, err := store.ListWorkspaceTaskLinks(ctx, sqlite.WorkspaceTaskLinkFilter{
		WorkspaceID: "ws-links",
		TaskID:      "task-a",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list workspace task links: %v", err)
	}
	if len(links) != 1 || links[0].LinkType != "BLOCKS" {
		t.Fatalf("expected BLOCKS task link, got %+v", links)
	}

	results, err := store.SearchWorkspace(ctx, sqlite.WorkspaceSearchFilter{
		WorkspaceID: "ws-links",
		Query:       "Telegram",
		EntityType:  "doc",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("search workspace: %v", err)
	}
	if len(results) != 1 || results[0].EntityType != "doc" || results[0].EntityID != "tooling" {
		t.Fatalf("expected tooling doc search result, got %+v", results)
	}

	snapshot, err := store.GetWorkspaceSnapshot(ctx, "ws-links", 10)
	if err != nil {
		t.Fatalf("get workspace snapshot: %v", err)
	}
	if len(snapshot.TaskLinks) != 1 || snapshot.TaskLinks[0].FromTaskID != "task-a" {
		t.Fatalf("expected task link in snapshot, got %+v", snapshot.TaskLinks)
	}
}

func TestTaskHydrationBundle(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-hydration",
		Title:       "Hydration Test",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, "ws-hydration")
	claimExternalWorkspaceAuthority(t, ctx, store, "ws-hydration")
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: "ws-hydration",
		AgentID:     "agent-hydration",
		OwnerUserID: "developer",
		DisplayName: "Hydration Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: "ws-hydration",
		DocKey:      "current_context",
		Title:       "Current Context",
		Content:     "Context hydration should gather task, docs, artifacts, and updates.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: "ws-hydration",
		DocKey:      "decisions",
		Title:       "Decisions",
		Content:     "Hydration should include only explicitly requested docs when doc keys are provided.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace decisions doc: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: "ws-hydration",
		DocKey:      "task.hydrate-related.evidence_gap",
		Title:       "Related Evidence Gap",
		Content:     "Related validation evidence should follow the task link into hydration.",
		UpdatedBy:   "agent-hydration",
	}); err != nil {
		t.Fatalf("upsert related evidence gap doc: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: "ws-hydration",
		DocKey:      "task.hydrate-related.result",
		Title:       "Related Result",
		Content:     "Canonical related task result should outrank transient coordination acknowledgements.",
		UpdatedBy:   "agent-hydration",
	}); err != nil {
		t.Fatalf("upsert related result doc: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "coordination", Type: "coordination_manual"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	for _, taskID := range []string{"hydrate-task", "hydrate-related"} {
		projectLane := "implementation"
		if taskID == "hydrate-related" {
			projectLane = "validation"
		}
		if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
			TaskID:       taskID,
			OwnerUserID:  "developer",
			Priority:     "normal",
			Title:        taskID,
			TaskKind:     "COORDINATION",
			TaskTemplate: "integration",
			ProjectLane:  projectLane,
		}, graph); err != nil {
			t.Fatalf("create task %s: %v", taskID, err)
		}
		if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
			WorkspaceID: "ws-hydration",
			TaskID:      taskID,
			LinkedBy:    "developer",
		}); err != nil {
			t.Fatalf("attach task %s: %v", taskID, err)
		}
	}
	if err := store.AddWorkspaceTaskLink(ctx, sqlite.WorkspaceTaskLinkInput{
		WorkspaceID: "ws-hydration",
		FromTaskID:  "hydrate-task",
		ToTaskID:    "hydrate-related",
		LinkType:    "BLOCKS",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("add workspace task link: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: "ws-hydration",
		TaskID:      "hydrate-task",
		AgentID:     "agent-hydration",
		Summary:     "hydrating context",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if err := store.RecordAgentUpdate(ctx, sqlite.AgentUpdateInput{
		WorkspaceID: "ws-hydration",
		AgentID:     "agent-hydration",
		UpdateType:  "progress",
		Summary:     "hydrate-task ready",
		PayloadJSON: `{"task_ids":["hydrate-task"],"status":"ready"}`,
	}); err != nil {
		t.Fatalf("record update: %v", err)
	}
	updateID := snapshotUpdateID(t, store, ctx, "ws-hydration")
	if err := store.CreateWorkspaceArtifact(ctx, sqlite.WorkspaceArtifactInput{
		WorkspaceID: "ws-hydration",
		TaskID:      "hydrate-task",
		UpdateID:    updateID,
		Title:       "Hydration Prompt",
		ArtifactRef: "artifacts/hydration-prompt.md",
		Kind:        "document",
		ContentType: "text/markdown",
		CreatedBy:   "agent-hydration",
	}); err != nil {
		t.Fatalf("create workspace artifact: %v", err)
	}

	bundle, err := store.GetTaskHydrationBundle(ctx, sqlite.TaskHydrationFilter{
		TaskID:         "hydrate-task",
		WorkspaceID:    "ws-hydration",
		DocKeys:        []string{"current_context"},
		IncludeAllDocs: false,
	})
	if err != nil {
		t.Fatalf("get task hydration bundle: %v", err)
	}
	if bundle.Workspace == nil || bundle.Workspace.WorkspaceID != "ws-hydration" {
		t.Fatalf("expected workspace in bundle, got %+v", bundle.Workspace)
	}
	if bundle.WorkspaceTask == nil || bundle.WorkspaceTask.ClaimAgentID == nil || *bundle.WorkspaceTask.ClaimAgentID != "agent-hydration" {
		t.Fatalf("expected claimed workspace task in bundle, got %+v", bundle.WorkspaceTask)
	}
	if len(bundle.Docs) != 3 || !hydrationBundleHasDoc(bundle, "current_context") || !hydrationBundleHasDoc(bundle, "task.hydrate-related.result") || !hydrationBundleHasDoc(bundle, "task.hydrate-related.evidence_gap") {
		t.Fatalf("expected selected doc in bundle, got %+v", bundle.Docs)
	}
	if len(bundle.TaskLinks) != 1 || bundle.TaskLinks[0].LinkType != "BLOCKS" {
		t.Fatalf("expected task link in bundle, got %+v", bundle.TaskLinks)
	}
	if len(bundle.RelatedTasks) != 1 || bundle.RelatedTasks[0].TaskID != "hydrate-related" {
		t.Fatalf("expected related task in bundle, got %+v", bundle.RelatedTasks)
	}
	if len(bundle.Artifacts) != 1 || bundle.Artifacts[0].Title != "Hydration Prompt" {
		t.Fatalf("expected artifact in bundle, got %+v", bundle.Artifacts)
	}
	if len(bundle.Updates) != 1 || bundle.Updates[0].Summary != "hydrate-task ready" {
		t.Fatalf("expected update in bundle, got %+v", bundle.Updates)
	}
}

func snapshotUpdateID(t *testing.T, store *sqlite.Store, ctx context.Context, workspaceID string) string {
	t.Helper()

	snapshot, err := store.GetWorkspaceSnapshot(ctx, workspaceID, 5)
	if err != nil {
		t.Fatalf("get snapshot for update id: %v", err)
	}
	if len(snapshot.RecentUpdates) == 0 {
		t.Fatalf("expected at least one recent update in snapshot")
	}
	return snapshot.RecentUpdates[0].UpdateID
}

func hydrationBundleHasDoc(bundle sqlite.TaskHydrationBundle, docKey string) bool {
	for _, doc := range bundle.Docs {
		if doc.DocKey == docKey {
			return true
		}
	}
	return false
}
