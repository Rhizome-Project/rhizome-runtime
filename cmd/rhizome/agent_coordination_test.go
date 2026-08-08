package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestToolRegistryAndBootstrapSnapshot(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-tools",
		"--title", "Tools Workspace",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, "ws-tools")
	if err := runAgentRegister([]string{
		"--workspace-id", "ws-tools",
		"--agent-id", "agent-tools",
		"--owner-user-id", "developer",
		"--display-name", "Tools Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}
	if err := runToolRegister([]string{
		"--workspace-id", "ws-tools",
		"--tool-id", "telegram-bot",
		"--display-name", "Telegram Bot",
		"--owner-user-id", "developer",
		"--owner-agent-id", "agent-tools",
		"--description", "Human-facing notification channel",
		"--kind", "bot",
		"--status", "planned",
		"--access-level", "human_gated",
		"--capabilities", "notify,human-ping",
	}); err != nil {
		t.Fatalf("runToolRegister failed: %v", err)
	}
	if err := runTaskSubmit([]string{
		"--task-id", "tooling-artifact-task",
		"--owner-user-id", "developer",
		"--workspace-id", "ws-tools",
		"--kind", "coordination",
		"--template", "tooling",
		"--title", "Telegram spec",
	}); err != nil {
		t.Fatalf("runTaskSubmit failed: %v", err)
	}
	if err := runAgentUpdatePost([]string{
		"--workspace-id", "ws-tools",
		"--agent-id", "agent-tools",
		"--type", "progress",
		"--summary", "Drafted bot surface",
		"--payload-schema", "v1",
		"--payload-status", "in_progress",
		"--task-ids", "tooling-artifact-task",
		"--notes", "Attaching design artifact",
	}); err != nil {
		t.Fatalf("runAgentUpdatePost failed: %v", err)
	}
	statusOut, err := captureStdout(t, func() error {
		return runWorkspaceStatus([]string{
			"--workspace-id", "ws-tools",
			"--updates-limit", "5",
		})
	})
	if err != nil {
		t.Fatalf("runWorkspaceStatus failed: %v", err)
	}
	var statusPayload struct {
		Snapshot struct {
			RecentUpdates []struct {
				UpdateID string `json:"update_id"`
			} `json:"recent_updates"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal([]byte(statusOut), &statusPayload); err != nil {
		t.Fatalf("decode workspace status: %v; output=%q", err, statusOut)
	}
	if len(statusPayload.Snapshot.RecentUpdates) == 0 {
		t.Fatalf("expected recent update for artifact attachment")
	}
	if err := runWorkspaceArtifactPut([]string{
		"--workspace-id", "ws-tools",
		"--task-id", "tooling-artifact-task",
		"--update-id", statusPayload.Snapshot.RecentUpdates[0].UpdateID,
		"--title", "Telegram Bot Surface",
		"--ref", "docs/telegram-bot-interface.md",
		"--kind", "document",
		"--content-type", "text/markdown",
		"--created-by", "agent-tools",
	}); err != nil {
		t.Fatalf("runWorkspaceArtifactPut failed: %v", err)
	}

	out, err := captureStdout(t, func() error {
		return runAgentBootstrap([]string{
			"--workspace-id", "ws-tools",
			"--agent-id", "agent-tools",
		})
	})
	if err != nil {
		t.Fatalf("runAgentBootstrap failed: %v", err)
	}

	var payload struct {
		Protocols []struct {
			Name string `json:"name"`
		} `json:"protocols"`
		Snapshot struct {
			Tools []struct {
				ToolID       string   `json:"tool_id"`
				Kind         string   `json:"kind"`
				Status       string   `json:"status"`
				AccessLevel  string   `json:"access_level"`
				Capabilities []string `json:"capabilities"`
			} `json:"tools"`
			RecentArtifacts []struct {
				Title       string `json:"title"`
				ArtifactRef string `json:"artifact_ref"`
			} `json:"recent_artifacts"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode bootstrap output: %v; output=%q", err, out)
	}
	if len(payload.Snapshot.Tools) != 1 || payload.Snapshot.Tools[0].ToolID != "telegram-bot" {
		t.Fatalf("expected telegram-bot in bootstrap tools, got %+v", payload.Snapshot.Tools)
	}
	if payload.Snapshot.Tools[0].Kind != "BOT" || payload.Snapshot.Tools[0].Status != "PLANNED" {
		t.Fatalf("unexpected tool metadata: %+v", payload.Snapshot.Tools[0])
	}
	if len(payload.Snapshot.RecentArtifacts) != 1 || payload.Snapshot.RecentArtifacts[0].ArtifactRef != "docs/telegram-bot-interface.md" {
		t.Fatalf("expected recent artifact in bootstrap, got %+v", payload.Snapshot.RecentArtifacts)
	}
	foundProtocol := false
	for _, protocol := range payload.Protocols {
		if protocol.Name == "tool.register" {
			foundProtocol = true
			break
		}
	}
	if !foundProtocol {
		t.Fatalf("expected tool.register protocol in bootstrap, got %+v", payload.Protocols)
	}
}

func TestAgentUpdatePayloadSchemaV1_FromFlags(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-updates",
		"--title", "Updates Workspace",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, "ws-updates")
	if err := runAgentRegister([]string{
		"--workspace-id", "ws-updates",
		"--agent-id", "agent-updates",
		"--owner-user-id", "developer",
		"--display-name", "Update Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}
	if err := runAgentUpdatePost([]string{
		"--workspace-id", "ws-updates",
		"--agent-id", "agent-updates",
		"--type", "blocker",
		"--summary", "Need bot token",
		"--payload-schema", "v1",
		"--payload-status", "blocked",
		"--task-ids", "design-telegram-bot-interface-v1",
		"--doc-keys", "open_questions,tooling",
		"--blocked-on", "auth:Need Telegram bot token",
		"--owner-user-id", "developer",
		"--human-reason", "auth",
		"--owner-action", "Provide Telegram bot token",
		"--notes", "Can continue protocol design while waiting",
		"--requires-human",
	}); err != nil {
		t.Fatalf("runAgentUpdatePost failed: %v", err)
	}

	out, err := captureStdout(t, func() error {
		return runWorkspaceStatus([]string{
			"--workspace-id", "ws-updates",
			"--updates-limit", "5",
		})
	})
	if err != nil {
		t.Fatalf("runWorkspaceStatus failed: %v", err)
	}

	var payload struct {
		Snapshot struct {
			RecentUpdates []struct {
				UpdateType    string `json:"update_type"`
				PayloadJSON   string `json:"payload_json"`
				RequiresHuman bool   `json:"requires_human"`
			} `json:"recent_updates"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode workspace status: %v; output=%q", err, out)
	}
	if len(payload.Snapshot.RecentUpdates) != 1 {
		t.Fatalf("expected one recent update, got %+v", payload.Snapshot.RecentUpdates)
	}
	update := payload.Snapshot.RecentUpdates[0]
	if update.UpdateType != "blocker" || !update.RequiresHuman {
		t.Fatalf("unexpected update record: %+v", update)
	}
	if !strings.Contains(update.PayloadJSON, "\"human_reason\":\"auth\"") || !strings.Contains(update.PayloadJSON, "\"task_ids\":[\"design-telegram-bot-interface-v1\"]") || !strings.Contains(update.PayloadJSON, "\"owner_user_id\":\"developer\"") {
		t.Fatalf("expected normalized payload json, got %s", update.PayloadJSON)
	}
}

func TestCoordinationTaskLifecycleViaCLI(t *testing.T) {
	setupFakeBridgeEnv(t)

	createCLITestWorkspace(t, "ws-coordination-lifecycle")
	if err := runTaskSubmit([]string{
		"--task-id", "coordination-backlog-item",
		"--owner-user-id", "developer",
		"--workspace-id", "ws-coordination-lifecycle",
		"--title", "Define tooling registry",
		"--description", "Draft first-class tool registry surface",
		"--kind", "coordination",
		"--template", "tooling",
	}); err != nil {
		t.Fatalf("runTaskSubmit failed: %v", err)
	}
	requireCLITaskRuntimeEvent(t, "ws-coordination-lifecycle", "coordination-backlog-item", "task.created", "task.submit")

	err := runTaskRun([]string{
		"--task-id", "coordination-backlog-item",
		"--wait",
		"--timeout-sec", "2",
	})
	if err == nil || !strings.Contains(err.Error(), "coordination-only") {
		t.Fatalf("expected coordination-only run error, got %v", err)
	}

	if err := runTaskClose([]string{
		"--task-id", "coordination-backlog-item",
		"--workspace-id", "ws-coordination-lifecycle",
		"--resolution", "resolved",
		"--reason", "Backlog item completed",
		"--actor-id", "agent-assistant-main",
	}); err != nil {
		t.Fatalf("runTaskClose failed: %v", err)
	}
	requireCLITaskRuntimeEvent(t, "ws-coordination-lifecycle", "coordination-backlog-item", "task.closed", "task.close")

	out, err := captureStdout(t, func() error {
		return runTaskStatus([]string{"--task-id", "coordination-backlog-item"})
	})
	if err != nil {
		t.Fatalf("runTaskStatus failed: %v", err)
	}

	var payload struct {
		Task struct {
			Status       string `json:"status"`
			TaskKind     string `json:"task_kind"`
			TaskTemplate string `json:"task_template"`
			Nodes        []struct {
				Status string `json:"status"`
			} `json:"nodes"`
		} `json:"task"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode task status: %v; output=%q", err, out)
	}
	if payload.Task.TaskKind != "COORDINATION" || payload.Task.TaskTemplate != "tooling" {
		t.Fatalf("unexpected coordination task metadata: %+v", payload.Task)
	}
	if payload.Task.Status != "RESOLVED" {
		t.Fatalf("expected resolved coordination task, got %s", payload.Task.Status)
	}
	if len(payload.Task.Nodes) != 1 || payload.Task.Nodes[0].Status != "RESOLVED" {
		t.Fatalf("expected resolved coordination node, got %+v", payload.Task.Nodes)
	}
}

func TestTaskSubmitProjectFieldsViaCLI(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-task-submit-cli-project-fields"
		projectID   = "project-cli-causal-board"
		taskID      = "task-cli-causal-board-root"
	)
	createCLITestWorkspace(t, workspaceID)

	store, err := openStore()
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateProject(ctx, sqlite.ProjectCreateInput{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		Title:       "CLI Causal Board",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create project: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	out, err := captureStdout(t, func() error {
		return runTaskSubmit([]string{
			"--task-id", taskID,
			"--owner-user-id", "developer",
			"--workspace-id", workspaceID,
			"--title", "Causal Board autonomous coordination smoke",
			"--description", "One root task; strategist creates plan and subtasks.",
			"--kind", "coordination",
			"--template", "integration",
			"--project-id", projectID,
			"--project-lane", "strategy",
			"--requires-project-gate",
		})
	})
	if err != nil {
		t.Fatalf("runTaskSubmit failed: %v", err)
	}
	var submitPayload struct {
		ProjectID           string `json:"project_id"`
		ProjectLane         string `json:"project_lane"`
		RequiresProjectGate bool   `json:"requires_project_gate"`
	}
	if err := json.Unmarshal([]byte(out), &submitPayload); err != nil {
		t.Fatalf("decode submit output: %v; output=%q", err, out)
	}
	if submitPayload.ProjectID != projectID || submitPayload.ProjectLane != "strategy" || !submitPayload.RequiresProjectGate {
		t.Fatalf("unexpected submit project fields: %+v", submitPayload)
	}

	statusOut, err := captureStdout(t, func() error {
		return runTaskStatus([]string{"--task-id", taskID})
	})
	if err != nil {
		t.Fatalf("runTaskStatus failed: %v", err)
	}
	var statusPayload struct {
		Task struct {
			ProjectID           string `json:"project_id"`
			ProjectLane         string `json:"project_lane"`
			RequiresProjectGate bool   `json:"requires_project_gate"`
		} `json:"task"`
	}
	if err := json.Unmarshal([]byte(statusOut), &statusPayload); err != nil {
		t.Fatalf("decode status output: %v; output=%q", err, statusOut)
	}
	if statusPayload.Task.ProjectID != projectID || statusPayload.Task.ProjectLane != "strategy" || !statusPayload.Task.RequiresProjectGate {
		t.Fatalf("task project fields were not persisted: %+v", statusPayload.Task)
	}
}

func TestTaskSubmitRequiresWorkspaceIDViaCLI(t *testing.T) {
	setupFakeBridgeEnv(t)

	err := runTaskSubmit([]string{
		"--task-id", "missing-workspace-task",
		"--owner-user-id", "developer",
	})
	if err == nil || !strings.Contains(err.Error(), "--workspace-id is required") {
		t.Fatalf("expected workspace-id requirement, got %v", err)
	}
}

func TestTaskSubmitRejectsUnknownWorkspaceWithoutOrphanViaCLI(t *testing.T) {
	setupFakeBridgeEnv(t)

	err := runTaskSubmit([]string{
		"--task-id", "unknown-workspace-task",
		"--owner-user-id", "developer",
		"--workspace-id", "ws-does-not-exist",
	})
	if err == nil || !strings.Contains(err.Error(), "validate workspace") {
		t.Fatalf("expected validate workspace error, got %v", err)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.ApplyMigrations(context.Background()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if status, err := store.GetTaskStatus(context.Background(), "", "unknown-workspace-task"); err == nil {
		t.Fatalf("expected no orphan task after rejected workspace, got %+v", status)
	}
}

func TestTaskCloseRequiresWorkspaceIDViaCLI(t *testing.T) {
	setupFakeBridgeEnv(t)

	err := runTaskClose([]string{
		"--task-id", "any-task",
		"--resolution", "resolved",
	})
	if err == nil || !strings.Contains(err.Error(), "--workspace-id is required") {
		t.Fatalf("expected workspace-id requirement, got %v", err)
	}
}

func TestTaskTemplateListAndValidation(t *testing.T) {
	setupFakeBridgeEnv(t)

	out, err := captureStdout(t, func() error {
		return runTaskTemplate([]string{"list"})
	})
	if err != nil {
		t.Fatalf("runTaskTemplate failed: %v", err)
	}

	var payload struct {
		Templates []struct {
			Name string `json:"name"`
		} `json:"templates"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode task template list: %v; output=%q", err, out)
	}
	if len(payload.Templates) == 0 {
		t.Fatalf("expected task templates in output")
	}

	foundTooling := false
	for _, template := range payload.Templates {
		if template.Name == "tooling" {
			foundTooling = true
			break
		}
	}
	if !foundTooling {
		t.Fatalf("expected tooling template in output, got %+v", payload.Templates)
	}

	createCLITestWorkspace(t, "ws-template-validation")
	err = runTaskSubmit([]string{
		"--task-id", "invalid-template-task",
		"--owner-user-id", "developer",
		"--workspace-id", "ws-template-validation",
		"--template", "made-up-template",
	})
	if err == nil || !strings.Contains(err.Error(), "invalid task template") {
		t.Fatalf("expected invalid task template error, got %v", err)
	}
}

func TestWorkspaceSearchDocHistoryAndTaskLinksViaCLI(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-search",
		"--title", "Search Workspace",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, "ws-search")
	if err := runAgentRegister([]string{
		"--workspace-id", "ws-search",
		"--agent-id", "agent-search",
		"--owner-user-id", "developer",
		"--display-name", "Search Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}
	for _, body := range []string{"Telegram bridge draft v1", "Telegram bridge draft v2 with reply routing"} {
		if err := runWorkspaceDocPut([]string{
			"--workspace-id", "ws-search",
			"--doc-key", "telegram_bridge",
			"--title", "Telegram Bridge",
			"--updated-by", "agent-search",
			"--content", body,
		}); err != nil {
			t.Fatalf("runWorkspaceDocPut failed: %v", err)
		}
	}
	for _, taskArgs := range [][]string{
		{
			"--task-id", "design-telegram-bridge",
			"--owner-user-id", "developer",
			"--workspace-id", "ws-search",
			"--kind", "coordination",
			"--template", "integration",
			"--title", "Design Telegram bridge",
		},
		{
			"--task-id", "register-telegram-bot",
			"--owner-user-id", "developer",
			"--workspace-id", "ws-search",
			"--kind", "coordination",
			"--template", "tooling",
			"--title", "Register Telegram bot tool",
		},
	} {
		if err := runTaskSubmit(taskArgs); err != nil {
			t.Fatalf("runTaskSubmit failed: %v", err)
		}
	}
	if err := runWorkspaceTaskLink([]string{
		"--workspace-id", "ws-search",
		"--from-task-id", "register-telegram-bot",
		"--to-task-id", "design-telegram-bridge",
		"--link-type", "blocks",
		"--created-by", "agent-search",
	}); err != nil {
		t.Fatalf("runWorkspaceTaskLink failed: %v", err)
	}

	historyOut, err := captureStdout(t, func() error {
		return runWorkspaceDocHistory([]string{
			"--workspace-id", "ws-search",
			"--doc-key", "telegram_bridge",
			"--limit", "5",
		})
	})
	if err != nil {
		t.Fatalf("runWorkspaceDocHistory failed: %v", err)
	}
	var historyPayload struct {
		Revisions []struct {
			Title   string `json:"title"`
			Content string `json:"content"`
		} `json:"revisions"`
	}
	if err := json.Unmarshal([]byte(historyOut), &historyPayload); err != nil {
		t.Fatalf("decode doc history: %v; output=%q", err, historyOut)
	}
	if len(historyPayload.Revisions) != 2 {
		t.Fatalf("expected 2 doc revisions, got %+v", historyPayload.Revisions)
	}
	if !strings.Contains(historyPayload.Revisions[0].Content, "reply routing") {
		t.Fatalf("expected latest revision content, got %+v", historyPayload.Revisions[0])
	}

	searchOut, err := captureStdout(t, func() error {
		return runWorkspaceSearch([]string{
			"--workspace-id", "ws-search",
			"--query", "Telegram",
			"--type", "task",
		})
	})
	if err != nil {
		t.Fatalf("runWorkspaceSearch failed: %v", err)
	}
	var searchPayload struct {
		Results []struct {
			EntityType string `json:"entity_type"`
			EntityID   string `json:"entity_id"`
			Title      string `json:"title"`
		} `json:"results"`
	}
	if err := json.Unmarshal([]byte(searchOut), &searchPayload); err != nil {
		t.Fatalf("decode workspace search: %v; output=%q", err, searchOut)
	}
	if len(searchPayload.Results) == 0 || searchPayload.Results[0].EntityType != "task" {
		t.Fatalf("expected task search results, got %+v", searchPayload.Results)
	}

	statusOut, err := captureStdout(t, func() error {
		return runWorkspaceStatus([]string{
			"--workspace-id", "ws-search",
			"--updates-limit", "5",
		})
	})
	if err != nil {
		t.Fatalf("runWorkspaceStatus failed: %v", err)
	}
	var statusPayload struct {
		Snapshot struct {
			TaskLinks []struct {
				FromTaskID string `json:"from_task_id"`
				ToTaskID   string `json:"to_task_id"`
				LinkType   string `json:"link_type"`
			} `json:"task_links"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal([]byte(statusOut), &statusPayload); err != nil {
		t.Fatalf("decode workspace status: %v; output=%q", err, statusOut)
	}
	if len(statusPayload.Snapshot.TaskLinks) != 1 {
		t.Fatalf("expected task link in snapshot, got %+v", statusPayload.Snapshot.TaskLinks)
	}
	if statusPayload.Snapshot.TaskLinks[0].LinkType != "BLOCKS" {
		t.Fatalf("expected BLOCKS link, got %+v", statusPayload.Snapshot.TaskLinks[0])
	}
}

func TestTaskHydrationBundleViaCLI(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-hydrate",
		"--title", "Hydrate Workspace",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, "ws-hydrate")
	if err := runAgentRegister([]string{
		"--workspace-id", "ws-hydrate",
		"--agent-id", "agent-hydrate",
		"--owner-user-id", "developer",
		"--display-name", "Hydrate Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}
	if err := runWorkspaceDocPut([]string{
		"--workspace-id", "ws-hydrate",
		"--doc-key", "current_context",
		"--title", "Current Context",
		"--updated-by", "agent-hydrate",
		"--content", "Need a dense task hydration payload for provider bridges.",
	}); err != nil {
		t.Fatalf("runWorkspaceDocPut failed: %v", err)
	}
	if err := runWorkspaceDocPut([]string{
		"--workspace-id", "ws-hydrate",
		"--doc-key", "decisions",
		"--title", "Decisions",
		"--updated-by", "agent-hydrate",
		"--content", "Do not leak irrelevant docs into provider hydration.",
	}); err != nil {
		t.Fatalf("runWorkspaceDocPut decisions failed: %v", err)
	}
	for _, taskArgs := range [][]string{
		{
			"--task-id", "provider-bridge-task",
			"--owner-user-id", "developer",
			"--workspace-id", "ws-hydrate",
			"--kind", "coordination",
			"--template", "integration",
			"--title", "Hydrate provider bridge request",
		},
		{
			"--task-id", "provider-bridge-prep",
			"--owner-user-id", "developer",
			"--workspace-id", "ws-hydrate",
			"--kind", "coordination",
			"--template", "research",
			"--title", "Prepare provider context",
		},
	} {
		if err := runTaskSubmit(taskArgs); err != nil {
			t.Fatalf("runTaskSubmit failed: %v", err)
		}
	}
	if err := runWorkspaceTaskLink([]string{
		"--workspace-id", "ws-hydrate",
		"--from-task-id", "provider-bridge-task",
		"--to-task-id", "provider-bridge-prep",
		"--link-type", "relates_to",
		"--created-by", "agent-hydrate",
	}); err != nil {
		t.Fatalf("runWorkspaceTaskLink failed: %v", err)
	}
	if err := runAgentTaskClaim([]string{
		"--workspace-id", "ws-hydrate",
		"--agent-id", "agent-hydrate",
		"--task-id", "provider-bridge-task",
		"--summary", "Preparing hydration payload",
	}); err != nil {
		t.Fatalf("runAgentTaskClaim failed: %v", err)
	}
	if err := runAgentUpdatePost([]string{
		"--workspace-id", "ws-hydrate",
		"--agent-id", "agent-hydrate",
		"--type", "progress",
		"--summary", "Hydration request prepared",
		"--payload-schema", "v1",
		"--payload-status", "ready_for_provider",
		"--task-ids", "provider-bridge-task",
		"--doc-keys", "current_context",
		"--notes", "Bridge can hydrate from task context now",
	}); err != nil {
		t.Fatalf("runAgentUpdatePost failed: %v", err)
	}

	statusOut, err := captureStdout(t, func() error {
		return runWorkspaceStatus([]string{
			"--workspace-id", "ws-hydrate",
			"--updates-limit", "5",
		})
	})
	if err != nil {
		t.Fatalf("runWorkspaceStatus failed: %v", err)
	}
	var statusPayload struct {
		Snapshot struct {
			RecentUpdates []struct {
				UpdateID string `json:"update_id"`
			} `json:"recent_updates"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal([]byte(statusOut), &statusPayload); err != nil {
		t.Fatalf("decode workspace status: %v; output=%q", err, statusOut)
	}
	if len(statusPayload.Snapshot.RecentUpdates) == 0 {
		t.Fatalf("expected update for hydration bundle")
	}
	if err := runWorkspaceArtifactPut([]string{
		"--workspace-id", "ws-hydrate",
		"--task-id", "provider-bridge-task",
		"--update-id", statusPayload.Snapshot.RecentUpdates[0].UpdateID,
		"--title", "Provider Prompt Draft",
		"--ref", "artifacts/provider-prompt.md",
		"--kind", "document",
		"--content-type", "text/markdown",
		"--created-by", "agent-hydrate",
	}); err != nil {
		t.Fatalf("runWorkspaceArtifactPut failed: %v", err)
	}

	out, err := captureStdout(t, func() error {
		return runTaskHydrate([]string{
			"--task-id", "provider-bridge-task",
			"--workspace-id", "ws-hydrate",
			"--doc-keys", "current_context",
		})
	})
	if err != nil {
		t.Fatalf("runTaskHydrate failed: %v", err)
	}

	var payload struct {
		Bundle struct {
			Workspace *struct {
				WorkspaceID string `json:"workspace_id"`
			} `json:"workspace"`
			WorkspaceTask *struct {
				TaskID       string  `json:"task_id"`
				ClaimAgentID *string `json:"claim_agent_id"`
			} `json:"workspace_task"`
			Docs []struct {
				DocKey string `json:"doc_key"`
			} `json:"docs"`
			TaskLinks []struct {
				LinkType string `json:"link_type"`
			} `json:"task_links"`
			RelatedTasks []struct {
				TaskID string `json:"task_id"`
			} `json:"related_tasks"`
			Artifacts []struct {
				Title string `json:"title"`
			} `json:"artifacts"`
			Updates []struct {
				Summary string `json:"summary"`
			} `json:"updates"`
		} `json:"bundle"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode task hydrate output: %v; output=%q", err, out)
	}
	if payload.Bundle.Workspace == nil || payload.Bundle.Workspace.WorkspaceID != "ws-hydrate" {
		t.Fatalf("expected workspace in hydration bundle, got %+v", payload.Bundle.Workspace)
	}
	if payload.Bundle.WorkspaceTask == nil || payload.Bundle.WorkspaceTask.ClaimAgentID == nil || *payload.Bundle.WorkspaceTask.ClaimAgentID != "agent-hydrate" {
		t.Fatalf("expected claimed workspace task in hydration bundle, got %+v", payload.Bundle.WorkspaceTask)
	}
	if len(payload.Bundle.Docs) != 1 || payload.Bundle.Docs[0].DocKey != "current_context" {
		t.Fatalf("expected selected doc in hydration bundle, got %+v", payload.Bundle.Docs)
	}
	if len(payload.Bundle.TaskLinks) != 1 || payload.Bundle.TaskLinks[0].LinkType != "RELATES_TO" {
		t.Fatalf("expected task link in hydration bundle, got %+v", payload.Bundle.TaskLinks)
	}
	if len(payload.Bundle.RelatedTasks) != 1 || payload.Bundle.RelatedTasks[0].TaskID != "provider-bridge-prep" {
		t.Fatalf("expected related task in hydration bundle, got %+v", payload.Bundle.RelatedTasks)
	}
	if len(payload.Bundle.Artifacts) != 1 || payload.Bundle.Artifacts[0].Title != "Provider Prompt Draft" {
		t.Fatalf("expected artifact in hydration bundle, got %+v", payload.Bundle.Artifacts)
	}
	if len(payload.Bundle.Updates) != 1 || payload.Bundle.Updates[0].Summary != "Hydration request prepared" {
		t.Fatalf("expected update in hydration bundle, got %+v", payload.Bundle.Updates)
	}
}
