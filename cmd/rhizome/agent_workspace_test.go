package main

import (
	"encoding/json"
	"testing"
)

func TestAgentBootstrapFlow_EndToEnd(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-rhizome",
		"--title", "Rhizome Alpha",
		"--description", "Internal multi-agent workspace",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, "ws-rhizome")

	if err := runWorkspaceDocPut([]string{
		"--workspace-id", "ws-rhizome",
		"--doc-key", "charter",
		"--title", "Charter",
		"--updated-by", "developer",
		"--content", "Agents coordinate through Rhizome.",
	}); err != nil {
		t.Fatalf("runWorkspaceDocPut failed: %v", err)
	}

	if err := runAgentRegister([]string{
		"--workspace-id", "ws-rhizome",
		"--agent-id", "agent-codex",
		"--owner-user-id", "developer",
		"--display-name", "Codex",
		"--role", "coordinator",
		"--capabilities", "code,planning,deployment",
		"--summary", "Ready for bootstrap",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}

	if err := runTaskSubmit([]string{
		"--task-id", "task-agent-flow",
		"--owner-user-id", "developer",
		"--workspace-id", "ws-rhizome",
	}); err != nil {
		t.Fatalf("runTaskSubmit failed: %v", err)
	}

	if err := runAgentUpdatePost([]string{
		"--workspace-id", "ws-rhizome",
		"--agent-id", "agent-codex",
		"--type", "progress",
		"--summary", "Bootstrap started",
		"--payload", `{"stage":"bootstrap"}`,
	}); err != nil {
		t.Fatalf("runAgentUpdatePost failed: %v", err)
	}

	if err := runAgentTaskClaim([]string{
		"--workspace-id", "ws-rhizome",
		"--agent-id", "agent-codex",
		"--task-id", "task-agent-flow",
		"--summary", "Taking first coordination task",
	}); err != nil {
		t.Fatalf("runAgentTaskClaim failed: %v", err)
	}

	out, err := captureStdout(t, func() error {
		return runAgentBootstrap([]string{
			"--workspace-id", "ws-rhizome",
			"--agent-id", "agent-codex",
			"--updates-limit", "5",
		})
	})
	if err != nil {
		t.Fatalf("runAgentBootstrap failed: %v", err)
	}

	var payload struct {
		Agent struct {
			AgentID string `json:"agent_id"`
			Role    string `json:"role"`
		} `json:"agent"`
		Protocols []struct {
			Name string `json:"name"`
		} `json:"protocols"`
		Snapshot struct {
			Workspace struct {
				WorkspaceID string `json:"workspace_id"`
			} `json:"workspace"`
			Docs []struct {
				DocKey string `json:"doc_key"`
			} `json:"docs"`
			Agents []struct {
				AgentID string `json:"agent_id"`
			} `json:"agents"`
			Tasks []struct {
				TaskID       string  `json:"task_id"`
				ClaimAgentID *string `json:"claim_agent_id"`
				ClaimStatus  *string `json:"claim_status"`
			} `json:"tasks"`
			RecentUpdates []struct {
				UpdateType string `json:"update_type"`
				Summary    string `json:"summary"`
			} `json:"recent_updates"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode bootstrap output: %v; output=%q", err, out)
	}

	if payload.Agent.AgentID != "agent-codex" {
		t.Fatalf("expected bootstrap agent agent-codex, got %q", payload.Agent.AgentID)
	}
	if payload.Snapshot.Workspace.WorkspaceID != "ws-rhizome" {
		t.Fatalf("expected workspace ws-rhizome, got %q", payload.Snapshot.Workspace.WorkspaceID)
	}
	if len(payload.Protocols) == 0 {
		t.Fatalf("expected bootstrap protocols, got none")
	}
	if len(payload.Snapshot.Docs) != 1 || payload.Snapshot.Docs[0].DocKey != "charter" {
		t.Fatalf("expected charter doc in bootstrap, got %+v", payload.Snapshot.Docs)
	}
	if len(payload.Snapshot.Agents) != 1 || payload.Snapshot.Agents[0].AgentID != "agent-codex" {
		t.Fatalf("expected agent-codex in bootstrap agents, got %+v", payload.Snapshot.Agents)
	}
	if len(payload.Snapshot.Tasks) != 1 || payload.Snapshot.Tasks[0].TaskID != "task-agent-flow" {
		t.Fatalf("expected task-agent-flow in bootstrap tasks, got %+v", payload.Snapshot.Tasks)
	}
	if payload.Snapshot.Tasks[0].ClaimAgentID == nil || *payload.Snapshot.Tasks[0].ClaimAgentID != "agent-codex" {
		t.Fatalf("expected task claim agent-codex, got %+v", payload.Snapshot.Tasks[0].ClaimAgentID)
	}
	if payload.Snapshot.Tasks[0].ClaimStatus == nil || *payload.Snapshot.Tasks[0].ClaimStatus != "CLAIMED" {
		t.Fatalf("expected task claim status CLAIMED, got %+v", payload.Snapshot.Tasks[0].ClaimStatus)
	}
	if len(payload.Snapshot.RecentUpdates) == 0 || payload.Snapshot.RecentUpdates[0].UpdateType != "progress" {
		t.Fatalf("expected progress update in bootstrap, got %+v", payload.Snapshot.RecentUpdates)
	}
}
