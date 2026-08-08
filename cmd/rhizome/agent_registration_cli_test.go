package main

import (
	"encoding/json"
	"testing"
)

func TestAgentRegisterAndHeartbeatCLISharedTruthContract(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-cli-agent-contract",
		"--title", "CLI Agent Contract",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}

	registerOut, err := captureStdout(t, func() error {
		return runAgentRegister([]string{
			"--workspace-id", "ws-cli-agent-contract",
			"--agent-id", "agent-cli-partner",
			"--owner-user-id", "developer",
			"--display-name", "CLI Partner Agent",
			"--role", "reviewer",
			"--protocol-version", "partner-runtime/v2",
			"--capabilities", "analysis,coordination,analysis,review",
			"--summary", "Registered for partner queue",
		})
	})
	if err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}

	var registerPayload struct {
		Agent struct {
			AgentID         string   `json:"agent_id"`
			Role            string   `json:"role"`
			Status          string   `json:"status"`
			ProtocolVersion string   `json:"protocol_version"`
			Capabilities    []string `json:"capabilities"`
			Summary         string   `json:"summary"`
			LastSeenAt      *string  `json:"last_seen_at"`
			IsOnline        bool     `json:"is_online"`
		} `json:"agent"`
	}
	if err := json.Unmarshal([]byte(registerOut), &registerPayload); err != nil {
		t.Fatalf("decode agent register output: %v; output=%q", err, registerOut)
	}
	if registerPayload.Agent.AgentID != "agent-cli-partner" || registerPayload.Agent.Role != "reviewer" {
		t.Fatalf("unexpected registered agent payload: %+v", registerPayload.Agent)
	}
	if registerPayload.Agent.ProtocolVersion != "partner-runtime/v2" || registerPayload.Agent.Summary != "Registered for partner queue" {
		t.Fatalf("expected register output to preserve protocol/summary, got %+v", registerPayload.Agent)
	}
	if got := registerPayload.Agent.Capabilities; len(got) != 3 || got[0] != "analysis" || got[1] != "coordination" || got[2] != "review" {
		t.Fatalf("expected normalized capabilities from CLI register, got %+v", got)
	}
	if registerPayload.Agent.LastSeenAt != nil || registerPayload.Agent.IsOnline {
		t.Fatalf("expected CLI register to remain offline until heartbeat, got %+v", registerPayload.Agent)
	}

	if err := runAgentHeartbeat([]string{
		"--workspace-id", "ws-cli-agent-contract",
		"--agent-id", "agent-cli-partner",
		"--status", "active",
		"--summary", "Serving partner queue",
	}); err != nil {
		t.Fatalf("runAgentHeartbeat failed: %v", err)
	}

	store, err := openStore()
	if err != nil {
		t.Fatalf("openStore failed: %v", err)
	}
	defer func() { _ = store.Close() }()

	agent, err := store.GetAgent(t.Context(), "ws-cli-agent-contract", "agent-cli-partner")
	if err != nil {
		t.Fatalf("get agent after heartbeat: %v", err)
	}
	if agent.LastSeenAt == nil || !agent.IsOnline {
		t.Fatalf("expected heartbeat to establish online presence, got %+v", agent)
	}
	if agent.Role != "reviewer" || agent.ProtocolVersion != "partner-runtime/v2" {
		t.Fatalf("expected heartbeat not to rewrite identity metadata, got %+v", agent)
	}
	if got := agent.Capabilities; len(got) != 3 || got[0] != "analysis" || got[1] != "coordination" || got[2] != "review" {
		t.Fatalf("expected heartbeat to preserve CLI capabilities, got %+v", got)
	}
	if agent.Summary != "Serving partner queue" || agent.Status != "ACTIVE" {
		t.Fatalf("expected heartbeat to refresh status/summary, got %+v", agent)
	}
}

func TestAgentRegisterCLIPreservesMetadataOnPartialReregister(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-cli-agent-reregister",
		"--title", "CLI Agent Reregister",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}

	if err := runAgentRegister([]string{
		"--workspace-id", "ws-cli-agent-reregister",
		"--agent-id", "agent-cli-reregister",
		"--owner-user-id", "developer",
		"--display-name", "CLI Registered Partner",
		"--role", "reviewer",
		"--protocol-version", "partner-runtime/v6",
		"--capabilities", "analysis,tool.call",
		"--summary", "registered summary",
	}); err != nil {
		t.Fatalf("initial runAgentRegister failed: %v", err)
	}

	if err := runAgentHeartbeat([]string{
		"--workspace-id", "ws-cli-agent-reregister",
		"--agent-id", "agent-cli-reregister",
		"--status", "active",
		"--summary", "live summary",
	}); err != nil {
		t.Fatalf("runAgentHeartbeat failed: %v", err)
	}

	registerOut, err := captureStdout(t, func() error {
		return runAgentRegister([]string{
			"--workspace-id", "ws-cli-agent-reregister",
			"--agent-id", "agent-cli-reregister",
		})
	})
	if err != nil {
		t.Fatalf("partial runAgentRegister failed: %v", err)
	}

	var registerPayload struct {
		Agent struct {
			AgentID         string   `json:"agent_id"`
			OwnerUserID     string   `json:"owner_user_id"`
			DisplayName     string   `json:"display_name"`
			Role            string   `json:"role"`
			ProtocolVersion string   `json:"protocol_version"`
			Capabilities    []string `json:"capabilities"`
			Summary         string   `json:"summary"`
			LastSeenAt      *string  `json:"last_seen_at"`
			IsOnline        bool     `json:"is_online"`
		} `json:"agent"`
	}
	if err := json.Unmarshal([]byte(registerOut), &registerPayload); err != nil {
		t.Fatalf("decode partial agent register output: %v; output=%q", err, registerOut)
	}
	if registerPayload.Agent.OwnerUserID != "developer" || registerPayload.Agent.DisplayName != "CLI Registered Partner" {
		t.Fatalf("expected partial CLI register to preserve owner/display, got %+v", registerPayload.Agent)
	}
	if registerPayload.Agent.Role != "reviewer" || registerPayload.Agent.ProtocolVersion != "partner-runtime/v6" {
		t.Fatalf("expected partial CLI register to preserve role/protocol, got %+v", registerPayload.Agent)
	}
	if len(registerPayload.Agent.Capabilities) != 2 || registerPayload.Agent.Capabilities[0] != "analysis" || registerPayload.Agent.Capabilities[1] != "tool.call" {
		t.Fatalf("expected partial CLI register to preserve capabilities, got %+v", registerPayload.Agent)
	}
	if registerPayload.Agent.Summary != "live summary" || registerPayload.Agent.LastSeenAt == nil || !registerPayload.Agent.IsOnline {
		t.Fatalf("expected partial CLI register to preserve live summary and liveness, got %+v", registerPayload.Agent)
	}
}
