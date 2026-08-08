package main

import (
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestAgentSessionCLIAndBootstrapSnapshot(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-session-cli",
		"--title", "Session CLI Workspace",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, "ws-session-cli")
	if err := runAgentRegister([]string{
		"--workspace-id", "ws-session-cli",
		"--agent-id", "agent-session-cli",
		"--owner-user-id", "developer",
		"--display-name", "Session CLI Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}

	if err := runAgentSession([]string{
		"start",
		"--workspace-id", "ws-session-cli",
		"--session-id", "sess-cli-1",
		"--agent-id", "agent-session-cli",
		"--summary", "Taking active ownership",
		"--owner-scope", "task/session",
	}); err != nil {
		t.Fatalf("runAgentSession start failed: %v", err)
	}
	if err := runAgentSession([]string{
		"decision-needed",
		"--workspace-id", "ws-session-cli",
		"--session-id", "sess-cli-1",
		"--agent-id", "agent-session-cli",
		"--summary", "Need product decision",
		"--decision-needed-from", "developer",
		"--decision-type", "scope",
		"--keep-session-active", "false",
	}); err != nil {
		t.Fatalf("runAgentSession decision-needed failed: %v", err)
	}
	requireCLIAgentSessionRuntimeEvent(t, "ws-session-cli", "sess-cli-1", model.SessionEventStart, "agent.session.start", "agent-session-cli")
	requireCLIAgentSessionRuntimeEvent(t, "ws-session-cli", "sess-cli-1", model.SessionEventDecisionNeeded, "agent.session.decision_needed", "agent-session-cli")

	listOut, err := captureStdout(t, func() error {
		return runAgentSession([]string{
			"list",
			"--workspace-id", "ws-session-cli",
			"--active-only=true",
		})
	})
	if err != nil {
		t.Fatalf("capture session list failed: %v", err)
	}

	var listPayload struct {
		Sessions []struct {
			SessionID          string `json:"session_id"`
			Status             string `json:"status"`
			DecisionNeededFrom string `json:"decision_needed_from"`
			KeepSessionActive  bool   `json:"keep_session_active"`
		} `json:"sessions"`
	}
	if err := json.Unmarshal([]byte(listOut), &listPayload); err != nil {
		t.Fatalf("decode session list output: %v; output=%q", err, listOut)
	}
	if len(listPayload.Sessions) != 1 {
		t.Fatalf("expected one active session, got %+v", listPayload.Sessions)
	}
	if listPayload.Sessions[0].SessionID != "sess-cli-1" || listPayload.Sessions[0].Status != "WAITING_DECISION" {
		t.Fatalf("unexpected session list payload: %+v", listPayload.Sessions[0])
	}
	if listPayload.Sessions[0].DecisionNeededFrom != "developer" || listPayload.Sessions[0].KeepSessionActive {
		t.Fatalf("expected decision-needed state with keep_session_active=false, got %+v", listPayload.Sessions[0])
	}

	bootstrapOut, err := captureStdout(t, func() error {
		return runAgentBootstrap([]string{
			"--workspace-id", "ws-session-cli",
			"--agent-id", "agent-session-cli",
			"--updates-limit", "10",
		})
	})
	if err != nil {
		t.Fatalf("capture bootstrap failed: %v", err)
	}

	var bootstrapPayload struct {
		Protocols []struct {
			Name string `json:"name"`
		} `json:"protocols"`
		Snapshot struct {
			Sessions []struct {
				SessionID          string `json:"session_id"`
				Status             string `json:"status"`
				DecisionNeededFrom string `json:"decision_needed_from"`
			} `json:"sessions"`
			Agents []struct {
				AgentID        string `json:"agent_id"`
				CurrentSession *struct {
					SessionID string `json:"session_id"`
					Status    string `json:"status"`
				} `json:"current_session"`
			} `json:"agents"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal([]byte(bootstrapOut), &bootstrapPayload); err != nil {
		t.Fatalf("decode bootstrap output: %v; output=%q", err, bootstrapOut)
	}
	foundProtocol := false
	for _, protocol := range bootstrapPayload.Protocols {
		if protocol.Name == "session.coordination" {
			foundProtocol = true
			break
		}
	}
	if !foundProtocol {
		t.Fatalf("expected session.coordination protocol in bootstrap, got %+v", bootstrapPayload.Protocols)
	}
	if len(bootstrapPayload.Snapshot.Sessions) != 1 || bootstrapPayload.Snapshot.Sessions[0].SessionID != "sess-cli-1" {
		t.Fatalf("expected sess-cli-1 in bootstrap sessions, got %+v", bootstrapPayload.Snapshot.Sessions)
	}
	if bootstrapPayload.Snapshot.Sessions[0].Status != "WAITING_DECISION" {
		t.Fatalf("expected WAITING_DECISION in bootstrap session, got %+v", bootstrapPayload.Snapshot.Sessions[0])
	}
	if len(bootstrapPayload.Snapshot.Agents) != 1 || bootstrapPayload.Snapshot.Agents[0].CurrentSession == nil {
		t.Fatalf("expected current_session on bootstrap agent, got %+v", bootstrapPayload.Snapshot.Agents)
	}
	if bootstrapPayload.Snapshot.Agents[0].CurrentSession.SessionID != "sess-cli-1" {
		t.Fatalf("expected current session sess-cli-1, got %+v", bootstrapPayload.Snapshot.Agents[0].CurrentSession)
	}
}

func TestAgentSessionTakeoverCLIPromptContextEnvelope(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID        = "ws-session-takeover-cli"
		sourceAgentID      = "agent-session-takeover-cli-a"
		successorAgentID   = "agent-session-takeover-cli-b"
		sourceSessionID    = "sess-session-takeover-cli-a"
		successorSessionID = "sess-session-takeover-cli-b"
	)
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "Session Takeover CLI Workspace",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)
	for _, agentID := range []string{sourceAgentID, successorAgentID} {
		if err := runAgentRegister([]string{
			"--workspace-id", workspaceID,
			"--agent-id", agentID,
			"--owner-user-id", "developer",
			"--display-name", agentID,
		}); err != nil {
			t.Fatalf("runAgentRegister %s failed: %v", agentID, err)
		}
	}
	if err := runAgentSession([]string{
		"start",
		"--workspace-id", workspaceID,
		"--session-id", sourceSessionID,
		"--agent-id", sourceAgentID,
		"--summary", "Source starts takeover candidate",
	}); err != nil {
		t.Fatalf("runAgentSession start failed: %v", err)
	}
	if err := runAgentSession([]string{
		"status",
		"--workspace-id", workspaceID,
		"--session-id", sourceSessionID,
		"--agent-id", sourceAgentID,
		"--status", model.SessionStatusHandoffPending,
		"--summary", "Handoff to successor",
		"--handoff-to", successorAgentID,
	}); err != nil {
		t.Fatalf("runAgentSession status failed: %v", err)
	}

	out, err := captureStdout(t, func() error {
		return runAgentSession([]string{
			"takeover",
			"--workspace-id", workspaceID,
			"--session-id", sourceSessionID,
			"--takeover-agent-id", successorAgentID,
			"--successor-session-id", successorSessionID,
			"--summary", "Successor takes over",
		})
	})
	if err != nil {
		t.Fatalf("capture takeover failed: %v", err)
	}
	var payload struct {
		RuntimeEventID string `json:"runtime_event_id"`
		Status         string `json:"status"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode takeover output: %v; output=%q", err, out)
	}
	if payload.Status != "TAKEN_OVER" || payload.RuntimeEventID == "" {
		t.Fatalf("expected takeover runtime_event_id, got %+v", payload)
	}

	requireCLIAgentSessionRuntimeEvent(t, workspaceID, sourceSessionID, "session.takeover", "agent.session.takeover", successorAgentID)
	requireCLIAgentSessionRuntimeEvent(t, workspaceID, sourceSessionID, model.SessionEventEnd, "agent.session.takeover", successorAgentID)
	requireCLIAgentSessionRuntimeEvent(t, workspaceID, successorSessionID, model.SessionEventStart, "agent.session.takeover", successorAgentID)
}

func TestAgentSessionSyncQueueCLISeedsBlockedQueue(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-session-sync-queue-cli",
		"--title", "Session Sync Queue CLI Workspace",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, "ws-session-sync-queue-cli")
	if err := runAgentRegister([]string{
		"--workspace-id", "ws-session-sync-queue-cli",
		"--agent-id", "agent-session-sync-queue-cli",
		"--owner-user-id", "developer",
		"--display-name", "Session Sync Queue CLI Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}
	if err := runAgentSession([]string{
		"start",
		"--workspace-id", "ws-session-sync-queue-cli",
		"--session-id", "sess-sync-queue-cli-1",
		"--agent-id", "agent-session-sync-queue-cli",
		"--summary", "Taking active ownership before blocker",
	}); err != nil {
		t.Fatalf("runAgentSession start failed: %v", err)
	}
	if err := runAgentSession([]string{
		"blocked",
		"--workspace-id", "ws-session-sync-queue-cli",
		"--session-id", "sess-sync-queue-cli-1",
		"--agent-id", "agent-session-sync-queue-cli",
		"--summary", "Blocked on dependency",
		"--blocked-on", "dependency:waiting for token",
	}); err != nil {
		t.Fatalf("runAgentSession blocked failed: %v", err)
	}

	out, err := captureStdout(t, func() error {
		return runAgentSession([]string{
			"sync-queue",
			"--workspace-id", "ws-session-sync-queue-cli",
			"--session-id", "sess-sync-queue-cli-1",
		})
	})
	if err != nil {
		t.Fatalf("capture session sync-queue failed: %v", err)
	}

	var payload struct {
		Status string `json:"status"`
		Result struct {
			Opened []struct {
				Record struct {
					QueueKey  string `json:"queue_key"`
					QueueType string `json:"queue_type"`
					Status    string `json:"status"`
				} `json:"record"`
			} `json:"opened"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode sync-queue output: %v; output=%q", err, out)
	}
	if payload.Status != "SYNCED" {
		t.Fatalf("expected SYNCED status, got %+v", payload)
	}
	if len(payload.Result.Opened) != 1 {
		t.Fatalf("expected one opened queue, got %+v", payload.Result.Opened)
	}
	if payload.Result.Opened[0].Record.QueueKey != "session:sess-sync-queue-cli-1:blocker" ||
		payload.Result.Opened[0].Record.QueueType != "BLOCKER" ||
		payload.Result.Opened[0].Record.Status != "OPEN" {
		t.Fatalf("unexpected opened queue payload: %+v", payload.Result.Opened[0].Record)
	}
}

func TestAgentSessionSyncQueueCLISeedsDecisionQueue(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-session-sync-decision-queue-cli",
		"--title", "Session Sync Decision Queue CLI Workspace",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, "ws-session-sync-decision-queue-cli")
	if err := runAgentRegister([]string{
		"--workspace-id", "ws-session-sync-decision-queue-cli",
		"--agent-id", "agent-session-sync-decision-queue-cli",
		"--owner-user-id", "developer",
		"--display-name", "Session Sync Decision Queue CLI Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}
	if err := runAgentSession([]string{
		"start",
		"--workspace-id", "ws-session-sync-decision-queue-cli",
		"--session-id", "sess-sync-decision-queue-cli-1",
		"--agent-id", "agent-session-sync-decision-queue-cli",
		"--summary", "Taking active ownership before decision",
	}); err != nil {
		t.Fatalf("runAgentSession start failed: %v", err)
	}
	if err := runAgentSession([]string{
		"decision-needed",
		"--workspace-id", "ws-session-sync-decision-queue-cli",
		"--session-id", "sess-sync-decision-queue-cli-1",
		"--agent-id", "agent-session-sync-decision-queue-cli",
		"--summary", "Need operator approval",
		"--decision-needed-from", "developer",
		"--decision-type", "approval",
		"--keep-session-active", "false",
	}); err != nil {
		t.Fatalf("runAgentSession decision-needed failed: %v", err)
	}

	out, err := captureStdout(t, func() error {
		return runAgentSession([]string{
			"sync-queue",
			"--workspace-id", "ws-session-sync-decision-queue-cli",
			"--session-id", "sess-sync-decision-queue-cli-1",
		})
	})
	if err != nil {
		t.Fatalf("capture session sync-queue failed: %v", err)
	}

	var payload struct {
		Status string `json:"status"`
		Result struct {
			Opened []struct {
				Record struct {
					QueueKey  string `json:"queue_key"`
					QueueType string `json:"queue_type"`
					Status    string `json:"status"`
				} `json:"record"`
			} `json:"opened"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode sync-queue output: %v; output=%q", err, out)
	}
	if payload.Status != "SYNCED" {
		t.Fatalf("expected SYNCED status, got %+v", payload)
	}
	if len(payload.Result.Opened) != 1 {
		t.Fatalf("expected one opened queue, got %+v", payload.Result.Opened)
	}
	if payload.Result.Opened[0].Record.QueueKey != "session:sess-sync-decision-queue-cli-1:decision" ||
		payload.Result.Opened[0].Record.QueueType != "DECISION" ||
		payload.Result.Opened[0].Record.Status != "OPEN" {
		t.Fatalf("unexpected opened queue payload: %+v", payload.Result.Opened[0].Record)
	}
}

func TestAgentSessionSyncQueueCLISeedsHandoffQueue(t *testing.T) {
	setupFakeBridgeEnv(t)

	if err := runWorkspaceCreate([]string{
		"--workspace-id", "ws-session-sync-handoff-queue-cli",
		"--title", "Session Sync Handoff Queue CLI Workspace",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, "ws-session-sync-handoff-queue-cli")
	if err := runAgentRegister([]string{
		"--workspace-id", "ws-session-sync-handoff-queue-cli",
		"--agent-id", "agent-session-sync-handoff-queue-cli",
		"--owner-user-id", "developer",
		"--display-name", "Session Sync Handoff Queue CLI Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister failed: %v", err)
	}
	if err := runAgentRegister([]string{
		"--workspace-id", "ws-session-sync-handoff-queue-cli",
		"--agent-id", "agent-session-sync-handoff-target-cli",
		"--owner-user-id", "developer",
		"--display-name", "Session Sync Handoff Target CLI Agent",
	}); err != nil {
		t.Fatalf("runAgentRegister handoff target failed: %v", err)
	}
	if err := runAgentSession([]string{
		"start",
		"--workspace-id", "ws-session-sync-handoff-queue-cli",
		"--session-id", "sess-sync-handoff-queue-cli-1",
		"--agent-id", "agent-session-sync-handoff-queue-cli",
		"--summary", "Taking active ownership before handoff",
	}); err != nil {
		t.Fatalf("runAgentSession start failed: %v", err)
	}
	if err := runAgentSession([]string{
		"status",
		"--workspace-id", "ws-session-sync-handoff-queue-cli",
		"--session-id", "sess-sync-handoff-queue-cli-1",
		"--agent-id", "agent-session-sync-handoff-queue-cli",
		"--summary", "Pending specialist handoff",
		"--status", "HANDOFF_PENDING",
		"--handoff-to", "agent-session-sync-handoff-target-cli",
		"--keep-session-active", "false",
	}); err != nil {
		t.Fatalf("runAgentSession handoff status failed: %v", err)
	}

	out, err := captureStdout(t, func() error {
		return runAgentSession([]string{
			"sync-queue",
			"--workspace-id", "ws-session-sync-handoff-queue-cli",
			"--session-id", "sess-sync-handoff-queue-cli-1",
		})
	})
	if err != nil {
		t.Fatalf("capture session sync-queue failed: %v", err)
	}

	var payload struct {
		Status string `json:"status"`
		Result struct {
			Opened []struct {
				Record struct {
					QueueKey  string `json:"queue_key"`
					QueueType string `json:"queue_type"`
					Status    string `json:"status"`
				} `json:"record"`
			} `json:"opened"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode sync-queue output: %v; output=%q", err, out)
	}
	if payload.Status != "SYNCED" {
		t.Fatalf("expected SYNCED status, got %+v", payload)
	}
	if len(payload.Result.Opened) != 1 {
		t.Fatalf("expected one opened queue, got %+v", payload.Result.Opened)
	}
	if payload.Result.Opened[0].Record.QueueKey != "session:sess-sync-handoff-queue-cli-1:handoff" ||
		payload.Result.Opened[0].Record.QueueType != "HANDOFF" ||
		payload.Result.Opened[0].Record.Status != "OPEN" {
		t.Fatalf("unexpected opened queue payload: %+v", payload.Result.Opened[0].Record)
	}
}
