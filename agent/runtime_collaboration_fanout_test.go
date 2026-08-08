package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestStartupIdentitySummaryUsesRuntimeConfigOverDeterministicProfile(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:        RuntimeModeDaemon,
			Workdir:     filepath.Join(t.TempDir(), "alpha"),
			WorkspaceID: "ws-1",
			AgentID:     "alpha",
			DisplayName: "Alpha Strategist",
			ProviderID:  "codex",
			GroupID:     "codex",
			LLMBackend:  llmBackendCodex,
			Model:       "gpt-test",
			Role:        "strategist",
			OwnerUserID: "owner-1",
		},
	}

	summary := runtime.startupIdentitySummary()
	for _, want := range []string{"profile=alpha", "agent=alpha", "display=Alpha Strategist", "role=strategist", "workspace=ws-1"} {
		if !strings.Contains(summary, want) {
			t.Fatalf("expected startup identity to contain %q, got %q", want, summary)
		}
	}
	if strings.Contains(summary, "role=generalist") {
		t.Fatalf("deterministic profile role should not override configured role: %q", summary)
	}
}

func TestCollaborationFanoutExplicitFalseDisablesHeuristicFanout(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		Role:        "strategist",
	}}
	task := WorkspaceTaskRecord{
		TaskID:              "task-broad-no-fanout",
		Title:               "Build dashboard frontend integration tests",
		Description:         "Broad coordination and review wording that would otherwise trigger fanout.",
		Priority:            "high",
		TaskKind:            "COORDINATION",
		TaskTemplate:        "generic",
		RequiresProjectGate: boolPtr(false),
	}
	if runtime.collaborationFanoutTaskRequiresFanout(task) {
		t.Fatal("expected explicit requires_project_gate=false to disable heuristic collaboration fanout")
	}
	task.RequiresProjectGate = nil
	if !runtime.collaborationFanoutTaskRequiresFanout(task) {
		t.Fatal("expected nil requires_project_gate to keep existing broad-task fanout heuristic")
	}
}

func TestCollaborationFanoutSkipsStrategicLeadCorrectionTask(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		Role:        "strategist",
	}}
	task := WorkspaceTaskRecord{
		TaskID:               "task-role-scope-abc123",
		Title:                "Adjust gamma lane authority",
		Description:          "Fresh phrasing without the legacy role/scope header.",
		Priority:             "high",
		TaskKind:             "COORDINATION",
		TaskTemplate:         "generic",
		ProjectID:            "project-demo",
		ProjectLane:          "coordination",
		Tags:                 []string{"project-role-scope", "strategic-lead", "coordination", "blocker-unblock"},
		TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","required_transition":"project_role_assign"}`,
	}
	if runtime.collaborationFanoutTaskRequiresFanout(task) {
		t.Fatal("expected structured strategic-lead role/scope service task to bypass broad collaboration fanout")
	}
}

func TestCollaborationFanoutDoesNotBypassArbitraryTaggedCoordinationTask(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		Role:        "strategist",
	}}
	task := WorkspaceTaskRecord{
		TaskID:       "task-custom-role-scope",
		Title:        "Build dashboard frontend integration review",
		Description:  "Broad task that should still fan out despite a misleading tag.",
		Priority:     "high",
		TaskKind:     "COORDINATION",
		TaskTemplate: "generic",
		ProjectID:    "project-demo",
		ProjectLane:  "coordination",
		Tags:         []string{"project-role-scope"},
	}
	if !runtime.collaborationFanoutTaskRequiresFanout(task) {
		t.Fatal("expected arbitrary project-role-scope tag without canonical task shape not to bypass fanout")
	}
}

func TestCollaborationFanoutQueuesBroadTaskPeerRequestsOnce(t *testing.T) {
	var requestTargets []string
	var requestPrompts []string
	var saved RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.request":
			target := rpcString(req.Params, "to_agent_id")
			requestTargets = append(requestTargets, target)
			payload := rpcString(req.Params, "payload_json")
			var decoded map[string]string
			if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
				t.Fatalf("decode fanout payload: %v", err)
			}
			requestPrompts = append(requestPrompts, decoded["prompt"])
			writeRPCResult(w, req, map[string]any{
				"request_id":   "req-" + target,
				"workspace_id": "ws-1",
				"to_agent_id":  target,
				"status":       "PENDING",
			})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &saved); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:         RuntimeModeDaemon,
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "alpha",
			Role:         "strategist",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Agents: []AgentRecord{
					{AgentID: "alpha", Role: "strategist", Status: "ACTIVE", IsOnline: true},
					{AgentID: "beta", Role: "frontend", Status: "ACTIVE", IsOnline: true},
					{AgentID: "delta", Role: "integrator", Status: "STOPPED", IsOnline: false},
					{AgentID: "epsilon", Role: "reviewer", Status: "ACTIVE", IsOnline: true},
				},
			},
		},
	}

	task := WorkspaceTaskRecord{
		TaskID:       "task-broad",
		Title:        "Build a local dashboard",
		Description:  "Create a dashboard with backend generation, frontend controls, integration, and tests.",
		Priority:     "HIGH",
		Status:       "RUNNING",
		TaskKind:     "EXECUTION",
		TaskTemplate: "generic",
	}
	session := AgentSessionStateRecord{SessionID: "session-1", TaskID: task.TaskID, Status: "ACTIVE"}

	if err := runtime.ensureCollaborationFanout(context.Background(), task, &session, "run-1"); err != nil {
		t.Fatalf("ensureCollaborationFanout() error = %v", err)
	}
	if got := strings.Join(requestTargets, ","); got != "epsilon,delta,beta" {
		t.Fatalf("expected ranked peer fanout including offline registered peers, got %q", got)
	}
	if len(requestPrompts) != 3 || !strings.Contains(requestPrompts[0], "task_submit") || !strings.Contains(requestPrompts[0], "private local files") {
		t.Fatalf("expected actionable coordination prompt, got %+v", requestPrompts)
	}
	for _, want := range []string{"reuse or claim an existing similar lane", "default lanes are strategy", "root task claimant remains finalization owner"} {
		if !strings.Contains(requestPrompts[0], want) {
			t.Fatalf("expected convergence guidance %q in prompt:\n%s", want, requestPrompts[0])
		}
	}
	if saved.CollaborationFanoutTaskID != task.TaskID || saved.CollaborationFanoutRunID != "run-1" {
		t.Fatalf("expected fanout scratch marker, got %+v", saved)
	}
	if saved.CollaborationFanoutRequests["epsilon"] != "req-epsilon" || saved.CollaborationFanoutRequests["delta"] != "req-delta" || saved.CollaborationFanoutRequests["beta"] != "req-beta" {
		t.Fatalf("expected per-peer request IDs, got %+v", saved.CollaborationFanoutRequests)
	}
	if len(saved.AdvisorySignals) == 0 || !strings.Contains(saved.AdvisorySignals[len(saved.AdvisorySignals)-1], "SYSTEM COLLABORATION FANOUT") {
		t.Fatalf("expected collaboration advisory signal, got %+v", saved.AdvisorySignals)
	}
	if !strings.Contains(saved.AdvisorySignals[len(saved.AdvisorySignals)-1], "one task per lane") {
		t.Fatalf("expected fanout advisory to include convergence lane guidance, got %+v", saved.AdvisorySignals)
	}

	if err := runtime.ensureCollaborationFanout(context.Background(), task, &session, "run-1"); err != nil {
		t.Fatalf("second ensureCollaborationFanout() error = %v", err)
	}
	if len(requestTargets) != 3 {
		t.Fatalf("expected idempotent fanout, got targets %+v", requestTargets)
	}
}
