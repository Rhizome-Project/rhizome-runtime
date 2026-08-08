package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProjectImplementationFanoutDirectiveForLeadWithNoImplementationTasks(t *testing.T) {
	coordination := projectFanoutCoordination("project-rq", "alpha", nil)
	server := newProjectFanoutGateServer(t, []ProjectRecord{coordination.Project}, map[string]ProjectCoordinationRecord{
		"project-rq": coordination,
	})
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := WorkspaceTaskRecord{
		TaskID:      "root-rq",
		Title:       "Build rq interpreter",
		Status:      "RUNNING",
		TaskKind:    "COORDINATION",
		ProjectLane: "strategy",
	}
	trace := &TaskRunTrace{SuccessfulToolCalls: []string{"workspace_doc_get", "workspace_doc_put", "project_patch_queue_list"}}
	if !traceNeedsProjectImplementationFanoutDirective(trace) {
		t.Fatal("expected doc/patch queue trace without task_submit to require fanout probe")
	}

	directive := runtime.projectImplementationFanoutDirective(context.Background(), task)
	for _, want := range []string{"IMPLEMENTATION FANOUT REQUIRED", "task_submit", "project_id=project-rq", "project_lane=implementation", "write_scope_hints"} {
		if !strings.Contains(directive, want) {
			t.Fatalf("expected fanout directive to contain %q, got %q", want, directive)
		}
	}
}

func TestProjectImplementationFanoutDirectiveSkipsAfterTaskSubmitTrace(t *testing.T) {
	trace := &TaskRunTrace{SuccessfulToolCalls: []string{"project_phase_transition", "task_submit", "workspace_doc_put"}}
	if traceNeedsProjectImplementationFanoutDirective(trace) {
		t.Fatal("trace that already created a task must not trigger fanout directive lookup")
	}
}

func TestProjectImplementationFanoutDirectiveSkipsWhenImplementationTaskExists(t *testing.T) {
	implementationTask := WorkspaceTaskRecord{
		TaskID:      "task-parser",
		Title:       "Build parser",
		Status:      "PENDING",
		TaskKind:    "EXECUTION",
		ProjectID:   "project-rq",
		ProjectLane: "implementation",
	}
	coordination := projectFanoutCoordination("project-rq", "alpha", []WorkspaceTaskRecord{implementationTask})
	server := newProjectFanoutGateServer(t, nil, map[string]ProjectCoordinationRecord{
		"project-rq": coordination,
	})
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := WorkspaceTaskRecord{
		TaskID:      "root-rq",
		Status:      "RUNNING",
		TaskKind:    "COORDINATION",
		ProjectID:   "project-rq",
		ProjectLane: "strategy",
	}
	if directive := runtime.projectImplementationFanoutDirective(context.Background(), task); directive != "" {
		t.Fatalf("existing implementation task must suppress fanout directive, got %q", directive)
	}
}

func TestProjectImplementationFanoutDirectiveSkipsNonLead(t *testing.T) {
	coordination := projectFanoutCoordination("project-rq", "alpha", nil)
	server := newProjectFanoutGateServer(t, nil, map[string]ProjectCoordinationRecord{
		"project-rq": coordination,
	})
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "beta"},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := WorkspaceTaskRecord{
		TaskID:      "root-rq",
		Status:      "RUNNING",
		TaskKind:    "COORDINATION",
		ProjectID:   "project-rq",
		ProjectLane: "strategy",
	}
	if directive := runtime.projectImplementationFanoutDirective(context.Background(), task); directive != "" {
		t.Fatalf("non-lead agent must not get strategic fanout directive, got %q", directive)
	}
}

func TestContinueTaskCyclePersistsProjectFanoutAdvisory(t *testing.T) {
	var saved RuntimeScratchState
	var methods []string
	coordination := projectFanoutCoordination("project-rq", "alpha", nil)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.session.status":
			writeRPCResult(w, req, map[string]any{
				"state": map[string]any{
					"session_id":          "session-root",
					"workspace_id":        "ws",
					"agent_id":            "alpha",
					"task_id":             "root-rq",
					"status":              "ACTIVE",
					"summary":             rpcString(req.Params, "summary"),
					"updated_at":          "2026-05-31T00:00:01Z",
					"started_at":          "2026-05-31T00:00:00Z",
					"keep_session_active": true,
				},
			})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": coordination})
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
			WorkspaceID:  "ws",
			AgentID:      "alpha",
			PlannerEvery: 45 * time.Second,
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs: map[string]string{},
		},
	}
	task := WorkspaceTaskRecord{
		TaskID:      "root-rq",
		Title:       "Build rq interpreter",
		Status:      "RUNNING",
		TaskKind:    "COORDINATION",
		ProjectID:   "project-rq",
		ProjectLane: "strategy",
	}
	session := AgentSessionStateRecord{SessionID: "session-root", AgentID: "alpha", TaskID: task.TaskID, Status: "ACTIVE"}
	trace := &TaskRunTrace{SuccessfulToolCalls: []string{"workspace_doc_get", "workspace_doc_put", "project_patch_queue_list"}}

	err := runtime.continueTaskCycle(context.Background(), task, session, "run-root", StructuredTaskResult{
		Outcome:    "continue",
		Summary:    "Published implementation notes",
		NextAction: "Create or verify implementation tasks",
	}, trace)
	if err != nil {
		t.Fatalf("continueTaskCycle() error = %v", err)
	}
	if !containsAll(methods, []string{"agent.session.status", "workspace.execution.run.write", "project.coordination.get", "agent.state.set"}) {
		t.Fatalf("expected continuation persistence plus fanout coordination lookup, got %#v", methods)
	}
	if len(saved.AdvisorySignals) == 0 || !strings.Contains(saved.AdvisorySignals[len(saved.AdvisorySignals)-1], "IMPLEMENTATION FANOUT REQUIRED") {
		t.Fatalf("expected fanout advisory in saved scratch state, got %+v", saved.AdvisorySignals)
	}
	if !strings.Contains(saved.AdvisorySignals[len(saved.AdvisorySignals)-1], "task_submit") {
		t.Fatalf("expected fanout advisory to name task_submit, got %+v", saved.AdvisorySignals)
	}
}

func projectFanoutCoordination(projectID, leadAgentID string, tasks []WorkspaceTaskRecord) ProjectCoordinationRecord {
	return ProjectCoordinationRecord{
		Project: ProjectRecord{
			WorkspaceID: "ws",
			ProjectID:   projectID,
			Title:       "RQ JSON Interpreter",
			Status:      "ACTIVE",
		},
		Profile: ProjectProfileRecord{
			WorkspaceID:       "ws",
			ProjectID:         projectID,
			CurrentPhase:      "IMPLEMENTATION",
			RepoRequired:      true,
			RepoStatus:        "READY",
			RepoURL:           "file:///tmp/rq.git",
			UpdatedBy:         leadAgentID,
			RepoDefaultBranch: "main",
		},
		StrategicLead: &ProjectRoleRecord{
			WorkspaceID: "ws",
			ProjectID:   projectID,
			AgentID:     leadAgentID,
			RoleType:    "STRATEGIC_LEAD",
			Status:      "ACTIVE",
		},
		Tasks:            tasks,
		TaskCountsByLane: map[string]int{},
	}
}

func newProjectFanoutGateServer(t *testing.T, projects []ProjectRecord, coordinationByProject map[string]ProjectCoordinationRecord) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.list":
			writeRPCResult(w, req, map[string]any{"projects": projects})
		case "project.coordination.get":
			projectID, _ := req.Params["project_id"].(string)
			coordination, ok := coordinationByProject[strings.TrimSpace(projectID)]
			if !ok {
				t.Fatalf("unexpected project.coordination.get project_id %q", projectID)
			}
			writeRPCResult(w, req, map[string]any{"coordination": coordination})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
}
