package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestEnsureStrategicLeadLeaseForTaskClaimsExpiredLead(t *testing.T) {
	var methods []string
	expiresAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "project.coordination.get":
			if rpcString(req.Params, "workspace_id") != "ws-1" || rpcString(req.Params, "project_id") != "project-1" {
				t.Fatalf("unexpected coordination params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"snapshot_at":           time.Now().UTC().Format(time.RFC3339Nano),
					"coordination_version":  "test",
					"open_task_count":       1,
					"task_counts_by_lane":   map[string]int{"strategy": 1},
					"task_counts_by_status": map[string]int{"RUNNING": 1},
					"project": map[string]any{
						"workspace_id": "ws-1",
						"project_id":   "project-1",
						"title":        "Project One",
						"status":       "ACTIVE",
					},
					"strategic_lead": map[string]any{
						"role_id":          "role-old",
						"workspace_id":     "ws-1",
						"project_id":       "project-1",
						"agent_id":         "delta",
						"role_type":        "STRATEGIC_LEAD",
						"status":           "ACTIVE",
						"lease_expires_at": expiresAt,
					},
				},
			})
		case "project.lead.claim":
			if rpcString(req.Params, "workspace_id") != "ws-1" || rpcString(req.Params, "project_id") != "project-1" {
				t.Fatalf("unexpected claim params: %+v", req.Params)
			}
			if rpcString(req.Params, "actor_id") != "alpha" || rpcString(req.Params, "agent_id") != "alpha" {
				t.Fatalf("expected alpha to claim strategic lead, got %+v", req.Params)
			}
			got, _ := req.Params["lease_seconds"].(float64)
			if got != float64(defaultProjectLeadLeaseSeconds) {
				t.Fatalf("expected default lease seconds, got %+v params=%+v", got, req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"role": map[string]any{
					"role_id":          "role-alpha",
					"workspace_id":     "ws-1",
					"project_id":       "project-1",
					"agent_id":         "alpha",
					"role_type":        "STRATEGIC_LEAD",
					"status":           "ACTIVE",
					"lease_expires_at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
				},
			})
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "alpha",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	t.Cleanup(func() { _ = runtime.Close() })

	err := runtime.ensureStrategicLeadLeaseForTask(context.Background(), WorkspaceTaskRecord{
		TaskID:      "task-root",
		Title:       "Project strategy",
		Status:      "RUNNING",
		TaskKind:    "COORDINATION",
		ProjectID:   "project-1",
		ProjectLane: "strategy",
	})
	if err != nil {
		t.Fatalf("ensureStrategicLeadLeaseForTask() error = %v", err)
	}
	expected := []string{"project.coordination.get", "project.lead.claim"}
	if !reflect.DeepEqual(methods, expected) {
		t.Fatalf("unexpected methods: %#v", methods)
	}
}

func TestEnsureStrategicLeadLeaseForTaskIgnoresImplementationLane(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("implementation lane should not touch project lead lease")
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "beta",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	t.Cleanup(func() { _ = runtime.Close() })

	err := runtime.ensureStrategicLeadLeaseForTask(context.Background(), WorkspaceTaskRecord{
		TaskID:      "task-build",
		Title:       "Implement product",
		Status:      "PENDING",
		TaskKind:    "EXECUTION",
		ProjectID:   "project-1",
		ProjectLane: "implementation",
	})
	if err != nil {
		t.Fatalf("ensureStrategicLeadLeaseForTask() error = %v", err)
	}
}

func TestRuntimeTaskCanStewardProjectLeadUsesAutonomousRootFallback(t *testing.T) {
	if !runtimeTaskCanStewardProjectLead(WorkspaceTaskRecord{
		TaskKind:     "COORDINATION",
		TaskTemplate: "integration",
		Title:        "Autonomous coordination",
		Description:  "Create any needed subtasks and coordinate builders, reviewer, and integration.",
	}) {
		t.Fatal("expected autonomous root task to be eligible for project lead stewardship")
	}
	if runtimeTaskCanStewardProjectLead(WorkspaceTaskRecord{
		TaskKind:    "EXECUTION",
		ProjectLane: "implementation",
		Title:       "Implement feature",
		Description: strings.Repeat("implementation ", 3),
	}) {
		t.Fatal("implementation task must not be eligible for project lead stewardship")
	}
}

func TestRuntimeTaskCanStewardProjectLeadAllowsCanonicalRoleScopeRequest(t *testing.T) {
	if !runtimeTaskCanStewardProjectLead(WorkspaceTaskRecord{
		TaskID:               "task-role-scope-gamma",
		TaskKind:             "COORDINATION",
		ProjectID:            "project-1",
		ProjectLane:          "coordination",
		Title:                "Rebalance gamma authority",
		Description:          "Fresh agent phrasing without canonical role/scope literals.",
		Tags:                 []string{"project-role-scope", "strategic-lead", "coordination"},
		TaskRequirementsJSON: `{"schema":"project_role_scope_authority_transition.v1","required_transition":"project_role_assign"}`,
	}) {
		t.Fatal("structured role/scope request should be eligible for strategic lead lease stewardship")
	}
	if runtimeTaskCanStewardProjectLead(WorkspaceTaskRecord{
		TaskID:               "task-role-scope-random",
		TaskKind:             "COORDINATION",
		ProjectID:            "project-1",
		ProjectLane:          "coordination",
		Title:                "Informal role discussion",
		Description:          "This has a tag and old prose, but no structural authority-transition stamp.",
		Tags:                 []string{"project-role-scope"},
		TaskRequirementsJSON: `{"schema":"task_requirements.v1"}`,
	}) {
		t.Fatal("unstamped role/scope tag should not be eligible for lead stewardship")
	}
}
