package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServiceVentureToolsRegisteredAndAmbientAllowed(t *testing.T) {
	agent := &Agent{
		Client:      NewRhizomeClient("http://127.0.0.1/rpc", "token"),
		WorkspaceID: "ws-service",
		AgentID:     "agent-service",
	}
	names := agent.baseToolNames()
	for _, name := range []string{"service_direction_upsert", "service_direction_get", "service_candidate_upsert", "service_candidate_get", "service_run_start", "service_run_get", "service_outcome_record", "service_coordination_get"} {
		if _, ok := names[name]; !ok {
			t.Fatalf("expected registered service venture tool %s", name)
		}
	}
	for _, name := range []string{"service_direction_upsert", "service_candidate_upsert", "service_run_start", "service_run_update", "service_approval_grant", "service_resource_record", "service_spend_record", "service_revenue_record", "service_outcome_record"} {
		if ambientAutonomyToolAllowed(name) {
			t.Fatalf("expected ambient autonomy to block service governance/outcome mutation tool %s", name)
		}
	}
	for _, name := range []string{"service_direction_list", "service_direction_get", "service_candidate_list", "service_candidate_get", "service_run_list", "service_run_get", "service_coordination_get"} {
		if !ambientAutonomyToolAllowed(name) {
			t.Fatalf("expected ambient autonomy to allow service read/context tool %s", name)
		}
	}
	for _, name := range []string{"project_bootstrap", "project_role_assign", "project_phase_transition"} {
		if ambientAutonomyToolAllowed(name) {
			t.Fatalf("expected ambient autonomy to block durable project authority tool %s", name)
		}
	}
}

func TestServiceVentureToolInjectsWorkspaceAndActor(t *testing.T) {
	var gotMethod string
	var gotParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		gotMethod = req.Method
		raw, err := json.Marshal(req.Params)
		if err != nil {
			t.Fatalf("marshal params: %v", err)
		}
		if err := json.Unmarshal(raw, &gotParams); err != nil {
			t.Fatalf("decode params: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"rpc-test","result":{"direction":{"direction_id":"direction-tools"}}}`))
	}))
	defer server.Close()

	client := NewRhizomeClient(server.URL, "token")
	tool := NewServiceVentureTool(client, "ws-service", "agent-service", "service_direction_upsert", "service.direction.upsert", "test", serviceProps(serviceString("title", "title")), []string{"title"}, true)
	result := tool.Execute(context.Background(), map[string]any{"title": "Small web tools"})
	if result == nil || result.IsError {
		t.Fatalf("service tool execute failed: %+v", result)
	}
	if gotMethod != "service.direction.upsert" {
		t.Fatalf("method = %q", gotMethod)
	}
	if gotParams["workspace_id"] != "ws-service" || gotParams["actor_id"] != "agent-service" || gotParams["title"] != "Small web tools" {
		t.Fatalf("unexpected params %+v", gotParams)
	}
	if !strings.Contains(result.Output, "direction-tools") {
		t.Fatalf("unexpected output %q", result.Output)
	}
}
