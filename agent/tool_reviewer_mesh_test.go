package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReviewerRouteToolFailsClosedWhenScarcitySaturated(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"advisory":                      true,
				"level":                         2,
				"integrity_band":                "PARTIAL",
				"near_reviewer_status":          "OMITTED_REVIEWER_SCARCITY_SATURATED",
				"near_reviewer_basis":           "saturated_reviewer_mesh",
				"near_reviewer_fallback_reason": "reviewer_scarcity_saturated",
				"far_reviewer_status":           "OMITTED_REVIEWER_SCARCITY_SATURATED",
				"far_reviewer_basis":            "saturated_reviewer_mesh",
				"warnings":                      []string{"reviewer_scarcity_saturated"},
				"reasons":                       []string{"reviewer_scarcity_saturated"},
			},
		})
	}))
	defer server.Close()

	tool := NewReviewerRouteTool(NewRhizomeClient(server.URL, ""), "ws-test", "agent-gen")
	result := tool.Execute(context.Background(), map[string]any{
		"available_reviewers":     []any{"reviewer-a"},
		"is_multi_patch":          false,
		"impact_score":            0.2,
		"contradiction_pressure":  0.1,
		"has_active_dissent":      false,
		"touches_hard_constraint": false,
		"cluster_mode":            "explore",
		"merge_risk":              0.2,
	})
	if result == nil {
		t.Fatal("expected tool result")
	}
	if !result.IsError {
		t.Fatalf("expected scarcity-saturated route to fail closed, got %+v", result)
	}
	if !strings.Contains(result.Output, "Reviewer route deferred: reviewer mesh is saturated") {
		t.Fatalf("expected saturation deferral message, got %q", result.Output)
	}
	if !strings.Contains(result.Output, "\"near_reviewer_status\": \"OMITTED_REVIEWER_SCARCITY_SATURATED\"") {
		t.Fatalf("expected tool output to include saturated route payload, got %q", result.Output)
	}
}

func reviewerMeshJSONRPCErrorServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"error":   map[string]any{"code": -32000, "message": "mesh transiently unavailable"},
		})
	}))
}

func TestReviewerRouteToolEnrichesOpaqueRoutingError(t *testing.T) {
	server := reviewerMeshJSONRPCErrorServer(t)
	defer server.Close()
	tool := NewReviewerRouteTool(NewRhizomeClient(server.URL, ""), "ws-test", "agent-gen")
	result := tool.Execute(context.Background(), map[string]any{
		"available_reviewers":     []any{"reviewer-a"},
		"is_multi_patch":          false,
		"impact_score":            0.2,
		"contradiction_pressure":  0.1,
		"has_active_dissent":      false,
		"touches_hard_constraint": false,
		"cluster_mode":            "explore",
		"merge_risk":              0.2,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected routing failure to be an error result, got %+v", result)
	}
	for _, want := range []string{"available_reviewers", "reviewer_scarcity", "retry", "blocker"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected enriched routing-error guidance to contain %q, got %q", want, result.Output)
		}
	}
}

func TestReviewerScarcityToolEnrichesOpaqueSnapshotError(t *testing.T) {
	server := reviewerMeshJSONRPCErrorServer(t)
	defer server.Close()
	tool := NewReviewerScarcityTool(NewRhizomeClient(server.URL, ""), "ws-test", "agent-gen")
	result := tool.Execute(context.Background(), map[string]any{})
	if result == nil || !result.IsError {
		t.Fatalf("expected scarcity read failure to be an error result, got %+v", result)
	}
	for _, want := range []string{"unknown, not zero", "defer", "retry"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected enriched scarcity-error guidance to contain %q, got %q", want, result.Output)
		}
	}
}

func TestReviewerRouteToolKeepsSuccessfulRouteWhenScarcityNotSaturated(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"advisory":             true,
				"level":                2,
				"integrity_band":       "PARTIAL",
				"near_reviewer_status": "SELECTED_COLLABORATION_EVIDENCE",
				"near_reviewer":        "reviewer-a",
				"warnings":             []string{"reviewer_collaboration_workspace_scoped"},
				"reasons":              []string{"near_reviewer_collaboration_observed"},
			},
		})
	}))
	defer server.Close()

	tool := NewReviewerRouteTool(NewRhizomeClient(server.URL, ""), "ws-test", "agent-gen")
	result := tool.Execute(context.Background(), map[string]any{
		"available_reviewers":     []any{"reviewer-a"},
		"is_multi_patch":          false,
		"impact_score":            0.2,
		"contradiction_pressure":  0.1,
		"has_active_dissent":      false,
		"touches_hard_constraint": false,
		"cluster_mode":            "explore",
		"merge_risk":              0.2,
	})
	if result == nil {
		t.Fatal("expected tool result")
	}
	if result.IsError {
		t.Fatalf("expected non-saturated route to remain successful, got %+v", result)
	}
	if !strings.Contains(result.Output, "Reviewer route requested successfully") {
		t.Fatalf("expected success message, got %q", result.Output)
	}
	// SG-RM-1: success result must carry the routing-receipt next-step contract.
	for _, want := range []string{"ROUTING RECEIPT", "do NOT re-route", "Await the reviewer verdict"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected reviewer-route success to contain next-step contract %q, got %q", want, result.Output)
		}
	}
	if !strings.Contains(result.Output, "\"near_reviewer\": \"reviewer-a\"") {
		t.Fatalf("expected concrete reviewer in successful route output, got %q", result.Output)
	}
}

func TestReviewerScarcityToolFailsClosedWhenSnapshotIsSaturated(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"workspace_id":       "ws-test",
				"status":             "SATURATED",
				"integrity_band":     "PARTIAL",
				"active_assignments": 4,
				"reasons":            []string{"reviewer_load_highly_concentrated"},
			},
		})
	}))
	defer server.Close()

	tool := NewReviewerScarcityTool(NewRhizomeClient(server.URL, ""), "ws-test", "agent-gen")
	result := tool.Execute(context.Background(), map[string]any{})
	if result == nil {
		t.Fatal("expected tool result")
	}
	if !result.IsError {
		t.Fatalf("expected saturated scarcity snapshot to fail closed, got %+v", result)
	}
	if !strings.Contains(result.Output, "Reviewer scarcity is saturated; defer new reviewer admission work.") {
		t.Fatalf("expected saturated scarcity message, got %q", result.Output)
	}
	if !strings.Contains(result.Output, "\"status\": \"SATURATED\"") {
		t.Fatalf("expected saturated scarcity payload in output, got %q", result.Output)
	}
}

func TestReviewerScarcityToolKeepsSuccessfulSnapshotWhenNotSaturated(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req["id"],
			"result": map[string]any{
				"workspace_id":   "ws-test",
				"status":         "SCARCE",
				"integrity_band": "PARTIAL",
				"reasons":        []string{"reviewer_load_highly_concentrated"},
			},
		})
	}))
	defer server.Close()

	tool := NewReviewerScarcityTool(NewRhizomeClient(server.URL, ""), "ws-test", "agent-gen")
	result := tool.Execute(context.Background(), map[string]any{})
	if result == nil {
		t.Fatal("expected tool result")
	}
	if result.IsError {
		t.Fatalf("expected non-saturated scarcity snapshot to remain successful, got %+v", result)
	}
	if !strings.Contains(result.Output, "Reviewer scarcity snapshot:") {
		t.Fatalf("expected success message, got %q", result.Output)
	}
	if !strings.Contains(result.Output, "\"status\": \"SCARCE\"") {
		t.Fatalf("expected scarcity payload in output, got %q", result.Output)
	}
}
