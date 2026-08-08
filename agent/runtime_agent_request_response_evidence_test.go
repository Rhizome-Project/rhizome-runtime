package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestHandleModelAskPublishesPublicResponseEvidence(t *testing.T) {
	var mu sync.Mutex
	var methods []string
	var respondParams map[string]any
	var docParams map[string]any
	var updateParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		mu.Lock()
		methods = append(methods, req.Method)
		mu.Unlock()

		switch req.Method {
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"agent": map[string]any{
					"agent_id":     "delta",
					"workspace_id": "ws",
					"display_name": "Delta",
					"role":         "reviewer",
					"status":       "IDLE",
				},
				"workspace": map[string]any{
					"workspace_id": "ws",
					"title":        "Coordination Workspace",
					"status":       "ACTIVE",
				},
				"snapshot": map[string]any{
					"workspace":        map[string]any{"workspace_id": "ws", "title": "Coordination Workspace", "status": "ACTIVE"},
					"docs":             []any{},
					"agents":           []any{},
					"sessions":         []any{},
					"tools":            []any{},
					"tasks":            []any{},
					"task_links":       []any{},
					"recent_memory":    []any{},
					"recent_artifacts": []any{},
					"recent_updates":   []any{},
					"recent_messages":  []any{},
					"projects":         []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "test-group", "daily_remaining": 1000, "weekly_remaining": 5000})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "workspace.instrumentation.control.report":
			writeRPCResult(w, req, map[string]any{"report": map[string]any{"workspace": map[string]any{}, "clusters": []any{}}})
		case "workspace.tension.frontier":
			writeRPCResult(w, req, map[string]any{"items": []any{}})
		case "agent.respond":
			mu.Lock()
			respondParams = req.Params
			mu.Unlock()
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			mu.Lock()
			docParams = req.Params
			mu.Unlock()
			writeRPCResult(w, req, map[string]any{"sha": "sha-public-response"})
		case "agent.update.post":
			mu.Lock()
			updateParams = req.Params
			mu.Unlock()
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected RPC method during model.ask evidence test: %s", req.Method)
		}
	}))
	defer server.Close()

	llm := &sequenceLLM{responses: []*LLMResponse{{Content: "Reviewer feedback for task-signal-loom-20260504T1732Z: revise the integration note before finalizing."}}}
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		RhizomeRPC:  server.URL,
		WorkspaceID: "ws",
		AgentID:     "delta",
		OwnerUserID: "owner",
	}, llm)
	t.Cleanup(func() { _ = runtime.Close() })

	err := runtime.handleRequest(context.Background(), AgentRequestRecord{
		RequestID:   "areq-public-review",
		WorkspaceID: "ws",
		FromAgentID: "beta",
		ToAgentID:   "delta",
		Method:      "model.ask",
		Payload:     `{"prompt":"Review branch evidence for task-signal-loom-20260504T1732Z and say whether Alpha can finalize."}`,
		CreatedAt:   "2026-05-04T19:21:00Z",
	})
	if err != nil {
		t.Fatalf("handleRequest() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if !containsAll(methods, []string{"agent.respond", "workspace.doc.put", "agent.update.post"}) {
		t.Fatalf("expected response evidence RPCs, got %#v", methods)
	}
	if got := rpcString(respondParams, "request_id"); got != "areq-public-review" {
		t.Fatalf("unexpected responded request_id %q", got)
	}
	if !strings.Contains(rpcString(respondParams, "response"), "Reviewer feedback") {
		t.Fatalf("expected LLM response to be returned, got %#v", respondParams)
	}

	docKey := rpcString(docParams, "doc_key")
	if !strings.HasPrefix(docKey, "task.task-signal-loom-20260504t1732z.agent_response.areq-public-review") {
		t.Fatalf("unexpected public response doc key %q", docKey)
	}
	if !strings.Contains(rpcString(docParams, "content"), "Reviewer feedback for task-signal-loom-20260504T1732Z") {
		t.Fatalf("expected response evidence document to contain the answer, got %#v", docParams)
	}

	summary := rpcString(updateParams, "summary")
	if !strings.Contains(summary, "task-signal-loom-20260504T1732Z") || !strings.Contains(summary, "doc_key="+docKey) {
		t.Fatalf("public update summary should carry task and doc references, got %q", summary)
	}
	if got := rpcString(updateParams, "update_type"); got != agentRequestResponseUpdateType {
		t.Fatalf("unexpected update type %q", got)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(rpcString(updateParams, "payload_json")), &payload); err != nil {
		t.Fatalf("decode public update payload: %v", err)
	}
	if payload["request_id"] != "areq-public-review" || payload["doc_key"] != docKey {
		t.Fatalf("unexpected public update payload %+v", payload)
	}
	taskIDs, _ := payload["task_ids"].([]any)
	if len(taskIDs) != 1 || taskIDs[0] != "task-signal-loom-20260504T1732Z" {
		t.Fatalf("expected task_ids to link update to task hydration, got %+v", payload["task_ids"])
	}
}

func TestPublishAgentRequestResponseEvidenceStillPostsUpdateWhenDocFails(t *testing.T) {
	var updateParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.doc.put":
			http.Error(w, "doc store unavailable", http.StatusInternalServerError)
		case "agent.update.post":
			updateParams = req.Params
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected RPC method during evidence failure test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "delta",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}

	err := runtime.publishAgentRequestResponseEvidence(context.Background(), AgentRequestRecord{
		RequestID:   "areq-doc-fail",
		WorkspaceID: "ws",
		FromAgentID: "beta",
		ToAgentID:   "delta",
		Method:      "model.ask",
		Payload:     `{"prompt":"Please review task-visible despite doc failure."}`,
	}, "Public summary still matters.")
	if err == nil || !strings.Contains(err.Error(), "workspace doc publish failed") {
		t.Fatalf("expected doc publish error, got %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(rpcString(updateParams, "payload_json")), &payload); err != nil {
		t.Fatalf("decode update payload: %v", err)
	}
	if payload["doc_status"] != "failed" || payload["request_id"] != "areq-doc-fail" {
		t.Fatalf("expected update to carry failed doc status, got %+v", payload)
	}
}

func TestAgentRequestResponseTaskIDsIgnoreWorkspaceDocSuffixes(t *testing.T) {
	request := AgentRequestRecord{
		RequestID: "areq-doc-suffixes",
		Payload: `{
			"prompt": "Use task-signal-loom-20260504T1732Z and docs task.task-signal-loom-20260504T1732Z.cycle_status plus task.task-signal-loom-20260504t1732z.agent_response.areq-1."
		}`,
	}
	got := agentRequestResponseTaskIDs(request, "Review task-signal-loom-20260504T1732Z only.")
	want := []string{"task-signal-loom-20260504T1732Z"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected only real task id, got %#v", got)
	}
}

func TestAgentRequestResponseTaskIDsIgnoreJSONFieldNames(t *testing.T) {
	request := AgentRequestRecord{
		RequestID: "areq-json-fields",
		Payload: `{
			"task_id": "task-causal-board-20260504T204721Z",
			"task_kind": "COORDINATION",
			"task_template": "integration",
			"task_submit": true
		}`,
	}
	got := agentRequestResponseTaskIDs(request, `{"response":"Use task-causal-board-20260504T204721Z only."}`)
	want := []string{"task-causal-board-20260504T204721Z"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected JSON field names to be ignored, got %#v", got)
	}
}
