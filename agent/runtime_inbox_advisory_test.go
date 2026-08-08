package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRefreshInboxDrainAdvisoryPersistsOpenPeerRequests(t *testing.T) {
	var saved RuntimeScratchState
	methods := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.request.open.list":
			writeRPCResult(w, req, map[string]any{
				"requests": []map[string]any{
					{
						"request_id":    "req-release-1",
						"workspace_id":  "ws-1",
						"from_agent_id": "eta",
						"to_agent_id":   "delta",
						"method":        "model.ask",
						"payload":       "Please publish or release the overlapping evaluator lane.",
						"status":        "PENDING",
					},
				},
				"count": 1,
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

	runtime := NewRuntime(RuntimeConfig{
		Mode:               RuntimeModeDaemon,
		Workdir:            t.TempDir(),
		RhizomeRPC:         server.URL,
		RhizomeToken:       "token",
		WorkspaceID:        "ws-1",
		AgentID:            "delta",
		InboxDrainAdvisory: true,
	}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch = RuntimeScratchState{DocSHAs: map[string]string{}}
	t.Cleanup(func() { _ = runtime.Close() })

	changed, err := runtime.refreshInboxDrainAdvisory(context.Background())
	if err != nil {
		t.Fatalf("refreshInboxDrainAdvisory() error = %v", err)
	}
	if !changed {
		t.Fatal("expected advisory change")
	}
	if got := strings.Join(methods, ","); got != "agent.request.open.list,agent.state.set" {
		t.Fatalf("unexpected methods: %s", got)
	}
	joined := strings.Join(saved.AdvisorySignals, "\n")
	for _, want := range []string{"SYSTEM INBOX DRAIN", "req-release-1", "eta", "publish or release"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected advisory to contain %q, got %+v", want, saved.AdvisorySignals)
		}
	}
}

func TestRefreshInboxDrainAdvisoryDedupesStableOpenRequestList(t *testing.T) {
	stateSetCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.request.open.list":
			writeRPCResult(w, req, map[string]any{
				"requests": []map[string]any{
					{"request_id": "req-1", "workspace_id": "ws-1", "from_agent_id": "beta", "to_agent_id": "alpha", "method": "model.ask", "payload": "status?", "status": "PENDING"},
				},
				"count": 1,
			})
		case "agent.state.set":
			stateSetCalls++
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{Mode: RuntimeModeDaemon, Workdir: t.TempDir(), RhizomeRPC: server.URL, RhizomeToken: "token", WorkspaceID: "ws-1", AgentID: "alpha", InboxDrainAdvisory: true}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.scratch = RuntimeScratchState{DocSHAs: map[string]string{}}
	t.Cleanup(func() { _ = runtime.Close() })

	changed, err := runtime.refreshInboxDrainAdvisory(context.Background())
	if err != nil || !changed {
		t.Fatalf("first refresh changed=%v err=%v", changed, err)
	}
	changed, err = runtime.refreshInboxDrainAdvisory(context.Background())
	if err != nil {
		t.Fatalf("second refresh error = %v", err)
	}
	if changed {
		t.Fatal("expected identical open request list to dedupe")
	}
	if stateSetCalls != 1 {
		t.Fatalf("expected one state save, got %d", stateSetCalls)
	}
}
