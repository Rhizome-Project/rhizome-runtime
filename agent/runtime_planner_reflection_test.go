package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRuntimePlannerRepeatedFailureSelfPausesAndDocuments(t *testing.T) {
	var methods []string
	var savedScratch []string
	var docParams map[string]any
	var updateParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.state.set":
			savedScratch = append(savedScratch, rpcString(req.Params, "value"))
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			docParams = req.Params
			writeRPCResult(w, req, map[string]any{"sha": "sha-reflection"})
		case "agent.update.post":
			updateParams = req.Params
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %s params=%+v", req.Method, req.Params)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "gamma",
		},
		client:  NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	err := errors.New("rpc agent.task.claim: task claim project admission invalid: branch_id branch-1 is active on task task-revision")
	startedAt := time.Date(2026, 5, 7, 8, 30, 0, 0, time.UTC)

	for i := 0; i < plannerRepeatedFailureThreshold-1; i++ {
		handled, handleErr := runtime.handlePlannerRepeatedFailure(context.Background(), err, startedAt.Add(time.Duration(i)*time.Second))
		if handleErr != nil {
			t.Fatalf("handle repeated planner failure before threshold: %v", handleErr)
		}
		if handled {
			t.Fatalf("failure %d should not be handled before threshold", i+1)
		}
	}
	handled, handleErr := runtime.handlePlannerRepeatedFailure(context.Background(), err, startedAt.Add(3*time.Second))
	if handleErr != nil {
		t.Fatalf("handle repeated planner failure at threshold: %v", handleErr)
	}
	if !handled {
		t.Fatal("expected repeated planner failure to be handled")
	}
	if got := strings.Join(methods, ","); got != "agent.state.set,workspace.doc.put,agent.state.set,agent.update.post,agent.state.set" {
		t.Fatalf("unexpected method order: %s", got)
	}
	if !runtime.runtimePaused() {
		t.Fatalf("expected runtime to self-pause after repeated planner failures")
	}
	if rpcString(docParams, "doc_key") != "agent.gamma.planner_loop_reflection" || !strings.Contains(rpcString(docParams, "content"), "branch_id branch-1") {
		t.Fatalf("unexpected reflection doc params: %+v", docParams)
	}
	if rpcString(updateParams, "update_type") != "issue" || !strings.Contains(rpcString(updateParams, "summary"), "Planner self-paused") {
		t.Fatalf("unexpected reflection update params: %+v", updateParams)
	}
	lastScratch := savedScratch[len(savedScratch)-1]
	if !strings.Contains(lastScratch, `"control_paused":true`) || !strings.Contains(strings.Join(savedScratch, "\n"), "SYSTEM SELF-REFLECTION") {
		t.Fatalf("scratch did not record self-pause/advisory: %+v", savedScratch)
	}
}
