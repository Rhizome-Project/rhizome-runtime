package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func rpcStringSlice(params map[string]any, key string) []string {
	raw, ok := params[key]
	if !ok {
		return nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if str, ok := item.(string); ok {
			out = append(out, str)
		}
	}
	return out
}

func TestTaskHydrationDoesNotReuseScopeMismatchedCache(t *testing.T) {
	var hydrateCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task.hydrate":
			hydrateCalls++
			docKeys := rpcStringSlice(req.Params, "doc_keys")
			if !containsTrimmed(docKeys, "task.task-1") {
				t.Fatalf("expected canonical hydration to request task-scoped doc key, got %+v", docKeys)
			}
			if !containsTrimmed(docKeys, "task.task-1.evidence_gap") {
				t.Fatalf("expected canonical hydration to request task evidence gap doc key, got %+v", docKeys)
			}
			if !containsTrimmed(docKeys, "task.task-1.artifact_reality_check") {
				t.Fatalf("expected canonical hydration to request task artifact reality doc key, got %+v", docKeys)
			}
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"generated_at": "2026-03-23T11:00:00Z",
					"task":         map[string]any{"task_id": "task-1"},
					"docs": []any{
						map[string]any{"doc_key": "task.task-1", "title": "Task 1"},
						map[string]any{"doc_key": "current_context", "title": "Context"},
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	includeAllDocs := boolPtr(false)
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
		},
		client: NewRhizomeClient(server.URL, "token"),
		activeHydration: &TaskHydrationBundle{
			Task: TaskStatus{TaskID: "task-1"},
			Docs: []WorkspaceDocRecord{{DocKey: "current_context", Title: "Context"}},
		},
		hydrationScope: hydrationScopeKey(TaskHydrationInput{
			WorkspaceID:      "ws",
			TaskID:           "task-1",
			DocKeys:          defaultHydrationDocKeys(""),
			IncludeAllDocs:   includeAllDocs,
			UpdatesLimit:     10,
			ArtifactLimit:    10,
			RelatedTaskLimit: 10,
		}),
		hydrationAt: time.Now().UTC(),
	}

	task := &WorkspaceTaskRecord{TaskID: "task-1"}

	bundle := runtime.taskHydration(context.Background(), task)
	if bundle == nil {
		t.Fatal("expected hydration bundle")
	}
	if hydrateCalls != 1 {
		t.Fatalf("expected scope mismatch to force one hydrate call, got %d", hydrateCalls)
	}
	if !containsTrimmed(hydrationDocKeys(bundle), "task.task-1") {
		t.Fatalf("expected refreshed hydration docs to include task-scoped doc, got %+v", hydrationDocKeys(bundle))
	}

	bundle = runtime.taskHydration(context.Background(), task)
	if bundle == nil {
		t.Fatal("expected cached hydration bundle")
	}
	if hydrateCalls != 1 {
		t.Fatalf("expected canonical hydration cache reuse after refresh, got %d calls", hydrateCalls)
	}
}

func TestTaskHydrationRequestsDocKeysMentionedByTask(t *testing.T) {
	var gotDocKeys []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task.hydrate":
			gotDocKeys = rpcStringSlice(req.Params, "doc_keys")
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"generated_at": "2026-03-23T11:00:00Z",
					"task":         map[string]any{"task_id": "task-1"},
					"docs": []any{
						map[string]any{"doc_key": "pilot.code-error.kvparser.spec.20260426-082139", "title": "Spec"},
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws"},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := &WorkspaceTaskRecord{
		TaskID:      "task-1",
		Title:       "Implement parser",
		Description: "Read workspace doc pilot.code-error.kvparser.spec.20260426-082139 and write pilot.code-error.kvparser.alpha.20260426-082139.",
	}

	if bundle := runtime.taskHydration(context.Background(), task); bundle == nil {
		t.Fatal("expected hydration bundle")
	}
	for _, want := range []string{
		"task.task-1",
		"task.task-1.result",
		"task.task-1.evidence_gap",
		"pilot.code-error.kvparser.spec.20260426-082139",
		"pilot.code-error.kvparser.alpha.20260426-082139",
	} {
		if !containsTrimmed(gotDocKeys, want) {
			t.Fatalf("expected hydration doc_keys to contain %q, got %+v", want, gotDocKeys)
		}
	}
}

func TestTaskHydrationRefreshesAfterPendingTrigger(t *testing.T) {
	var hydrateCalls int
	var stateSetCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task.hydrate":
			hydrateCalls++
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"generated_at": "2026-03-23T11:00:00Z",
					"task":         map[string]any{"task_id": "task-1"},
					"docs": []any{
						map[string]any{"doc_key": "task.task-1", "title": "Task 1"},
						map[string]any{"doc_key": "current_context", "title": "Context"},
					},
				},
			})
		case "agent.state.set":
			stateSetCalls++
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
		},
		client:  NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	task := &WorkspaceTaskRecord{TaskID: "task-1"}

	if bundle := runtime.taskHydration(context.Background(), task); bundle == nil {
		t.Fatal("expected initial hydration bundle")
	}
	if hydrateCalls != 1 {
		t.Fatalf("expected first hydration fetch, got %d calls", hydrateCalls)
	}

	if err := runtime.setPendingWorkTrigger(context.Background(), "system_news", "task-1", "sess-1"); err != nil {
		t.Fatalf("setPendingWorkTrigger() error: %v", err)
	}
	if stateSetCalls == 0 {
		t.Fatal("expected pending trigger to persist scratch state")
	}

	if bundle := runtime.taskHydration(context.Background(), task); bundle == nil {
		t.Fatal("expected hydration bundle after pending trigger")
	}
	if hydrateCalls != 2 {
		t.Fatalf("expected pending trigger to force hydration refresh, got %d calls", hydrateCalls)
	}
}

func TestTaskHydrationRefreshesAfterTTLExpiry(t *testing.T) {
	var hydrateCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task.hydrate":
			hydrateCalls++
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"generated_at": "2026-03-23T11:00:00Z",
					"task":         map[string]any{"task_id": "task-1"},
					"docs": []any{
						map[string]any{"doc_key": "task.task-1", "title": "Task 1"},
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := &WorkspaceTaskRecord{TaskID: "task-1"}

	if bundle := runtime.taskHydration(context.Background(), task); bundle == nil {
		t.Fatal("expected initial hydration bundle")
	}
	if hydrateCalls != 1 {
		t.Fatalf("expected first hydration fetch, got %d calls", hydrateCalls)
	}

	runtime.mu.Lock()
	runtime.hydrationAt = time.Now().UTC().Add(-runtimeHydrationTTL - time.Second)
	runtime.mu.Unlock()

	if bundle := runtime.taskHydration(context.Background(), task); bundle == nil {
		t.Fatal("expected hydration bundle after ttl expiry")
	}
	if hydrateCalls != 2 {
		t.Fatalf("expected ttl expiry to force hydration refresh, got %d calls", hydrateCalls)
	}
}

func TestTaskHydrationRefreshesAfterExplicitStaleMark(t *testing.T) {
	var hydrateCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task.hydrate":
			hydrateCalls++
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"generated_at": "2026-03-23T11:00:00Z",
					"task":         map[string]any{"task_id": "task-1"},
					"docs": []any{
						map[string]any{"doc_key": "task.task-1", "title": "Task 1"},
					},
				},
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	task := &WorkspaceTaskRecord{TaskID: "task-1"}

	if bundle := runtime.taskHydration(context.Background(), task); bundle == nil {
		t.Fatal("expected initial hydration bundle")
	}
	if hydrateCalls != 1 {
		t.Fatalf("expected first hydration fetch, got %d calls", hydrateCalls)
	}

	runtime.markHydrationStale()

	if bundle := runtime.taskHydration(context.Background(), task); bundle == nil {
		t.Fatal("expected hydration bundle after explicit stale mark")
	}
	if hydrateCalls != 2 {
		t.Fatalf("expected explicit stale mark to force hydration refresh, got %d calls", hydrateCalls)
	}
}
