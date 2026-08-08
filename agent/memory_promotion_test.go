package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDeriveTaskResultPromotionCandidateBuildsIncidentWithoutExplicitMemoryBody(t *testing.T) {
	candidate := deriveTaskResultPromotionCandidate(
		WorkspaceTaskRecord{TaskID: "task-1", Title: "Task One"},
		AgentSessionStateRecord{SessionID: "session-1"},
		"run-1",
		StructuredTaskResult{
			Outcome:     "blocked",
			Summary:     "Blocked on credential gate",
			Details:     "The target requires interactive OAuth.",
			NextAction:  "wait for authorization",
			BlockedOn:   []BlockedRef{{Kind: "credential", Detail: "Interactive login required"}},
			OwnerAction: "Complete the OAuth login",
			HumanReason: "Interactive credential flow is required",
		},
		localMemoryNodeBlocker,
		"tension-1",
		"cluster-1",
		[]string{"doc:artifact-1"},
		[]string{"task.task-1"},
	)
	if candidate == nil {
		t.Fatal("expected derived promotion candidate")
	}
	if candidate.MemoryType != "INCIDENT" {
		t.Fatalf("expected INCIDENT memory type, got %+v", candidate)
	}
	if !strings.Contains(candidate.Body, "Interactive login required") || !strings.Contains(candidate.Body, "task.task-1") {
		t.Fatalf("expected derived body to include blocker and doc context, got %q", candidate.Body)
	}
}

func TestFlushPendingMemoryPromotionsWritesMemoryAndClaim(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var memoryParams map[string]any
	var claimParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{
				{"task_id": "task-1", "status": "RUNNING", "project_id": "project-current"},
			}})
		case "workspace.memory.write":
			memoryParams = req.Params
			writeRPCResult(w, req, map[string]any{
				"memory": map[string]any{
					"memory_id":    rpcString(req.Params, "memory_id"),
					"workspace_id": "ws",
					"memory_type":  rpcString(req.Params, "memory_type"),
					"title":        rpcString(req.Params, "title"),
					"body":         rpcString(req.Params, "body"),
					"summary":      rpcString(req.Params, "summary"),
				},
			})
		case "workspace.claim.write":
			claimParams = req.Params
			writeRPCResult(w, req, map[string]any{"status": "ok"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	service := openTestAgentMemoryService(t, "ws", "agent-1")
	if err := service.queuePromotion(LocalPromotionCandidate{
		NodeType:       localMemoryNodeBlocker,
		MemoryType:     "INCIDENT",
		SourceID:       "run-1",
		Title:          "Credential gate",
		Body:           "Blocked by interactive login.",
		Summary:        "Blocked on credential gate",
		TaskID:         "task-1",
		SessionID:      "session-1",
		TensionID:      "tension-1",
		ProtoClusterID: "cluster-1",
		DocKeys:        []string{"task.task-1"},
		ArtifactRefs:   []string{"doc:artifact-1"},
	}); err != nil {
		t.Fatalf("queuePromotion() error = %v", err)
	}

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:           "ws",
			AgentID:               "agent-1",
			MaxPromotionSyncBatch: 4,
		},
		client: serverClient(server),
		memory: service,
	}

	if err := runtime.flushPendingMemoryPromotions(context.Background(), 4); err != nil {
		t.Fatalf("flushPendingMemoryPromotions() error = %v", err)
	}
	if memoryParams["memory_type"] != "INCIDENT" || memoryParams["source_kind"] != "local_memory_promotion" {
		t.Fatalf("unexpected memory write params: %+v", memoryParams)
	}
	if claimParams["claim_type"] != "INCIDENT" || claimParams["memory_id"] != memoryParams["memory_id"] {
		t.Fatalf("unexpected claim params: %+v", claimParams)
	}
	row := service.store.db.QueryRow("SELECT COUNT(*), COALESCE(promoted_at, ''), COALESCE(last_error, '') FROM local_memory_promotions")
	var count int
	var promotedAt, lastError string
	if err := row.Scan(&count, &promotedAt, &lastError); err != nil {
		t.Fatalf("query error = %v", err)
	}
	if count != 1 || promotedAt == "" || lastError != "" {
		t.Fatalf("expected 1 promoted promotion, got %d (promoted_at: %q, error: %q)", count, promotedAt, lastError)
	}
}

func TestFlushPendingMemoryPromotionsPersistsIDsAcrossRetry(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var memoryIDs []string
	var claimIDs []string
	claimCalls := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{
				{"task_id": "task-9", "status": "RUNNING", "project_id": "project-retry"},
			}})
		case "workspace.memory.write":
			memoryIDs = append(memoryIDs, rpcString(req.Params, "memory_id"))
			writeRPCResult(w, req, map[string]any{
				"memory": map[string]any{
					"memory_id":    rpcString(req.Params, "memory_id"),
					"workspace_id": "ws",
					"memory_type":  rpcString(req.Params, "memory_type"),
					"title":        rpcString(req.Params, "title"),
					"body":         rpcString(req.Params, "body"),
					"summary":      rpcString(req.Params, "summary"),
				},
			})
		case "workspace.claim.write":
			claimCalls++
			claimIDs = append(claimIDs, rpcString(req.Params, "claim_id"))
			if claimCalls == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"jsonrpc": "2.0",
					"id":      req.ID,
					"error": map[string]any{
						"code":    -32000,
						"message": "claim write failed",
					},
				})
				return
			}
			writeRPCResult(w, req, map[string]any{"status": "ok"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	service := openTestAgentMemoryService(t, "ws", "agent-2")
	if err := service.queuePromotion(LocalPromotionCandidate{
		NodeType:   localMemoryNodeDecision,
		MemoryType: "DECISION",
		SourceID:   "run-9",
		Title:      "Decision promotion",
		Body:       "Choose the proof-first path.",
		Summary:    "Decision recorded",
		TaskID:     "task-9",
		SessionID:  "session-9",
	}); err != nil {
		t.Fatalf("queuePromotion() error = %v", err)
	}

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:           "ws",
			AgentID:               "agent-2",
			MaxPromotionSyncBatch: 2,
		},
		client: serverClient(server),
		memory: service,
	}

	if err := runtime.flushPendingMemoryPromotions(context.Background(), 2); err == nil {
		t.Fatal("expected first flush to fail on claim write")
	}
	row := service.store.db.QueryRow("SELECT COALESCE(memory_id, ''), COALESCE(claim_id, ''), COALESCE(last_error, ''), COALESCE(promoted_at, ''), attempt_count FROM local_memory_promotions")
	var memoryID, claimID, lastError, promotedAt string
	var attemptCount int
	if err := row.Scan(&memoryID, &claimID, &lastError, &promotedAt, &attemptCount); err != nil {
		t.Fatalf("expected 1 row, err: %v", err)
	}
	if memoryID == "" || claimID == "" || lastError == "" || promotedAt != "" || attemptCount != 1 {
		t.Fatalf("unexpected failed promotion state: memoryID=%q, claimID=%q, lastError=%q, attempts=%d", memoryID, claimID, lastError, attemptCount)
	}

	if err := runtime.flushPendingMemoryPromotions(context.Background(), 2); err != nil {
		t.Fatalf("second flushPendingMemoryPromotions() error = %v", err)
	}
	row = service.store.db.QueryRow("SELECT COALESCE(last_error, ''), COALESCE(promoted_at, ''), attempt_count FROM local_memory_promotions")
	if err := row.Scan(&lastError, &promotedAt, &attemptCount); err != nil {
		t.Fatalf("second query err: %v", err)
	}
	if promotedAt == "" || lastError != "" || attemptCount != 2 {
		t.Fatalf("expected promotion to succeed on retry: promotedAt=%q, error=%q, attempts=%d", promotedAt, lastError, attemptCount)
	}
	if len(memoryIDs) != 2 || memoryIDs[0] == "" || memoryIDs[0] != memoryIDs[1] {
		t.Fatalf("expected stable memory_id across retries, got %+v", memoryIDs)
	}
	if len(claimIDs) != 2 || claimIDs[0] == "" || claimIDs[0] != claimIDs[1] {
		t.Fatalf("expected stable claim_id across retries, got %+v", claimIDs)
	}
}

func TestFlushPendingMemoryPromotionsSkipsTerminalTask(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{
				{"task_id": "task-old", "status": "CANCELLED", "project_id": "project-old"},
			}})
		case "workspace.memory.write", "workspace.claim.write":
			t.Fatalf("stale terminal promotion must not write canonical memory: %s %+v", req.Method, req.Params)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	service := openTestAgentMemoryService(t, "ws", "agent-3")
	if err := service.queuePromotion(LocalPromotionCandidate{
		NodeType:   localMemoryNodeBlocker,
		MemoryType: "INCIDENT",
		SourceID:   "run-old",
		Title:      "Old blocker",
		Body:       "Review lane blocked in a previous run.",
		Summary:    "Old blocked context",
		TaskID:     "task-old",
		SessionID:  "session-old",
	}); err != nil {
		t.Fatalf("queuePromotion() error = %v", err)
	}

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:           "ws",
			AgentID:               "agent-3",
			MaxPromotionSyncBatch: 1,
		},
		client: serverClient(server),
		memory: service,
	}

	if err := runtime.flushPendingMemoryPromotions(context.Background(), 1); err != nil {
		t.Fatalf("flushPendingMemoryPromotions() error = %v", err)
	}
	row := service.store.db.QueryRow("SELECT status, COALESCE(last_error, ''), COALESCE(promoted_at, ''), attempt_count FROM local_memory_promotions")
	var status, lastError, promotedAt string
	var attemptCount int
	if err := row.Scan(&status, &lastError, &promotedAt, &attemptCount); err != nil {
		t.Fatalf("query error = %v", err)
	}
	if status != localMemoryWriteStateSkipped || !strings.Contains(lastError, "terminal") || promotedAt != "" || attemptCount != 0 {
		t.Fatalf("expected skipped terminal promotion, status=%q error=%q promotedAt=%q attempts=%d", status, lastError, promotedAt, attemptCount)
	}
}

func TestFlushPendingMemoryPromotionsSkipsForeignActiveProject(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{
				{"task_id": "task-foreign", "status": "RUNNING", "project_id": "project-old"},
			}})
		case "workspace.memory.write", "workspace.claim.write":
			t.Fatalf("foreign project promotion must not write canonical memory: %s %+v", req.Method, req.Params)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	service := openTestAgentMemoryService(t, "ws", "agent-4")
	if err := service.queuePromotion(LocalPromotionCandidate{
		NodeType:   localMemoryNodeDecision,
		MemoryType: "DECISION",
		SourceID:   "run-foreign",
		Title:      "Foreign project decision",
		Body:       "Old project decision must not leak into the current run.",
		Summary:    "Foreign decision",
		TaskID:     "task-foreign",
		SessionID:  "session-foreign",
	}); err != nil {
		t.Fatalf("queuePromotion() error = %v", err)
	}

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:           "ws",
			AgentID:               "agent-4",
			MaxPromotionSyncBatch: 1,
		},
		client:     serverClient(server),
		memory:     service,
		activeTask: &WorkspaceTaskRecord{TaskID: "task-current", Status: "RUNNING", ProjectID: "project-current"},
		scratch: RuntimeScratchState{
			ActiveTaskID: "task-current",
		},
	}

	if err := runtime.flushPendingMemoryPromotions(context.Background(), 1); err != nil {
		t.Fatalf("flushPendingMemoryPromotions() error = %v", err)
	}
	row := service.store.db.QueryRow("SELECT status, COALESCE(last_error, ''), COALESCE(promoted_at, ''), attempt_count FROM local_memory_promotions")
	var status, lastError, promotedAt string
	var attemptCount int
	if err := row.Scan(&status, &lastError, &promotedAt, &attemptCount); err != nil {
		t.Fatalf("query error = %v", err)
	}
	if status != localMemoryWriteStateSkipped || !strings.Contains(lastError, "active project") || promotedAt != "" || attemptCount != 0 {
		t.Fatalf("expected skipped foreign promotion, status=%q error=%q promotedAt=%q attempts=%d", status, lastError, promotedAt, attemptCount)
	}
}

func serverClient(server *httptest.Server) *RhizomeClient {
	return NewRhizomeClient(server.URL, "token")
}
