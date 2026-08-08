package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBacklogDuplicateDoesNotReopenClosedStates(t *testing.T) {
	store := newAgentBacklogTestStore(t, "agent-closed")
	at := time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC)

	promoted := mustUpsertBacklog(t, store, "visual:promoted", "Promoted issue", 40)
	if err := store.MarkBacklogItemPromoted(promoted.ItemID, []string{"task:stable"}, at); err != nil {
		t.Fatal(err)
	}
	if err := store.SuppressBacklogItem(promoted.ItemID, at.Add(time.Hour), "should not reopen", at); err == nil {
		t.Fatal("expected suppressing a promoted item to be rejected")
	}
	mustUpsertBacklog(t, store, "visual:promoted", "Promoted issue duplicate", 90)
	assertBacklogStatus(t, store, "visual:promoted", "promoted")
	item := findBacklogByDedup(t, store, "visual:promoted")
	if item.Score != 90 || !containsTrimmedString(item.PromotionRefs, "task:stable") {
		t.Fatalf("duplicate should merge stronger signal without reopening or losing promotion refs, got %+v", item)
	}

	suppressed := mustUpsertBacklog(t, store, "visual:suppressed", "Suppressed issue", 60)
	if err := store.SuppressBacklogItem(suppressed.ItemID, at.Add(time.Hour), "cooldown", at); err != nil {
		t.Fatal(err)
	}
	mustUpsertBacklog(t, store, "visual:suppressed", "Suppressed issue duplicate", 80)
	assertBacklogStatus(t, store, "visual:suppressed", "suppressed")

	completed := mustUpsertBacklog(t, store, "visual:completed", "Completed issue", 60)
	if err := store.CompleteBacklogItem(completed.ItemID, []string{"evidence:fixed"}, "fixed", at); err != nil {
		t.Fatal(err)
	}
	if err := store.SuppressBacklogItem(completed.ItemID, at.Add(time.Hour), "should not reopen", at); err == nil {
		t.Fatal("expected suppressing a completed item to be rejected")
	}
	mustUpsertBacklog(t, store, "visual:completed", "Completed issue duplicate", 80)
	assertBacklogStatus(t, store, "visual:completed", "completed")

	stale := mustUpsertBacklog(t, store, "visual:stale", "Stale issue", 60)
	if err := store.MarkBacklogItemPromoted(stale.ItemID, []string{"task:old"}, at); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkBacklogItemsStaleByPromotionRef("task:old", "superseded", at); err != nil {
		t.Fatal(err)
	}
	if err := store.SuppressBacklogItem(stale.ItemID, at.Add(time.Hour), "should not reopen", at); err == nil {
		t.Fatal("expected suppressing a stale item to be rejected")
	}
	mustUpsertBacklog(t, store, "visual:stale", "Stale issue duplicate", 80)
	assertBacklogStatus(t, store, "visual:stale", "stale")
}

func TestBacklogUpsertDerivesStableDedupKeyAndMergesScore(t *testing.T) {
	store := newAgentBacklogTestStore(t, "agent-dedup")
	first, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		HeartbeatID:  "visual_audit",
		Kind:         "visual_finding",
		Title:        "Hero text overlaps controls",
		Summary:      "The hero line collides with the primary action.",
		Score:        25,
		EvidenceRefs: []string{"screenshot:first"},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		HeartbeatID:  "visual_audit",
		Kind:         "visual_finding",
		Title:        "Hero text overlaps controls",
		Summary:      "The hero line collides with the primary action.",
		Score:        85,
		EvidenceRefs: []string{"screenshot:second"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.ItemID != second.ItemID || first.DedupKey == "" {
		t.Fatalf("expected stable derived dedup, first=%+v second=%+v", first, second)
	}
	state := store.Snapshot()
	if len(state.Backlog) != 1 {
		t.Fatalf("expected one merged backlog item, got %+v", state.Backlog)
	}
	item := state.Backlog[0]
	if item.Score != 85 || item.SeenCount != 2 || len(item.EvidenceRefs) != 2 {
		t.Fatalf("expected merged score/evidence/seen count, got %+v", item)
	}
	if _, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{}); err == nil {
		t.Fatal("expected empty backlog item to be rejected instead of generating an unstable dedup key")
	}
}

func TestBacklogPromotionCandidatesFilterAndSort(t *testing.T) {
	store := newAgentBacklogTestStore(t, "agent-candidates")
	now := time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC)
	low := mustUpsertBacklog(t, store, "candidate:low", "Low score", 20)
	_ = low
	high := mustUpsertBacklog(t, store, "candidate:high", "High score", 95)
	expired := mustUpsertBacklog(t, store, "candidate:expired", "Expired suppression", 80)
	if err := store.SuppressBacklogItem(expired.ItemID, now.Add(-time.Minute), "retry later", now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	future := mustUpsertBacklog(t, store, "candidate:future", "Future suppression", 100)
	if err := store.SuppressBacklogItem(future.ItemID, now.Add(time.Hour), "cooldown", now); err != nil {
		t.Fatal(err)
	}
	promoted := mustUpsertBacklog(t, store, "candidate:promoted", "Promoted", 99)
	if err := store.MarkBacklogItemPromoted(promoted.ItemID, []string{"task:promoted"}, now); err != nil {
		t.Fatal(err)
	}
	completed := mustUpsertBacklog(t, store, "candidate:completed", "Completed", 98)
	if err := store.CompleteBacklogItem(completed.ItemID, nil, "done", now); err != nil {
		t.Fatal(err)
	}

	candidates := store.ListBacklogPromotionCandidates(10, 50, now)
	if len(candidates) != 2 {
		t.Fatalf("expected two candidates, got %+v", candidates)
	}
	if candidates[0].ItemID != high.ItemID || candidates[1].ItemID != expired.ItemID {
		t.Fatalf("unexpected candidate ordering/filtering: %+v", candidates)
	}
}

func TestBacklogSuppressUntilExcludesThenReincludesAfterExpiry(t *testing.T) {
	store := newAgentBacklogTestStore(t, "agent-suppress")
	now := time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC)
	item := mustUpsertBacklog(t, store, "suppress:one", "Temporarily noisy", 90)
	if err := store.SuppressBacklogItem(item.ItemID, now.Add(time.Hour), "wait for evidence", now); err != nil {
		t.Fatal(err)
	}
	if got := store.ListBacklogPromotionCandidates(10, 0, now); len(got) != 0 {
		t.Fatalf("suppressed item should be excluded, got %+v", got)
	}
	got := store.ListBacklogPromotionCandidates(10, 0, now.Add(2*time.Hour))
	if len(got) != 1 || got[0].ItemID != item.ItemID {
		t.Fatalf("expired suppression should re-enter candidates, got %+v", got)
	}
}

func TestBacklogPromoteCreatesDeterministicTaskAndDocOnce(t *testing.T) {
	store := newAgentBacklogTestStore(t, "agent-promote")
	item := mustUpsertBacklog(t, store, "promote:visual", "Fix obvious visual regression", 92)
	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	client := NewRhizomeClient(server.URL, "")
	at := time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC)

	first, err := store.PromoteBacklogItem(context.Background(), client, item.ItemID, AgentBacklogPromotionTarget{
		ProjectID:   "project-ui",
		ProjectLane: "review",
		Reason:      "visual critic found a real-user failure",
	}, at)
	if err != nil {
		t.Fatal(err)
	}
	if first.TaskID == "" || first.DocKey != "task."+first.TaskID {
		t.Fatalf("unexpected promotion contract: %+v", first)
	}
	if server.putDocCount() != 1 || server.submitTaskCount() != 1 {
		t.Fatalf("expected one doc and one task write, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
	assertBacklogStatus(t, store, "promote:visual", "promoted")

	second, err := store.PromoteBacklogItem(context.Background(), client, item.ItemID, AgentBacklogPromotionTarget{
		ProjectID:   "project-ui",
		ProjectLane: "review",
		Reason:      "visual critic found a real-user failure",
	}, at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if first.TaskID != second.TaskID || first.DocKey != second.DocKey {
		t.Fatalf("promotion contract should be stable, first=%+v second=%+v", first, second)
	}
	if server.putDocCount() != 1 || server.submitTaskCount() != 1 {
		t.Fatalf("second promotion should be local-idempotent, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
}

func TestProjectInitiativePromotionTaskIDIsProjectScopedAcrossAgents(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	alpha, err := OpenAgentInternalSessionStore("ws-1", "alpha")
	if err != nil {
		t.Fatal(err)
	}
	beta, err := OpenAgentInternalSessionStore("ws-1", "beta")
	if err != nil {
		t.Fatal(err)
	}
	for _, store := range []*AgentInternalSessionStore{alpha, beta} {
		if _, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
			DedupKey:    "project-initiative:post-mvp-quality-loop:project-ui",
			HeartbeatID: "project_role_initiative",
			Kind:        "project_post_mvp_quality_gap",
			Status:      "open",
			Title:       "Post-MVP project quality loop is unowned",
			Summary:     "The same project-level initiative should share one public task id across agents.",
			Score:       90,
			Meta: map[string]string{
				"finding_source": internalHeartbeatProjectInitiativeSensorSource,
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	target := AgentBacklogPromotionTarget{
		ProjectID:   "project-ui",
		ProjectLane: "qa",
		Tags:        []string{"internal-heartbeat", "project_role_initiative", "project_post_mvp_quality_gap"},
	}
	at := time.Date(2026, 5, 14, 9, 5, 0, 0, time.UTC)
	alphaItem := findBacklogByDedup(t, alpha, "project-initiative:post-mvp-quality-loop:project-ui")
	betaItem := findBacklogByDedup(t, beta, "project-initiative:post-mvp-quality-loop:project-ui")
	alphaContract, err := alpha.BuildBacklogPromotionContract(alphaItem.ItemID, target, at)
	if err != nil {
		t.Fatal(err)
	}
	betaContract, err := beta.BuildBacklogPromotionContract(betaItem.ItemID, target, at)
	if err != nil {
		t.Fatal(err)
	}
	if alphaContract.TaskID != betaContract.TaskID || alphaContract.DocKey != betaContract.DocKey {
		t.Fatalf("project initiative promotion IDs should be project-scoped across agents, alpha=%+v beta=%+v", alphaContract, betaContract)
	}
	if strings.Contains(alphaContract.TaskID, "alpha") || strings.Contains(alphaContract.TaskID, "beta") {
		t.Fatalf("project-scoped initiative task id should not include agent id: %s", alphaContract.TaskID)
	}
}

func TestBacklogPromotionDocFailureDoesNotMarkPromotedAndRetryUsesSameIDs(t *testing.T) {
	store := newAgentBacklogTestStore(t, "agent-retry")
	item := mustUpsertBacklog(t, store, "promote:retry", "Retry stable promotion", 92)
	server := newBacklogPromotionTestServer(t, true)
	defer server.Close()
	client := NewRhizomeClient(server.URL, "")
	at := time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC)

	first, err := store.PromoteBacklogItem(context.Background(), client, item.ItemID, AgentBacklogPromotionTarget{}, at)
	if err == nil {
		t.Fatal("expected first promotion to fail on doc write")
	}
	if server.submitTaskCount() != 0 {
		t.Fatalf("task should not be submitted after doc failure, got %d", server.submitTaskCount())
	}
	assertBacklogStatus(t, store, "promote:retry", "open")

	server.setFailNextDocPut(false)
	second, err := store.PromoteBacklogItem(context.Background(), client, item.ItemID, AgentBacklogPromotionTarget{}, at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if first.TaskID != second.TaskID || first.DocKey != second.DocKey {
		t.Fatalf("retry should use stable ids, first=%+v second=%+v", first, second)
	}
	if server.putDocCount() != 2 || server.submitTaskCount() != 1 {
		t.Fatalf("expected retry to write same doc then task once, got doc=%d task=%d", server.putDocCount(), server.submitTaskCount())
	}
	assertBacklogStatus(t, store, "promote:retry", "promoted")
}

func TestBacklogPromotionTaskListFailureFailsClosed(t *testing.T) {
	store := newAgentBacklogTestStore(t, "agent-list-fail")
	item := mustUpsertBacklog(t, store, "promote:list-fail", "List failure should not submit", 91)
	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	server.setFailTaskList(true)
	client := NewRhizomeClient(server.URL, "")

	_, err := store.PromoteBacklogItem(context.Background(), client, item.ItemID, AgentBacklogPromotionTarget{}, time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("expected promotion to fail closed when task list cannot be read")
	}
	if server.submitTaskCount() != 0 {
		t.Fatalf("task submit should not run when task list fails, got %d", server.submitTaskCount())
	}
	if server.putDocCount() != 0 {
		t.Fatalf("promotion doc should not be written before task list succeeds, got %d", server.putDocCount())
	}
	assertBacklogStatus(t, store, "promote:list-fail", "open")

	server.setFailTaskList(false)
	if _, err := store.PromoteBacklogItem(context.Background(), client, item.ItemID, AgentBacklogPromotionTarget{}, time.Date(2026, 5, 14, 9, 1, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if server.submitTaskCount() != 1 {
		t.Fatalf("expected one submit after list recovers, got %d", server.submitTaskCount())
	}
}

func TestBacklogPromotionContractFiltersPrivateEvidenceRefs(t *testing.T) {
	store := newAgentBacklogTestStore(t, "agent-private-evidence")
	item, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		DedupKey:    "promote:private-evidence",
		HeartbeatID: "project_role_initiative",
		Kind:        "strategic_gap",
		Title:       "Private evidence should not leak",
		Summary:     "Only public evidence refs should be rendered in the promotion contract.",
		Score:       90,
		EvidenceRefs: []string{
			"internal_session:session-1",
			"internal:trace",
			"local:file",
			"memory:local:self-check",
			"doc:project.contract",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	contract, err := store.BuildBacklogPromotionContract(item.ItemID, AgentBacklogPromotionTarget{}, time.Date(2026, 5, 14, 9, 2, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !containsTrimmedString(contract.EvidenceRefs, "doc:project.contract") {
		t.Fatalf("expected public doc evidence to remain, got %+v", contract.EvidenceRefs)
	}
	for _, privateRef := range []string{"internal_session:", "internal:", "local:", "memory:local"} {
		if strings.Contains(contract.DocContent, privateRef) {
			t.Fatalf("private evidence ref %s leaked into doc: %s", privateRef, contract.DocContent)
		}
	}
}

func TestAgentInternalSessionStoreUpdatedAtAdvancesOnMutation(t *testing.T) {
	store := newAgentBacklogTestStore(t, "agent-updated-at")
	item := mustUpsertBacklog(t, store, "updated-at:item", "UpdatedAt item", 70)
	first := store.Snapshot().UpdatedAt
	at := time.Now().UTC().Add(2 * time.Hour)
	if err := store.MarkBacklogItemPromoted(item.ItemID, []string{"task:updated-at"}, at); err != nil {
		t.Fatal(err)
	}
	second := store.Snapshot().UpdatedAt
	if first == second {
		t.Fatalf("UpdatedAt should advance after mutation, stayed %s", second)
	}
	if second != at.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("UpdatedAt = %s, want %s", second, at.UTC().Format(time.RFC3339Nano))
	}
}

type backlogPromotionTestServer struct {
	*httptest.Server
	mu           sync.Mutex
	failDocPut   bool
	failTaskList bool
	failUpdate   bool
	putDoc       int
	submitTask   int
	postUpdate   int
	docs         map[string]WorkspaceDocRecord
	tasks        map[string]WorkspaceTaskRecord
	lastTaskIn   TaskSubmitInput
	lastDocIn    WorkspaceDocPutInput
	lastUpdateIn UpdatePostInput
}

func newBacklogPromotionTestServer(t *testing.T, failFirstDocPut bool) *backlogPromotionTestServer {
	t.Helper()
	state := &backlogPromotionTestServer{
		failDocPut: failFirstDocPut,
		docs:       map[string]WorkspaceDocRecord{},
		tasks:      map[string]WorkspaceTaskRecord{},
	}
	state.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		switch req.Method {
		case "workspace.doc.get":
			var params map[string]string
			decodeRPCParams(t, req.Params, &params)
			state.mu.Lock()
			doc, ok := state.docs[params["doc_key"]]
			state.mu.Unlock()
			if !ok {
				writeBacklogRPCError(w, req.ID, "not found")
				return
			}
			writeBacklogRPCResult(w, req.ID, doc)
		case "workspace.doc.put":
			var input WorkspaceDocPutInput
			decodeRPCParams(t, req.Params, &input)
			state.mu.Lock()
			state.putDoc++
			state.lastDocIn = input
			if state.failDocPut {
				state.failDocPut = false
				state.mu.Unlock()
				writeBacklogRPCError(w, req.ID, "doc write failed")
				return
			}
			state.docs[input.DocKey] = WorkspaceDocRecord{
				DocKey:    input.DocKey,
				Title:     input.Title,
				Content:   input.Content,
				UpdatedBy: input.UpdatedBy,
				SHA:       "sha-" + input.DocKey,
			}
			state.mu.Unlock()
			writeBacklogRPCResult(w, req.ID, map[string]string{"sha": "sha-" + input.DocKey})
		case "workspace.tasks.list":
			state.mu.Lock()
			if state.failTaskList {
				state.mu.Unlock()
				writeBacklogRPCError(w, req.ID, "task list failed")
				return
			}
			tasks := make([]WorkspaceTaskRecord, 0, len(state.tasks))
			for _, task := range state.tasks {
				tasks = append(tasks, task)
			}
			state.mu.Unlock()
			writeBacklogRPCResult(w, req.ID, map[string]any{"tasks": tasks})
		case "task.submit":
			var input TaskSubmitInput
			decodeRPCParams(t, req.Params, &input)
			state.mu.Lock()
			state.submitTask++
			state.lastTaskIn = input
			state.tasks[input.TaskID] = WorkspaceTaskRecord{
				TaskID:      input.TaskID,
				Title:       input.Title,
				Description: input.Description,
				OwnerUserID: input.OwnerUserID,
				Priority:    input.Priority,
				Status:      "PENDING",
				TaskKind:    input.TaskKind,
				ProjectID:   input.ProjectID,
				ProjectLane: input.ProjectLane,
				Tags:        input.Tags,
				LinkedBy:    input.LinkedBy,
			}
			state.mu.Unlock()
			writeBacklogRPCResult(w, req.ID, TaskSubmitResult{TaskID: input.TaskID, WorkspaceID: input.WorkspaceID, Status: "PENDING"})
		case "agent.update.post":
			var input UpdatePostInput
			decodeRPCParams(t, req.Params, &input)
			state.mu.Lock()
			state.postUpdate++
			state.lastUpdateIn = input
			if state.failUpdate {
				state.failUpdate = false
				state.mu.Unlock()
				writeBacklogRPCError(w, req.ID, "update post failed")
				return
			}
			state.mu.Unlock()
			writeBacklogRPCResult(w, req.ID, map[string]any{"status": "posted"})
		default:
			writeBacklogRPCError(w, req.ID, "unexpected method "+req.Method)
		}
	}))
	return state
}

func (s *backlogPromotionTestServer) putDocCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putDoc
}

func (s *backlogPromotionTestServer) submitTaskCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.submitTask
}

func (s *backlogPromotionTestServer) postUpdateCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.postUpdate
}

func (s *backlogPromotionTestServer) lastUpdate() UpdatePostInput {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastUpdateIn
}

func (s *backlogPromotionTestServer) setFailNextDocPut(value bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failDocPut = value
}

func (s *backlogPromotionTestServer) setFailTaskList(value bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failTaskList = value
}

func (s *backlogPromotionTestServer) setFailNextUpdate(value bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failUpdate = value
}

func newAgentBacklogTestStore(t *testing.T, agentID string) *AgentInternalSessionStore {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	store, err := OpenAgentInternalSessionStore("ws-1", agentID)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func mustUpsertBacklog(t *testing.T, store *AgentInternalSessionStore, dedupKey, title string, score int) AgentPersonalBacklogItem {
	t.Helper()
	item, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		DedupKey:     dedupKey,
		HeartbeatID:  "visual_product_audit",
		Kind:         "visual_finding",
		Title:        title,
		Summary:      title + " summary",
		Score:        score,
		EvidenceRefs: []string{"evidence:" + dedupKey},
	})
	if err != nil {
		t.Fatal(err)
	}
	return item
}

func assertBacklogStatus(t *testing.T, store *AgentInternalSessionStore, dedupKey, status string) {
	t.Helper()
	item := findBacklogByDedup(t, store, dedupKey)
	if item.Status != status {
		t.Fatalf("dedup %s status = %s, want %s; item=%+v", dedupKey, item.Status, status, item)
	}
}

func findBacklogByDedup(t *testing.T, store *AgentInternalSessionStore, dedupKey string) AgentPersonalBacklogItem {
	t.Helper()
	for _, item := range store.Snapshot().Backlog {
		if item.DedupKey == dedupKey {
			return item
		}
	}
	t.Fatalf("backlog item with dedup %s not found", dedupKey)
	return AgentPersonalBacklogItem{}
}

func decodeRPCParams(t *testing.T, params any, out any) {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatalf("decode params: %v", err)
	}
}

func writeBacklogRPCResult(w http.ResponseWriter, id string, result any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
}

func writeBacklogRPCError(w http.ResponseWriter, id, message string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    -32000,
			"message": message,
		},
	})
}
