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

func TestIdleReflectionSuppressionStopsAfterRepeatedLocalHeartbeatFindings(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Date(2026, 5, 16, 13, 0, 0, 0, time.UTC)
	store, err := OpenAgentInternalSessionStore("ws-suppress", "agent-suppress")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		DedupKey:    "local:fresh",
		HeartbeatID: "loop_self_check",
		Kind:        "metacognition",
		Status:      "open",
		Title:       "Fresh local finding",
		LastSeenAt:  now.Format(time.RFC3339Nano),
		SeenCount:   1,
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{internalSessions: store}
	if !runtime.idleReflectionSuppressedByInternalHeartbeat(now) {
		t.Fatal("expected fresh first local heartbeat finding to suppress generic idle duty")
	}

	if _, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		DedupKey:    "local:fresh",
		HeartbeatID: "loop_self_check",
		Kind:        "metacognition",
		Status:      "open",
		Title:       "Fresh local finding",
		LastSeenAt:  now.Add(time.Minute).Format(time.RFC3339Nano),
		SeenCount:   internalHeartbeatIdleSuppressionSeenThreshold,
	}); err != nil {
		t.Fatal(err)
	}
	if runtime.idleReflectionSuppressedByInternalHeartbeat(now.Add(time.Minute)) {
		t.Fatal("repeated unresolved local heartbeat finding should stop suppressing ambient/public idle action")
	}
}

func TestRuntimePinnedTelicBuildSuppressesIdleReflection(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{
			TelicLoopEnabled: true,
			PinnedTaskID:     "task-eval",
		},
		idleNoWorkKey:   "project:signal01",
		idleNoWorkCount: idleReflectionNoWorkThreshold,
	}

	if err := runtime.maybeMaterializeIdleReflection(context.Background(), AgentWorkNextResult{}, pendingWorkTrigger{}); err != nil {
		t.Fatalf("maybeMaterializeIdleReflection() error = %v", err)
	}
	if runtime.idleNoWorkKey != "" || runtime.idleNoWorkCount != 0 {
		t.Fatalf("pinned telic build should reset and suppress idle reflection, got key=%q count=%d", runtime.idleNoWorkKey, runtime.idleNoWorkCount)
	}
	if !runtime.campaignBuildBreakActive() {
		t.Fatal("pinned telic lane should be treated as an active build break")
	}
}

func TestRuntimeRepeatedNoWorkCreatesIdleReflectionTaskAndQueuesWake(t *testing.T) {
	var workNextCalls int
	var taskSubmitCalls int
	var submittedTask map[string]any
	var materializedDoc string
	var lastScratch RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.work.next":
			workNextCalls++
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-07T00:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "agent-gamma",
				"has_work":     false,
				"reason":       "idle",
			})
		case "task.submit":
			taskSubmitCalls++
			submittedTask = req.Params
			if got := rpcString(req.Params, "task_kind"); got != "EXECUTION" {
				t.Fatalf("idle reflection task_kind = %q, want EXECUTION", got)
			}
			if got := rpcString(req.Params, "task_template"); got != "generic" {
				t.Fatalf("idle reflection task_template = %q, want generic", got)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-subpixel" {
				t.Fatalf("idle reflection project_id = %q, want project-subpixel", got)
			}
			if got := rpcString(req.Params, "project_lane"); got != "qa" {
				t.Fatalf("idle reflection project_lane = %q, want qa", got)
			}
			if _, hasDeps := req.Params["dependency_task_ids"]; hasDeps {
				t.Fatalf("idle product-quality task should not create blocking dependency links, got %+v", req.Params["dependency_task_ids"])
			}
			description := rpcString(req.Params, "description")
			if !strings.Contains(description, "product-quality iteration") || !strings.Contains(description, "acceptance criteria") || !strings.Contains(description, "spec-fidelity") || !strings.Contains(description, "task_submit follow-up") {
				t.Fatalf("idle reflection description did not include product-quality guidance: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws-1",
				"status":       "PENDING",
			})
		case "workspace.doc.put":
			materializedDoc = rpcString(req.Params, "content")
			if !strings.Contains(materializedDoc, "# Idle Product-Quality Task") || !strings.Contains(materializedDoc, "blocked_task_ids: task-revision") || !strings.Contains(materializedDoc, "project.<project_id>.acceptance_criteria") || !strings.Contains(materializedDoc, "reflection_board_doc_key: project.project-subpixel.reflection_board") || !strings.Contains(materializedDoc, "plan_review_doc_key: project.project-subpixel.plan_review") {
				t.Fatalf("unexpected idle reflection doc: %s", materializedDoc)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-idle-reflection"})
		case "agent.state.set":
			raw := rpcString(req.Params, "value")
			if err := json.Unmarshal([]byte(raw), &lastScratch); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during idle reflection test: %s", req.Method)
		}
	}))
	defer server.Close()

	blocked := "BLOCKED"
	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-gamma",
			OwnerUserID:  "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
				Projects: []ProjectRecord{
					{ProjectID: "project-subpixel", WorkspaceID: "ws-1", Title: "Subpixel Art", Status: "ACTIVE"},
				},
				Tasks: []WorkspaceTaskRecord{
					{
						TaskID:       "task-revision",
						ProjectID:    "project-subpixel",
						Title:        "Revision lane is parked",
						OwnerUserID:  "owner-1",
						Priority:     "HIGH",
						Status:       "RUNNING",
						TaskKind:     "EXECUTION",
						TaskTemplate: "generic",
						LinkedBy:     "alpha",
						LinkedAt:     "2026-05-07T00:00:00Z",
						ClaimAgentID: stringPtr("agent-gamma"),
						ClaimStatus:  &blocked,
					},
					{
						TaskID:       "task-root",
						ProjectID:    "project-subpixel",
						Title:        "Root project task",
						OwnerUserID:  "owner-1",
						Priority:     "HIGH",
						Status:       "RUNNING",
						TaskKind:     "COORDINATION",
						TaskTemplate: "generic",
						LinkedBy:     "alpha",
						LinkedAt:     "2026-05-07T00:00:01Z",
					},
				},
			},
		},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	for i := 0; i < idleReflectionNoWorkThreshold; i++ {
		task, err := runtime.ensureRunnableTask(context.Background())
		if err != nil {
			t.Fatalf("ensureRunnableTask tick %d error: %v", i+1, err)
		}
		if task != nil {
			t.Fatalf("idle reflection should not directly select work on tick %d, got %+v", i+1, task)
		}
	}

	if workNextCalls != idleReflectionNoWorkThreshold {
		t.Fatalf("work.next calls = %d, want %d", workNextCalls, idleReflectionNoWorkThreshold)
	}
	if taskSubmitCalls != 1 {
		t.Fatalf("task.submit calls = %d, want 1", taskSubmitCalls)
	}
	taskID := rpcString(submittedTask, "task_id")
	if !strings.HasPrefix(taskID, "task-idle-reflection-") {
		t.Fatalf("unexpected idle reflection task id %q", taskID)
	}
	if lastScratch.PendingTrigger != "request_resume" || lastScratch.PendingTriggerTask != taskID {
		t.Fatalf("expected idle reflection wake trigger for %s, got %+v", taskID, lastScratch)
	}
}

func TestRuntimeIdleReflectionShortCircuitsAfterInternalHeartbeatPromotion(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	server := newBacklogPromotionTestServer(t, false)
	defer server.Close()
	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{
		"contract_version":"internal-heartbeat-local-result/v1",
		"outcome":"backlog_recorded",
		"summary":"Promote one project-scoped initiative and do not also create generic idle work.",
		"backlog_items":[
			{"dedup_key":"strategy:short-circuit","kind":"strategic_gap","title":"Project-scoped initiative","summary":"A concrete project-scoped follow-up exists.","score":95,"evidence_refs":["doc:project.contract"],"promote":true}
		]
	}`}}}
	workdir := t.TempDir()
	profile := DefaultAgentProfile("alpha", "Alpha", "strategist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	store, err := OpenAgentInternalSessionStore("ws-1", "alpha")
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		CoordinationMode: CoordinationModeTrustFirst,
		WorkspaceID:      "ws-1",
		AgentID:          "alpha",
		DisplayName:      "Alpha",
		OwnerUserID:      "owner-1",
		RhizomeRPC:       server.URL,
		Workdir:          workdir,
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "")
	runtime.internalSessions = store
	runtime.bootstrap = BootstrapResult{
		Snapshot: WorkspaceSnapshot{
			Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
			Projects:  []ProjectRecord{{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "UI Project", Status: "ACTIVE"}},
		},
	}
	runtime.scratch = RuntimeScratchState{DocSHAs: map[string]string{}}
	runtime.mu.Lock()
	runtime.activeWorkPacket = &AgentWorkPacket{ProjectID: "project-ui", ProjectLane: "qa"}
	runtime.internalHeartbeatState.LastRun["loop_self_check"] = time.Now().UTC()
	runtime.mu.Unlock()
	t.Cleanup(func() { _ = runtime.Close() })

	for i := 0; i < idleReflectionNoWorkThreshold; i++ {
		if err := runtime.maybeMaterializeIdleReflection(context.Background(), AgentWorkNextResult{}, pendingWorkTrigger{}); err != nil {
			t.Fatalf("maybeMaterializeIdleReflection tick %d: %v", i+1, err)
		}
	}
	if server.submitTaskCount() != 1 || server.putDocCount() != 1 {
		t.Fatalf("heartbeat promotion should be the only public task/doc write, got task=%d doc=%d", server.submitTaskCount(), server.putDocCount())
	}
	if runtime.idleNoWorkCount != 1 {
		t.Fatalf("idle reflection should reset after heartbeat promotion and only count the later no-work tick, got %d", runtime.idleNoWorkCount)
	}
	item := findBacklogByDedup(t, runtime.internalSessions, "strategy:short-circuit")
	if item.Status != "promoted" {
		t.Fatalf("expected heartbeat backlog item promoted, got %+v", item)
	}
}

func TestRuntimeRecentInternalHeartbeatSuppressesGenericIdleReflectionTask(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var taskSubmitCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.doc.get":
			writeRPCResult(w, req, WorkspaceDocRecord{})
		case "project.coordination.get":
			writeRPCResult(w, req, ProjectCoordinationRecord{})
		case "task.submit":
			taskSubmitCalls++
			t.Fatalf("generic idle reflection should stay private when a recent internal heartbeat already handled no-work")
		default:
			t.Fatalf("unexpected method during internal-heartbeat suppression test: %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	profile := DefaultAgentProfile("alpha", "Alpha", "strategist")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	store, err := OpenAgentInternalSessionStore("ws-1", "alpha")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := store.RecordSession(AgentInternalSessionRecord{
		SessionID:     "recent-private-heartbeat",
		HeartbeatID:   "project_role_initiative",
		HeartbeatKind: "global_metacognition",
		Status:        "completed",
		Outcome:       "backlog_recorded",
		Summary:       "Private project-role sensemaking recorded locally.",
		StartedAt:     now.Add(-time.Minute).Format(time.RFC3339Nano),
		EndedAt:       now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		CoordinationMode: CoordinationModeTrustFirst,
		WorkspaceID:      "ws-1",
		AgentID:          "alpha",
		DisplayName:      "Alpha",
		OwnerUserID:      "owner-1",
		RhizomeRPC:       server.URL,
		Workdir:          workdir,
	}, nil)
	runtime.client = NewRhizomeClient(server.URL, "")
	runtime.internalSessions = store
	runtime.bootstrap = BootstrapResult{
		Snapshot: WorkspaceSnapshot{
			Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
			Projects:  []ProjectRecord{{ProjectID: "project-ui", WorkspaceID: "ws-1", Title: "UI Project", Status: "ACTIVE"}},
			Tasks: []WorkspaceTaskRecord{
				{
					TaskID:       "task-root",
					ProjectID:    "project-ui",
					Title:        "Root project task",
					OwnerUserID:  "owner-1",
					Priority:     "HIGH",
					Status:       "COMPLETED",
					TaskKind:     "COORDINATION",
					TaskTemplate: "generic",
					LinkedBy:     "alpha",
					LinkedAt:     now.Add(-time.Hour).Format(time.RFC3339Nano),
				},
			},
		},
	}
	runtime.scratch = RuntimeScratchState{DocSHAs: map[string]string{}}
	runtime.mu.Lock()
	for _, heartbeat := range DefaultAgentAnatomyConfigForPreset(profile, "strategist").Heartbeats {
		runtime.internalHeartbeatState.LastRun[heartbeat.ID] = now
	}
	runtime.mu.Unlock()
	t.Cleanup(func() { _ = runtime.Close() })

	for i := 0; i < idleReflectionNoWorkThreshold; i++ {
		if err := runtime.maybeMaterializeIdleReflection(context.Background(), AgentWorkNextResult{}, pendingWorkTrigger{}); err != nil {
			t.Fatalf("maybeMaterializeIdleReflection tick %d: %v", i+1, err)
		}
	}
	if taskSubmitCalls != 0 {
		t.Fatalf("task.submit calls = %d, want 0", taskSubmitCalls)
	}
	if runtime.idleNoWorkCount != 0 {
		t.Fatalf("recent internal heartbeat should reset generic idle no-work counter, got %d", runtime.idleNoWorkCount)
	}
}

func TestRuntimeOldInternalBacklogDoesNotPermanentlySuppressGenericIdleReflectionTask(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	now := time.Now().UTC()
	store, err := OpenAgentInternalSessionStore("ws-1", "alpha")
	if err != nil {
		t.Fatal(err)
	}
	old := now.Add(-2 * idleReflectionCooldown).Format(time.RFC3339Nano)
	if _, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		DedupKey:    "old:private-backlog",
		HeartbeatID: "project_role_initiative",
		Kind:        "strategic_gap",
		Status:      "open",
		Title:       "Old private backlog item",
		Summary:     "This stale-aged private item should not suppress compatibility fallback forever.",
		Score:       80,
		CreatedAt:   old,
		UpdatedAt:   old,
		LastSeenAt:  old,
	}); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "alpha",
		DisplayName: "Alpha",
		Workdir:     t.TempDir(),
	}, nil)
	runtime.internalSessions = store
	t.Cleanup(func() { _ = runtime.Close() })
	if runtime.idleReflectionSuppressedByInternalHeartbeat(now) {
		t.Fatalf("old open personal backlog should not permanently suppress generic idle fallback")
	}
}

func TestRuntimeRecentInternalHeartbeatDoesNotSuppressDeterministicIdleTarget(t *testing.T) {
	target := idleReflectionTarget{
		Key:         "project:project-ui|visual_acceptance_gap:queue-1:item-1",
		TaskID:      "task-visual-gap",
		ProjectID:   "project-ui",
		ProjectLane: "qa",
		Title:       "Visual acceptance debt: accepted UI patch still needs real screenshots",
		Description: "visual_acceptance_gap requires durable screenshot evidence.",
	}
	if !idleReflectionTargetRequiresDeterministicTask(target) {
		t.Fatalf("test setup should be deterministic, got %+v", target)
	}
	if idleReflectionShouldSuppressNewGenericTarget(target, true, true) {
		t.Fatalf("recent internal heartbeat must not suppress deterministic visual/integration/convergence targets")
	}
}

func TestRuntimeRecentInternalHeartbeatDoesNotSuppressFreshEvidenceWakeTarget(t *testing.T) {
	target := idleReflectionTarget{
		Key:                   "project:project-ui",
		TaskID:                "task-idle-reflection-fresh-evidence",
		ProjectID:             "project-ui",
		ProjectLane:           "qa",
		Title:                 "Product quality iteration: inspect fresh project evidence",
		Description:           appendFreshProjectEvidenceGuidance("Inspect the project after new evidence arrived.", []string{"project-ui.validation"}),
		RecentEvidenceDocKeys: []string{"project-ui.validation"},
	}
	if !idleReflectionTargetRequiresDeterministicTask(target) {
		t.Fatalf("fresh-evidence wake target should be deterministic, got %+v", target)
	}
	if idleReflectionShouldSuppressNewGenericTarget(target, true, true) {
		t.Fatalf("recent internal heartbeat must not suppress fresh-evidence wake follow-ups")
	}
}

func TestRuntimeNoWorkPatchQueueSupersedePacketCreatesStewardshipTask(t *testing.T) {
	var workNextCalls int
	var taskSubmitCalls int
	var lifecycleCalls int
	var submittedTask map[string]any
	var submittedTaskID string
	var materializedDoc string
	var lastScratch RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.work.next":
			workNextCalls++
			if workNextCalls > 1 {
				if got := rpcString(req.Params, "trigger"); got != "runtime_switch_task" {
					t.Fatalf("second work.next trigger = %q, want runtime_switch_task", got)
				}
				if got := rpcString(req.Params, "candidate_task_id"); got != submittedTaskID {
					t.Fatalf("second work.next candidate_task_id = %q, want %q", got, submittedTaskID)
				}
				writeRPCResult(w, req, map[string]any{
					"generated_at":   "2026-05-13T00:00:01Z",
					"workspace_id":   "ws-1",
					"agent_id":       "agent-epsilon",
					"has_work":       true,
					"reason":         "next_pending",
					"claim_action":   "claim_required",
					"session_action": "start_new",
					"task": map[string]any{
						"task_id":       submittedTaskID,
						"title":         "Supersede blocked patch queue item after fresh evidence",
						"owner_user_id": "owner-1",
						"priority":      "high",
						"status":        "PENDING",
						"task_kind":     "EXECUTION",
						"task_template": "generic",
						"project_id":    "project-icon-sprite",
						"project_lane":  "integration",
						"linked_by":     "agent-epsilon",
						"linked_at":     "2026-05-13T00:00:00Z",
					},
				})
				return
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-13T00:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "agent-epsilon",
				"has_work":     false,
				"reason":       "project_patch_queue_supersede_available",
				"project_id":   "project-icon-sprite",
				"task_kind":    "EXECUTION",
				"project_lane": "integration",
				"packet": map[string]any{
					"work_type":            "project_patch_queue_supersede_available",
					"project_id":           "project-icon-sprite",
					"task_kind":            "EXECUTION",
					"project_lane":         "integration",
					"coordination_state":   "blocked_queue_has_fresh_evidence",
					"preferred_transition": "create_or_claim_patch_queue_supersede_stewardship",
					"why_now":              "blocked item has fresh evidence",
					"patch_queue_supersede": map[string]any{
						"project_id":       "project-icon-sprite",
						"queue_id":         "patchqueue-project-icon-sprite",
						"item_id":          "patchitem-old",
						"new_item_id":      "supersede-abc123",
						"branch_id":        "branch-beta",
						"branch_name":      "agent/beta/icon-sprite",
						"head_sha":         "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
						"evidence_doc_key": "task.patchitem-old.browser_smoke_recheck_evidence",
						"decision_doc_key": "task.patchitem-old.blocked_decision",
						"review_doc_key":   "project.icon.branch.review",
					},
				},
			})
		case "task.submit":
			taskSubmitCalls++
			submittedTask = req.Params
			if got := rpcString(req.Params, "task_kind"); got != "EXECUTION" {
				t.Fatalf("supersede task_kind = %q, want EXECUTION", got)
			}
			if got := rpcString(req.Params, "project_lane"); got != "integration" {
				t.Fatalf("supersede project_lane = %q, want integration", got)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-icon-sprite" {
				t.Fatalf("supersede project_id = %q, want project-icon-sprite", got)
			}
			description := rpcString(req.Params, "description")
			for _, want := range []string{"project_patch_queue_lifecycle", "action=supersede", "patchqueue-project-icon-sprite", "patchitem-old", "supersede-abc123", "task.patchitem-old.browser_smoke_recheck_evidence"} {
				if !strings.Contains(description, want) {
					t.Fatalf("supersede description missing %q: %s", want, description)
				}
			}
			tagsJSON, _ := json.Marshal(req.Params["tags"])
			if !strings.Contains(string(tagsJSON), "queue-stewardship") || !strings.Contains(string(tagsJSON), "supersede") {
				t.Fatalf("supersede tags missing stewardship markers: %s", string(tagsJSON))
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws-1",
				"status":       "PENDING",
			})
			submittedTaskID = rpcString(req.Params, "task_id")
		case "workspace.doc.put":
			if strings.HasPrefix(rpcString(req.Params, "doc_key"), "task.") {
				materializedDoc = rpcString(req.Params, "content")
				for _, want := range []string{"# Patch Queue Supersede Stewardship", "new_item_id: supersede-abc123", "expected_head_sha: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "metadata-only"} {
					if !strings.Contains(materializedDoc, want) {
						t.Fatalf("supersede doc missing %q: %s", want, materializedDoc)
					}
				}
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-supersede-task"})
		case "agent.state.set":
			raw := rpcString(req.Params, "value")
			if err := json.Unmarshal([]byte(raw), &lastScratch); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "agent.task.claim":
			if got := rpcString(req.Params, "task_id"); got != submittedTaskID {
				t.Fatalf("claim task_id = %q, want %q", got, submittedTaskID)
			}
			writeRPCResult(w, req, nil)
		case "agent.session.start":
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"session_id":   rpcString(req.Params, "session_id"),
				"workspace_id": "ws-1",
				"agent_id":     "agent-epsilon",
				"task_id":      submittedTaskID,
				"status":       "ACTIVE",
				"summary":      "Supersede blocked patch queue item after fresh evidence",
				"owner_scope":  "task/session",
				"started_at":   "2026-05-13T00:00:01Z",
				"updated_at":   "2026-05-13T00:00:01Z",
			}})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{"run": map[string]any{
				"run_id":       rpcString(req.Params, "run_id"),
				"workspace_id": "ws-1",
				"task_id":      submittedTaskID,
				"session_id":   rpcString(req.Params, "session_id"),
				"agent_id":     "agent-epsilon",
				"title":        "Supersede blocked patch queue item after fresh evidence",
				"status":       "ACTIVE",
				"created_at":   "2026-05-13T00:00:01Z",
				"updated_at":   "2026-05-13T00:00:01Z",
			}})
		case "project.patch_queue.supersede":
			lifecycleCalls++
			t.Fatalf("runtime must not call patch queue lifecycle directly from no-work packet")
		default:
			t.Fatalf("unexpected method during patch queue supersede materialization test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-epsilon",
			OwnerUserID:  "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	t.Cleanup(func() { _ = runtime.Close() })

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task != nil {
		t.Fatalf("supersede materialization should queue a task and return no runnable task this tick, got %+v", task)
	}
	if taskSubmitCalls != 1 || lifecycleCalls != 0 {
		t.Fatalf("task submits=%d lifecycle calls=%d submitted=%+v", taskSubmitCalls, lifecycleCalls, submittedTask)
	}
	if lastScratch.PendingTrigger != "runtime_switch_task" || lastScratch.PendingTriggerTask == "" || !strings.Contains(lastScratch.PendingTriggerTask, "task-patchq-supersede") {
		t.Fatalf("expected runtime_switch_task trigger for supersede task, got %+v", lastScratch)
	}
	if !strings.Contains(materializedDoc, "queue_id: patchqueue-project-icon-sprite") {
		t.Fatalf("expected materialized doc to name queue id, got %s", materializedDoc)
	}
	resumed, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() second tick error = %v", err)
	}
	if resumed == nil || resumed.TaskID != submittedTaskID {
		t.Fatalf("expected second tick to select stewardship task %q, got %+v", submittedTaskID, resumed)
	}
}

func TestRuntimeNoWorkPatchQueueSubmitHandoffPacketCreatesOwnerTask(t *testing.T) {
	var taskSubmitCalls int
	var submittedTaskID string
	var lastScratch RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.work.next":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-23T23:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "beta",
				"has_work":     false,
				"reason":       "project_patch_queue_submit_handoff_available",
				"project_id":   "project-clearpress",
				"task_kind":    "EXECUTION",
				"project_lane": "integration",
				"packet": map[string]any{
					"work_type":            "project_patch_queue_submit_handoff_available",
					"project_id":           "project-clearpress",
					"task_kind":            "EXECUTION",
					"project_lane":         "integration",
					"coordination_state":   "ready_branch_missing_patch_queue_handoff",
					"preferred_transition": "create_or_claim_owner_bound_patch_queue_submit",
					"why_now":              "READY_FOR_REVIEW branch has no patch queue handoff",
					"owner_bound": map[string]any{
						"kind":              "patch_queue_submit",
						"required_agent_id": "beta",
						"branch_id":         "projbranch-beta-ready",
						"branch_name":       "agent/beta/clearpress",
						"head_sha":          "39eccfd94469d7b5e4e96314898b4c54abc57231",
						"review_doc_key":    "project.clearpress.branch.projbranch-beta-ready.review",
					},
				},
			})
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{
				"branches": []map[string]any{{
					"branch_id":        "projbranch-beta-ready",
					"workspace_id":     "ws-1",
					"project_id":       "project-clearpress",
					"repo_id":          "repo-clearpress",
					"checkout_id":      "checkout-beta",
					"agent_id":         "beta",
					"branch_name":      "agent/beta/clearpress",
					"branch_kind":      "feature",
					"base_branch":      "main",
					"base_sha":         "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"head_sha":         "39eccfd94469d7b5e4e96314898b4c54abc57231",
					"write_scope_json": `{"paths":["cmd/rq/**","README.md"]}`,
					"review_doc_key":   "project.clearpress.branch.projbranch-beta-ready.review",
					"status":           "READY_FOR_REVIEW",
				}},
			}})
		case "task.submit":
			taskSubmitCalls++
			submittedTaskID = rpcString(req.Params, "task_id")
			if got := rpcString(req.Params, "task_class"); got != "INTEGRATION" {
				t.Fatalf("task_class = %q", got)
			}
			if got := rpcString(req.Params, "task_class_source"); got != "EXPLICIT" {
				t.Fatalf("task_class_source = %q", got)
			}
			if got := rpcString(req.Params, "project_lane"); got != "integration" {
				t.Fatalf("project_lane = %q", got)
			}
			description := rpcString(req.Params, "description")
			for _, want := range []string{"project_patch_queue_submit", "project_id: project-clearpress", "repo_id: repo-clearpress", "projbranch-beta-ready", "39eccfd94469d7b5e4e96314898b4c54abc57231", "project.clearpress.branch.projbranch-beta-ready.review", `pathset_json: {"paths":["cmd/rq/**","README.md"]}`} {
				if !strings.Contains(description, want) {
					t.Fatalf("submit handoff description missing %q: %s", want, description)
				}
			}
			tagsJSON, _ := json.Marshal(req.Params["tags"])
			for _, want := range []string{"owner-bound-kind:patch_queue_submit", "owner-branch:projbranch-beta-ready", "required-agent:beta", "queue-handoff"} {
				if !strings.Contains(string(tagsJSON), want) {
					t.Fatalf("submit handoff tags missing %q: %s", want, string(tagsJSON))
				}
			}
			reqsJSON, _ := json.Marshal(req.Params["task_requirements"])
			for _, want := range []string{"rhizome_project_patch_queue_submit_handoff_v1", "project_patch_queue_submit", "project-clearpress", "repo-clearpress", "projbranch-beta-ready", "README.md", "repoauthority_controlled_queue"} {
				if !strings.Contains(string(reqsJSON), want) {
					t.Fatalf("submit handoff requirements missing %q: %s", want, string(reqsJSON))
				}
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      submittedTaskID,
				"workspace_id": "ws-1",
				"status":       "PENDING",
			})
		case "workspace.doc.put":
			if strings.HasPrefix(rpcString(req.Params, "doc_key"), "task.") {
				content := rpcString(req.Params, "content")
				for _, want := range []string{"# Patch Queue Submit Handoff", "project_id: project-clearpress", "repo_id: repo-clearpress", "branch_id: projbranch-beta-ready", "review_doc_key: project.clearpress.branch.projbranch-beta-ready.review", `pathset_json: {"paths":["cmd/rq/**","README.md"]}`} {
					if !strings.Contains(content, want) {
						t.Fatalf("submit handoff doc missing %q: %s", want, content)
					}
				}
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-submit-handoff"})
		case "agent.state.set":
			raw := rpcString(req.Params, "value")
			if err := json.Unmarshal([]byte(raw), &lastScratch); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method during patch queue submit handoff materialization test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "beta",
			OwnerUserID:  "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	t.Cleanup(func() { _ = runtime.Close() })

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task != nil {
		t.Fatalf("submit handoff materialization should queue a task and return no runnable task this tick, got %+v", task)
	}
	if taskSubmitCalls != 1 || submittedTaskID == "" {
		t.Fatalf("expected one submit handoff task, calls=%d id=%q", taskSubmitCalls, submittedTaskID)
	}
	if lastScratch.PendingTrigger != "runtime_switch_task" || lastScratch.PendingTriggerTask != submittedTaskID {
		t.Fatalf("expected runtime_switch_task trigger for submit handoff task %s, got %+v", submittedTaskID, lastScratch)
	}
}

func TestRuntimeNoWorkPatchQueueSupersedeDuplicateDoesNotForceWake(t *testing.T) {
	var workNextCalls int
	var taskSubmitCalls int
	methods := []string{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.work.next":
			workNextCalls++
			if workNextCalls > 1 {
				t.Fatalf("duplicate supersede materialization must not force a second work.next wake")
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-13T00:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "agent-epsilon",
				"has_work":     false,
				"reason":       "project_patch_queue_supersede_available",
				"project_id":   "project-icon-sprite",
				"task_kind":    "EXECUTION",
				"project_lane": "integration",
				"packet": map[string]any{
					"work_type":            "project_patch_queue_supersede_available",
					"project_id":           "project-icon-sprite",
					"task_kind":            "EXECUTION",
					"project_lane":         "integration",
					"coordination_state":   "blocked_queue_has_fresh_evidence",
					"preferred_transition": "create_or_claim_patch_queue_supersede_stewardship",
					"patch_queue_supersede": map[string]any{
						"project_id":       "project-icon-sprite",
						"queue_id":         "patchqueue-project-icon-sprite",
						"item_id":          "patchitem-old",
						"new_item_id":      "supersede-abc123",
						"branch_id":        "branch-beta",
						"branch_name":      "agent/beta/icon-sprite",
						"head_sha":         "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
						"evidence_doc_key": "task.patchitem-old.browser_smoke_recheck_evidence",
					},
				},
			})
		case "task.submit":
			taskSubmitCalls++
			writeRPCError(w, req, -32602, "task already exists")
		case "workspace.doc.put", "agent.state.set", "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			t.Fatalf("duplicate supersede materialization must not write docs, wake, claim, or start sessions; got %s", req.Method)
		default:
			t.Fatalf("unexpected method during duplicate supersede materialization test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-epsilon",
			OwnerUserID:  "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	t.Cleanup(func() { _ = runtime.Close() })

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task != nil {
		t.Fatalf("duplicate supersede materialization should not return runnable task, got %+v", task)
	}
	if workNextCalls != 1 || taskSubmitCalls != 1 {
		t.Fatalf("work.next calls=%d task.submit calls=%d methods=%v", workNextCalls, taskSubmitCalls, methods)
	}
}

func TestRuntimePatchQueueSupersedeDuplicateCooldownSuppressesRepeatSubmit(t *testing.T) {
	var workNextCalls int
	var taskSubmitCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.work.next":
			workNextCalls++
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-13T00:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "agent-epsilon",
				"has_work":     false,
				"reason":       "project_patch_queue_supersede_available",
				"project_id":   "project-icon-sprite",
				"task_kind":    "EXECUTION",
				"project_lane": "integration",
				"packet": map[string]any{
					"work_type":  "project_patch_queue_supersede_available",
					"project_id": "project-icon-sprite",
					"patch_queue_supersede": map[string]any{
						"project_id":       "project-icon-sprite",
						"queue_id":         "patchqueue-project-icon-sprite",
						"item_id":          "patchitem-old",
						"new_item_id":      "supersede-abc123",
						"branch_id":        "branch-beta",
						"head_sha":         "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
						"evidence_doc_key": "task.patchitem-old.browser_smoke_recheck_evidence",
					},
				},
			})
		case "task.submit":
			taskSubmitCalls++
			if taskSubmitCalls > 1 {
				t.Fatalf("duplicate cooldown should suppress repeated task.submit")
			}
			writeRPCError(w, req, -32602, "task already exists")
		case "workspace.doc.put", "agent.state.set":
			t.Fatalf("duplicate cooldown must not write docs or wake; got %s", req.Method)
		default:
			t.Fatalf("unexpected rpc method: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-epsilon",
			OwnerUserID:  "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	t.Cleanup(func() { _ = runtime.Close() })

	for i := 0; i < 2; i++ {
		task, err := runtime.ensureRunnableTask(context.Background())
		if err != nil {
			t.Fatalf("ensureRunnableTask(%d) error = %v", i, err)
		}
		if task != nil {
			t.Fatalf("duplicate supersede tick should not return runnable task, got %+v", task)
		}
	}
	if workNextCalls != 2 || taskSubmitCalls != 1 {
		t.Fatalf("work.next calls=%d task.submit calls=%d", workNextCalls, taskSubmitCalls)
	}
}

func TestPatchQueueSupersedeTaskIDIncludesHeadSHA(t *testing.T) {
	a := patchQueueSupersedeTaskID("project-1", "queue-1", "item-1", "branch-1", "sha-a", "doc-1")
	b := patchQueueSupersedeTaskID("project-1", "queue-1", "item-1", "branch-1", "sha-b", "doc-1")
	if a == b {
		t.Fatalf("expected supersede task ID to vary by head sha, got %q", a)
	}
}

func TestInternalHeartbeatHandledNoWorkRequiresActionableOutcome(t *testing.T) {
	if internalHeartbeatResultsHandledNoWork([]InternalHeartbeatExecutionResult{{Outcome: "no_action"}}) {
		t.Fatal("no_action heartbeat must not suppress generic idle fallback")
	}
	if internalHeartbeatResultsHandledNoWork([]InternalHeartbeatExecutionResult{{Outcome: "typed_policy_recorded"}}) {
		t.Fatal("policy-only heartbeat must not suppress generic idle fallback")
	}
	if !internalHeartbeatResultsHandledNoWork([]InternalHeartbeatExecutionResult{{Outcome: "backlog_recorded"}}) {
		t.Fatal("backlog_recorded heartbeat should count as handling no-work")
	}
	if !internalHeartbeatResultsHandledNoWork([]InternalHeartbeatExecutionResult{{PromotedRefs: []string{"task:one"}}}) {
		t.Fatal("promoted heartbeat refs should count as handling no-work")
	}
}

func TestRuntimeNoWorkPatchQueueClaimPacketCreatesStewardshipTask(t *testing.T) {
	var taskSubmitCalls int
	var submittedTask map[string]any
	var materializedDoc string
	var lastScratch RuntimeScratchState

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.work.next":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-13T00:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "agent-zeta",
				"has_work":     false,
				"reason":       "project_patch_queue_claim_stewardship_available",
				"project_id":   "project-icon-sprite",
				"task_kind":    "EXECUTION",
				"project_lane": "integration",
				"packet": map[string]any{
					"work_type":            "project_patch_queue_claim_stewardship_available",
					"project_id":           "project-icon-sprite",
					"task_kind":            "EXECUTION",
					"project_lane":         "integration",
					"coordination_state":   "claimed_patch_queue_item_needs_lifecycle",
					"preferred_transition": "create_or_claim_patch_queue_claim_stewardship",
					"why_now":              "claimed item has no terminal decision",
					"patch_queue_claim_stewardship": map[string]any{
						"project_id":                "project-icon-sprite",
						"queue_id":                  "patchqueue-project-icon-sprite",
						"item_id":                   "patchitem-claimed",
						"branch_id":                 "branch-beta",
						"branch_name":               "agent/beta/icon-sprite",
						"head_sha":                  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
						"state":                     "CLAIMED",
						"claimed_by":                "agent-theta",
						"claim_expires_at":          "2000-01-01T00:00:00Z",
						"claim_active":              false,
						"operation_binding_present": false,
						"allowed_actions":           []string{"claim", "reviewer_advisory", "release", "accept", "reject", "block", "cancel"},
					},
				},
			})
		case "task.submit":
			taskSubmitCalls++
			submittedTask = req.Params
			if got := rpcString(req.Params, "task_kind"); got != "COORDINATION" {
				t.Fatalf("claim stewardship task_kind = %q, want COORDINATION", got)
			}
			if got := rpcString(req.Params, "project_lane"); got != "review" {
				t.Fatalf("claim stewardship project_lane = %q, want review", got)
			}
			// RPF-58B(i): the steward task carries a deterministic lifecycle contract so the
			// required-tool gate drives claim->decide-or-release, never integrate (which refuses
			// a CLAIMED item and blocked zeta's steward in R58).
			reqJSON, _ := json.Marshal(req.Params["task_requirements"])
			for _, want := range []string{`"required_tool":"project_patch_queue_lifecycle"`, `"patch_queue_task_kind":"claim_stewardship"`} {
				if !strings.Contains(string(reqJSON), want) {
					t.Fatalf("claim stewardship task_requirements missing %q: %s", want, string(reqJSON))
				}
			}
			description := rpcString(req.Params, "description")
			for _, want := range []string{"project_patch_queue_lifecycle", "action=claim", "patchqueue-project-icon-sprite", "patchitem-claimed", "Do not attempt to release another agent's stale claim directly"} {
				if !strings.Contains(description, want) {
					t.Fatalf("claim stewardship description missing %q: %s", want, description)
				}
			}
			tagsJSON, _ := json.Marshal(req.Params["tags"])
			for _, want := range []string{"queue-stewardship", "claim-stewardship", "claim-expired"} {
				if !strings.Contains(string(tagsJSON), want) {
					t.Fatalf("claim stewardship tags missing %q: %s", want, string(tagsJSON))
				}
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws-1",
				"status":       "PENDING",
			})
		case "workspace.doc.put":
			materializedDoc = rpcString(req.Params, "content")
			for _, want := range []string{"# Patch Queue Claim Stewardship", "claim_active_at_packet_time: false", "Reclaim expired items", "never release another agent's stale claim"} {
				if !strings.Contains(materializedDoc, want) {
					t.Fatalf("claim stewardship doc missing %q: %s", want, materializedDoc)
				}
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-claim-stewardship-task"})
		case "agent.state.set":
			raw := rpcString(req.Params, "value")
			if err := json.Unmarshal([]byte(raw), &lastScratch); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "project.patch_queue.claim", "project.patch_queue.decision", "project.patch_queue.release":
			t.Fatalf("runtime must not mutate patch queue directly from no-work claim packet; got %s", req.Method)
		default:
			t.Fatalf("unexpected method during patch queue claim stewardship materialization test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-zeta",
			OwnerUserID:  "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	t.Cleanup(func() { _ = runtime.Close() })

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task != nil {
		t.Fatalf("claim stewardship materialization should queue a task and return no runnable task this tick, got %+v", task)
	}
	if taskSubmitCalls != 1 || submittedTask == nil {
		t.Fatalf("expected one task.submit, got calls=%d submitted=%+v", taskSubmitCalls, submittedTask)
	}
	if lastScratch.PendingTrigger != "runtime_switch_task" || !strings.Contains(lastScratch.PendingTriggerTask, "task-patchq-claim-steward") {
		t.Fatalf("expected runtime_switch_task trigger for claim stewardship task, got %+v", lastScratch)
	}
}

func TestRuntimeNoWorkPatchQueueClaimDuplicateDoesNotForceWake(t *testing.T) {
	var workNextCalls int
	var taskSubmitCalls int
	methods := []string{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.work.next":
			workNextCalls++
			if workNextCalls > 1 {
				t.Fatalf("duplicate claim stewardship materialization must not force a second work.next wake")
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-13T00:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "agent-zeta",
				"has_work":     false,
				"reason":       "project_patch_queue_claim_stewardship_available",
				"project_id":   "project-icon-sprite",
				"packet": map[string]any{
					"work_type": "project_patch_queue_claim_stewardship_available",
					"patch_queue_claim_stewardship": map[string]any{
						"project_id":       "project-icon-sprite",
						"queue_id":         "patchqueue-project-icon-sprite",
						"item_id":          "patchitem-claimed",
						"branch_id":        "branch-beta",
						"claimed_by":       "agent-theta",
						"claim_expires_at": "2000-01-01T00:00:00Z",
					},
				},
			})
		case "task.submit":
			taskSubmitCalls++
			writeRPCError(w, req, -32602, "task already exists")
		case "workspace.doc.put", "agent.state.set", "agent.task.claim", "agent.session.start", "workspace.execution.run.write":
			t.Fatalf("duplicate claim stewardship materialization must not write docs, wake, claim, or start sessions; got %s", req.Method)
		default:
			t.Fatalf("unexpected method during duplicate claim stewardship materialization test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-zeta",
			OwnerUserID:  "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	t.Cleanup(func() { _ = runtime.Close() })

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task != nil {
		t.Fatalf("duplicate claim stewardship materialization should not return runnable task, got %+v", task)
	}
	if workNextCalls != 1 || taskSubmitCalls != 1 {
		t.Fatalf("work.next calls=%d task.submit calls=%d methods=%v", workNextCalls, taskSubmitCalls, methods)
	}
}

func TestRuntimeNoWorkDoesNotMaterializeIdleReflectionForClosedGate(t *testing.T) {
	work := AgentWorkNextResult{
		HasWork: false,
		Reason:  "profile_gate_closed",
		Packet: &AgentWorkPacket{
			Gate: &AgentWorkGate{GateState: "closed", GateType: "profile_autonomous_execution"},
		},
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws-1", AgentID: "agent-1"},
		bootstrap: BootstrapResult{Snapshot: WorkspaceSnapshot{
			Tasks: []WorkspaceTaskRecord{{TaskID: "task-1", Status: "RUNNING", Title: "Work exists"}},
		}},
	}

	for i := 0; i < idleReflectionNoWorkThreshold+1; i++ {
		if err := runtime.maybeMaterializeIdleReflection(context.Background(), work, pendingWorkTrigger{}); err != nil {
			t.Fatalf("maybeMaterializeIdleReflection() error: %v", err)
		}
	}
	if runtime.idleNoWorkCount != 0 || runtime.idleReflectionKey != "" {
		t.Fatalf("closed gates must not accumulate idle reflection state, count=%d key=%q", runtime.idleNoWorkCount, runtime.idleReflectionKey)
	}
}

func TestRuntimeNoWorkDoesNotMaterializeIdleReflectionWhilePeerWorkRunning(t *testing.T) {
	var taskSubmitCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.work.next":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-07T00:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "agent-gamma",
				"has_work":     false,
				"reason":       "idle",
			})
		case "task.submit":
			taskSubmitCalls++
			t.Fatalf("idle reflection should not create meta tasks while another product task is actively running")
		default:
			t.Fatalf("unexpected method during active-work idle suppression test: %s", req.Method)
		}
	}))
	defer server.Close()

	claimed := "CLAIMED"
	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-gamma",
			OwnerUserID:  "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
				Tasks: []WorkspaceTaskRecord{{
					TaskID:       "task-root",
					ProjectID:    "project-subpixel",
					Title:        "Root project task",
					OwnerUserID:  "owner-1",
					Priority:     "HIGH",
					Status:       "RUNNING",
					TaskKind:     "EXECUTION",
					TaskTemplate: "generic",
					LinkedBy:     "beta",
					LinkedAt:     "2026-05-07T00:00:01Z",
					ClaimAgentID: stringPtr("beta"),
					ClaimStatus:  &claimed,
				}},
			},
		},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	for i := 0; i < idleReflectionNoWorkThreshold+1; i++ {
		task, err := runtime.ensureRunnableTask(context.Background())
		if err != nil {
			t.Fatalf("ensureRunnableTask tick %d error: %v", i+1, err)
		}
		if task != nil {
			t.Fatalf("active-work idle suppression should not directly select work on tick %d, got %+v", i+1, task)
		}
	}
	if taskSubmitCalls != 0 {
		t.Fatalf("task.submit calls = %d, want 0", taskSubmitCalls)
	}
	if runtime.idleNoWorkCount != 0 || runtime.idleNoWorkKey != "" {
		t.Fatalf("active work must suppress idle counters, count=%d key=%q", runtime.idleNoWorkCount, runtime.idleNoWorkKey)
	}
}

func TestRuntimeNoWorkDoesNotMaterializeIdleReflectionForOpenPatchQueueReviewTask(t *testing.T) {
	var taskSubmitCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.work.next":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-13T01:30:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "agent-alpha",
				"has_work":     false,
				"reason":       "project_patch_queue_review_role_required",
			})
		case "workspace.doc.get":
			writeRPCError(w, req, 404, "not found")
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{}})
		case "task.submit":
			taskSubmitCalls++
			t.Fatalf("idle reflection should not create another task while patch queue review work is open")
		default:
			t.Fatalf("unexpected method during patch-queue idle suppression test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:          t.TempDir(),
			RhizomeRPC:       server.URL,
			RhizomeToken:     "token",
			WorkspaceID:      "ws-1",
			AgentID:          "agent-alpha",
			OwnerUserID:      "owner-1",
			Role:             "strategist",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
				Projects:  []ProjectRecord{{ProjectID: "project-subpixel", WorkspaceID: "ws-1", Title: "Subpixel Art", Status: "ACTIVE"}},
				Tasks: []WorkspaceTaskRecord{{
					TaskID:      "task-review-patchq-queue-1-item-1",
					ProjectID:   "project-subpixel",
					Title:       "Review patch queue candidate for project-subpixel",
					Description: "Patch queue candidate is ready for independent review.\n\n- queue_id: queue-1\n- item_id: item-1\n- branch_id: branch-1",
					OwnerUserID: "owner-1",
					Priority:    "critical",
					Status:      "PENDING",
					TaskKind:    "EXECUTION",
					LinkedBy:    "beta",
					LinkedAt:    "2026-05-13T01:29:00Z",
					ProjectLane: "review",
					Tags:        []string{"project", "review", "patch_queue"},
				}},
			},
		},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	for i := 0; i < idleReflectionNoWorkThreshold+1; i++ {
		task, err := runtime.ensureRunnableTask(context.Background())
		if err != nil {
			t.Fatalf("ensureRunnableTask tick %d error: %v", i+1, err)
		}
		if task != nil {
			t.Fatalf("patch-queue idle suppression should not directly select work on tick %d, got %+v", i+1, task)
		}
	}
	if taskSubmitCalls != 0 {
		t.Fatalf("task.submit calls = %d, want 0", taskSubmitCalls)
	}
}

func TestRuntimeTrustFirstNoWorkDoesNotCreateWorkspaceReflectionWhileUnprojectedRootIsOpen(t *testing.T) {
	var taskSubmitCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.work.next":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-07T00:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "agent-alpha",
				"has_work":     false,
				"reason":       "idle",
			})
		case "task.submit":
			taskSubmitCalls++
			t.Fatalf("idle reflection should not create workspace-scope tasks while an unprojected root task is still open")
		default:
			t.Fatalf("unexpected method during unprojected-root idle suppression test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:          t.TempDir(),
			RhizomeRPC:       server.URL,
			RhizomeToken:     "token",
			WorkspaceID:      "ws-1",
			AgentID:          "agent-alpha",
			OwnerUserID:      "owner-1",
			Role:             "strategist",
			CoordinationMode: CoordinationModeTrustFirst,
		},
		client: NewRhizomeClient(server.URL, "token"),
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
				Tasks: []WorkspaceTaskRecord{{
					TaskID:       "task-root",
					Title:        "Build polished product",
					OwnerUserID:  "owner-1",
					Priority:     "HIGH",
					Status:       "RUNNING",
					TaskKind:     "COORDINATION",
					TaskTemplate: "generic",
					LinkedBy:     "operator-seed",
					LinkedAt:     "2026-05-07T00:00:01Z",
				}},
			},
		},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	for i := 0; i < idleReflectionNoWorkThreshold+1; i++ {
		task, err := runtime.ensureRunnableTask(context.Background())
		if err != nil {
			t.Fatalf("ensureRunnableTask tick %d error: %v", i+1, err)
		}
		if task != nil {
			t.Fatalf("unprojected-root idle suppression should not directly select work on tick %d, got %+v", i+1, task)
		}
	}
	if taskSubmitCalls != 0 {
		t.Fatalf("task.submit calls = %d, want 0", taskSubmitCalls)
	}
	if runtime.idleNoWorkCount != 0 || runtime.idleNoWorkKey != "" {
		t.Fatalf("unprojected root work must suppress idle counters, count=%d key=%q", runtime.idleNoWorkCount, runtime.idleNoWorkKey)
	}
}

func TestRuntimeNoWorkDoesNotMaterializeIdleReflectionForBlockedSiblingWhenProductWorkIsActive(t *testing.T) {
	var taskSubmitCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.work.next":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-07T00:00:00Z",
				"workspace_id": "ws-1",
				"agent_id":     "agent-zeta",
				"has_work":     false,
				"reason":       "idle",
			})
		case "task.submit":
			taskSubmitCalls++
			t.Fatalf("idle reflection should not create meta tasks while implementation work is actively owned")
		default:
			t.Fatalf("unexpected method during blocked-sibling idle suppression test: %s", req.Method)
		}
	}))
	defer server.Close()

	blocked := "BLOCKED"
	claimed := "CLAIMED"
	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-zeta",
			OwnerUserID:  "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		bootstrap: BootstrapResult{
			Snapshot: WorkspaceSnapshot{
				Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
				Tasks: []WorkspaceTaskRecord{
					{
						TaskID:       "task-review",
						ProjectID:    "project-subpixel",
						Title:        "Review lane is blocked on pushed branch evidence",
						OwnerUserID:  "owner-1",
						Priority:     "HIGH",
						Status:       "RUNNING",
						TaskKind:     "EXECUTION",
						TaskTemplate: "generic",
						LinkedBy:     "epsilon",
						LinkedAt:     "2026-05-07T00:00:00Z",
						ClaimAgentID: stringPtr("epsilon"),
						ClaimStatus:  &blocked,
					},
					{
						TaskID:       "task-pipeline",
						ProjectID:    "project-subpixel",
						Title:        "Processing pipeline implementation",
						OwnerUserID:  "owner-1",
						Priority:     "HIGH",
						Status:       "RUNNING",
						TaskKind:     "EXECUTION",
						TaskTemplate: "generic",
						LinkedBy:     "beta",
						LinkedAt:     "2026-05-07T00:00:01Z",
						ClaimAgentID: stringPtr("delta"),
						ClaimStatus:  &claimed,
					},
				},
			},
		},
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	for i := 0; i < idleReflectionNoWorkThreshold+1; i++ {
		task, err := runtime.ensureRunnableTask(context.Background())
		if err != nil {
			t.Fatalf("ensureRunnableTask tick %d error: %v", i+1, err)
		}
		if task != nil {
			t.Fatalf("active implementation should suppress idle reflection on tick %d, got %+v", i+1, task)
		}
	}
	if taskSubmitCalls != 0 {
		t.Fatalf("task.submit calls = %d, want 0", taskSubmitCalls)
	}
	if runtime.idleNoWorkCount != 0 || runtime.idleNoWorkKey != "" {
		t.Fatalf("active implementation must suppress idle counters, count=%d key=%q", runtime.idleNoWorkCount, runtime.idleNoWorkKey)
	}
}

func TestTrustFirstIdleReflectionTargetAllowsActiveSiblingWork(t *testing.T) {
	claimed := "CLAIMED"
	snapshot := WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-subpixel", WorkspaceID: "ws-1", Title: "Subpixel Art", Status: "ACTIVE"}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:       "task-pipeline",
			ProjectID:    "project-subpixel",
			Title:        "Processing pipeline implementation",
			Status:       "RUNNING",
			Priority:     "HIGH",
			TaskKind:     "EXECUTION",
			ClaimAgentID: stringPtr("delta"),
			ClaimStatus:  &claimed,
		}},
	}
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)

	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "strategist", CoordinationMode: CoordinationModeTrustFirst})
	strict := buildIdleReflectionTarget(snapshot, "ws-1", now, false, policy)
	if strict.Key != "" {
		t.Fatalf("strict mode should still suppress idle reflection for active work, got %+v", strict)
	}
	trustFirst := buildIdleReflectionTarget(snapshot, "ws-1", now, true, policy)
	if trustFirst.Key != "project:project-subpixel" || trustFirst.ProjectID != "project-subpixel" {
		t.Fatalf("trust-first should allow proactive meta-reflection alongside active work, got %+v", trustFirst)
	}
	if !strings.Contains(trustFirst.Title, "Project metacognition pass") || !strings.Contains(trustFirst.Description, "task_submit follow-up") || !strings.Contains(trustFirst.Description, "acceptance criteria") {
		t.Fatalf("trust-first idle target should describe product-quality follow-up work, got %+v", trustFirst)
	}
	if len(trustFirst.OpenTaskIDs) != 1 || trustFirst.OpenTaskIDs[0] != "task-pipeline" {
		t.Fatalf("expected active task context to be carried into target, got %+v", trustFirst)
	}
	if len(trustFirst.ActiveTaskIDs) != 1 || trustFirst.ActiveTaskIDs[0] != "task-pipeline" {
		t.Fatalf("expected active task ownership to be carried into target, got %+v", trustFirst)
	}
	if ambientAutonomyShouldEscalateToIdleDuty(trustFirst, AgentWorkNextResult{HasWork: false, Reason: "idle"}, policy, ambientAutonomyDisposition{OwnsNoWork: true, Ran: true}) {
		t.Fatalf("active owned implementation work must not escalate a no-task heartbeat into a competing idle duty task")
	}
}

func TestTrustFirstIdleReflectionTargetTreatsPendingPatchQueueReviewAsActiveWork(t *testing.T) {
	snapshot := WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-subpixel", WorkspaceID: "ws-1", Title: "Subpixel Art", Status: "ACTIVE"}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      "task-review-patchq-queue-1-item-1",
			ProjectID:   "project-subpixel",
			Title:       "Review patch queue candidate for project-subpixel",
			Description: "Patch queue candidate is ready for independent review.\n\n- queue_id: queue-1\n- item_id: item-1\n- branch_id: branch-1",
			Status:      "PENDING",
			Priority:    "critical",
			TaskKind:    "EXECUTION",
			ProjectLane: "review",
			Tags:        []string{"project", "review", "patch_queue"},
		}},
	}
	now := time.Date(2026, 5, 13, 1, 30, 0, 0, time.UTC)
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "strategist", CoordinationMode: CoordinationModeTrustFirst})

	target := buildIdleReflectionTarget(snapshot, "ws-1", now, true, policy)
	if target.Key != "project:project-subpixel" {
		t.Fatalf("expected project idle target context, got %+v", target)
	}
	if len(target.ActiveTaskIDs) != 1 || target.ActiveTaskIDs[0] != "task-review-patchq-queue-1-item-1" {
		t.Fatalf("pending patch queue review must be treated as active continuation work, got %+v", target)
	}
	if ambientAutonomyShouldEscalateToIdleDuty(target, AgentWorkNextResult{HasWork: false, Reason: "project_patch_queue_review_role_required"}, policy, ambientAutonomyDisposition{OwnsNoWork: true, Ran: true}) {
		t.Fatalf("open patch queue review work must not escalate into another idle reflection task")
	}
}

func TestTrustFirstIdleReflectionTargetPrunesStaleSupersedeTaskAfterAcceptedSuccessor(t *testing.T) {
	taskID := "task-patchq-supersede-project-subpixel-old"
	target := idleReflectionTarget{
		Key:            "project:project-subpixel",
		TaskID:         "task-idle-reflection-project-subpixel",
		ProjectID:      "project-subpixel",
		ProjectLane:    "qa",
		Title:          "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description:    "Inspect current project quality.",
		OpenTaskIDs:    []string{taskID},
		BlockedTaskIDs: []string{taskID},
		ActiveTaskIDs:  []string{taskID},
		AnchorTaskIDs:  []string{taskID},
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "integrator", CoordinationMode: CoordinationModeTrustFirst})
	coordination := ProjectCoordinationRecord{
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      taskID,
			ProjectID:   "project-subpixel",
			Title:       "Supersede blocked patch queue item after fresh evidence",
			Description: "Patch queue stewardship task.\n\n- action: supersede\n- queue_id: queue-old\n- item_id: item-old\n- branch_id: branch-beta\n- expected_head_sha: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Status:      "PENDING",
			TaskKind:    "EXECUTION",
			ProjectLane: "integration",
			Tags:        []string{"project", "patch-queue", "supersede", "queue-stewardship"},
		}},
		PatchQueueItems: []ProjectPatchQueueItemRecord{
			{
				QueueID:   "queue-old",
				ItemID:    "item-old",
				ProjectID: "project-subpixel",
				BranchID:  "branch-beta",
				State:     "BLOCKED",
				HeadSHA:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			},
			{
				QueueID:           "queue-new",
				ItemID:            "item-new",
				ProjectID:         "project-subpixel",
				BranchID:          "branch-beta",
				State:             "ACCEPTED",
				HeadSHA:           "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				SupersedesQueueID: "queue-old",
				SupersedesItemID:  "item-old",
				ReviewDocKey:      "project.project-subpixel.branch.branch-beta.review",
			},
		},
		Branches: []ProjectBranchRecord{{
			BranchID:   "branch-beta",
			BranchName: "agent-beta/project-subpixel/pipeline",
			HeadSHA:    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Status:     "READY_FOR_REVIEW",
		}},
	}

	got := idleReflectionTargetWithProjectCoordination(target, coordination, policy)
	if stringSliceContains(got.OpenTaskIDs, taskID) || stringSliceContains(got.BlockedTaskIDs, taskID) || stringSliceContains(got.ActiveTaskIDs, taskID) || stringSliceContains(got.AnchorTaskIDs, taskID) {
		t.Fatalf("stale supersede task should be pruned from target ids, got %+v", got)
	}
	if !stringSliceContains(got.StaleTaskIDs, taskID) || !strings.Contains(got.Description, "ignored_stale_task_ids: "+taskID) {
		t.Fatalf("expected stale task guidance for %s, got description:\n%s", taskID, got.Description)
	}
	if got.ProjectLane != "integration" || !strings.Contains(got.Description, "project_patch_queue_integrate") {
		t.Fatalf("accepted successor should still drive integration guidance after pruning, got %+v", got)
	}
	if ambientAutonomyShouldEscalateToIdleDuty(got, AgentWorkNextResult{HasWork: false, Reason: "idle"}, policy, ambientAutonomyDisposition{OwnsNoWork: true, Ran: true}) == false {
		t.Fatalf("stale-only active task ids must not block idle escalation after pruning, got %+v", got)
	}
}

func TestTrustFirstIdleReflectionTargetPrunesClaimStewardshipAfterDecision(t *testing.T) {
	taskID := "task-patchq-claim-steward-project-subpixel"
	target := idleReflectionTarget{
		ProjectID:     "project-subpixel",
		ProjectLane:   "qa",
		Title:         "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description:   "Inspect current project quality.",
		OpenTaskIDs:   []string{taskID},
		ActiveTaskIDs: []string{taskID},
		AnchorTaskIDs: []string{taskID},
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "reviewer", CoordinationMode: CoordinationModeTrustFirst})
	coordination := ProjectCoordinationRecord{
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      taskID,
			ProjectID:   "project-subpixel",
			Title:       "Resolve claimed patch queue item lifecycle",
			Description: "Patch queue claim stewardship task.\n\n- queue_id: queue-1\n- item_id: item-1\n- branch_id: branch-beta",
			Status:      "PENDING",
			TaskKind:    "EXECUTION",
			ProjectLane: "integration",
			Tags:        []string{"project", "patch-queue", "queue-stewardship", "claim-stewardship"},
		}},
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:   "queue-1",
			ItemID:    "item-1",
			ProjectID: "project-subpixel",
			BranchID:  "branch-beta",
			State:     "BLOCKED",
			HeadSHA:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		}},
	}

	got := idleReflectionTargetWithProjectCoordination(target, coordination, policy)
	if stringSliceContains(got.OpenTaskIDs, taskID) || stringSliceContains(got.ActiveTaskIDs, taskID) || stringSliceContains(got.AnchorTaskIDs, taskID) {
		t.Fatalf("claim stewardship task for non-CLAIMED item should be pruned, got %+v", got)
	}
	if !stringSliceContains(got.StaleTaskIDs, taskID) || !strings.Contains(got.Description, "Current project coordination shows") {
		t.Fatalf("expected stale claim stewardship guidance, got description:\n%s", got.Description)
	}
}

func TestTrustFirstIdleReflectionTargetPrunesOwnerSubmitAfterAcceptedBranchHead(t *testing.T) {
	taskID := "task-requeue-project-subpixel-beta-owner-submit"
	target := idleReflectionTarget{
		ProjectID:     "project-subpixel",
		ProjectLane:   "qa",
		Title:         "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description:   "Inspect current project quality.",
		OpenTaskIDs:   []string{taskID},
		ActiveTaskIDs: []string{taskID},
		AnchorTaskIDs: []string{taskID},
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "strategist", CoordinationMode: CoordinationModeTrustFirst})
	coordination := ProjectCoordinationRecord{
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      taskID,
			ProjectID:   "project-subpixel",
			Title:       "Owner requeue submit for beta accepted branch",
			Description: "Patch queue owner submit follow-up.\n\n- branch_id: branch-beta\n- head_sha: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Status:      "PENDING",
			TaskKind:    "EXECUTION",
			ProjectLane: "coordination",
			Tags:        []string{"project", "patch-queue", "owner-submit", "owner-bound-kind:patch_queue_submit"},
		}},
		PatchQueueItems: []ProjectPatchQueueItemRecord{
			{
				QueueID:   "queue-old",
				ItemID:    "item-old",
				ProjectID: "project-subpixel",
				BranchID:  "branch-beta",
				State:     "BLOCKED",
				HeadSHA:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				UpdatedAt: "2026-05-13T00:00:00Z",
			},
			{
				QueueID:   "queue-new",
				ItemID:    "item-new",
				ProjectID: "project-subpixel",
				BranchID:  "branch-beta",
				State:     "ACCEPTED",
				HeadSHA:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				UpdatedAt: "2026-05-13T00:01:00Z",
			},
		},
	}

	got := idleReflectionTargetWithProjectCoordination(target, coordination, policy)
	if stringSliceContains(got.OpenTaskIDs, taskID) || stringSliceContains(got.ActiveTaskIDs, taskID) || stringSliceContains(got.AnchorTaskIDs, taskID) {
		t.Fatalf("owner-submit task for accepted branch/head should be pruned, got %+v", got)
	}
	if !stringSliceContains(got.StaleTaskIDs, taskID) {
		t.Fatalf("expected owner-submit stale id to be recorded, got %+v", got)
	}
}

func TestTrustFirstIdleReflectionTargetPrunesHistoricalBlockedSupersedeAfterMergedAcceptance(t *testing.T) {
	taskID := "task-patchq-supersede-old-branch"
	task := WorkspaceTaskRecord{
		TaskID:      taskID,
		ProjectID:   "project-subpixel",
		Title:       "Supersede blocked patch queue item after fresh evidence",
		Description: "Patch queue stewardship task.\n\n- action: supersede\n- queue_id: queue-old\n- item_id: item-old\n- branch_id: branch-old\n- expected_head_sha: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Status:      "PENDING",
		TaskKind:    "EXECUTION",
		ProjectLane: "integration",
		Tags:        []string{"project", "patch-queue", "supersede", "queue-stewardship"},
	}
	target := idleReflectionTarget{
		ProjectID:     "project-subpixel",
		ProjectLane:   "qa",
		Title:         "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description:   "Inspect current project quality.",
		OpenTaskIDs:   []string{taskID},
		ActiveTaskIDs: []string{taskID},
		AnchorTaskIDs: []string{taskID},
		TaskRecords:   []WorkspaceTaskRecord{task},
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "strategist", CoordinationMode: CoordinationModeTrustFirst})
	coordination := ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{
			{
				QueueID:   "queue-old",
				ItemID:    "item-old",
				RepoID:    "repo-main",
				ProjectID: "project-subpixel",
				BranchID:  "branch-old",
				State:     "BLOCKED",
				HeadSHA:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Pathset:   []string{"src/**", "package.json"},
				UpdatedAt: "2026-05-12T07:56:05Z",
			},
			{
				QueueID:   "queue-current",
				ItemID:    "item-current",
				RepoID:    "repo-main",
				ProjectID: "project-subpixel",
				BranchID:  "branch-current",
				State:     "ACCEPTED",
				HeadSHA:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				Pathset:   []string{"src/App.tsx", "package.json"},
				UpdatedAt: "2026-05-13T21:22:39Z",
			},
		},
		Branches: []ProjectBranchRecord{{
			BranchID: "branch-current",
			HeadSHA:  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Status:   "MERGED",
		}},
	}

	got := idleReflectionTargetWithProjectCoordination(target, coordination, policy)
	if stringSliceContains(got.OpenTaskIDs, taskID) || stringSliceContains(got.ActiveTaskIDs, taskID) {
		t.Fatalf("historical blocked supersede should not remain active after newer merged acceptance, got %+v", got)
	}
	if !stringSliceContains(got.StaleTaskIDs, taskID) || !strings.Contains(got.Description, "ignored_stale_task_ids: "+taskID) {
		t.Fatalf("expected historical stale supersede guidance, got description:\n%s", got.Description)
	}
}

func TestTrustFirstIdleReflectionTargetKeepsHistoricalBlockedSupersedeWhenAcceptedCandidateUnrelated(t *testing.T) {
	taskID := "task-patchq-supersede-old-docs-branch"
	task := WorkspaceTaskRecord{
		TaskID:      taskID,
		ProjectID:   "project-subpixel",
		Title:       "Supersede blocked patch queue item after fresh evidence",
		Description: "Patch queue stewardship task.\n\n- action: supersede\n- queue_id: queue-old\n- item_id: item-old\n- branch_id: branch-old\n- expected_head_sha: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Status:      "PENDING",
		TaskKind:    "EXECUTION",
		ProjectLane: "integration",
		Tags:        []string{"project", "patch-queue", "supersede", "queue-stewardship"},
	}
	target := idleReflectionTarget{
		ProjectID:     "project-subpixel",
		ProjectLane:   "qa",
		Title:         "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description:   "Inspect current project quality.",
		OpenTaskIDs:   []string{taskID},
		ActiveTaskIDs: []string{taskID},
		AnchorTaskIDs: []string{taskID},
		TaskRecords:   []WorkspaceTaskRecord{task},
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "strategist", CoordinationMode: CoordinationModeTrustFirst})
	coordination := ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{
			{
				QueueID:   "queue-old",
				ItemID:    "item-old",
				RepoID:    "repo-main",
				ProjectID: "project-subpixel",
				BranchID:  "branch-old",
				State:     "BLOCKED",
				HeadSHA:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Pathset:   []string{"docs/**"},
				UpdatedAt: "2026-05-12T07:56:05Z",
			},
			{
				QueueID:   "queue-current",
				ItemID:    "item-current",
				RepoID:    "repo-main",
				ProjectID: "project-subpixel",
				BranchID:  "branch-current",
				State:     "ACCEPTED",
				HeadSHA:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				Pathset:   []string{"src/**", "package.json"},
				UpdatedAt: "2026-05-13T21:22:39Z",
			},
		},
		Branches: []ProjectBranchRecord{{
			BranchID: "branch-current",
			HeadSHA:  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Status:   "MERGED",
		}},
	}

	got := idleReflectionTargetWithProjectCoordination(target, coordination, policy)
	if !stringSliceContains(got.OpenTaskIDs, taskID) || !stringSliceContains(got.ActiveTaskIDs, taskID) {
		t.Fatalf("unrelated newer accepted candidate must not prune historical blocked supersede, got %+v", got)
	}
	if stringSliceContains(got.StaleTaskIDs, taskID) {
		t.Fatalf("unrelated newer accepted candidate should not mark stale id, got %+v", got)
	}
}

func TestTrustFirstIdleReflectionTargetKeepsLivePatchQueueReviewTask(t *testing.T) {
	taskID := "task-review-patchq-queue-1-item-1"
	target := idleReflectionTarget{
		ProjectID:     "project-subpixel",
		ProjectLane:   "qa",
		Title:         "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description:   "Inspect current project quality.",
		OpenTaskIDs:   []string{taskID},
		ActiveTaskIDs: []string{taskID},
		AnchorTaskIDs: []string{taskID},
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "strategist", CoordinationMode: CoordinationModeTrustFirst})
	coordination := ProjectCoordinationRecord{
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      taskID,
			ProjectID:   "project-subpixel",
			Title:       "Review patch queue candidate for project-subpixel",
			Description: "Patch queue candidate is ready for independent review.\n\n- queue_id: queue-1\n- item_id: item-1\n- branch_id: branch-1",
			Status:      "PENDING",
			TaskKind:    "EXECUTION",
			ProjectLane: "review",
			Tags:        []string{"project", "review", "patch_queue"},
		}},
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:   "queue-1",
			ItemID:    "item-1",
			ProjectID: "project-subpixel",
			BranchID:  "branch-1",
			State:     "PROPOSED",
			HeadSHA:   "1111111111111111111111111111111111111111",
		}},
	}

	got := idleReflectionTargetWithProjectCoordination(target, coordination, policy)
	if !stringSliceContains(got.ActiveTaskIDs, taskID) || stringSliceContains(got.StaleTaskIDs, taskID) {
		t.Fatalf("live PROPOSED review task must remain active, got %+v", got)
	}
	if ambientAutonomyShouldEscalateToIdleDuty(got, AgentWorkNextResult{HasWork: false, Reason: "project_patch_queue_review_role_required"}, policy, ambientAutonomyDisposition{OwnsNoWork: true, Ran: true}) {
		t.Fatalf("live patch queue review should still suppress competing idle task")
	}
}

func TestTrustFirstIdleReflectionTargetDoesNotPruneAmbiguousQueueOnlyTask(t *testing.T) {
	taskID := "task-review-patchq-queue-only"
	target := idleReflectionTarget{
		ProjectID:     "project-subpixel",
		ProjectLane:   "qa",
		Title:         "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description:   "Inspect current project quality.",
		OpenTaskIDs:   []string{taskID},
		ActiveTaskIDs: []string{taskID},
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "strategist", CoordinationMode: CoordinationModeTrustFirst})
	coordination := ProjectCoordinationRecord{
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      taskID,
			ProjectID:   "project-subpixel",
			Title:       "Review patch queue candidate",
			Description: "Patch queue candidate needs review.\n\n- queue_id: queue-1",
			Status:      "PENDING",
			TaskKind:    "EXECUTION",
			ProjectLane: "review",
			Tags:        []string{"project", "review", "patch_queue"},
		}},
		PatchQueueItems: []ProjectPatchQueueItemRecord{
			{QueueID: "queue-1", ItemID: "item-a", ProjectID: "project-subpixel", BranchID: "branch-a", State: "ACCEPTED", HeadSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			{QueueID: "queue-1", ItemID: "item-b", ProjectID: "project-subpixel", BranchID: "branch-b", State: "PROPOSED", HeadSHA: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		},
	}

	got := idleReflectionTargetWithProjectCoordination(target, coordination, policy)
	if !stringSliceContains(got.ActiveTaskIDs, taskID) || stringSliceContains(got.StaleTaskIDs, taskID) {
		t.Fatalf("queue-only task is ambiguous and must fail open, got %+v", got)
	}
}

func TestTrustFirstIdleReflectionTargetRespectsLocalWorkerScope(t *testing.T) {
	claimed := "CLAIMED"
	snapshot := WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-subpixel", WorkspaceID: "ws-1", Title: "Subpixel Art", Status: "ACTIVE"}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:       "task-pipeline",
			ProjectID:    "project-subpixel",
			Title:        "Processing pipeline implementation",
			Status:       "RUNNING",
			Priority:     "HIGH",
			TaskKind:     "EXECUTION",
			ClaimAgentID: stringPtr("delta"),
			ClaimStatus:  &claimed,
		}},
	}
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "worker", CoordinationMode: CoordinationModeTrustFirst})

	target := buildIdleReflectionTarget(snapshot, "ws-1", now, true, policy)
	if target.Key != "" || target.TaskID != "" {
		t.Fatalf("local worker metacognition should not spawn broad idle reflection tasks, got %+v", target)
	}
}

func TestTrustFirstIdleReflectionTargetReusesExistingReflectionTask(t *testing.T) {
	snapshot := WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-subpixel", WorkspaceID: "ws-1", Title: "Subpixel Art", Status: "ACTIVE"}},
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID:      "task-idle-reflection-existing",
				ProjectID:   "project-subpixel",
				Title:       "Project reflection pass: join active quality work",
				Status:      "PENDING",
				Priority:    "normal",
				TaskKind:    "EXECUTION",
				ProjectLane: "qa",
				Tags:        []string{"meta-reflection", "anti-idle"},
			},
			{
				TaskID:      "task-review-gap",
				ProjectID:   "project-subpixel",
				Title:       "Review export UX",
				Status:      "PENDING",
				Priority:    "high",
				TaskKind:    "EXECUTION",
				ProjectLane: "qa",
			},
		},
	}
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "integrator", CoordinationMode: CoordinationModeTrustFirst})

	target := buildIdleReflectionTarget(snapshot, "ws-1", now, true, policy)
	if target.TaskID != "task-idle-reflection-existing" || target.ExistingTask == nil {
		t.Fatalf("expected idle reflection to reuse existing active reflection task, got %+v", target)
	}
}

func TestTrustFirstIdleReflectionTargetIgnoresTerminalPriorCooldownBucket(t *testing.T) {
	now := time.Date(2026, 5, 9, 13, 22, 0, 0, time.UTC)
	prior := time.Date(2026, 5, 9, 13, 1, 0, 0, time.UTC)
	scopeKey := "project:project-subpixel"
	priorTaskID := idleReflectionTaskID(scopeKey, prior)
	currentTaskID := idleReflectionTaskID(scopeKey, now)
	if priorTaskID == currentTaskID {
		t.Fatalf("test setup requires distinct cooldown buckets, got %q", priorTaskID)
	}

	snapshot := WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-subpixel", WorkspaceID: "ws-1", Title: "Subpixel Art", Status: "ACTIVE"}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      priorTaskID,
			ProjectID:   "project-subpixel",
			Title:       "Project metacognition pass: inspect plan, blockers, and next improvements",
			Status:      "RESOLVED",
			Priority:    "normal",
			TaskKind:    "EXECUTION",
			ProjectLane: "qa",
			Tags:        []string{"meta-reflection", "anti-idle", "metacognition-scope-project"},
		}},
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "strategist", CoordinationMode: CoordinationModeTrustFirst})

	target := buildIdleReflectionTarget(snapshot, "ws-1", now, true, policy)
	if target.TaskID != currentTaskID {
		t.Fatalf("expected new idle reflection task in current cooldown bucket %q, got %+v", currentTaskID, target)
	}
	if target.ExistingTask != nil {
		t.Fatalf("terminal prior-bucket idle reflection must not suppress new quality cycle, got %+v", target.ExistingTask)
	}
}

func TestTrustFirstIdleReflectionTargetReusesOpenCurrentCooldownBucketTask(t *testing.T) {
	now := time.Date(2026, 5, 9, 13, 22, 0, 0, time.UTC)
	taskID := idleReflectionTaskID("project:project-subpixel", now)
	snapshot := WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-subpixel", WorkspaceID: "ws-1", Title: "Subpixel Art", Status: "ACTIVE"}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      taskID,
			ProjectID:   "project-subpixel",
			Title:       "Project metacognition pass: inspect plan, blockers, and next improvements",
			Status:      "PENDING",
			Priority:    "normal",
			TaskKind:    "EXECUTION",
			ProjectLane: "qa",
			Tags:        []string{"meta-reflection", "anti-idle", "metacognition-scope-project"},
		}},
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "strategist", CoordinationMode: CoordinationModeTrustFirst})

	target := buildIdleReflectionTarget(snapshot, "ws-1", now, true, policy)
	if target.TaskID != taskID || target.ExistingTask == nil {
		t.Fatalf("expected open current-bucket idle reflection to be reused, got %+v", target)
	}
}

func TestTrustFirstIdleReflectionTargetPromotesAcceptedPatchQueueToIntegration(t *testing.T) {
	target := idleReflectionTarget{
		ProjectID:   "project-subpixel",
		ProjectLane: "qa",
		Title:       "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description: "Inspect current project quality.",
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "integrator", CoordinationMode: CoordinationModeTrustFirst})
	coordination := ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:         "queue-1",
			ItemID:          "item-1",
			ProjectID:       "project-subpixel",
			BranchID:        "branch-theta",
			State:           "ACCEPTED",
			HeadSHA:         "b7f175bf9b8d027163f730e244f2ce2c8f186313",
			DecisionSummary: "Accepted MVP editor after build and smoke.",
			ReviewDocKey:    "project.project-subpixel.branch.branch-theta.review",
		}},
		Branches: []ProjectBranchRecord{{
			BranchID:   "branch-theta",
			BranchName: "agent-theta/project-subpixel/editor",
			HeadSHA:    "b7f175bf9b8d027163f730e244f2ce2c8f186313",
			Status:     "READY_FOR_REVIEW",
		}},
	}

	got := idleReflectionTargetWithProjectCoordination(target, coordination, policy)
	if got.ProjectLane != "integration" {
		t.Fatalf("expected accepted patch queue candidate to promote lane to integration, got %+v", got)
	}
	for _, want := range []string{
		"Integration convergence pass",
		"project_patch_queue_integrate",
		"queue_id=queue-1",
		"item_id=item-1",
		"branch_id=branch-theta",
		"not a request for another scaffold from seed",
	} {
		if !strings.Contains(got.Title+"\n"+got.Description, want) {
			t.Fatalf("expected integration guidance %q, got title=%q description=\n%s", want, got.Title, got.Description)
		}
	}
}

func TestTrustFirstIdleReflectionDetectsAcceptedUIWithoutVisualAcceptance(t *testing.T) {
	target := idleReflectionTarget{
		ProjectID:   "project-subpixel",
		ProjectLane: "qa",
		Title:       "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description: "Inspect current project quality.",
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "reviewer", CoordinationMode: CoordinationModeTrustFirst})
	coordination := ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-subpixel"},
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:         "queue-1",
			ItemID:          "item-1",
			ProjectID:       "project-subpixel",
			BranchID:        "branch-theta",
			State:           "ACCEPTED",
			HeadSHA:         "b7f175bf9b8d027163f730e244f2ce2c8f186313",
			DecisionSummary: "Accepted MVP editor after build and browser smoke.",
			ReviewDocKey:    "project.project-subpixel.branch.branch-theta.review",
			Pathset:         []string{"src/App.tsx", "src/styles.css"},
		}},
		Branches: []ProjectBranchRecord{{
			BranchID:     "branch-theta",
			BranchName:   "agent-theta/project-subpixel/editor",
			HeadSHA:      "b7f175bf9b8d027163f730e244f2ce2c8f186313",
			Status:       "MERGED",
			ReviewDocKey: "project.project-subpixel.branch.branch-theta.review",
		}},
	}
	docsByKey := map[string]WorkspaceDocRecord{
		"project.project-subpixel.branch.branch-theta.review": {
			DocKey:  "project.project-subpixel.branch.branch-theta.review",
			Title:   "Branch Review",
			Content: "Build and smoke passed, but this packet has no screenshot matrix.",
		},
	}

	gap, ok := acceptedPatchQueueVisualAcceptanceGapFromDocs(coordination, docsByKey)
	if !ok {
		t.Fatalf("expected accepted UI candidate without visual packet to produce a visual acceptance gap")
	}
	got := idleReflectionTargetWithVisualAcceptanceGap(target, gap, policy, time.Date(2026, 5, 14, 7, 30, 0, 0, time.UTC))
	if got.ProjectLane != "qa" || !strings.Contains(got.Key, "visual_acceptance_gap") || got.ExistingTask != nil {
		t.Fatalf("expected visual acceptance QA target, got %+v", got)
	}
	for _, want := range []string{
		"Visual acceptance pass",
		"ACCEPTED UI-facing patch queue candidate without durable visual acceptance evidence",
		"rhizome_visual_acceptance_v1",
		"queue_id: queue-1",
		"item_id: item-1",
		"branch_id: branch-theta",
		"path:react-layout",
		"visual_verdict: pass",
	} {
		if !strings.Contains(got.Title+"\n"+got.Description, want) {
			t.Fatalf("expected visual gap guidance %q, got title=%q description=\n%s", want, got.Title, got.Description)
		}
	}
}

func TestTrustFirstIdleReflectionSkipsAcceptedUIWithVisualAcceptancePacket(t *testing.T) {
	packet := completeStructuredVisualPacketWithRealScreenshots(t)
	coordination := ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-subpixel"},
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:        "queue-1",
			ItemID:         "item-1",
			ProjectID:      "project-subpixel",
			BranchID:       "branch-theta",
			State:          "ACCEPTED",
			HeadSHA:        "b7f175bf9b8d027163f730e244f2ce2c8f186313",
			ReviewDocKey:   "project.project-subpixel.branch.branch-theta.review",
			EvidenceDocKey: "project.project-subpixel.visual.acceptance",
			Pathset:        []string{"src/App.tsx", "src/styles.css"},
		}},
		Branches: []ProjectBranchRecord{{
			BranchID:   "branch-theta",
			BranchName: "agent-theta/project-subpixel/editor",
			HeadSHA:    "b7f175bf9b8d027163f730e244f2ce2c8f186313",
			Status:     "MERGED",
		}},
	}
	docsByKey := map[string]WorkspaceDocRecord{
		"project.project-subpixel.visual.acceptance": {
			DocKey:  "project.project-subpixel.visual.acceptance",
			Title:   "Visual Acceptance",
			Content: packet,
		},
	}

	if gap, ok := acceptedPatchQueueVisualAcceptanceGapFromDocs(coordination, docsByKey); ok {
		t.Fatalf("visual packet should satisfy accepted UI gap, got %+v", gap)
	}
}

func TestTrustFirstIdleReflectionResurfacesWeakAcceptedVisualPacket(t *testing.T) {
	coordination := ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-subpixel"},
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:        "queue-1",
			ItemID:         "item-1",
			ProjectID:      "project-subpixel",
			BranchID:       "branch-theta",
			State:          "ACCEPTED",
			HeadSHA:        "b7f175bf9b8d027163f730e244f2ce2c8f186313",
			EvidenceDocKey: "project.project-subpixel.visual.acceptance",
			Pathset:        []string{"src/**"},
		}},
		Branches: []ProjectBranchRecord{{
			BranchID:   "branch-theta",
			BranchName: "agent-theta/project-subpixel/editor",
			HeadSHA:    "b7f175bf9b8d027163f730e244f2ce2c8f186313",
			Status:     "MERGED",
		}},
	}
	docsByKey := map[string]WorkspaceDocRecord{
		"project.project-subpixel.visual.acceptance": {
			DocKey: "project.project-subpixel.visual.acceptance",
			Title:  "Visual Acceptance",
			Content: strings.Join([]string{
				"schema: rhizome_visual_acceptance_v1",
				"visual_verdict: pass",
				"reviewed_against: AC-13, EV-01",
				"observed_url: http://127.0.0.1:59432/",
				"validation_checkout: C:/work/project",
				"branch_id: branch-theta",
				"head_sha: b7f175bf9b8d027163f730e244f2ce2c8f186313",
				"screenshot_refs: artifacts/desktop-samples.png, artifacts/desktop-upload.png, artifacts/mobile-samples.png",
				"viewport_matrix: desktop 1440x1100, mobile 390x844",
				"scenarios: sample flow, upload flow, search/filter flow, mobile responsive flow",
				"checks: overlap, clipping, contrast, readability, responsive fit, typography, hierarchy, spacing, real-user usability",
			}, "\n"),
		},
	}

	gap, ok := acceptedPatchQueueVisualAcceptanceGapFromDocs(coordination, docsByKey)
	if !ok {
		t.Fatalf("weak accepted visual packet should resurface as visual acceptance debt")
	}
	if !containsAnySignal(strings.Join(gap.Missing, "\n"), []string{"initial_state screenshot ref/path", "primary_flow screenshot ref/path", "result_state screenshot ref/path"}) {
		t.Fatalf("expected state-specific missing evidence, got %+v", gap)
	}
	if len(gap.EvidenceDocKeys) != 1 || gap.EvidenceDocKeys[0] != "project.project-subpixel.visual.acceptance" {
		t.Fatalf("expected inspected weak visual packet key, got %+v", gap.EvidenceDocKeys)
	}
}

func TestTrustFirstIdleReflectionDoesNotRequireVisualAcceptanceForNonUIAcceptedCandidate(t *testing.T) {
	coordination := ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-workflow"},
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:   "queue-1",
			ItemID:    "item-1",
			ProjectID: "project-workflow",
			BranchID:  "branch-runner",
			State:     "ACCEPTED",
			HeadSHA:   "b7f175bf9b8d027163f730e244f2ce2c8f186313",
			Pathset:   []string{"cmd/workflow/main.go", "internal/dag/graph.go"},
		}},
		Branches: []ProjectBranchRecord{{
			BranchID: "branch-runner",
			HeadSHA:  "b7f175bf9b8d027163f730e244f2ce2c8f186313",
			Status:   "MERGED",
		}},
	}

	if gap, ok := acceptedPatchQueueVisualAcceptanceGapFromDocs(coordination, nil); ok {
		t.Fatalf("non-UI candidate should not require visual acceptance, got %+v", gap)
	}
}

func TestTrustFirstIdleReflectionSkipsAcceptedUIWhenVisualFollowupAlreadyOpen(t *testing.T) {
	coordination := ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-subpixel"},
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:   "queue-1",
			ItemID:    "item-1",
			ProjectID: "project-subpixel",
			BranchID:  "branch-theta",
			State:     "ACCEPTED",
			HeadSHA:   "b7f175bf9b8d027163f730e244f2ce2c8f186313",
			Pathset:   []string{"src/App.tsx", "src/styles.css"},
		}},
		Branches: []ProjectBranchRecord{{
			BranchID: "branch-theta",
			HeadSHA:  "b7f175bf9b8d027163f730e244f2ce2c8f186313",
			Status:   "MERGED",
		}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      "task-visual-acceptance-branch-theta",
			ProjectID:   "project-subpixel",
			Status:      "PENDING",
			Title:       "Visual acceptance pass for branch-theta",
			Description: "Publish rhizome_visual_acceptance_v1 for queue-1 item-1 branch-theta with screenshots and viewport matrix.",
			ProjectLane: "qa",
			Tags:        []string{"visual-acceptance", "qa"},
		}},
	}

	if gap, ok := acceptedPatchQueueVisualAcceptanceGapFromDocs(coordination, nil); ok {
		t.Fatalf("open exact visual acceptance follow-up should suppress duplicate idle gap, got %+v", gap)
	}
}

func TestTrustFirstIdleReflectionDoesNotSuppressVisualGapForUnrelatedScreenshotTask(t *testing.T) {
	coordination := ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-subpixel"},
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:   "queue-1",
			ItemID:    "item-1",
			ProjectID: "project-subpixel",
			BranchID:  "branch-theta",
			State:     "ACCEPTED",
			HeadSHA:   "b7f175bf9b8d027163f730e244f2ce2c8f186313",
			Pathset:   []string{"src/App.tsx", "src/styles.css"},
		}},
		Branches: []ProjectBranchRecord{{
			BranchID: "branch-theta",
			HeadSHA:  "b7f175bf9b8d027163f730e244f2ce2c8f186313",
			Status:   "MERGED",
		}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      "task-unrelated-screenshots",
			ProjectID:   "project-subpixel",
			Status:      "PENDING",
			Title:       "Capture screenshots for branch-theta docs",
			Description: "Collect screenshot and viewport examples for branch-theta marketing docs only.",
			ProjectLane: "qa",
			Tags:        []string{"screenshots"},
		}},
	}

	if gap, ok := acceptedPatchQueueVisualAcceptanceGapFromDocs(coordination, nil); !ok {
		t.Fatalf("unrelated screenshot task should not suppress exact visual acceptance gap")
	} else if gap.ItemID != "item-1" {
		t.Fatalf("unexpected visual gap %+v", gap)
	}
}

func TestRuntimeEnrichIdleReflectionPromotesAcceptedUIVisualGap(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.doc.get":
			switch rpcString(req.Params, "doc_key") {
			case "project.project-subpixel.reflection_board":
				writeRPCResult(w, req, map[string]any{"doc_key": rpcString(req.Params, "doc_key"), "content": ""})
			case "project.project-subpixel.branch.branch-theta.review":
				writeRPCResult(w, req, map[string]any{
					"doc_key": rpcString(req.Params, "doc_key"),
					"title":   "Branch Review",
					"content": "Build passed and browser smoke loaded the app, but no visual acceptance packet was published.",
				})
			default:
				writeRPCError(w, req, -32000, "workspace doc not found: "+rpcString(req.Params, "doc_key"))
			}
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"project": map[string]any{"project_id": "project-subpixel", "title": "Subpixel Art"},
					"patch_queue_items": []map[string]any{{
						"queue_id":         "queue-1",
						"item_id":          "item-1",
						"project_id":       "project-subpixel",
						"branch_id":        "branch-theta",
						"state":            "ACCEPTED",
						"head_sha":         "b7f175bf9b8d027163f730e244f2ce2c8f186313",
						"decision_summary": "Accepted after build and smoke.",
						"review_doc_key":   "project.project-subpixel.branch.branch-theta.review",
						"pathset":          []string{"src/App.tsx", "src/styles.css"},
					}},
					"branches": []map[string]any{{
						"branch_id":      "branch-theta",
						"branch_name":    "agent-theta/project-subpixel/editor",
						"head_sha":       "b7f175bf9b8d027163f730e244f2ce2c8f186313",
						"status":         "MERGED",
						"review_doc_key": "project.project-subpixel.branch.branch-theta.review",
					}},
				},
			})
		case "workspace.doc.list":
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{}})
		default:
			t.Fatalf("unexpected method during visual gap enrich test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			Workdir:      t.TempDir(),
			RhizomeRPC:   server.URL,
			RhizomeToken: "token",
			WorkspaceID:  "ws-1",
			AgentID:      "agent-gamma",
			OwnerUserID:  "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	t.Cleanup(func() { _ = runtime.Close() })

	target := idleReflectionTarget{
		ProjectID:   "project-subpixel",
		ProjectLane: "qa",
		Key:         "project:project-subpixel",
		TaskID:      "task-idle-reflection-project-project-subpixel-20260514-0730",
		Title:       "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description: "Inspect current project quality.",
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "strategist", CoordinationMode: CoordinationModeTrustFirst})
	got := runtime.enrichIdleReflectionTargetWithProjectCoordination(context.Background(), target, policy, time.Date(2026, 5, 14, 7, 30, 0, 0, time.UTC))
	if got.ProjectLane != "qa" || !strings.Contains(got.Key, "visual_acceptance_gap") || !strings.Contains(got.Title, "Visual acceptance pass") {
		t.Fatalf("expected runtime enrichment to promote accepted UI debt to visual acceptance target, got %+v", got)
	}
	if !strings.Contains(got.Description, "rhizome_visual_acceptance_v1") || !strings.Contains(got.Description, "branch_id: branch-theta") {
		t.Fatalf("visual acceptance guidance missing durable packet refs: %s", got.Description)
	}
}

func TestIdleReflectionConcreteCoordinationTargetsBypassAmbientTaskCreation(t *testing.T) {
	cases := []idleReflectionTarget{
		{Key: "project:icon|visual_acceptance_gap:abc", Title: "Visual acceptance pass", Description: "Visual acceptance debt:\n- publish rhizome_visual_acceptance_v1"},
		{Title: "Integration convergence pass", Description: "Canonical integration hint:\n- call project_patch_queue_integrate"},
		{Title: "Patch queue convergence pass", Description: "Blocked patch queue convergence hint:\n- resolve this queue-facing state"},
	}
	for _, target := range cases {
		if !idleReflectionTargetRequiresDeterministicTask(target) {
			t.Fatalf("expected deterministic task target for %+v", target)
		}
	}
	generic := idleReflectionTarget{
		Key:         "project:icon-sprite-forge",
		Title:       "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description: "Inspect current project quality and open a bounded follow-up if concrete gaps exist.",
	}
	if idleReflectionTargetRequiresDeterministicTask(generic) {
		t.Fatalf("generic metacognition target should remain ambient-eligible")
	}
}

func TestTrustFirstIdleReflectionTargetJoinsActiveReflectionBoardThread(t *testing.T) {
	target := idleReflectionTarget{
		ProjectID:       "project-subpixel",
		ProjectLane:     "qa",
		ReflectionScope: reflectionScopeProject,
		Title:           "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description:     "Inspect current project quality.",
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "strategist", CoordinationMode: CoordinationModeTrustFirst})
	board := `
- reflection_id: refl-ux
- scope: project
- status: active
- direction: compare current upload/convert/export UX against the product contract
- asks_review_from: reviewer
`

	got := idleReflectionTargetWithReflectionBoard(target, "project.project-subpixel.reflection_board", board, policy)
	if !strings.Contains(got.Title, "Project metacognition pass") || !strings.Contains(got.Description, "active in-scope reflection thread") || !strings.Contains(got.Description, "project.project-subpixel.reflection_board") {
		t.Fatalf("expected active board guidance to be joined, got %+v", got)
	}
	if !strings.Contains(got.Description, "before opening a new reflection direction") {
		t.Fatalf("expected board guidance to discourage duplicate directions, got:\n%s", got.Description)
	}
}

func TestTrustFirstIdleReflectionTargetIgnoresDifferentScopeReflectionBoardThread(t *testing.T) {
	target := idleReflectionTarget{
		ProjectID:       "project-subpixel",
		ProjectLane:     "qa",
		ReflectionScope: reflectionScopeArtifact,
		Title:           "Artifact quality iteration: inspect evidence and concrete gaps",
		Description:     "Inspect current project quality.",
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "reviewer", CoordinationMode: CoordinationModeTrustFirst})
	board := `
- reflection_id: refl-strategy
- scope: project
- status: active
- direction: global project strategy
`

	got := idleReflectionTargetWithReflectionBoard(target, "project.project-subpixel.reflection_board", board, policy)
	if got.Title != target.Title || got.Description != target.Description {
		t.Fatalf("different-scope board entry should not redirect artifact reflection, got %+v", got)
	}
}

func TestTrustFirstIdleReflectionTargetIgnoresResolvedReflectionBoardThread(t *testing.T) {
	target := idleReflectionTarget{
		ProjectID:       "project-subpixel",
		ProjectLane:     "qa",
		ReflectionScope: reflectionScopeProject,
		Title:           "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description:     "Inspect current project quality.",
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "strategist", CoordinationMode: CoordinationModeTrustFirst})
	board := `
- reflection_id: refl-old
- scope: project
- status: resolved
- direction: old blocker review
`

	got := idleReflectionTargetWithReflectionBoard(target, "project.project-subpixel.reflection_board", board, policy)
	if got.Title != target.Title || got.Description != target.Description {
		t.Fatalf("resolved board entry should not redirect new reflection, got %+v", got)
	}
}

func TestTrustFirstIdleReflectionTargetIgnoresMergedAcceptedPatchQueue(t *testing.T) {
	target := idleReflectionTarget{
		ProjectID:   "project-subpixel",
		ProjectLane: "qa",
		Title:       "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description: "Inspect current project quality.",
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "integrator", CoordinationMode: CoordinationModeTrustFirst})
	coordination := ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:  "queue-1",
			ItemID:   "item-1",
			BranchID: "branch-theta",
			State:    "ACCEPTED",
			HeadSHA:  "b7f175bf9b8d027163f730e244f2ce2c8f186313",
		}},
		Branches: []ProjectBranchRecord{{
			BranchID: "branch-theta",
			HeadSHA:  "b7f175bf9b8d027163f730e244f2ce2c8f186313",
			Status:   "MERGED",
		}},
	}

	got := idleReflectionTargetWithProjectCoordination(target, coordination, policy)
	if got.ProjectLane != "qa" || got.Title != target.Title || strings.Contains(got.Description, "project_patch_queue_integrate") {
		t.Fatalf("merged accepted candidate should not create another integration hint, got %+v", got)
	}
}

func TestTrustFirstIdleReflectionTargetIgnoresAcceptedPatchQueueWithMissingBranchEvidence(t *testing.T) {
	target := idleReflectionTarget{
		ProjectID:   "project-subpixel",
		ProjectLane: "qa",
		Title:       "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description: "Inspect current project quality.",
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "integrator", CoordinationMode: CoordinationModeTrustFirst})
	coordination := ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:  "queue-1",
			ItemID:   "item-1",
			BranchID: "branch-filtered-terminal",
			State:    "ACCEPTED",
			HeadSHA:  "b7f175bf9b8d027163f730e244f2ce2c8f186313",
		}},
	}

	got := idleReflectionTargetWithProjectCoordination(target, coordination, policy)
	if got.ProjectLane != "qa" || got.Title != target.Title || strings.Contains(got.Description, "project_patch_queue_integrate") {
		t.Fatalf("accepted item without visible source branch should not create integration hint, got %+v", got)
	}
}

func TestTrustFirstIdleReflectionTargetPromotesBlockedPatchQueueToConvergence(t *testing.T) {
	target := idleReflectionTarget{
		ProjectID:   "project-subpixel",
		ProjectLane: "qa",
		Title:       "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description: "Inspect current project quality.",
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "integrator", CoordinationMode: CoordinationModeTrustFirst})
	coordination := ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:         "queue-1",
			ItemID:          "item-1",
			BranchID:        "branch-kappa",
			State:           "BLOCKED",
			HeadSHA:         "34c0158c6577775b400e4b58e0470598e11e94ac",
			DecisionSummary: "Missing browser smoke evidence",
			DecisionDocKey:  "project.project-subpixel.patchq.item-1.decision",
			ReviewDocKey:    "project.project-subpixel.branch.branch-kappa.review",
		}},
		Branches: []ProjectBranchRecord{{
			BranchID:   "branch-kappa",
			BranchName: "agent-kappa/project-subpixel/editor",
			HeadSHA:    "34c0158c6577775b400e4b58e0470598e11e94ac",
			Status:     "READY_FOR_REVIEW",
		}},
	}

	got := idleReflectionTargetWithProjectCoordination(target, coordination, policy)
	if got.ProjectLane != "integration" || !strings.Contains(got.Title, "Patch queue convergence") {
		t.Fatalf("expected blocked patch queue to promote queue convergence, got %+v", got)
	}
	for _, want := range []string{
		"BLOCKED patch queue candidate",
		"same branch_id and head_sha",
		"project_patch_queue_lifecycle action=supersede/requeue",
		"queue_id=queue-1",
		"item_id=item-1",
		"new_item_id=item-1-requeue-34c0158c6577",
		"validation_doc_key/evidence_doc_key",
		"Missing browser smoke evidence",
	} {
		if !strings.Contains(got.Title+"\n"+got.Description, want) {
			t.Fatalf("expected blocked convergence guidance %q, got title=%q description=\n%s", want, got.Title, got.Description)
		}
	}
}

func TestTrustFirstIdleReflectionTargetSkipsBlockedPatchQueueWhenFollowupOpen(t *testing.T) {
	target := idleReflectionTarget{
		ProjectID:   "project-subpixel",
		ProjectLane: "qa",
		Title:       "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description: "Inspect current project quality.",
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "integrator", CoordinationMode: CoordinationModeTrustFirst})
	coordination := ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:  "queue-1",
			ItemID:   "item-1",
			BranchID: "branch-kappa",
			State:    "BLOCKED",
			HeadSHA:  "34c0158c6577775b400e4b58e0470598e11e94ac",
		}},
		Branches: []ProjectBranchRecord{{
			BranchID: "branch-kappa",
			HeadSHA:  "34c0158c6577775b400e4b58e0470598e11e94ac",
			Status:   "READY_FOR_REVIEW",
		}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      "task-patchq-validation-queue-1-item-1",
			ProjectID:   "project-subpixel",
			Status:      "PENDING",
			Title:       "Validate blocked integration candidate branch-kappa",
			Description: "Patch queue decision follow-up.\n\n- queue_id: queue-1\n- item_id: item-1\n- branch_id: branch-kappa",
			ProjectLane: "validation",
			Tags:        []string{"project", "patch-queue", "validation", "blocked"},
		}},
	}

	got := idleReflectionTargetWithProjectCoordination(target, coordination, policy)
	if got.ProjectLane != "qa" || got.Title != target.Title || strings.Contains(got.Description, "Blocked patch queue convergence hint") {
		t.Fatalf("open followup should own blocked queue convergence, got %+v", got)
	}
}

func TestTrustFirstIdleReflectionTargetDoesNotLetQueueWideSiblingFollowupHideBlockedItem(t *testing.T) {
	target := idleReflectionTarget{
		ProjectID:   "project-subpixel",
		ProjectLane: "qa",
		Title:       "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description: "Inspect current project quality.",
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "integrator", CoordinationMode: CoordinationModeTrustFirst})
	coordination := ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:         "queue-1",
			ItemID:          "item-blocked",
			BranchID:        "branch-blocked",
			State:           "BLOCKED",
			HeadSHA:         "34c0158c6577775b400e4b58e0470598e11e94ac",
			DecisionSummary: "Missing browser smoke evidence",
		}},
		Branches: []ProjectBranchRecord{{
			BranchID: "branch-blocked",
			HeadSHA:  "34c0158c6577775b400e4b58e0470598e11e94ac",
			Status:   "READY_FOR_REVIEW",
		}},
		Tasks: []WorkspaceTaskRecord{{
			TaskID:      "task-requeue-sibling-owner-submit",
			ProjectID:   "project-subpixel",
			Status:      "PENDING",
			Title:       "Owner-submit follow-up for a different accepted branch",
			Description: "Patch queue decision follow-up.\n\n- queue_id: queue-1\n- item_id: item-accepted\n- branch_id: branch-accepted",
			ProjectLane: "coordination",
			Tags:        []string{"project", "patch-queue", "requeue"},
		}},
	}

	got := idleReflectionTargetWithProjectCoordination(target, coordination, policy)
	if got.ProjectLane != "integration" || !strings.Contains(got.Description, "Blocked patch queue convergence hint") {
		t.Fatalf("queue-wide sibling followup must not hide blocked item convergence, got %+v", got)
	}
	for _, want := range []string{"item_id=item-blocked", "branch_id=branch-blocked", "project_patch_queue_lifecycle action=supersede/requeue"} {
		if !strings.Contains(got.Description, want) {
			t.Fatalf("expected blocked convergence guidance %q, got:\n%s", want, got.Description)
		}
	}
}

func TestTrustFirstIdleReflectionTargetIgnoresRejectedPatchQueue(t *testing.T) {
	target := idleReflectionTarget{
		ProjectID:   "project-subpixel",
		ProjectLane: "qa",
		Title:       "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description: "Inspect current project quality.",
	}
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "integrator", CoordinationMode: CoordinationModeTrustFirst})
	coordination := ProjectCoordinationRecord{
		PatchQueueItems: []ProjectPatchQueueItemRecord{{
			QueueID:  "queue-1",
			ItemID:   "item-1",
			BranchID: "branch-kappa",
			State:    "REJECTED",
			HeadSHA:  "34c0158c6577775b400e4b58e0470598e11e94ac",
		}},
		Branches: []ProjectBranchRecord{{
			BranchID: "branch-kappa",
			HeadSHA:  "34c0158c6577775b400e4b58e0470598e11e94ac",
			Status:   "READY_FOR_REVIEW",
		}},
	}

	got := idleReflectionTargetWithProjectCoordination(target, coordination, policy)
	if got.ProjectLane != "qa" || got.Title != target.Title || strings.Contains(got.Description, "project_patch_queue_integrate") || strings.Contains(got.Description, "Blocked patch queue convergence hint") {
		t.Fatalf("rejected candidate should not create integration or blocked convergence hint, got %+v", got)
	}
}

func TestTrustFirstIdleReflectionTargetReusesOnlyMatchingReflectionScope(t *testing.T) {
	snapshot := WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-subpixel", WorkspaceID: "ws-1", Title: "Subpixel Art", Status: "ACTIVE"}},
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID:      "task-idle-reflection-project",
				ProjectID:   "project-subpixel",
				Title:       "Project metacognition pass: inspect plan and blockers",
				Status:      "PENDING",
				Priority:    "critical",
				TaskKind:    "EXECUTION",
				ProjectLane: "qa",
				Tags:        []string{"meta-reflection", "anti-idle", "metacognition-scope-project"},
			},
			{
				TaskID:      "task-review-gap",
				ProjectID:   "project-subpixel",
				Title:       "Review export UX",
				Status:      "PENDING",
				Priority:    "high",
				TaskKind:    "EXECUTION",
				ProjectLane: "qa",
			},
		},
	}
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "reviewer", CoordinationMode: CoordinationModeTrustFirst})

	target := buildIdleReflectionTarget(snapshot, "ws-1", now, true, policy)
	if target.TaskID == "task-idle-reflection-project" || target.ExistingTask != nil {
		t.Fatalf("artifact-scope reviewer should not reuse project-scope idle reflection, got %+v", target)
	}
	if target.ReflectionScope != "artifact" || !strings.Contains(target.Title, "Artifact quality iteration") {
		t.Fatalf("expected reviewer to open artifact-scoped reflection target, got %+v", target)
	}
}

func TestTrustFirstIdleReflectionTargetTreatsLegacyUnscopedProjectReflectionAsProjectScope(t *testing.T) {
	snapshot := WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-subpixel", WorkspaceID: "ws-1", Title: "Subpixel Art", Status: "ACTIVE"}},
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID:      "task-idle-reflection-project-legacy",
				ProjectID:   "project-subpixel",
				Title:       "Product quality iteration: inspect and improve Subpixel Art",
				Status:      "PENDING",
				Priority:    "critical",
				TaskKind:    "EXECUTION",
				ProjectLane: "qa",
				Tags:        []string{"meta-reflection", "anti-idle"},
			},
			{
				TaskID:      "task-review-gap",
				ProjectID:   "project-subpixel",
				Title:       "Review export UX",
				Status:      "PENDING",
				Priority:    "high",
				TaskKind:    "EXECUTION",
				ProjectLane: "qa",
			},
		},
	}
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)

	reviewerPolicy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "reviewer", CoordinationMode: CoordinationModeTrustFirst})
	reviewerTarget := buildIdleReflectionTarget(snapshot, "ws-1", now, true, reviewerPolicy)
	if reviewerTarget.TaskID == "task-idle-reflection-project-legacy" || reviewerTarget.ExistingTask != nil {
		t.Fatalf("artifact-scope reviewer should not reuse legacy project idle reflection, got %+v", reviewerTarget)
	}

	strategistPolicy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "strategist", CoordinationMode: CoordinationModeTrustFirst})
	strategistTarget := buildIdleReflectionTarget(snapshot, "ws-1", now, true, strategistPolicy)
	if strategistTarget.TaskID != "task-idle-reflection-project-legacy" || strategistTarget.ExistingTask == nil {
		t.Fatalf("project-scope strategist should reuse legacy project idle reflection, got %+v", strategistTarget)
	}
}

// TestIdleReflectionEmptyFrontierProtocolActivatesAndDeactivates locks the R24-R26 fix plus the
// convergence-cluster root fix: when an ACTIVE project has no open CONVERGENCE-BLOCKING (spec-required)
// task, the minted reflection contract carries the convergence protocol (spec-gap continuation or
// project_phase_transition to DONE) so agents either keep improving the product or terminalize it as
// converged instead of holding position forever. The protocol must NOT leak in while real spec-required
// frontier work remains open, but discretionary housekeeping (side-effect classification, claim/role
// repair, polish) must NOT keep it suppressed - that was the wrong "empty frontier" trigger.
func TestIdleReflectionEmptyFrontierProtocolActivatesAndDeactivates(t *testing.T) {
	now := time.Date(2026, 6, 12, 9, 0, 0, 0, time.UTC)
	policy := metacognitionPolicyForRuntimeConfig(RuntimeConfig{Role: "strategist", CoordinationMode: CoordinationModeTrustFirst})

	emptySnapshot := WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Main Workspace"},
		Projects:  []ProjectRecord{{ProjectID: "project-rq", WorkspaceID: "ws-1", Title: "rq interpreter", Status: "ACTIVE"}},
	}
	empty := buildIdleReflectionTarget(emptySnapshot, "ws-1", now, true, policy)
	if empty.Key != "project:project-rq" {
		t.Fatalf("expected project idle target from ACTIVE-project fallback, got %+v", empty)
	}
	if !empty.EmptyProductFrontier {
		t.Fatalf("empty project target must carry structured empty-frontier flag: %+v", empty)
	}
	for _, marker := range []string{
		"PRODUCT CONVERGENCE PROTOCOL",
		"project_phase_transition",
		"exactly one bounded implementation task",
		"protocol violation",
	} {
		if !strings.Contains(empty.Description, marker) {
			t.Fatalf("empty-frontier reflection contract missing %q:\n%s", marker, empty.Description)
		}
	}

	busySnapshot := emptySnapshot
	busySnapshot.Tasks = []WorkspaceTaskRecord{{
		TaskID:      "task-impl-percent-op",
		ProjectID:   "project-rq",
		Title:       "Implement % operator",
		Status:      "PENDING",
		TaskKind:    "EXECUTION",
		ProjectLane: "implementation",
	}}
	busy := buildIdleReflectionTarget(busySnapshot, "ws-1", now, true, policy)
	if busy.Key != "project:project-rq" {
		t.Fatalf("expected project idle target, got %+v", busy)
	}
	if strings.Contains(busy.Description, "PRODUCT CONVERGENCE PROTOCOL") {
		t.Fatalf("protocol must not activate while spec-required frontier work is open:\n%s", busy.Description)
	}
	if busy.EmptyProductFrontier {
		t.Fatalf("busy project target must not carry empty-frontier flag: %+v", busy)
	}

	// Convergence-cluster root fix: a project whose ONLY open task is discretionary housekeeping
	// (here a side-effect classification coordination task) is convergeable - the protocol MUST
	// activate so the lead converges instead of treating the minted housekeeping as live frontier.
	discretionarySnapshot := emptySnapshot
	discretionarySnapshot.Tasks = []WorkspaceTaskRecord{{
		TaskID:      "task-side-effect-classify-branch-x",
		ProjectID:   "project-rq",
		Title:       "Classify side effects for branch x",
		Status:      "PENDING",
		TaskKind:    "COORDINATION",
		ProjectLane: "coordination",
		Tags:        []string{"side-effect-classification", "abpc", "project-coordination"},
	}}
	discretionary := buildIdleReflectionTarget(discretionarySnapshot, "ws-1", now, true, policy)
	if !discretionary.EmptyProductFrontier {
		t.Fatalf("discretionary-only frontier must be convergeable (protocol active): %+v", discretionary)
	}
	if !strings.Contains(discretionary.Description, "PRODUCT CONVERGENCE PROTOCOL") {
		t.Fatalf("convergence protocol must activate with only discretionary work open:\n%s", discretionary.Description)
	}

	workspaceSnapshot := WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-2", Title: "Empty Workspace"},
	}
	workspaceTarget := buildIdleReflectionTarget(workspaceSnapshot, "ws-2", now, true, policy)
	if workspaceTarget.ScopeKind != "workspace" {
		t.Fatalf("expected workspace idle target, got %+v", workspaceTarget)
	}
	if !strings.Contains(workspaceTarget.Description, "PRODUCT CONVERGENCE PROTOCOL") || !strings.Contains(workspaceTarget.Description, "Terminalize the workspace as converged") {
		t.Fatalf("workspace empty-frontier contract missing convergence branch:\n%s", workspaceTarget.Description)
	}
}

func TestIdleReflectionEmptyFrontierTaskCarriesRequiredOutcome(t *testing.T) {
	var submitted map[string]any
	var materializedDoc string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "task.submit":
			submitted = req.Params
			writeRPCResult(w, req, map[string]any{
				"task_id":      rpcString(req.Params, "task_id"),
				"workspace_id": "ws-1",
				"status":       "PENDING",
			})
		case "workspace.doc.put":
			materializedDoc = rpcString(req.Params, "content")
			writeRPCResult(w, req, map[string]any{"sha": "sha-idle-reflection"})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "project.coordination.get":
			// FF3: ensureIdleReflectionTask now resolves the lead for an empty-frontier convergence
			// reflection. The test agent IS the lead, so creation proceeds.
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{
				"strategic_lead": map[string]any{"agent_id": "agent-alpha", "status": "ACTIVE"},
			}})
		default:
			t.Fatalf("unexpected method during empty-frontier task test: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws-1",
			AgentID:     "agent-alpha",
			OwnerUserID: "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}
	target := idleReflectionTarget{
		Key:                  "project:project-rq",
		TaskID:               "task-idle-reflection-project-rq-20260612-0900",
		ScopeKind:            "project",
		ScopeID:              "project-rq",
		ProjectID:            "project-rq",
		ProjectLane:          "qa",
		EmptyProductFrontier: true,
		Title:                "Project metacognition pass: inspect plan, blockers, and next improvements",
		Description:          "EMPTY PRODUCT FRONTIER PROTOCOL (active for this reflection): exactly one outcome required.",
	}

	taskID, wake, err := runtime.ensureIdleReflectionTask(context.Background(), target, time.Date(2026, 6, 12, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ensureIdleReflectionTask() error = %v", err)
	}
	if taskID != target.TaskID || !wake {
		t.Fatalf("ensureIdleReflectionTask() = taskID %q wake %v, want %q true", taskID, wake, target.TaskID)
	}
	requirements, ok := submitted["task_requirements"].(map[string]any)
	if !ok {
		t.Fatalf("task.submit missing task_requirements: %+v", submitted)
	}
	if got := rpcString(requirements, "runtime_contract"); got != "empty_product_frontier.v1" {
		t.Fatalf("runtime_contract = %q; requirements=%+v", got, requirements)
	}
	if got := rpcString(requirements, "required_transition"); got != "empty_product_frontier_outcome" {
		t.Fatalf("required_transition = %q; requirements=%+v", got, requirements)
	}
	if !strings.Contains(materializedDoc, "empty_product_frontier: true") {
		t.Fatalf("materialized task doc missing empty frontier metadata:\n%s", materializedDoc)
	}
}

// FF3: a NON-lead must not materialize/run the convergence reflection while an active lead exists -
// only the lead can write acceptance_coverage and transition DONE. ensureIdleReflectionTask returns
// empty (neither create nor claim) so the lead gets the convergence turn.
func TestEnsureIdleReflectionTaskGatesConvergenceToLead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{"coordination": map[string]any{
				"strategic_lead": map[string]any{
					"agent_id":         "agent-alpha",
					"status":           "ACTIVE",
					"lease_expires_at": "2030-01-01T00:00:00Z",
				},
			}})
		case "task.submit":
			t.Fatalf("non-lead must NOT create the convergence reflection (FF3)")
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws-1", AgentID: "agent-beta", OwnerUserID: "owner-1"},
		client: NewRhizomeClient(server.URL, "token"),
	}
	target := idleReflectionTarget{
		Key:                  "project:project-rq",
		TaskID:               "task-idle-reflection-project-rq-20260612-0900",
		ScopeKind:            "project",
		ScopeID:              "project-rq",
		ProjectID:            "project-rq",
		ProjectLane:          "qa",
		EmptyProductFrontier: true,
		Title:                "Project metacognition pass",
		Description:          "PRODUCT CONVERGENCE PROTOCOL",
	}
	taskID, wake, err := runtime.ensureIdleReflectionTask(context.Background(), target, time.Date(2026, 6, 12, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ensureIdleReflectionTask() error = %v", err)
	}
	if taskID != "" || wake {
		t.Fatalf("non-lead must not materialize the convergence reflection: taskID=%q wake=%v", taskID, wake)
	}
}
