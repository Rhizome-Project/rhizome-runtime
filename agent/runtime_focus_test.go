package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSelectFocusClusterReportUsesStableProtoClusterTieBreak(t *testing.T) {
	report := &ControlReport{
		Clusters: []ControlReportCluster{
			{
				ProtoClusterID:        "cluster-b",
				TaskIDs:               []string{"task-1"},
				ConfirmedTensionCount: 1,
			},
			{
				ProtoClusterID:        "cluster-a",
				TaskIDs:               []string{"task-1"},
				ConfirmedTensionCount: 1,
			},
		},
	}

	cluster := selectFocusClusterReport(report, "task-1", "", nil)
	if cluster == nil || cluster.ProtoClusterID != "cluster-a" {
		t.Fatalf("expected deterministic cluster-a tie-break, got %+v", cluster)
	}
}

func TestRuntimeEnsureFocusBuildsCachesAndInvalidatesLocus(t *testing.T) {
	var locusBundleCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)

		switch req.Method {
		case "workspace.instrumentation.locus.bundle":
			locusBundleCalls++
			if rpcString(req.Params, "workspace_id") != "ws" || rpcString(req.Params, "task_id") != "task-1" || rpcString(req.Params, "session_id") != "sess-1" {
				t.Fatalf("unexpected locus params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"workspace_id":     "ws",
					"generated_at":     "2026-03-23T10:00:00Z",
					"resolved":         true,
					"resolved_from":    "task_id",
					"match_score":      8,
					"proto_cluster_id": "cluster-1",
					"control": map[string]any{
						"cluster": map[string]any{
							"proto_cluster_id":        "cluster-1",
							"resolution_kind":         "task",
							"task_ids":                []any{"task-1"},
							"doc_keys":                []any{"task.task-1"},
							"artifact_refs":           []any{"doc:deliverable"},
							"summary":                 "Review pressure dominates this task cluster",
							"basis_stale":             false,
							"metrics":                 map[string]any{"event_count": 4, "active_session_count": 1},
							"signals":                 map[string]any{"attention_band": "WATCH", "pressure_score": 7},
							"suggested_controls":      map[string]any{"priority_focus": "review"},
							"confirmed_tension_count": 1,
							"pending_tension_count":   0,
						},
						"tensions": []any{
							map[string]any{
								"tension_id":       "tension-1",
								"workspace_id":     "ws",
								"proto_cluster_id": "cluster-1",
								"tension_type":     "gap",
								"review_status":    "CONFIRMED",
								"lifecycle_state":  "ACTIVE",
								"title":            "Need acceptance evidence",
								"summary":          "Acceptance evidence is still missing",
								"anchor_kind":      "task",
								"anchor_ref":       "task-1",
								"surface_score":    9,
								"task_ids":         []any{"task-1"},
								"created_at":       "2026-03-23T00:00:00Z",
								"updated_at":       "2026-03-23T00:00:00Z",
							},
						},
					},
					"control_state": map[string]any{
						"cluster": map[string]any{
							"proto_cluster_id": "cluster-1",
							"summary":          "cluster state summary",
						},
						"state": map[string]any{
							"proto_cluster_id":          "cluster-1",
							"resolution_kind":           "task",
							"heuristic_profile_context": map[string]any{"profile": "integration"},
							"state": map[string]any{
								"workspace_id":            "ws",
								"proto_cluster_id":        "cluster-1",
								"resolution_kind":         "task",
								"heuristic_profile":       "integration",
								"epoch":                   2,
								"stabilized_mode_hint":    "STEADY",
								"candidate_mode_hint":     "COHERENCE",
								"dominant_signal_kind":    "review",
								"attention_band":          "WATCH",
								"pressure_score":          7,
								"operator_hints":          map[string]any{"priority_focus": "review"},
								"signal_deviation_vector": map[string]any{"review": 0.3},
								"created_at":              "2026-03-23T00:00:00Z",
								"updated_at":              "2026-03-23T00:00:00Z",
							},
							"metrics":            map[string]any{"event_count": 4, "active_session_count": 1},
							"signals":            map[string]any{"attention_band": "WATCH", "pressure_score": 7},
							"suggested_controls": map[string]any{"priority_focus": "review"},
						},
					},
					"corridor": map[string]any{
						"cluster": map[string]any{
							"proto_cluster_id":      "cluster-1",
							"resolution_kind":       "task",
							"task_ids":              []any{"task-1"},
							"doc_keys":              []any{"task.task-1"},
							"artifact_refs":         []any{"doc:deliverable"},
							"task_class_hint":       "PROOF",
							"task_class_source":     "authored",
							"corridor_catalog_hint": "proof.review",
							"corridor_readiness":    "READY",
							"basis_stale":           false,
							"summary":               "This cluster is ready for proof-like work",
							"task_class_confidence": 0.92,
							"readiness_confidence":  0.88,
							"metrics":               map[string]any{"event_count": 4, "active_session_count": 1},
						},
					},
					"corridor_fit": map[string]any{
						"cluster": map[string]any{
							"proto_cluster_id":      "cluster-1",
							"resolution_kind":       "task",
							"task_class_hint":       "PROOF",
							"corridor_catalog_hint": "proof.review",
							"corridor_readiness":    "READY",
							"fit_status":            "IN_CORRIDOR",
							"fit_score":             0,
						},
					},
					"frontier": []any{
						map[string]any{
							"tension_id":       "tension-1",
							"proto_cluster_id": "cluster-1",
							"tension_type":     "gap",
							"review_status":    "CONFIRMED",
							"title":            "Need acceptance evidence",
							"summary":          "Acceptance evidence is still missing",
							"surface_score":    9,
							"evidence_count":   2,
							"last_seen_at":     "2026-03-23T09:59:00Z",
						},
					},
					"dominant_tension": map[string]any{
						"tension": map[string]any{
							"tension_id":       "tension-1",
							"workspace_id":     "ws",
							"proto_cluster_id": "cluster-1",
							"tension_type":     "gap",
							"review_status":    "CONFIRMED",
							"lifecycle_state":  "ACTIVE",
							"title":            "Need acceptance evidence",
							"summary":          "Acceptance evidence is still missing",
							"anchor_kind":      "task",
							"anchor_ref":       "task-1",
							"surface_score":    9,
							"task_ids":         []any{"task-1"},
							"doc_keys":         []any{"task.task-1"},
							"artifact_refs":    []any{"doc:deliverable"},
							"created_at":       "2026-03-23T00:00:00Z",
							"updated_at":       "2026-03-23T00:00:00Z",
						},
						"docs": []any{
							map[string]any{"doc_key": "task.task-1", "title": "Task 1"},
						},
						"artifacts": []any{
							map[string]any{
								"artifact_id":  "artifact-1",
								"workspace_id": "ws",
								"title":        "Deliverable",
								"artifact_ref": "doc:deliverable",
								"kind":         "workspace_doc",
								"content_type": "text/markdown",
								"created_by":   "agent-1",
								"created_at":   "2026-03-23T00:00:00Z",
							},
						},
					},
				},
			})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	task := &WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Task One",
		OwnerUserID:  "owner-1",
		Priority:     "HIGH",
		Status:       "RUNNING",
		TaskKind:     "EXECUTION",
		TaskTemplate: "integration",
	}
	session := &AgentSessionStateRecord{
		SessionID: "sess-1",
		TaskID:    "task-1",
		AgentID:   "agent-1",
		Status:    "ACTIVE",
		Summary:   "Resume proof-oriented work",
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:           map[string]string{},
			LastWakeTrigger:   "system_news",
			LastWakeReason:    "resume_session",
			LastWakeSummary:   "News changed the local tension frontier",
			LastWakeTaskID:    "task-1",
			LastWakeSessionID: "sess-1",
			LastWakeAt:        "2026-03-23T10:00:00Z",
			LastNewsID:        "news-1",
			LastNewsSummary:   "Deployment update changed runtime context",
			ActiveTaskID:      "task-1",
			ActiveSessionID:   "sess-1",
		},
		activeTask:    task,
		activeSession: session,
		activeHydration: &TaskHydrationBundle{
			Task: TaskStatus{TaskID: "task-1"},
			Docs: []WorkspaceDocRecord{{DocKey: "task.task-1", Title: "Task 1"}},
		},
		activeWorkPacket: &AgentWorkPacket{
			WorkType: "resume_session",
			Advisory: &AgentWorkAdvisory{ProtoClusterID: "cluster-1"},
		},
	}

	focus := runtime.ensureFocus(context.Background(), task)
	if focus == nil {
		t.Fatal("expected focus to be materialized")
	}
	if focus.ProtoClusterID != "cluster-1" || focus.FocusTensionID != "tension-1" {
		t.Fatalf("unexpected focus locus: %+v", focus)
	}
	if focus.CorridorReadiness != "READY" || focus.CorridorTaskClassHint != "PROOF" {
		t.Fatalf("expected corridor hint in focus, got %+v", focus)
	}
	if focus.ControlAttentionBand != "WATCH" || focus.ControlPressureScore != 7 {
		t.Fatalf("expected control pressure in focus, got %+v", focus)
	}
	if focus.ControlProfile != "integration" || focus.ControlModeHint != "STEADY" || focus.ControlCandidateMode != "COHERENCE" {
		t.Fatalf("expected control-state hints in focus, got %+v", focus)
	}
	if focus.WakeTrigger != "system_news" || !strings.Contains(focus.WakeSummary, "local tension frontier") {
		t.Fatalf("expected wake context in focus, got %+v", focus)
	}
	if focus.UnreadMessages != 0 || focus.LastNewsID != "news-1" {
		t.Fatalf("expected local pressure metadata, got %+v", focus)
	}

	pack := buildFocusPack(focus, 4000)
	for _, want := range []string{"## Native Locus", "cluster-1", "proof.review", "Control State", "Need acceptance evidence"} {
		if !strings.Contains(pack, want) {
			t.Fatalf("expected focus pack to contain %q, got:\n%s", want, pack)
		}
	}

	focusAgain := runtime.ensureFocus(context.Background(), task)
	if focusAgain == nil {
		t.Fatal("expected cached focus")
	}
	if locusBundleCalls != 1 {
		t.Fatalf("expected first focus read to populate cache once, got locus=%d", locusBundleCalls)
	}

	if err := runtime.setPendingWorkTrigger(context.Background(), "inbound_message", "task-1", "sess-1"); err != nil {
		t.Fatalf("setPendingWorkTrigger() error: %v", err)
	}
	focusAfterInvalidate := runtime.ensureFocus(context.Background(), task)
	if focusAfterInvalidate == nil {
		t.Fatal("expected focus after invalidation")
	}
	if focusAfterInvalidate.WakeTrigger != "inbound_message" {
		t.Fatalf("expected pending trigger to drive refreshed wake context, got %+v", focusAfterInvalidate)
	}
	if locusBundleCalls != 2 {
		t.Fatalf("expected invalidated focus to refresh locus bundle, got locus=%d", locusBundleCalls)
	}
}

func TestRuntimeRefreshBootstrapInvalidatesFocusCache(t *testing.T) {
	var locusBundleCalls int
	var bootstrapCalls int
	var stateSetCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)

		switch req.Method {
		case "workspace.instrumentation.locus.bundle":
			locusBundleCalls++
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"workspace_id":     "ws",
					"generated_at":     "2026-03-23T10:00:00Z",
					"resolved":         true,
					"resolved_from":    "task_id",
					"proto_cluster_id": "cluster-1",
					"control": map[string]any{
						"cluster": map[string]any{
							"proto_cluster_id": "cluster-1",
							"resolution_kind":  "task",
							"summary":          "cluster summary",
							"signals":          map[string]any{"attention_band": "WATCH", "pressure_score": 5},
							"metrics":          map[string]any{"event_count": 3},
						},
					},
				},
			})
		case "agent.bootstrap":
			bootstrapCalls++
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-03-23T10:05:00Z",
				"agent": map[string]any{
					"agent_id":         "agent-1",
					"workspace_id":     "ws",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []any{"tool.call"},
					"summary":          "online",
					"created_at":       "2026-03-23T10:00:00Z",
					"updated_at":       "2026-03-23T10:05:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{
						"workspace_id": "ws",
						"title":        "Workspace One",
						"status":       "ACTIVE",
					},
					"tasks": []any{
						map[string]any{
							"task_id":        "task-1",
							"title":          "Task One",
							"owner_user_id":  "owner-1",
							"priority":       "HIGH",
							"status":         "RUNNING",
							"task_kind":      "EXECUTION",
							"task_template":  "integration",
							"linked_by":      "system",
							"linked_at":      "2026-03-23T10:00:00Z",
							"claim_agent_id": "agent-1",
							"claim_status":   "CLAIMED",
						},
					},
					"sessions": []any{
						map[string]any{
							"session_id":   "sess-1",
							"workspace_id": "ws",
							"agent_id":     "agent-1",
							"task_id":      "task-1",
							"status":       "ACTIVE",
							"summary":      "resume",
							"updated_at":   "2026-03-23T10:00:00Z",
							"started_at":   "2026-03-23T10:00:00Z",
						},
					},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "test-group", "daily_remaining": 1000, "weekly_remaining": 5000})
		case "agent.state.set":
			stateSetCalls++
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	task := &WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Task One",
		OwnerUserID:  "owner-1",
		Priority:     "HIGH",
		Status:       "RUNNING",
		TaskKind:     "EXECUTION",
		TaskTemplate: "integration",
		ClaimAgentID: stringPtr("agent-1"),
		ClaimStatus:  stringPtr("CLAIMED"),
	}
	session := &AgentSessionStateRecord{
		SessionID: "sess-1",
		TaskID:    "task-1",
		AgentID:   "agent-1",
		Status:    "ACTIVE",
		Summary:   "Resume proof-oriented work",
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:         map[string]string{},
			ActiveTaskID:    "task-1",
			ActiveSessionID: "sess-1",
		},
		activeTask:      task,
		activeSession:   session,
		activeHydration: &TaskHydrationBundle{Task: TaskStatus{TaskID: "task-1"}},
		activeWorkPacket: &AgentWorkPacket{
			WorkType: "resume_session",
			Advisory: &AgentWorkAdvisory{ProtoClusterID: "cluster-1"},
		},
	}

	if focus := runtime.ensureFocus(context.Background(), task); focus == nil {
		t.Fatal("expected initial focus")
	}
	if locusBundleCalls != 1 {
		t.Fatalf("expected one focus materialization before bootstrap refresh, got %d", locusBundleCalls)
	}

	if err := runtime.refreshBootstrap(context.Background()); err != nil {
		t.Fatalf("refreshBootstrap() error: %v", err)
	}
	if bootstrapCalls != 1 {
		t.Fatalf("expected one bootstrap refresh, got %d", bootstrapCalls)
	}
	if stateSetCalls == 0 {
		t.Fatal("expected bootstrap refresh to persist reconciled scratch state")
	}

	if focus := runtime.ensureFocus(context.Background(), task); focus == nil {
		t.Fatal("expected focus after bootstrap refresh")
	}
	if locusBundleCalls != 2 {
		t.Fatalf("expected bootstrap refresh to invalidate cached focus, got locus=%d", locusBundleCalls)
	}
}

func TestBuildDaemonSpecPackSkipsBroadCoordinationWhenLocusResolved(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)

		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"generated_at": "2026-03-23T12:00:00Z",
					"task": map[string]any{
						"task_id":       "task-1",
						"title":         "Task One",
						"owner_user_id": "owner-1",
						"priority":      "HIGH",
						"status":        "RUNNING",
						"task_kind":     "EXECUTION",
						"task_template": "integration",
						"node_counts":   map[string]any{},
						"nodes":         []any{},
					},
					"docs":          []any{},
					"task_links":    []any{},
					"related_tasks": []any{},
					"artifacts":     []any{},
					"updates":       []any{},
				},
			})
		case "workspace.instrumentation.locus.bundle":
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"workspace_id":     "ws",
					"generated_at":     "2026-03-23T12:00:00Z",
					"resolved":         true,
					"resolved_from":    "task_id",
					"proto_cluster_id": "cluster-1",
					"control": map[string]any{
						"cluster": map[string]any{
							"proto_cluster_id": "cluster-1",
							"resolution_kind":  "task",
							"summary":          "cluster summary",
							"signals":          map[string]any{"attention_band": "WATCH", "pressure_score": 5},
							"metrics":          map[string]any{"event_count": 3},
						},
					},
					"corridor": map[string]any{
						"cluster": map[string]any{
							"proto_cluster_id":      "cluster-1",
							"resolution_kind":       "task",
							"task_class_hint":       "PROOF",
							"corridor_catalog_hint": "proof.review",
							"corridor_readiness":    "READY",
							"summary":               "corridor summary",
							"metrics":               map[string]any{"event_count": 3},
						},
					},
					"dominant_tension": map[string]any{
						"tension": map[string]any{
							"tension_id":       "tension-1",
							"workspace_id":     "ws",
							"proto_cluster_id": "cluster-1",
							"tension_type":     "gap",
							"review_status":    "CONFIRMED",
							"lifecycle_state":  "ACTIVE",
							"title":            "Need final evidence",
							"summary":          "Evidence is still missing",
							"anchor_kind":      "task",
							"anchor_ref":       "task-1",
							"surface_score":    7,
							"created_at":       "2026-03-23T12:00:00Z",
							"updated_at":       "2026-03-23T12:00:00Z",
						},
					},
				},
			})
		case "workspace.instrumentation.control.report", "workspace.tension.frontier":
			t.Fatalf("did not expect broad coordination fallback when locus is already resolved: %s", req.Method)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	task := &WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Task One",
		OwnerUserID:  "owner-1",
		Priority:     "HIGH",
		Status:       "RUNNING",
		TaskKind:     "EXECUTION",
		TaskTemplate: "integration",
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:        "ws",
			AgentID:            "agent-1",
			MaxPromptDocChars:  2000,
			MaxPromptSpecChars: 4000,
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:         map[string]string{},
			LastWakeTrigger: "runtime_resume",
			LastWakeSummary: "Resume selected task",
		},
		activeTask: task,
	}
	runtime.cfg.ApplyDefaults()

	spec := runtime.buildDaemonSpecPack(context.Background(), task)
	if !strings.Contains(spec, "## Native Locus") {
		t.Fatalf("expected Native Locus in daemon spec pack, got:\n%s", spec)
	}
	if strings.Contains(spec, "## Coordination Pressure") {
		t.Fatalf("did not expect broad coordination pack when locus resolved, got:\n%s", spec)
	}
}
