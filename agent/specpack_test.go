package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBuildCoordinationPackIncludesControlAndTensionSignals(t *testing.T) {
	report := &ControlReport{
		Workspace: ControlReportWorkspace{
			HotClusterCount:          2,
			AttentionClusterCount:    1,
			HighestPressureClusterID: "cluster-9",
			HighestPressureScore:     0.92,
		},
		Clusters: []ControlReportCluster{
			{
				ProtoClusterID:        "cluster-9",
				Summary:               "Handoff pressure around deployment",
				TaskIDs:               []string{"task-1", "task-2"},
				AgentIDs:              []string{"agent-a", "agent-b"},
				DocKeys:               []string{"handoff", "deploy"},
				PendingTensionCount:   3,
				ConfirmedTensionCount: 1,
				Signals: []map[string]any{
					{"signal_type": "handoff_gap"},
				},
				SuggestedControls: []map[string]any{
					{"summary": "force explicit handoff checkpoint"},
				},
			},
		},
	}
	frontier := []TensionFrontierItem{
		{
			TensionID:     "tension-1",
			TensionType:   "handoff",
			ReviewStatus:  "pending",
			Title:         "Deployment ownership unclear",
			SurfaceScore:  0.87,
			EvidenceCount: 2,
		},
	}

	got := buildCoordinationPack(report, frontier, 4000)

	for _, want := range []string{
		"## Coordination Pressure",
		"Highest Pressure Cluster: cluster-9",
		"Handoff pressure around deployment",
		"handoff_gap",
		"force explicit handoff checkpoint",
		"tension-1 | handoff | pending | Deployment ownership unclear",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected coordination pack to contain %q, got:\n%s", want, got)
		}
	}
}

func TestBuildSpecPackPrefersHydratedTaskContext(t *testing.T) {
	cfg := RuntimeConfig{
		MaxPromptSpecChars: 8000,
		MaxPromptDocChars:  2000,
	}
	cfg.ApplyDefaults()

	task := &WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Hydrated Task",
		Priority:     "HIGH",
		Status:       "PENDING",
		LinkedBy:     "system",
		LinkedAt:     "2026-03-23T00:00:00Z",
		TaskKind:     "general",
		TaskTemplate: "default",
	}
	hydration := &TaskHydrationBundle{
		Workspace: &WorkspaceRecord{
			WorkspaceID: "ws-1",
			Title:       "Workspace One",
			Status:      "ACTIVE",
		},
		Task: TaskStatus{
			TaskID:       "task-1",
			Title:        "Hydrated Task",
			OwnerUserID:  "owner-1",
			Priority:     "HIGH",
			Status:       "PENDING",
			TaskKind:     "general",
			TaskTemplate: "default",
			NodeCounts:   map[string]int{},
			Nodes:        []TaskStatusNode{},
		},
		Docs: []WorkspaceDocRecord{
			{DocKey: "current_context", Title: "Current Context", Content: "Use the hydrated task context first."},
		},
		RelatedTasks: []TaskStatus{
			{
				TaskID:      "task-upstream",
				Title:       "Upstream Draft",
				OwnerUserID: "owner-1",
				Priority:    "HIGH",
				Status:      "BLOCKED",
			},
		},
		Updates: []AgentUpdateRecord{
			{AgentID: "agent-a", AgentName: "Agent A", UpdateType: "progress", Summary: "task-1 is ready"},
		},
		SideEffects: []AgentUpdateSideEffectV1{
			{
				Schema:            "artifact_bound_side_effect.v1",
				SideEffectRef:     "side-effect:task-1:api-server",
				BoundaryRelation:  "out_of_boundary",
				IntegrationStatus: "pending_classification",
				Decision:          "none_pending",
				ArtifactRef:       "artifact:repo",
				RegionRef:         "region:adapter:git:path:api/server.go",
			},
		},
		Artifacts: []WorkspaceArtifactRecord{
			{ArtifactID: "artifact-1", Title: "Prompt", Kind: "document", ArtifactRef: "artifacts/task-1.md"},
		},
	}

	got := buildSpecPack(cfg, nil, hydration, task)
	for _, want := range []string{
		"## Hydrated Task Context",
		"Workspace One",
		"Hydrated Task",
		"task-upstream | status=BLOCKED",
		"Use the hydrated task context first.",
		"task-1 is ready",
		"Pending Side Effects",
		"side-effect:task-1:api-server",
		"pending_classification",
		"classify_before_integration",
		"artifacts/task-1.md",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected hydrated spec pack to contain %q, got:\n%s", want, got)
		}
	}
	if strings.Contains(got, "## Workspace Snapshot") {
		t.Fatalf("did not expect workspace snapshot fallback in hydrated spec pack:\n%s", got)
	}
}

func TestSpecPackPrioritizesEvidenceDocsBeforeGenericDocs(t *testing.T) {
	cfg := RuntimeConfig{
		MaxPromptSpecChars: 4000,
		MaxPromptDocChars:  120,
	}
	cfg.ApplyDefaults()
	task := &WorkspaceTaskRecord{TaskID: "task-1", ProjectID: "project-alpha", Title: "Revision Task"}
	hydration := &TaskHydrationBundle{
		Task: TaskStatus{TaskID: "task-1", Title: "Revision Task"},
		Docs: []WorkspaceDocRecord{
			{DocKey: "current_context", Title: "Current Context", Content: strings.Repeat("generic context ", 20)},
			{DocKey: "decisions", Title: "Decisions", Content: strings.Repeat("generic decision ", 20)},
			{DocKey: "task.task-1.agent_response.areq-1", Title: "Agent response", Content: "Queued runtime_switch_task; implementation is not covered until claim_admitted."},
			{DocKey: "task.task-1.result", Title: "Canonical Result", Content: "CANONICAL_PASS_RESULT browser smoke passed AC-13 EV-01"},
			{DocKey: "task.task-related.evidence_gap", Title: "Related Evidence Gap", Content: "CONCRETE_BUILD_FAILURE src/ui.ts:160"},
			{DocKey: "project.project-alpha.reflection_board", Title: "Reflection Board", Content: "SHARED_PRODUCT_REFLECTION upload flow needs UX critique"},
		},
	}

	got := buildSpecPack(cfg, nil, hydration, task)
	if !strings.Contains(got, "CANONICAL_PASS_RESULT") {
		t.Fatalf("expected canonical result to survive tight doc budget ahead of agent_response, got:\n%s", got)
	}
	if !strings.Contains(got, "CONCRETE_BUILD_FAILURE") {
		t.Fatalf("expected evidence gap to survive tight doc budget, got:\n%s", got)
	}
	if !strings.Contains(got, "SHARED_PRODUCT_REFLECTION") {
		t.Fatalf("expected project reflection board to survive tight doc budget, got:\n%s", got)
	}
}

func TestBuildSpecPackFiltersForeignProjectEvidenceDocs(t *testing.T) {
	cfg := RuntimeConfig{MaxPromptSpecChars: 8000, MaxPromptDocChars: 4000}
	cfg.ApplyDefaults()
	task := &WorkspaceTaskRecord{
		TaskID:       "task-current",
		ProjectID:    "project-current",
		Title:        "Current Project Task",
		Priority:     "HIGH",
		Status:       "PENDING",
		TaskKind:     "EXECUTION",
		TaskTemplate: "generic",
	}
	snapshot := &WorkspaceSnapshot{
		Workspace: WorkspaceRecord{WorkspaceID: "ws-1", Title: "Workspace One", Status: "ACTIVE"},
		Docs: []WorkspaceDocRecord{
			{DocKey: "project.project-current.product_contract", Title: "Current Product Contract", Content: "current product promise"},
			{DocKey: "project.project-old.product_contract", Title: "Old Product Contract", Content: "foreign stale product promise"},
			{DocKey: "current_context", Title: "Current Context", Content: "shared context"},
		},
	}

	got := buildSpecPack(cfg, snapshot, nil, task)
	if !strings.Contains(got, "current product promise") {
		t.Fatalf("expected current project contract in specpack, got:\n%s", got)
	}
	if strings.Contains(got, "foreign stale product promise") {
		t.Fatalf("foreign project contract leaked into specpack:\n%s", got)
	}
}

func TestBuildAgentRosterPackExposesDirectPeerTargets(t *testing.T) {
	got := buildAgentRosterPack([]AgentRecord{
		{AgentID: "alpha", DisplayName: "Alpha", Role: "strategist", Status: "ACTIVE", IsOnline: true},
		{AgentID: "beta", DisplayName: "Beta", Role: "frontend", Status: "ACTIVE", IsOnline: true, Capabilities: []string{"frontend", "dashboard"}},
		{AgentID: "epsilon", DisplayName: "Epsilon", Role: "reviewer", Status: "ACTIVE", IsOnline: true, Capabilities: []string{"review", "test-design"}},
		{AgentID: "zeta", DisplayName: "Zeta", Role: "synthesizer", Status: "ACTIVE", IsOnline: false},
	}, "alpha", 4000)

	for _, want := range []string{
		"## Workspace Agent Roster",
		"Use exact agent_id values for agent_request.to_agent_id",
		"Peer local workdirs are private",
		"workspace_doc_put",
		"epsilon | role=reviewer | status=ACTIVE/online",
		"beta | role=frontend | status=ACTIVE/online",
		"Alpha (alpha): role=strategist status=ACTIVE/online self=true",
		"capabilities=review, test-design",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected agent roster pack to contain %q, got:\n%s", want, got)
		}
	}
	if strings.Contains(got, "alpha | role=strategist") {
		t.Fatalf("self agent must not be listed as a direct request target:\n%s", got)
	}
	if strings.Index(got, "epsilon | role=reviewer") > strings.Index(got, "beta | role=frontend") {
		t.Fatalf("reviewer should be ranked before frontend peer:\n%s", got)
	}
}

func TestDaemonSpecPackIncludesAgentRosterWhenTaskHydrationIsAvailable(t *testing.T) {
	task := &WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Hydrated coordination task",
		Priority:     "critical",
		Status:       "RUNNING",
		TaskKind:     "EXECUTION",
		TaskTemplate: "generic",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"workspace": map[string]any{"workspace_id": "ws-1", "title": "Workspace", "status": "ACTIVE"},
				"task":      map[string]any{"task_id": task.TaskID, "title": task.Title, "status": task.Status, "priority": task.Priority},
				"docs": []any{
					map[string]any{"doc_key": "task.task-1", "title": "Task Doc", "content": "Hydrated docs are present."},
				},
			}})
		case "workspace.instrumentation.locus.bundle":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{
				"generated_at": "2026-04-27T00:00:00Z",
				"resolved":     true,
			}})
		case "workspace.instrumentation.control.report":
			writeRPCResult(w, req, map[string]any{"report": map[string]any{}})
		case "workspace.tension.frontier":
			writeRPCResult(w, req, map[string]any{"items": []any{}})
		default:
			t.Fatalf("unexpected RPC method during daemon spec pack build: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:               RuntimeModeDaemon,
		Workdir:            t.TempDir(),
		RhizomeRPC:         server.URL,
		RhizomeToken:       "token",
		WorkspaceID:        "ws-1",
		AgentID:            "alpha",
		MaxPromptSpecChars: 8000,
		MaxPromptDocChars:  2000,
	}, &sequenceLLM{})
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.bootstrap.Snapshot.Agents = []AgentRecord{
		{AgentID: "alpha", DisplayName: "Alpha", Role: "strategist", Status: "ACTIVE", IsOnline: true},
		{AgentID: "epsilon", DisplayName: "Epsilon", Role: "reviewer", Status: "ACTIVE", IsOnline: true},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	got := runtime.buildDaemonSpecPack(context.Background(), task)
	for _, want := range []string{
		"## Hydrated Task Context",
		"Hydrated docs are present.",
		"## Workspace Agent Roster",
		"epsilon | role=reviewer | status=ACTIVE/online",
		"agent_request.to_agent_id",
		"store the artifact with workspace_doc_put",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected daemon spec pack to contain %q, got:\n%s", want, got)
		}
	}
}

func TestBuildAgentRequestObjectiveWarnsAboutPrivateWorkdirs(t *testing.T) {
	got := buildAgentRequestObjective(AgentRequestRecord{
		RequestID:   "areq-1",
		FromAgentID: "alpha",
		Method:      "model.ask",
		Payload:     `{"prompt":"review subpixel-art-lab/index.html"}`,
	})
	for _, want := range []string{
		"This request context is read-only",
		"materialize checkouts",
		"claim through your normal planner/work loop",
		"your local workdir is not the sender's workdir",
		"workspace_doc doc_key",
		"review subpixel-art-lab/index.html",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected agent request objective to contain %q, got:\n%s", want, got)
		}
	}
}

func TestBuildWorkPacketPackIncludesTransitionSubpackets(t *testing.T) {
	packet := &AgentWorkPacket{
		WorkType:            "resume_session",
		CoordinationState:   "waiting_decision",
		PreferredTransition: "await_decision",
		WhyNow:              "inbound_message",
		ProjectID:           "project-alpha",
		TaskKind:            "EXECUTION",
		ProjectLane:         "implementation",
		RequiresProjectGate: boolPtr(true),
		ProjectGateBlock:    []byte(`{"state":"blocked","reason":"review pending"}`),
		ProjectCoordination: []byte(`{"lane":"implementation","write_scope_hints":["agent/**"]}`),
		Gate: &AgentWorkGate{
			GateState:  "open",
			GateType:   "approval",
			NeededFrom: "human",
			Summary:    "Need approval before continuing",
		},
		Unblock: &AgentWorkUnblock{
			UnblockState: "wake_selected",
			Trigger:      "request_resume",
			BlockerKinds: []string{"credential"},
			Summary:      "Credential blocker may now be resolvable",
		},
		Handoff: &AgentWorkHandoff{
			HandoffState: "pending",
			ToAgentID:    "agent-b",
			Summary:      "Waiting for specialist takeover",
		},
	}

	got := buildWorkPacketPack(packet, 4000)
	for _, want := range []string{
		"## Work Packet",
		"Project: id=project-alpha | task_kind=EXECUTION | lane=implementation | requires_gate=true",
		"Project Gate Block:",
		"Project Coordination:",
		"Gate: state=open | type=approval | needed_from=human",
		"Unblock: state=wake_selected | trigger=request_resume | blocker_kinds=credential",
		"Handoff: state=pending | to=agent-b",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected work packet pack to contain %q, got:\n%s", want, got)
		}
	}
}

func TestBuildWorkPacketPackSummarizesProjectCoordinationTasks(t *testing.T) {
	packet := &AgentWorkPacket{
		WorkType: "claim_task",
		ProjectCoordination: []byte(`{
			"project":{"project_id":"project-subpixel"},
			"profile":{"project_id":"project-subpixel","current_phase":"IMPLEMENTATION","repo_required":true,"repo_status":"READY","repo_url":"file:///tmp/subpixel.git","repo_default_branch":"main"},
			"gate_status":{"current_phase":"IMPLEMENTATION","overall_state":"OPEN"},
			"open_task_count":2,
			"repositories":[
				{"repo_id":"repo-canonical","is_canonical":true,"repo_status":"READY","remote_url":"file:///tmp/subpixel.git","default_branch":"main","updated_by":"alpha"}
			],
			"checkouts":[
				{"checkout_id":"checkout-beta","project_id":"project-subpixel","repo_id":"repo-canonical","agent_id":"agent-beta","local_path":"/tmp/checkouts/beta","branch_name":"agent/beta/task-backend","dirty_state":"CLEAN","active_task_id":"task-backend","status":"ACTIVE"}
			],
			"branches":[
				{"branch_id":"branch-beta","project_id":"project-subpixel","repo_id":"repo-canonical","checkout_id":"checkout-beta","agent_id":"agent-beta","active_task_id":"task-backend","branch_name":"agent/beta/task-backend","head_sha":"18da0e2abcdeff00","review_doc_key":"project.project-subpixel.branch.branch-beta.review","status":"READY_FOR_REVIEW"}
			],
			"service_runs":[
				{"run_id":"run-subpixel","candidate_id":"candidate-subpixel","project_id":"project-subpixel","title":"Subpixel launch","deploy_target":"vercel","public_url":"https://subpixel.example","budget_account_id":"budget-run-subpixel","budget_cap_micros":2000,"credential_policy":"APPROVED","status":"MEASURING","started_by":"agent-alpha"}
			],
			"tasks":[
				{"task_id":"task-frontend","title":"Build controls","status":"RUNNING","priority":"high","project_lane":"frontend","claim_agent_id":"agent-beta","claim_status":"ACTIVE","tags":["ui"],"updated_at":"2026-05-02T00:00:00Z"},
				{"task_id":"task-backend","title":"Generate sprites","status":"PENDING","priority":"normal","project_lane":"backend","tags":["generator"],"updated_at":"2026-05-02T00:01:00Z"}
			],
			"patch_queue_items":[
				{"queue_id":"queue-1","item_id":"item-1","state":"BLOCKED","branch_id":"branch-alpha","claimed_by":"agent-gamma","decision_summary":"Frontend waits on generator contract."}
			]
		}`),
	}

	got := buildWorkPacketPack(packet, 4000)
	for _, want := range []string{
		"Project Coordination: project=project-subpixel | phase=IMPLEMENTATION | gate=OPEN | open_tasks=2 | tasks=2 | patch_queue=1",
		"Project Profile: repo_required=true | repo_status=READY | repo_url=[local-ref-redacted] | default_branch=main",
		"Registry Truth: repository is READY in current project coordination",
		"repo-canonical | canonical=true | status=READY | remote=[local-ref-redacted] | branch=main | updated_by=alpha",
		"Project Checkouts:",
		"checkout-beta | agent=agent-beta | status=ACTIVE | branch=agent/beta/task-backend | dirty=CLEAN | task=task-backend | path=[local-ref-redacted]",
		"Project Branches:",
		"branch-beta | agent=agent-beta | status=READY_FOR_REVIEW | branch=agent/beta/task-backend | task=task-backend | head=18da0e2abcde | review=project.project-subpixel.branch.branch-beta.review",
		"Service Runs:",
		"run-subpixel | status=MEASURING | candidate=candidate-subpixel | deploy=vercel | public_url=https://subpixel.example | budget_account=budget-run-subpixel | cap=2000 | credential_policy=APPROVED | next=continue measurement or create iteration tasks",
		"Project Tasks:",
		"task-frontend | RUNNING | lane=frontend | priority=high | claim=agent-beta/ACTIVE | Build controls",
		"task-backend | PENDING | lane=backend | priority=normal | claim=- | Generate sprites",
		"Patch Queue:",
		"queue-1/item-1 | BLOCKED | branch=branch-alpha | claimed_by=agent-gamma | decision=Frontend waits on generator contract.",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected work packet pack to contain %q, got:\n%s", want, got)
		}
	}
	if strings.Contains(got, "file:///tmp/subpixel.git") || strings.Contains(got, "/tmp/checkouts/beta") {
		t.Fatalf("project coordination prompt leaked local refs:\n%s", got)
	}
}

func TestProjectCoordinationPromptRanksProvisionalCandidateCheckoutsAheadOfMainReviewNoise(t *testing.T) {
	checkouts := make([]ProjectCheckoutRecord, 0, 13)
	for i := 0; i < 12; i++ {
		checkouts = append(checkouts, ProjectCheckoutRecord{
			CheckoutID:   fmt.Sprintf("checkout-main-%02d", i),
			ProjectID:    "project-minesweeper",
			RepoID:       "repo-canonical",
			AgentID:      fmt.Sprintf("agent-review-%02d", i),
			LocalPath:    fmt.Sprintf(`C:\fixtures\agents\review-%02d\project-checkouts\review-main`, i),
			CheckoutKind: "review",
			BranchName:   "main",
			DirtyState:   "CLEAN",
			ActiveTaskID: fmt.Sprintf("task-review-%02d", i),
			Status:       "ACTIVE",
			UpdatedAt:    fmt.Sprintf("2026-05-16T06:%02d:00Z", i),
		})
	}
	checkouts = append(checkouts, ProjectCheckoutRecord{
		CheckoutID:   "checkout-delta-candidate",
		ProjectID:    "project-minesweeper",
		RepoID:       "repo-canonical",
		AgentID:      "delta",
		LocalPath:    `C:\fixtures\agents\delta\project-checkouts\p-633a581574-r-faf88a37f8`,
		CheckoutKind: "implementation",
		BranchName:   "agent-delta-p-633a581574-t-82744b9349",
		DirtyState:   "CLEAN",
		Status:       "ACTIVE",
		UpdatedAt:    "2026-05-16T05:49:59Z",
	})
	raw, err := json.Marshal(ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-minesweeper"},
		Profile: ProjectProfileRecord{
			ProjectID:               "project-minesweeper",
			CurrentPhase:            "IMPLEMENTATION",
			RepoRequired:            true,
			RepoStatus:              "READY",
			RepoDefaultBranch:       "main",
			ImplementationPlanDocID: "project.project-minesweeper.plan",
		},
		GateStatus: ProjectGateStatusRecord{CurrentPhase: "IMPLEMENTATION", OverallState: "OPEN"},
		Checkouts:  checkouts,
	})
	if err != nil {
		t.Fatalf("marshal coordination: %v", err)
	}

	got := projectCoordinationPromptSummary(raw, 0)
	for _, want := range []string{
		"ranking: non-main, headed, dirty, or task-bound checkouts are surfaced first",
		"checkout-delta-candidate | agent=delta | status=ACTIVE | branch=agent-delta-p-633a581574-t-82744b9349 | dirty=CLEAN | task=- | path=[local-ref-redacted] | path_ref=<local-checkout>/p-633a581574-r-faf88a37f8",
		"... 3 lower-signal checkouts omitted",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected project coordination summary to contain %q, got:\n%s", want, got)
		}
	}
	if strings.Contains(got, `C:\fixtures\agents\delta`) {
		t.Fatalf("project coordination prompt leaked full local checkout path:\n%s", got)
	}
}

func TestBuildWorkPacketPackFlagsAcceptedPatchQueueAsIntegrationWork(t *testing.T) {
	packet := &AgentWorkPacket{
		WorkType: "claim_task",
		ProjectCoordination: []byte(`{
			"project":{"project_id":"project-subpixel"},
			"profile":{"project_id":"project-subpixel","current_phase":"IMPLEMENTATION","repo_status":"READY","repo_default_branch":"main"},
			"branches":[
				{"branch_id":"branch-theta","branch_name":"agent/theta/editor","status":"READY_FOR_REVIEW","head_sha":"b7f175bf9b8d027163f730e244f2ce2c8f186313"}
			],
			"patch_queue_items":[
				{"queue_id":"queue-1","item_id":"item-1","state":"ACCEPTED","branch_id":"branch-theta","head_sha":"b7f175bf9b8d027163f730e244f2ce2c8f186313","decision_summary":"Accepted MVP editor."}
			]
		}`),
	}

	got := buildWorkPacketPack(packet, 4000)
	for _, want := range []string{
		"queue-1/item-1 | ACCEPTED | branch=branch-theta",
		"ACCEPTED is reviewed evidence, not canonical baseline",
		"project_patch_queue_integrate",
		"head=b7f175bf9b8d",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected accepted patch queue guidance %q, got:\n%s", want, got)
		}
	}
}

func TestBuildWorkPacketPackDoesNotReintegrateAcceptedPatchQueueWithoutVisibleUnmergedBranch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		branches string
	}{
		{
			name:     "missing branch evidence",
			branches: ``,
		},
		{
			name: "merged branch",
			branches: `"branches":[
				{"branch_id":"branch-theta","branch_name":"agent/theta/editor","status":"MERGED","head_sha":"b7f175bf9b8d027163f730e244f2ce2c8f186313"}
			],`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			packet := &AgentWorkPacket{
				WorkType: "claim_task",
				ProjectCoordination: []byte(`{
					"project":{"project_id":"project-subpixel"},
					"profile":{"project_id":"project-subpixel","current_phase":"IMPLEMENTATION","repo_status":"READY","repo_default_branch":"main"},
					` + tc.branches + `
					"patch_queue_items":[
						{"queue_id":"queue-1","item_id":"item-1","state":"ACCEPTED","branch_id":"branch-theta","head_sha":"b7f175bf9b8d027163f730e244f2ce2c8f186313","decision_summary":"Accepted MVP editor."}
					]
				}`),
			}

			got := buildWorkPacketPack(packet, 4000)
			if strings.Contains(got, "use project_patch_queue_integrate") {
				t.Fatalf("accepted item without visible unmerged branch should not instruct reintegration, got:\n%s", got)
			}
			if !strings.Contains(got, "inspect the canonical target branch") {
				t.Fatalf("expected canonical target guidance, got:\n%s", got)
			}
		})
	}
}

// TestSpecPackPacketPromptDietCapsDocBodiesPreservingCoordinationTruth is the
// TE-30 golden test. It builds a large coordination packet (many tasks,
// branches, checkouts, patch-queue items) plus several large coordination doc
// bodies and asserts:
//
//	(a) every preserved coordination field value still appears in the rendered
//	    prompt (project/phase/gate, task ids/status/lane, branch
//	    ids/status/head/review doc, patch-queue queue/item/claim state,
//	    blockers, suggested doc keys);
//	(b) the dieted spec-pack doc section is well under the un-dieted size
//	    (concrete reduction asserted, < 50% of pre-diet for this fixture);
//	(c) each capped doc shows its doc key + a workspace_doc_get fetch
//	    instruction;
//	(d) two renders are byte-identical (determinism).
func TestSpecPackPacketPromptDietCapsDocBodiesPreservingCoordinationTruth(t *testing.T) {
	cfg := RuntimeConfig{MaxPromptSpecChars: 200, MaxPromptDocChars: 14000}
	cfg.ApplyDefaults()

	task := &WorkspaceTaskRecord{TaskID: "task-1", ProjectID: "project-omega", Title: "Diet Task"}

	bigBody := func(tag string) string {
		// ~6KB body so it well exceeds the 2048-byte per-doc body cap.
		return tag + " " + strings.Repeat("coordination narrative body filler ", 170)
	}
	docs := []WorkspaceDocRecord{
		{DocKey: "task.task-1.result", Title: "Canonical Result", Content: "CANONICAL_PASS_RESULT " + bigBody("RESULT")},
		{DocKey: "task.task-1.implementation_evidence", Title: "Impl Evidence", Content: "IMPL_EVIDENCE " + bigBody("EVID")},
		{DocKey: "project.project-omega.product_contract", Title: "Product Contract", Content: "PRODUCT_CONTRACT " + bigBody("CONTRACT")},
		{DocKey: "project.project-omega.acceptance_criteria", Title: "Acceptance", Content: "ACCEPTANCE_CRITERIA " + bigBody("AC")},
		{DocKey: "project.project-omega.reflection_board", Title: "Reflection", Content: "REFLECTION_BOARD " + bigBody("REFL")},
		{DocKey: "current_context", Title: "Current Context", Content: "CURRENT_CONTEXT " + bigBody("CTX")},
		{DocKey: "decisions", Title: "Decisions", Content: "DECISIONS " + bigBody("DEC")},
	}
	hydration := &TaskHydrationBundle{
		Workspace: &WorkspaceRecord{WorkspaceID: "ws-1", Title: "Workspace Omega", Status: "ACTIVE"},
		Task:      TaskStatus{TaskID: "task-1", Title: "Diet Task"},
		Docs:      docs,
	}

	got := buildSpecPack(cfg, nil, hydration, task)
	got2 := buildSpecPack(cfg, nil, hydration, task)

	// (d) determinism.
	if got != got2 {
		t.Fatalf("expected deterministic spec pack render, two renders differed")
	}

	// (a) every preserved doc key still appears (no doc dropped).
	for _, doc := range docs {
		if !strings.Contains(got, doc.DocKey) {
			t.Fatalf("expected doc key %q to survive diet, got:\n%s", doc.DocKey, got)
		}
	}
	// Head excerpts of prioritized evidence/contract bodies still present.
	for _, marker := range []string{"CANONICAL_PASS_RESULT", "IMPL_EVIDENCE", "PRODUCT_CONTRACT", "ACCEPTANCE_CRITERIA"} {
		if !strings.Contains(got, marker) {
			t.Fatalf("expected head excerpt marker %q to survive diet, got:\n%s", marker, got)
		}
	}

	// (c) each capped doc shows a workspace_doc_get fetch instruction.
	if strings.Count(got, "workspace_doc_get doc_key=") < len(docs) {
		t.Fatalf("expected a workspace_doc_get fetch instruction for each capped doc, got %d:\n%s",
			strings.Count(got, "workspace_doc_get doc_key="), got)
	}
	if !strings.Contains(got, "[doc body diet]") {
		t.Fatalf("expected doc body diet marker, got:\n%s", got)
	}

	// (b) concrete byte reduction vs pre-diet inline size for the doc section.
	preDiet := preDietDocSectionBytes(prioritizedDocs(docs, task), cfg)
	postDiet := dietDocSectionBytes(got)
	if preDiet == 0 {
		t.Fatalf("pre-diet measurement was empty")
	}
	if postDiet >= preDiet/2 {
		t.Fatalf("expected dieted doc section < 50%% of pre-diet (%d), got %d bytes", preDiet, postDiet)
	}
	t.Logf("TE-30 doc section bytes: pre-diet=%d post-diet=%d (%.1f%%)", preDiet, postDiet, 100*float64(postDiet)/float64(preDiet))
}

// TestBuildWorkPacketPackLargeCoordinationPreservesGoldenContractFields is the
// TE-30 golden test for the ProjectCoordination prompt path: a large
// coordination fixture must keep every coordination source-of-truth field
// inline (project/phase/gate, task ids/status/lane, branch ids/status/head
// sha/review doc, patch-queue queue/item/claim state, roles via claim, blocker)
// even though doc bodies elsewhere are diet-capped.
func TestBuildWorkPacketPackLargeCoordinationPreservesGoldenContractFields(t *testing.T) {
	coordination := `{
		"project":{"project_id":"project-omega"},
		"profile":{"project_id":"project-omega","current_phase":"IMPLEMENTATION","repo_required":true,"repo_status":"READY","repo_url":"file:///tmp/omega.git","repo_default_branch":"main"},
		"gate_status":{"current_phase":"IMPLEMENTATION","overall_state":"OPEN"},
		"open_task_count":3,
		"branches":[
			{"branch_id":"branch-aa","branch_name":"agent/aa/lane-a","status":"READY_FOR_REVIEW","head_sha":"aa11bb22cc33dd44","review_doc_key":"project.project-omega.branch.branch-aa.review","active_task_id":"task-a","agent_id":"agent-a"},
			{"branch_id":"branch-bb","branch_name":"agent/bb/lane-b","status":"ACTIVE","head_sha":"bb22cc33dd44ee55","review_doc_key":"project.project-omega.branch.branch-bb.review","active_task_id":"task-b","agent_id":"agent-b"}
		],
		"checkouts":[
			{"checkout_id":"checkout-aa","agent_id":"agent-a","status":"ACTIVE","branch_name":"agent/aa/lane-a","dirty_state":"DIRTY","active_task_id":"task-a","head_sha":"aa11bb22cc33dd44","checkout_kind":"implementation","local_path":"/tmp/checkouts/aa"}
		],
		"tasks":[
			{"task_id":"task-a","title":"Implement lane A","status":"RUNNING","priority":"high","project_lane":"lane-a","claim_agent_id":"agent-a","claim_status":"ACTIVE"},
			{"task_id":"task-b","title":"Implement lane B","status":"PENDING","priority":"normal","project_lane":"lane-b","claim_agent_id":"agent-b","claim_status":"ACTIVE"},
			{"task_id":"task-c","title":"Validate lane C","status":"BLOCKED","priority":"high","project_lane":"lane-c"}
		],
		"patch_queue_items":[
			{"queue_id":"queue-1","item_id":"item-1","state":"BLOCKED","branch_id":"branch-aa","claimed_by":"agent-reviewer","decision_summary":"Lane A waits on lane B contract."}
		]
	}`
	packet := &AgentWorkPacket{
		WorkType:            "claim_task",
		ProjectID:           "project-omega",
		ProjectLane:         "lane-a",
		ProjectCoordination: []byte(coordination),
		Blockers: []BlockedRef{
			{Kind: "dependency", Detail: "lane B contract not yet published"},
		},
		ContextHints: AgentWorkContextHints{
			SuggestedDocKeys: []string{"project.project-omega.product_contract", "project.project-omega.acceptance_criteria"},
		},
	}

	got := buildWorkPacketPack(packet, 6000)
	got2 := buildWorkPacketPack(packet, 6000)
	if got != got2 {
		t.Fatalf("expected deterministic work packet render, two renders differed")
	}

	for _, want := range []string{
		// project / phase / gate
		"project=project-omega | phase=IMPLEMENTATION | gate=OPEN",
		// branch id / status / head sha / review doc
		"branch-aa | agent=agent-a | status=READY_FOR_REVIEW | branch=agent/aa/lane-a | task=task-a | head=aa11bb22cc33 | review=project.project-omega.branch.branch-aa.review",
		"branch-bb | agent=agent-b | status=ACTIVE",
		// checkout truth
		"checkout-aa | agent=agent-a | status=ACTIVE | branch=agent/aa/lane-a | dirty=DIRTY | task=task-a",
		// task ids / status / lane / claim (role/write-scope owner)
		"task-a | RUNNING | lane=lane-a | priority=high | claim=agent-a/ACTIVE",
		"task-b | PENDING | lane=lane-b | priority=normal | claim=agent-b/ACTIVE",
		"task-c | BLOCKED | lane=lane-c",
		// patch queue queue/item/claim state
		"queue-1/item-1 | BLOCKED | branch=branch-aa | claimed_by=agent-reviewer | decision=Lane A waits on lane B contract.",
		// blockers
		"Blockers: dependency: lane B contract not yet published",
		// suggested doc keys
		"Context Hints: docs=project.project-omega.product_contract, project.project-omega.acceptance_criteria",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected work packet pack to preserve coordination truth %q, got:\n%s", want, got)
		}
	}
	// local refs still redacted.
	if strings.Contains(got, "file:///tmp/omega.git") || strings.Contains(got, "/tmp/checkouts/aa") {
		t.Fatalf("project coordination prompt leaked local refs:\n%s", got)
	}
}

// preDietDocSectionBytes reproduces the pre-TE-30 inline doc-body rendering for
// the same fixture, so the golden test can assert a concrete byte reduction.
func preDietDocSectionBytes(docs []WorkspaceDocRecord, cfg RuntimeConfig) int {
	var b strings.Builder
	docBudget := cfg.MaxPromptDocChars
	for _, doc := range docs {
		if docBudget <= 0 {
			break
		}
		chunk := clipForPrompt(strings.TrimSpace(doc.Content), min(docBudget, cfg.MaxPromptDocChars/2))
		if chunk == "" {
			continue
		}
		b.WriteString("### Doc: ")
		b.WriteString(firstNonEmpty(doc.Title, doc.DocKey))
		b.WriteString("\n")
		b.WriteString(chunk)
		b.WriteString("\n\n")
		docBudget -= len(chunk)
	}
	return b.Len()
}

// dietDocSectionBytes measures the rendered "### Doc:" section length so the
// comparison is scoped to the dieted region rather than the constant spec/header.
func dietDocSectionBytes(rendered string) int {
	idx := strings.Index(rendered, "### Doc: ")
	if idx < 0 {
		return 0
	}
	return len(rendered[idx:])
}

func TestBuildWorkPacketPackBoundsProjectCoordinationItemText(t *testing.T) {
	longTitle := strings.Repeat("long-title-", 40)
	longDecision := strings.Repeat("long-decision-", 40)
	packet := &AgentWorkPacket{
		WorkType: "claim_task",
		ProjectCoordination: []byte(`{
			"project":{"project_id":"project-subpixel"},
			"gate_status":{"current_phase":"IMPLEMENTATION","overall_state":"OPEN"},
			"open_task_count":1,
			"tasks":[{"task_id":"task-long","title":"` + longTitle + `","status":"PENDING","priority":"normal","project_lane":"frontend"}],
			"patch_queue_items":[{"queue_id":"queue-1","item_id":"item-1","state":"BLOCKED","branch_id":"branch-alpha","decision_summary":"` + longDecision + `"}]
		}`),
		Gate: &AgentWorkGate{
			GateState:  "open",
			GateType:   "approval",
			NeededFrom: "human",
			Summary:    "Operational guidance must survive project coordination summary.",
		},
	}

	got := buildWorkPacketPack(packet, 1200)
	if strings.Contains(got, strings.Repeat("long-title-", 20)) || strings.Contains(got, strings.Repeat("long-decision-", 20)) {
		t.Fatalf("expected long project coordination text to be truncated, got:\n%s", got)
	}
	if !strings.Contains(got, "Gate: state=open | type=approval | needed_from=human") {
		t.Fatalf("expected later operational guidance to survive coordination summary, got:\n%s", got)
	}
}
