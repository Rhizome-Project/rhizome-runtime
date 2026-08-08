package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fastPathSideEffect builds an out-of-boundary side effect for a single dirty git
// path, mirroring the shape projectBranchCommitSideEffectsForBoundary emits.
func fastPathSideEffect(path string) AgentUpdateSideEffectV1 {
	region := "region:adapter:git:path:" + path
	return AgentUpdateSideEffectV1{
		Schema:               "artifact_bound_side_effect.v1",
		SideEffectRef:        "side-effect:ws:project:branch:" + sanitizeRefSegment(path),
		Actor:                "agent-beta",
		LaneRef:              "lane:task-ui",
		TensionRef:           "task-ui",
		ArtifactRef:          "artifact:repo",
		RegionRef:            region,
		Operation:            "change",
		SourceKind:           "adapter:git",
		BoundaryRef:          "boundary:branch-beta",
		BoundaryRelation:     "out_of_boundary",
		MaterializationState: "present_unintegrated",
		IntegrationIntent:    "unknown",
		IntegrationStatus:    "pending_classification",
		Decision:             "none_pending",
		Justification:        "classification required",
		DerivedRegionRefs:    []string{region},
	}
}

func TestClassifySideEffectFastPathDeterministicClasses(t *testing.T) {
	cases := []struct {
		name     string
		pathset  []string
		paths    []string
		wantKind sideEffectFastPathKind
		wantDec  string
		wantCls  string
	}{
		{
			name:     "in_scope_resolves_accept",
			pathset:  []string{"src/ui/**"},
			paths:    []string{"src/ui/widget.tsx"},
			wantKind: sideEffectFastPathResolve,
			wantDec:  "reroute_to_active_lane",
			wantCls:  "in_scope",
		},
		{
			name:     "multiple_in_scope_resolves_accept",
			pathset:  []string{"src/ui/**", "src/shared/**"},
			paths:    []string{"src/ui/widget.tsx", "src/shared/util.ts"},
			wantKind: sideEffectFastPathResolve,
			wantDec:  "reroute_to_active_lane",
			wantCls:  "in_scope",
		},
		{
			name:     "tracked_generated_artifact_escalates",
			pathset:  []string{"src/ui/**"},
			paths:    []string{"dist/bundle.js", "tsconfig.tsbuildinfo"},
			wantKind: sideEffectFastPathEscalate,
		},
		{
			name:     "run_bookkeeping_escalates",
			pathset:  []string{"src/ui/**"},
			paths:    []string{"postmortems/round-01.md"},
			wantKind: sideEffectFastPathEscalate,
		},
		{
			name:     "ambiguous_product_path_escalates",
			pathset:  []string{"src/ui/**"},
			paths:    []string{"package.json"},
			wantKind: sideEffectFastPathEscalate,
		},
		{
			name:     "mixed_in_scope_and_ambiguous_escalates",
			pathset:  []string{"src/ui/**"},
			paths:    []string{"src/ui/widget.tsx", "package.json"},
			wantKind: sideEffectFastPathEscalate,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			effects := make([]AgentUpdateSideEffectV1, 0, len(tc.paths))
			for _, p := range tc.paths {
				effects = append(effects, fastPathSideEffect(p))
			}
			got := classifySideEffectFastPath(tc.pathset, effects)
			if got.Kind != tc.wantKind {
				t.Fatalf("kind = %v, want %v (decision=%q class=%q)", got.Kind, tc.wantKind, got.Decision, got.Class)
			}
			if tc.wantKind == sideEffectFastPathResolve {
				if got.Decision != tc.wantDec {
					t.Fatalf("decision = %q, want %q", got.Decision, tc.wantDec)
				}
				if got.Class != tc.wantCls {
					t.Fatalf("class = %q, want %q", got.Class, tc.wantCls)
				}
			}
		})
	}
}

func TestClassifySideEffectFastPathMalformedFailsClosed(t *testing.T) {
	// Missing side_effect_ref must fail closed (escalate), never resolve.
	missingRef := fastPathSideEffect("dist/bundle.js")
	missingRef.SideEffectRef = ""
	if got := classifySideEffectFastPath([]string{"src/**"}, []AgentUpdateSideEffectV1{missingRef}); got.Kind != sideEffectFastPathEscalate {
		t.Fatalf("missing side_effect_ref must escalate, got %+v", got)
	}

	// Non-git / non-path region cannot be reasoned about deterministically.
	opaqueRegion := fastPathSideEffect("dist/bundle.js")
	opaqueRegion.RegionRef = "region:opaque:blob"
	opaqueRegion.DerivedRegionRefs = nil
	if got := classifySideEffectFastPath([]string{"src/**"}, []AgentUpdateSideEffectV1{opaqueRegion}); got.Kind != sideEffectFastPathEscalate {
		t.Fatalf("opaque region must escalate, got %+v", got)
	}

	// Derived region refs that do not corroborate the path must fail closed.
	mismatchedDerived := fastPathSideEffect("dist/bundle.js")
	mismatchedDerived.DerivedRegionRefs = []string{"region:adapter:git:path:dist/other.js"}
	if got := classifySideEffectFastPath([]string{"src/**"}, []AgentUpdateSideEffectV1{mismatchedDerived}); got.Kind != sideEffectFastPathEscalate {
		t.Fatalf("uncorroborated derived region must escalate, got %+v", got)
	}

	// Empty set escalates (nothing deterministic to resolve).
	if got := classifySideEffectFastPath([]string{"src/**"}, nil); got.Kind != sideEffectFastPathEscalate {
		t.Fatalf("empty side effects must escalate, got %+v", got)
	}
}

// TestDirtySideEffectFastPathResolvesInScopeWithoutClassificationTask verifies a
// homogeneous in-scope pathset (a write that should never have been flagged) is
// resolved through side_effect_resolve (accept) with ZERO LLM-bound classification
// task created.
func TestDirtySideEffectFastPathResolvesInScopeWithoutClassificationTask(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"updates": []map[string]any{}}})
		case "task.submit":
			// reroute_to_active_lane records no follow-up task; any task.submit here
			// would be an unexpected classification escalation.
			t.Fatalf("fast path must not submit any task for the in-scope reroute decision")
		case "agent.update.post":
			// Single resolution-decision update recorded by side_effect_resolve.
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("unexpected RPC method %q", req.Method)
		}
	}))
	defer server.Close()

	blocker := projectDirtySideEffectBlocker{
		client:      NewRhizomeClient(server.URL, "token"),
		workspaceID: "ws",
		agentID:     "agent-beta",
		ownerUserID: "owner-1",
	}
	result := blocker.returnSideEffectBlock(context.Background(), projectDirtySideEffectBlockInput{
		SourceTool: "project_branch_commit",
		ProjectID:  "project-clearpress",
		Branch: ProjectBranchRecord{
			BranchID:     "branch-beta",
			BranchName:   "agent/beta/ui",
			AgentID:      "agent-beta",
			ActiveTaskID: "task-ui",
		},
		DirtyPaths: []string{"src/ui/widget.tsx"},
		Pathset:    []string{"src/ui/**"},
	}, []AgentUpdateSideEffectV1{fastPathSideEffect("src/ui/widget.tsx")})

	if result == nil || !result.IsError {
		t.Fatalf("expected fast-path block result, got %+v", result)
	}
	for _, want := range []string{
		`"gate_type": "side_effect_fast_path"`,
		`"fast_path_class": "in_scope"`,
		`"fast_path_decision": "reroute_to_active_lane"`,
		`"deterministic_resolution": true`,
		`"classification_task_created": false`,
	} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
}

// TestDirtySideEffectReusesRecordedResolution is deterministic Class 4: when a
// prior side_effect_resolve decision already covers this exact pathset, reuse the
// recorded decision (R51 motivation) instead of opening a fresh classification task.
func TestDirtySideEffectReusesRecordedResolution(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "agent.task.hydrate" {
			t.Fatalf("recorded-resolution reuse must not create fresh updates/tasks; extra method %q", req.Method)
		}
		ref := "side-effect:ws:project:branch:" + sanitizeRefSegment("package.json")
		writeRPCResult(w, req, map[string]any{
			"bundle": map[string]any{
				"updates": []map[string]any{{
					"update_id":    "update-resolution-1",
					"update_type":  "side_effect_resolution",
					"payload_json": `{"schema":"artifact_bound_side_effect_resolution.v1","status":"resolved","decision":"accept","integration_status":"accepted","side_effect_refs":["` + ref + `"],"followup_task_id":"task-side-effect-prior","successor_key":"abpc-resolution-successor:prior"}`,
				}},
			},
		})
	}))
	defer server.Close()

	blocker := projectDirtySideEffectBlocker{
		client:      NewRhizomeClient(server.URL, "token"),
		workspaceID: "ws",
		agentID:     "agent-beta",
		ownerUserID: "owner-1",
	}
	result := blocker.returnSideEffectBlock(context.Background(), projectDirtySideEffectBlockInput{
		SourceTool: "project_branch_commit",
		ProjectID:  "project-clearpress",
		Branch: ProjectBranchRecord{
			BranchID:     "branch-beta",
			BranchName:   "agent/beta/ui",
			AgentID:      "agent-beta",
			ActiveTaskID: "task-ui",
		},
		DirtyPaths: []string{"package.json"},
		Pathset:    []string{"src/ui/**"},
	}, []AgentUpdateSideEffectV1{fastPathSideEffect("package.json")})

	if result == nil || !result.IsError {
		t.Fatalf("expected recorded-resolution reuse block, got %+v", result)
	}
	for _, want := range []string{
		`"gate_type": "side_effect_resolution_reused"`,
		`"reused_decision": "accept"`,
		`"deterministic_resolution": true`,
		`"classification_task_created": false`,
		"task-side-effect-prior",
	} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Contains(result.Output, `"integration_status": "pending_classification"`) {
		t.Fatalf("reused resolution must not advertise a fresh pending classification: %s", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected only hydration lookup, got %d calls", calls)
	}
}

// TestDirtySideEffectFastPathAmbiguousEscalatesExactlyOnce is the no-mesh
// regression: a mixed/ambiguous pathset must produce exactly ONE classification
// task (no fan-out), via the unchanged escalation path.
func TestDirtySideEffectFastPathAmbiguousEscalatesExactlyOnce(t *testing.T) {
	hydrateCalls := 0
	updatePosts := 0
	classificationSubmits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.task.hydrate":
			hydrateCalls++
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"updates": []map[string]any{}}})
		case "agent.update.post":
			updatePosts++
			writeRPCResult(w, req, map[string]any{})
		case "task.submit":
			classificationSubmits++
			requirements, _ := req.Params["task_requirements"].(map[string]any)
			if requirements == nil || requirements["abpc_task_class"] != "side_effect_classification" {
				t.Fatalf("expected single classification task submit, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"task_id": "task-side-effect-classify-mixed", "status": "PENDING"})
		default:
			t.Fatalf("unexpected RPC method %q", req.Method)
		}
	}))
	defer server.Close()

	blocker := projectDirtySideEffectBlocker{
		client:      NewRhizomeClient(server.URL, "token"),
		workspaceID: "ws",
		agentID:     "agent-beta",
		ownerUserID: "owner-1",
	}
	// Mixed: one generated-artifact + one ambiguous product path. Must NOT split
	// into a generated-artifact fast path AND a separate escalation.
	result := blocker.returnSideEffectBlock(context.Background(), projectDirtySideEffectBlockInput{
		SourceTool: "project_branch_commit",
		ProjectID:  "project-clearpress",
		Branch: ProjectBranchRecord{
			BranchID:     "branch-beta",
			BranchName:   "agent/beta/ui",
			AgentID:      "agent-beta",
			ActiveTaskID: "task-ui",
		},
		DirtyPaths: []string{"dist/bundle.js", "package.json"},
		Pathset:    []string{"src/ui/**"},
	}, []AgentUpdateSideEffectV1{
		fastPathSideEffect("dist/bundle.js"),
		fastPathSideEffect("package.json"),
	})

	if result == nil || !result.IsError {
		t.Fatalf("expected ambiguous escalation block, got %+v", result)
	}
	if classificationSubmits != 1 {
		t.Fatalf("expected exactly one classification task (no fan-out), got %d", classificationSubmits)
	}
	if updatePosts != 1 {
		t.Fatalf("expected exactly one blocked update post, got %d", updatePosts)
	}
	for _, want := range []string{
		`"gate_type": "side_effect_classification"`,
		`"integration_status": "pending_classification"`,
	} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected escalation output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Contains(result.Output, "side_effect_fast_path") {
		t.Fatalf("ambiguous pathset must not advertise a fast-path resolution: %s", result.Output)
	}
}

func TestDirtySideEffectBlockerDoesNotAdvertisePhantomClassificationTaskOnSubmitFailure(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "agent.task.hydrate" {
				t.Fatalf("expected preflight hydration, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"updates": []map[string]any{},
				},
			})
		case 2:
			if req.Method != "agent.update.post" {
				t.Fatalf("expected side-effect update post, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{})
		case 3:
			if req.Method != "task.submit" {
				t.Fatalf("expected classification task submit, got %q", req.Method)
			}
			writeRPCError(w, req, -32000, "invalid task_class: side_effect_classification")
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	blocker := projectDirtySideEffectBlocker{
		client:      NewRhizomeClient(server.URL, "token"),
		workspaceID: "ws",
		agentID:     "agent-beta",
		ownerUserID: "owner-1",
	}
	result := blocker.returnSideEffectBlock(context.Background(), projectDirtySideEffectBlockInput{
		SourceTool: "project_checkout_materialize",
		ProjectID:  "project-clearpress",
		Repo:       ProjectRepositoryRecord{RepoID: "repo-1", Name: "clearpress"},
		Branch: ProjectBranchRecord{
			BranchID:     "branch-beta",
			BranchName:   "agent/beta/ui",
			AgentID:      "agent-beta",
			ActiveTaskID: "task-ui",
		},
		DirtyPaths: []string{"README.md"},
		Pathset:    []string{"src/ui/**"},
	}, []AgentUpdateSideEffectV1{{
		Schema:               "artifact_bound_side_effect.v1",
		SideEffectRef:        "side-effect:phantom",
		Actor:                "agent-beta",
		LaneRef:              "lane:task-ui",
		TensionRef:           "task-ui",
		ArtifactRef:          "artifact:repo",
		RegionRef:            "region:adapter:git:path:README.md",
		Operation:            "change",
		SourceKind:           "adapter:git",
		BoundaryRef:          "boundary:branch-beta",
		BoundaryRelation:     "adjacent_foundation_candidate",
		MaterializationState: "present_unintegrated",
		IntegrationIntent:    "foundation_effect",
		IntegrationStatus:    "pending_classification",
		Decision:             "none_pending",
		Justification:        "classification required",
		DerivedRegionRefs:    []string{"region:adapter:git:path:README.md"},
	}})
	if result == nil || !result.IsError {
		t.Fatalf("expected blocked tool result, got %+v", result)
	}
	if strings.Contains(result.Output, `"classification_task_id"`) {
		t.Fatalf("failed task submit must not advertise a phantom classification task id, got %s", result.Output)
	}
	for _, want := range []string{`"classification_task_available": false`, "invalid task_class", "pending_classification"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 3 {
		t.Fatalf("expected hydration, update post, and task submit, got %d calls", calls)
	}
}

func TestDirtySideEffectBlockerTerminalDuplicateClassificationCreatesReconciliationTask(t *testing.T) {
	calls := 0
	classificationTaskID := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "agent.task.hydrate" {
				t.Fatalf("expected preflight hydration, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"updates": []map[string]any{},
				},
			})
		case 2:
			if req.Method != "agent.update.post" {
				t.Fatalf("expected side-effect update post, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{})
		case 3:
			if req.Method != "task.submit" {
				t.Fatalf("expected classification task submit, got %q", req.Method)
			}
			classificationTaskID = rpcString(req.Params, "task_id")
			writeRPCError(w, req, -32602, "workspace task already exists")
		case 4:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("expected duplicate task status lookup, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"tasks": []map[string]any{{
					"task_id": classificationTaskID,
					"status":  "RESOLVED",
				}},
			})
		case 5:
			if req.Method != "task.submit" {
				t.Fatalf("expected reconciliation task submit, got %q", req.Method)
			}
			requirements, _ := req.Params["task_requirements"].(map[string]any)
			if requirements["schema"] != "artifact_bound_side_effect_reconciliation_task.v1" {
				t.Fatalf("expected side-effect reconciliation schema, got %+v", requirements)
			}
			if requirements["stale_classification_task_id"] != classificationTaskID || requirements["stale_classification_status"] != "RESOLVED" {
				t.Fatalf("expected stale terminal classification metadata, got %+v", requirements)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id": "task-side-effect-reconcile-terminal",
				"status":  "PENDING",
			})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	blocker := projectDirtySideEffectBlocker{
		client:      NewRhizomeClient(server.URL, "token"),
		workspaceID: "ws",
		agentID:     "agent-beta",
		ownerUserID: "owner-1",
	}
	result := blocker.returnSideEffectBlock(context.Background(), projectDirtySideEffectBlockInput{
		SourceTool: "project_checkout_materialize",
		ProjectID:  "project-clearpress",
		Repo:       ProjectRepositoryRecord{RepoID: "repo-1", Name: "clearpress"},
		Branch: ProjectBranchRecord{
			BranchID:     "branch-beta",
			BranchName:   "agent/beta/ui",
			AgentID:      "agent-beta",
			ActiveTaskID: "task-ui",
		},
		DirtyPaths: []string{"package.json"},
		Pathset:    []string{"src/ui/**"},
	}, []AgentUpdateSideEffectV1{{
		Schema:               "artifact_bound_side_effect.v1",
		SideEffectRef:        "side-effect:terminal-classifier",
		Actor:                "agent-beta",
		LaneRef:              "lane:task-ui",
		TensionRef:           "task-ui",
		ArtifactRef:          "artifact:repo",
		RegionRef:            "region:adapter:git:path:package.json",
		Operation:            "change",
		SourceKind:           "adapter:git",
		BoundaryRef:          "boundary:branch-beta",
		BoundaryRelation:     "adjacent_foundation_candidate",
		MaterializationState: "present_unintegrated",
		IntegrationIntent:    "foundation_effect",
		IntegrationStatus:    "pending_classification",
		Decision:             "none_pending",
		Justification:        "classification required",
		DerivedRegionRefs:    []string{"region:adapter:git:path:package.json"},
	}})
	if result == nil || !result.IsError {
		t.Fatalf("expected blocked reconciliation route, got %+v", result)
	}
	for _, want := range []string{
		`"gate_type": "side_effect_reconciliation"`,
		`"classification_task_available": false`,
		`"reconciliation_task_id": "task-side-effect-reconcile-terminal"`,
		`"stale_classification_task_status": "RESOLVED"`,
		"already terminal",
	} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Contains(result.Output, `"classification_task_id"`) {
		t.Fatalf("terminal duplicate classification route must not advertise classification_task_id, got %s", result.Output)
	}
	if calls != 5 {
		t.Fatalf("expected hydration, update, duplicate submit, status lookup, reconciliation submit, got %d", calls)
	}
}

func TestDirtySideEffectBlockerReusesExistingBoundaryDenialRoute(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "agent.task.hydrate" {
				t.Fatalf("expected preflight hydration, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"updates": []map[string]any{{
						"update_id":    "update-denial-1",
						"update_type":  "boundary_transition_denial",
						"payload_json": `{"schema":"project_role_scope_authority_denial.v1","side_effect_refs":["side-effect:denied"],"active_task_id":"task-ui","branch_id":"branch-beta","boundary_transition_state":"boundary_expansion_denied_overlap","transition_denied":true,"denial_reason":"overlaps_live_owner_lane","preferred_transition":"wait_or_split_existing_owner_lane","allowed_next":["wait_for_conflicting_lane_publication","request_merge_or_adopt"],"conflicting_task_id":"task-gamma","conflicting_owner_agent_id":"gamma"}`,
					}},
				},
			})
		default:
			t.Fatalf("existing boundary denial must not create fresh side-effect updates or classification tasks; extra method %q", req.Method)
		}
	}))
	defer server.Close()

	blocker := projectDirtySideEffectBlocker{
		client:      NewRhizomeClient(server.URL, "token"),
		workspaceID: "ws",
		agentID:     "agent-beta",
		ownerUserID: "owner-1",
	}
	result := blocker.returnSideEffectBlock(context.Background(), projectDirtySideEffectBlockInput{
		SourceTool: "project_branch_commit",
		ProjectID:  "project-clearpress",
		Branch: ProjectBranchRecord{
			BranchID:     "branch-beta",
			BranchName:   "agent/beta/ui",
			AgentID:      "agent-beta",
			ActiveTaskID: "task-ui",
		},
		DirtyPaths: []string{"package.json"},
		Pathset:    []string{"src/ui/**"},
	}, []AgentUpdateSideEffectV1{{
		Schema:               "artifact_bound_side_effect.v1",
		SideEffectRef:        "side-effect:denied",
		Actor:                "agent-beta",
		LaneRef:              "lane:task-ui",
		TensionRef:           "task-ui",
		ArtifactRef:          "artifact:repo",
		RegionRef:            "region:adapter:git:path:package.json",
		Operation:            "change",
		SourceKind:           "adapter:git",
		BoundaryRef:          "boundary:branch-beta",
		BoundaryRelation:     "adjacent_foundation_candidate",
		MaterializationState: "present_unintegrated",
		IntegrationIntent:    "foundation_effect",
		IntegrationStatus:    "pending_classification",
		Decision:             "none_pending",
		Justification:        "classification required",
		DerivedRegionRefs:    []string{"region:adapter:git:path:package.json"},
	}})
	if result == nil || !result.IsError {
		t.Fatalf("expected existing denial to block repeated commit, got %+v", result)
	}
	for _, want := range []string{
		`"gate_type": "boundary_transition_denied"`,
		"boundary_expansion_denied_overlap",
		`"denial_recorded": true`,
		"wait_or_split_existing_owner_lane",
		"task-gamma",
		"gamma",
		`"classification_task_created": false`,
		"request_merge_or_adopt",
	} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Contains(result.Output, `"integration_status": "pending_classification"`) {
		t.Fatalf("existing denial route must not advertise a fresh pending classification blocker: %s", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected only hydration lookup, got %d calls", calls)
	}
}

func TestDirtySideEffectBlockerIgnoresStaleBoundaryDenialWhenConflictBranchReleased(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "agent.task.hydrate" {
				t.Fatalf("expected preflight hydration, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"updates": []map[string]any{{
						"update_id":    "update-denial-stale",
						"update_type":  "boundary_transition_denial",
						"payload_json": `{"schema":"project_role_scope_authority_denial.v1","side_effect_refs":["side-effect:stale-denial"],"active_task_id":"task-ui","branch_id":"branch-beta","boundary_transition_state":"boundary_expansion_denied_overlap","transition_denied":true,"denial_reason":"overlaps_live_owner_lane","preferred_transition":"wait_or_split_existing_owner_lane","allowed_next":["wait_for_conflicting_lane_publication"],"conflicting_task_id":"task-gamma","conflicting_active_task_id":"task-gamma","conflicting_owner_agent_id":"gamma","conflicting_branch_id":"branch-gamma"}`,
					}},
				},
			})
		case 2:
			if req.Method != "project.coordination.get" {
				t.Fatalf("expected coordination freshness lookup, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"branches": []map[string]any{{
						"branch_id":        "branch-gamma",
						"project_id":       "project-clearpress",
						"repo_id":          "repo-1",
						"agent_id":         "gamma",
						"active_task_id":   "task-gamma",
						"branch_name":      "agent/gamma/foundation",
						"branch_kind":      "implementation",
						"write_scope_json": `{"paths":["package.json"]}`,
						"status":           "ABANDONED",
					}},
				},
			})
		case 3:
			if req.Method != "agent.update.post" {
				t.Fatalf("expected fresh side-effect update after stale denial, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{})
		case 4:
			if req.Method != "task.submit" {
				t.Fatalf("expected fresh classification task submit after stale denial, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id": "task-side-effect-fresh",
				"status":  "PENDING",
			})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	blocker := projectDirtySideEffectBlocker{
		client:      NewRhizomeClient(server.URL, "token"),
		workspaceID: "ws",
		agentID:     "agent-beta",
		ownerUserID: "owner-1",
	}
	result := blocker.returnSideEffectBlock(context.Background(), projectDirtySideEffectBlockInput{
		SourceTool: "project_branch_commit",
		ProjectID:  "project-clearpress",
		Repo:       ProjectRepositoryRecord{RepoID: "repo-1", Name: "clearpress"},
		Branch: ProjectBranchRecord{
			BranchID:     "branch-beta",
			BranchName:   "agent/beta/ui",
			AgentID:      "agent-beta",
			ActiveTaskID: "task-ui",
		},
		DirtyPaths: []string{"package.json"},
		Pathset:    []string{"src/ui/**"},
	}, []AgentUpdateSideEffectV1{{
		Schema:               "artifact_bound_side_effect.v1",
		SideEffectRef:        "side-effect:stale-denial",
		Actor:                "agent-beta",
		LaneRef:              "lane:task-ui",
		TensionRef:           "task-ui",
		ArtifactRef:          "artifact:repo",
		RegionRef:            "region:adapter:git:path:package.json",
		Operation:            "change",
		SourceKind:           "adapter:git",
		BoundaryRef:          "boundary:branch-beta",
		BoundaryRelation:     "adjacent_foundation_candidate",
		MaterializationState: "present_unintegrated",
		IntegrationIntent:    "foundation_effect",
		IntegrationStatus:    "pending_classification",
		Decision:             "none_pending",
		Justification:        "classification required",
		DerivedRegionRefs:    []string{"region:adapter:git:path:package.json"},
	}})
	if result == nil || !result.IsError {
		t.Fatalf("expected stale denial to reopen side-effect classification, got %+v", result)
	}
	for _, want := range []string{
		`"gate_type": "side_effect_classification"`,
		`"classification_task_id": "task-side-effect-fresh"`,
		`"classification_task_created": true`,
		`"integration_status": "pending_classification"`,
	} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Contains(result.Output, `"gate_type": "boundary_transition_denied"`) {
		t.Fatalf("stale denial must not suppress a fresh side-effect route: %s", result.Output)
	}
	if calls != 4 {
		t.Fatalf("expected hydration, coordination freshness lookup, update post, and task submit, got %d calls", calls)
	}
}
