package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func writeSideEffectResolveRPCError(w http.ResponseWriter, req rpcRequest, code int, message string) {
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      req.ID,
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	})
}

// fastPathResolveSideEffect builds an out-of-boundary side effect for a single
// dirty git path, mirroring projectBranchCommitSideEffectsForBoundary's shape, for
// the TE-51 protocol guards that drive returnSideEffectBlock directly.
func fastPathResolveSideEffect(ref, path string) AgentUpdateSideEffectV1 {
	region := "region:adapter:git:path:" + path
	return AgentUpdateSideEffectV1{
		Schema:               "artifact_bound_side_effect.v1",
		SideEffectRef:        ref,
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

// TestDirtySideEffectReuseHonorsNewestFirstHydrationOrdering pins the answer to
// worker-e handoff concern 2: recorded-resolution reuse picks the FIRST matching
// side_effect_resolution in the hydration window, and the hydration window is
// newest-first (listTaskRelatedUpdates ORDER BY created_at DESC). So when a later
// superseding decision (revert) follows an earlier one (accept) for the same refs,
// reuse must surface the NEWEST decision, never the stale older one. This guard
// fails if hydration ordering or first-match selection ever drifts so that a
// superseded decision is reused.
func TestDirtySideEffectReuseHonorsNewestFirstHydrationOrdering(t *testing.T) {
	ref := "side-effect:ws:project:branch:" + sanitizeRefSegment("package.json")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "agent.task.hydrate" {
			t.Fatalf("recorded-resolution reuse must not create fresh updates/tasks; extra method %q", req.Method)
		}
		// Server returns newest-first. The revert (newer, superseding) precedes the
		// accept (older) exactly as the SQL ORDER BY created_at DESC produces.
		writeRPCResult(w, req, map[string]any{
			"bundle": map[string]any{
				"updates": []map[string]any{
					{
						"update_id":    "update-resolution-revert-newer",
						"update_type":  "side_effect_resolution",
						"payload_json": `{"schema":"artifact_bound_side_effect_resolution.v1","status":"resolved","decision":"revert","integration_status":"rejected","side_effect_refs":["` + ref + `"],"followup_task_id":"task-side-effect-revert"}`,
					},
					{
						"update_id":    "update-resolution-accept-older",
						"update_type":  "side_effect_resolution",
						"payload_json": `{"schema":"artifact_bound_side_effect_resolution.v1","status":"resolved","decision":"accept","integration_status":"accepted","side_effect_refs":["` + ref + `"],"followup_task_id":"task-side-effect-accept"}`,
					},
				},
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
	}, []AgentUpdateSideEffectV1{fastPathResolveSideEffect(ref, "package.json")})

	if result == nil || !result.IsError {
		t.Fatalf("expected recorded-resolution reuse block, got %+v", result)
	}
	// Newest decision (revert) must win.
	for _, want := range []string{
		`"gate_type": "side_effect_resolution_reused"`,
		`"reused_decision": "revert"`,
		`"reused_resolution_update_id": "update-resolution-revert-newer"`,
		"task-side-effect-revert",
	} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected newest decision to be reused, missing %q in %s", want, result.Output)
		}
	}
	// Stale older decision must NOT be the reused one.
	if strings.Contains(result.Output, `"reused_decision": "accept"`) {
		t.Fatalf("reuse picked the superseded older decision: %s", result.Output)
	}
	if strings.Contains(result.Output, "update-resolution-accept-older") {
		t.Fatalf("reuse surfaced the stale older resolution update id: %s", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected a single shared hydration lookup, got %d calls", calls)
	}
}

// TestDirtySideEffectMalformedRefFailsClosedThroughFastPathEntry is the TE-51
// malformed-payload guard exercised through the fast-path entry: a side effect
// missing its stable ref (or with an opaque, non-git region) must never resolve
// deterministically. It falls through to the unchanged escalation path, which
// posts a schema-valid blocked update and opens exactly one classification task --
// fail closed, no fabricated resolution.
func TestDirtySideEffectMalformedRefFailsClosedThroughFastPathEntry(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(AgentUpdateSideEffectV1) AgentUpdateSideEffectV1
	}{
		{
			name: "empty_side_effect_ref",
			mutate: func(e AgentUpdateSideEffectV1) AgentUpdateSideEffectV1 {
				e.SideEffectRef = ""
				return e
			},
		},
		{
			name: "opaque_non_git_region",
			mutate: func(e AgentUpdateSideEffectV1) AgentUpdateSideEffectV1 {
				e.RegionRef = "region:opaque:blob"
				e.DerivedRegionRefs = nil
				return e
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hydrateCalls := 0
			updatePosts := 0
			classificationSubmits := 0
			var blockedPayload string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				req := decodeRPCRequest(t, r)
				switch req.Method {
				case "agent.task.hydrate":
					hydrateCalls++
					writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"updates": []map[string]any{}}})
				case "agent.update.post":
					updatePosts++
					blockedPayload = rpcString(req.Params, "payload_json")
					writeRPCResult(w, req, map[string]any{})
				case "task.submit":
					classificationSubmits++
					requirements, _ := req.Params["task_requirements"].(map[string]any)
					if requirements == nil || requirements["abpc_task_class"] != "side_effect_classification" {
						t.Fatalf("expected single classification task submit, got %+v", req.Params)
					}
					writeRPCResult(w, req, map[string]any{"task_id": "task-side-effect-classify-malformed", "status": "PENDING"})
				default:
					t.Fatalf("malformed side effect must not drive any resolution RPC; extra method %q", req.Method)
				}
			}))
			defer server.Close()

			blocker := projectDirtySideEffectBlocker{
				client:      NewRhizomeClient(server.URL, "token"),
				workspaceID: "ws",
				agentID:     "agent-beta",
				ownerUserID: "owner-1",
			}
			effect := tc.mutate(fastPathResolveSideEffect("side-effect:malformed", "src/ui/widget.tsx"))
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
			}, []AgentUpdateSideEffectV1{effect})

			if result == nil || !result.IsError {
				t.Fatalf("expected malformed side effect to fail closed to escalation, got %+v", result)
			}
			if strings.Contains(result.Output, "side_effect_fast_path") || strings.Contains(result.Output, "side_effect_resolution_reused") {
				t.Fatalf("malformed side effect must not advertise a deterministic resolution: %s", result.Output)
			}
			for _, want := range []string{
				`"gate_type": "side_effect_classification"`,
				`"integration_status": "pending_classification"`,
			} {
				if !strings.Contains(result.Output, want) {
					t.Fatalf("expected escalation output to contain %q, got %s", want, result.Output)
				}
			}
			if classificationSubmits != 1 {
				t.Fatalf("expected exactly one classification task (fail closed, no fan-out), got %d", classificationSubmits)
			}
			if updatePosts != 1 {
				t.Fatalf("expected exactly one blocked update post, got %d", updatePosts)
			}
			// The escalation update payload must remain schema-valid: the blocked
			// update carries the original side_effects, so a malformed ref must NOT
			// have been smuggled into a durable side_effect_resolution.
			if strings.Contains(blockedPayload, "artifact_bound_side_effect_resolution.v1") {
				t.Fatalf("malformed side effect must not produce a resolution payload, got %s", blockedPayload)
			}
		})
	}
}

func TestSideEffectResolveToolSplitTensionCreatesFollowupAndDecisionUpdate(t *testing.T) {
	var updatePayload string
	var submitted map[string]any
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			submitted = req.Params
			if got := rpcString(submitted, "task_class"); got != "INTEGRATION" {
				t.Fatalf("expected universal integration task class, got %q params=%+v", got, submitted)
			}
			if got := rpcString(submitted, "task_class_source"); got != "EXPLICIT" {
				t.Fatalf("expected explicit task class source, got %q params=%+v", got, submitted)
			}
			requirements, _ := submitted["task_requirements"].(map[string]any)
			if requirements["abpc_task_class"] != "side_effect_foundation" {
				t.Fatalf("expected ABPC foundation class in requirements, got %+v", requirements)
			}
			if requirements["admission_kind"] != "abpc_recovery_action" || requirements["action_kind"] != "split_foundation_bucket" {
				t.Fatalf("expected executable ABPC recovery requirements, got %+v", requirements)
			}
			if requirements["preserve_write_scope_hints"] != true || requirements["write_scope_hints_authoritative"] != true {
				t.Fatalf("expected ABPC recovery scope hints to be authoritative, got %+v", requirements)
			}
			if got := strings.Join(stringSliceFromAny(requirements["write_scope_hints"]), ","); got != "package.json,index.html" {
				t.Fatalf("expected dirty path write_scope_hints, got %q requirements=%+v", got, requirements)
			}
			if _, ok := submitted["dependency_task_ids"]; ok {
				t.Fatalf("side-effect successor must not depend on its parent classifier, got %+v", submitted["dependency_task_ids"])
			}
			if got := rpcString(submitted, "project_lane"); got != "implementation" {
				t.Fatalf("expected implementation lane, got %q", got)
			}
			if !strings.Contains(rpcString(submitted, "description"), "Original lane") && !strings.Contains(rpcString(submitted, "description"), "active_task_id: task-auth") {
				t.Fatalf("expected follow-up description to preserve original lane context, got %s", rpcString(submitted, "description"))
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-side-effect-foundation",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 3:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case 4:
			if req.Method != "task.close" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_id"); got != "task-side-effect-classify-1" {
				t.Fatalf("expected parent classifier close, got %q", got)
			}
			if got := rpcString(req.Params, "resolution"); got != "RESOLVED" {
				t.Fatalf("expected parent classifier resolved split, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"task_id": "task-side-effect-classify-1", "status": "RESOLVED"})
		case 5:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			updatePayload = rpcString(req.Params, "payload_json")
			for _, want := range []string{"artifact_bound_side_effect_resolution.v1", "split_tension", "split_requested", "side-effect:one", "task-auth", "resolved_waiting_on_successors"} {
				if !strings.Contains(updatePayload, want) {
					t.Fatalf("expected decision update payload to contain %q, got %s", want, updatePayload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-reviewer", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":             "project-clearpress",
		"decision":               "split_tension",
		"side_effect_refs":       []string{"side-effect:one"},
		"justification":          "root scaffold is a useful foundation but outside the auth/profile lane",
		"owner_agent_id":         "agent-gamma",
		"active_task_id":         "task-auth",
		"branch_id":              "branch-gamma",
		"branch_name":            "agent/gamma/auth",
		"dirty_paths":            []string{"package.json", "index.html"},
		"classification_task_id": "task-side-effect-classify-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected split_tension resolution to succeed, got %+v", result)
	}
	for _, want := range []string{"decision_recorded", "create_foundation_lane", "task-side-effect-foundation", "split_requested"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if updatePayload == "" || submitted == nil || calls != 5 {
		t.Fatalf("expected decision update and follow-up task, calls=%d update=%q task=%+v", calls, updatePayload, submitted)
	}
}

func TestSideEffectResolveToolSplitTensionSuppressesPathlessFoundationCarrierCreation(t *testing.T) {
	var updatePayload string
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case 2:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			updatePayload = rpcString(req.Params, "payload_json")
			for _, want := range []string{
				"abpc_pathless_foundation_carrier_no_substrate_identity",
				"pathless_carrier_suppressed",
				"typed_terminal_blocker",
			} {
				if !strings.Contains(updatePayload, want) {
					t.Fatalf("expected update payload to contain %q, got %s", want, updatePayload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("pathless split_tension must not submit a follow-up task; extra call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"decision":         "split_tension",
		"side_effect_refs": []string{"side-effect:opaque-region"},
		"justification":    "R15 shape: split tension without project, branch, active task, or dirty paths must terminalize instead of minting unowned implementation work",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected typed terminal blocker result, got %+v", result)
	}
	for _, want := range []string{
		`"followup_created": false`,
		`"typed_terminal_blocker": true`,
		"abpc_pathless_foundation_carrier_no_substrate_identity",
		`"pathless_carrier_suppressed": true`,
	} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if updatePayload == "" || calls != 2 {
		t.Fatalf("expected successor lookup and update only; calls=%d update=%q", calls, updatePayload)
	}
}

func TestSideEffectResolveToolSplitTensionSuppressesProjectBoundNoPathFoundationCarrier(t *testing.T) {
	var updatePayload string
	calls := 0
	existingRequirements, _ := json.Marshal(map[string]any{
		"schema":                          "artifact_bound_side_effect_resolution_followup.v1",
		"admission_kind":                  "abpc_recovery_action",
		"abpc_task_class":                 "side_effect_foundation",
		"action_kind":                     "split_foundation_bucket",
		"successor_key":                   "abpc-resolution-successor:old-lua",
		"resolution_saga_key":             "abpc-resolution-saga:old-lua",
		"decision":                        "split_tension",
		"project_id":                      "project-lua",
		"branch_id":                       "projbranch-old",
		"active_task_id":                  "task-lua-lexer",
		"side_effect_refs":                []string{"side-effect:ws:project-lua:projbranch-old:cmd-glua-main.go"},
		"dirty_paths":                     []string{},
		"path_bucket":                     []string{},
		"write_scope_hints":               []string{},
		"write_scope_hints_authoritative": true,
	})
	existingTask := map[string]any{
		"task_id":                "task-side-effect-aea7fd1cdc",
		"status":                 "PENDING",
		"project_id":             "project-lua",
		"project_lane":           "implementation",
		"task_kind":              "EXECUTION",
		"task_requirements_json": string(existingRequirements),
		"updated_at":             "2026-06-17T01:30:31Z",
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{existingTask}})
		case 2:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			updatePayload = rpcString(req.Params, "payload_json")
			for _, want := range []string{
				"abpc_pathless_foundation_carrier_no_substrate_identity",
				"pathless_carrier_suppressed",
				"typed_terminal_blocker",
			} {
				if !strings.Contains(updatePayload, want) {
					t.Fatalf("expected update payload to contain %q, got %s", want, updatePayload)
				}
			}
			if strings.Contains(updatePayload, "task-side-effect-aea7fd1cdc") || strings.Contains(updatePayload, "active_recovery_successor_reused") {
				t.Fatalf("stale no-path successor must not be reused, got %s", updatePayload)
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("R16 no-path split_tension must not submit or reuse stale follow-up; extra call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "delta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-lua",
		"decision":         "split_tension",
		"side_effect_refs": []string{"side-effect:ws:project-lua:projbranch-old:cmd-glua-main.go"},
		"justification":    "R16 shape: project-bound stale side-effect carrier has no dirty path/write-scope identity and must not be reused or recreated",
		"owner_agent_id":   "delta",
		"target_agent_id":  "zeta",
		"active_task_id":   "task-lua-lexer",
		"branch_id":        "projbranch-current",
		"branch_name":      "agent-delta-p-0df6e988e5-r-current",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected typed terminal blocker result, got %+v", result)
	}
	for _, want := range []string{
		`"followup_created": false`,
		`"typed_terminal_blocker": true`,
		"abpc_pathless_foundation_carrier_no_substrate_identity",
		`"pathless_carrier_suppressed": true`,
	} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Contains(result.Output, "task-side-effect-aea7fd1cdc") {
		t.Fatalf("stale no-path carrier must not appear as reused follow-up, got %s", result.Output)
	}
	if updatePayload == "" || calls != 2 {
		t.Fatalf("expected successor lookup and update only; calls=%d update=%q", calls, updatePayload)
	}
}

func TestSideEffectResolveToolRequestVerificationUsesUniversalTaskClass(t *testing.T) {
	calls := 0
	var submitted map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			submitted = req.Params
			if got := rpcString(submitted, "task_class"); got != "PROOF" {
				t.Fatalf("expected universal proof task class, got %q params=%+v", got, submitted)
			}
			if got := rpcString(submitted, "task_class_source"); got != "EXPLICIT" {
				t.Fatalf("expected explicit task class source, got %q params=%+v", got, submitted)
			}
			if got := rpcString(submitted, "task_kind"); got != "EXECUTION" {
				t.Fatalf("expected verification to use executable storage task kind, got %q params=%+v", got, submitted)
			}
			if got := rpcString(submitted, "project_lane"); got != "verification" {
				t.Fatalf("expected verification lane to avoid product review-role admission, got %q params=%+v", got, submitted)
			}
			requirements, _ := submitted["task_requirements"].(map[string]any)
			if requirements["abpc_task_class"] != "side_effect_verification" {
				t.Fatalf("expected ABPC verification class in requirements, got %+v", requirements)
			}
			if requirements["admission_kind"] != "abpc_recovery_action" || requirements["action_kind"] != "verify_bucket" {
				t.Fatalf("expected executable ABPC verification recovery requirements, got %+v", requirements)
			}
			if _, ok := submitted["dependency_task_ids"]; ok {
				t.Fatalf("side-effect successor must not depend on its parent classifier, got %+v", submitted["dependency_task_ids"])
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-side-effect-verification",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 3:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case 4:
			if req.Method != "task.close" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_id"); got != "task-side-effect-classify-1" {
				t.Fatalf("expected parent classifier close, got %q", got)
			}
			if got := rpcString(req.Params, "resolution"); got != "RESOLVED" {
				t.Fatalf("expected parent classifier resolved split, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"task_id": "task-side-effect-classify-1", "status": "RESOLVED"})
		case 5:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			payload := rpcString(req.Params, "payload_json")
			for _, want := range []string{"artifact_bound_side_effect_resolution.v1", "request_verification", "verification_requested", "side-effect:verify", "resolved_waiting_on_successors"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("expected verification decision payload to contain %q, got %s", want, payload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-reviewer", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":             "project-clearpress",
		"decision":               "request_verification",
		"side_effect_refs":       []string{"side-effect:verify"},
		"justification":          "dirty scaffold needs independent classification",
		"owner_agent_id":         "agent-beta",
		"active_task_id":         "task-ui",
		"branch_id":              "branch-beta",
		"dirty_paths":            []string{"README.md"},
		"classification_task_id": "task-side-effect-classify-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected request_verification resolution to succeed, got %+v", result)
	}
	for _, want := range []string{"decision_recorded", "route_to_verifier", "task-side-effect-verification", "verification_requested"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 5 || submitted == nil {
		t.Fatalf("expected follow-up task and decision update, calls=%d task=%+v", calls, submitted)
	}
}

func TestSideEffectResolveToolRequestVerificationOnActiveVerificationSuccessorReusesCurrentTask(t *testing.T) {
	calls := 0
	var updatePayload string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch req.Method {
		case "workspace.tasks.list":
			requirements, _ := json.Marshal(map[string]any{
				"schema":                    "artifact_bound_side_effect_resolution_followup.v1",
				"admission_kind":            "abpc_recovery_action",
				"abpc_task_class":           "side_effect_verification",
				"action_kind":               "verify_bucket",
				"successor_key":             "abpc-resolution-successor:verify",
				"resolution_saga_key":       "abpc-resolution-saga:verify",
				"decision":                  "request_verification",
				"project_id":                "project-clearpress",
				"active_task_id":            "task-editor",
				"parent_classifier_task_id": "task-side-effect-classify-1",
				"classification_task_id":    "task-side-effect-classify-1",
				"side_effect_refs":          []string{"side-effect:verify"},
				"dirty_paths":               []string{"README.md"},
				"path_bucket":               []string{"README.md"},
			})
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{{
				"task_id":                "task-side-effect-verify-tooling",
				"status":                 "RUNNING",
				"project_id":             "project-clearpress",
				"project_lane":           "verification",
				"task_requirements_json": string(requirements),
			}}})
		case "agent.update.post":
			updatePayload = rpcString(req.Params, "payload_json")
			for _, want := range []string{"self_recursive_recovery_collapsed", "active_recovery_successor_reused", "task-side-effect-verify-tooling", "verification_requested"} {
				if !strings.Contains(updatePayload, want) {
					t.Fatalf("expected idempotent verification update payload to contain %q, got %s", want, updatePayload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("active verification successor must not submit/block another successor; extra method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "iota", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":             "project-clearpress",
		"decision":               "request_verification",
		"side_effect_refs":       []string{"side-effect:verify"},
		"justification":          "current verification successor needs no duplicate verifier for the same bucket",
		"owner_agent_id":         "iota",
		"active_task_id":         "task-side-effect-verify-tooling",
		"branch_id":              "branch-beta",
		"dirty_paths":            []string{"README.md"},
		"classification_task_id": "task-side-effect-classify-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected repeated request_verification to reuse active successor, got %+v", result)
	}
	for _, want := range []string{"decision_recorded", "task-side-effect-verify-tooling", "self_recursive_recovery_collapsed", `"followup_created": false`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 2 || updatePayload == "" {
		t.Fatalf("expected task lookup and decision update only; calls=%d update=%q", calls, updatePayload)
	}
}

func TestSideEffectResolveToolDecisionPivotOnActiveSuccessorReusesCurrentTask(t *testing.T) {
	calls := 0
	var updatePayload string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch req.Method {
		case "workspace.tasks.list":
			requirements, _ := json.Marshal(map[string]any{
				"schema":                    "artifact_bound_side_effect_resolution_followup.v1",
				"admission_kind":            "abpc_recovery_action",
				"abpc_task_class":           "side_effect_verification",
				"action_kind":               "verify_bucket",
				"successor_key":             "abpc-resolution-successor:verify",
				"resolution_saga_key":       "abpc-resolution-saga:verify",
				"decision":                  "request_verification",
				"project_id":                "project-clearpress",
				"active_task_id":            "task-editor",
				"parent_classifier_task_id": "task-side-effect-classify-1",
				"classification_task_id":    "task-side-effect-classify-1",
				"side_effect_refs":          []string{"side-effect:verify"},
				"dirty_paths":               []string{"README.md"},
				"path_bucket":               []string{"README.md"},
			})
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{{
				"task_id":                "task-side-effect-verify-tooling",
				"status":                 "RUNNING",
				"project_id":             "project-clearpress",
				"project_lane":           "verification",
				"task_requirements_json": string(requirements),
			}}})
		case "agent.update.post":
			updatePayload = rpcString(req.Params, "payload_json")
			for _, want := range []string{
				"cross_decision_recovery_pivot_collapsed",
				"active_recovery_successor_reused",
				"active_recovery_successor_previous_action_kind",
				"task-side-effect-verify-tooling",
				"quarantine",
				"quarantine_materialization",
			} {
				if !strings.Contains(updatePayload, want) {
					t.Fatalf("expected pivot update payload to contain %q, got %s", want, updatePayload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("active successor pivot must not submit/block a sibling successor; extra method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "iota", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":             "project-clearpress",
		"decision":               "quarantine",
		"side_effect_refs":       []string{"side-effect:verify"},
		"justification":          "verification found this bucket is drift; quarantine current successor rather than spawn a sibling",
		"owner_agent_id":         "iota",
		"active_task_id":         "task-side-effect-verify-tooling",
		"branch_id":              "branch-beta",
		"dirty_paths":            []string{"README.md"},
		"classification_task_id": "task-side-effect-classify-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected decision pivot to reuse active successor, got %+v", result)
	}
	for _, want := range []string{"decision_recorded", "task-side-effect-verify-tooling", "cross_decision_recovery_pivot_collapsed", `"followup_created": false`, "quarantine_materialization"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 2 || updatePayload == "" {
		t.Fatalf("expected task lookup and pivot decision update only; calls=%d update=%q", calls, updatePayload)
	}
}

func TestSideEffectResolveToolParentClassifierClosesAfterSuccessor(t *testing.T) {
	calls := 0
	var closedParent string
	var updatePayload string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-side-effect-quarantine-package-lock",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 3:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case 4:
			if req.Method != "task.close" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			closedParent = rpcString(req.Params, "task_id")
			if closedParent != "task-side-effect-classify-beta" {
				t.Fatalf("expected parent classifier to close, got %q", closedParent)
			}
			if got := rpcString(req.Params, "resolution"); got != "RESOLVED" {
				t.Fatalf("expected resolved split fallback, got %q", got)
			}
			if !strings.Contains(rpcString(req.Params, "reason"), "resolved_split_waiting_on_side_effect_resolution_successor") {
				t.Fatalf("expected split fallback reason, got %q", rpcString(req.Params, "reason"))
			}
			writeRPCResult(w, req, map[string]any{"task_id": closedParent, "status": "RESOLVED"})
		case 5:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			updatePayload = rpcString(req.Params, "payload_json")
			for _, want := range []string{"parent_classification_resolved_split", "resolved_waiting_on_successors", "quarantined"} {
				if !strings.Contains(updatePayload, want) {
					t.Fatalf("expected update payload to contain %q, got %s", want, updatePayload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "kappa", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":             "project-clearpress",
		"decision":               "quarantine",
		"side_effect_refs":       []string{"side-effect:package-lock"},
		"justification":          "package-lock belongs in quarantine until the owner lane removes or isolates it",
		"owner_agent_id":         "beta",
		"active_task_id":         "task-editor",
		"branch_name":            "beta/task-editor",
		"dirty_paths":            []string{"package-lock.json"},
		"classification_task_id": "task-side-effect-classify-beta",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected quarantine resolution to succeed with parent classifier close, got %+v", result)
	}
	if closedParent == "" || updatePayload == "" || calls != 5 {
		t.Fatalf("expected parent classifier close and update; calls=%d closed=%q update=%q", calls, closedParent, updatePayload)
	}
}

func TestSideEffectResolveToolReusesSagaSuccessorWhenDirtyPathsOmitted(t *testing.T) {
	calls := 0
	var updatePayload string
	existingRequirements, _ := json.Marshal(map[string]any{
		"schema":                    "artifact_bound_side_effect_resolution_followup.v1",
		"admission_kind":            "abpc_recovery_action",
		"abpc_task_class":           "side_effect_foundation",
		"action_kind":               "split_foundation_bucket",
		"successor_key":             "abpc-resolution-successor:legacy-dirty-paths",
		"resolution_saga_key":       "abpc-resolution-saga:legacy-dirty-paths",
		"decision":                  "split_tension",
		"project_id":                "project-clearpress",
		"branch_id":                 "branch-eval",
		"active_task_id":            "task-eval-builtins",
		"classification_task_id":    "task-side-effect-classify-r31",
		"parent_classifier_task_id": "task-side-effect-classify-r31",
		"side_effect_refs":          []string{"side-effect:readme", "side-effect:acceptance-tests"},
		"dirty_paths":               []string{"README.md", "internal/cli/acceptance_matrix_test.go"},
		"path_bucket":               []string{"README.md", "internal/cli/acceptance_matrix_test.go"},
	})
	existingTask := map[string]any{
		"task_id":                "task-side-effect-existing-r31",
		"status":                 "PENDING",
		"project_id":             "project-clearpress",
		"project_lane":           "implementation",
		"task_requirements_json": string(existingRequirements),
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{existingTask}})
		case 2:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{existingTask}})
		case 3:
			if req.Method != "task.close" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_id"); got != "task-side-effect-classify-r31" {
				t.Fatalf("expected parent classifier close, got %q", got)
			}
			if got := rpcString(req.Params, "resolution"); got != "RESOLVED" {
				t.Fatalf("expected parent classifier resolved, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"task_id": "task-side-effect-classify-r31", "status": "RESOLVED"})
		case 4:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			updatePayload = rpcString(req.Params, "payload_json")
			for _, want := range []string{
				"active_recovery_successor_reused",
				"task-side-effect-existing-r31",
				"existing ABPC recovery successor",
				"parent_classification_resolved_split",
			} {
				if !strings.Contains(updatePayload, want) {
					t.Fatalf("expected update payload to contain %q, got %s", want, updatePayload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("resume reuse must not submit a sibling successor; extra call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "epsilon", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":             "project-clearpress",
		"decision":               "split_tension",
		"side_effect_refs":       []string{"side-effect:readme", "side-effect:acceptance-tests"},
		"justification":          "same side-effect saga resumed after the classifier omitted dirty_paths",
		"owner_agent_id":         "epsilon",
		"target_agent_id":        "beta",
		"active_task_id":         "task-eval-builtins",
		"branch_id":              "branch-eval",
		"classification_task_id": "task-side-effect-classify-r31",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected existing successor reuse, got %+v", result)
	}
	for _, want := range []string{"task-side-effect-existing-r31", `"followup_created": false`, "existing ABPC recovery successor", "resolved_waiting_on_successors"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if updatePayload == "" || calls != 4 {
		t.Fatalf("expected two lookups, parent close, and update only; calls=%d update=%q", calls, updatePayload)
	}
}

func TestSideEffectResolveToolReusesBranchBucketSuccessorAcrossClassifierActiveTask(t *testing.T) {
	calls := 0
	var updatePayload string
	branchID := "projbranch-lua-eta"
	branchName := "agent-eta-p-0df6e988e5-t-cli"
	refs := []string{
		"side-effect:ws:project-lua:" + branchID + ":cmd-glua-main.go",
		"side-effect:ws:project-lua:" + branchID + ":internal-runner-runner.go",
		"side-effect:ws:project-lua:" + branchID + ":internal-runner-runner_test.go",
		"side-effect:ws:project-lua:" + branchID + ":readme.md",
	}
	existingRequirements, _ := json.Marshal(map[string]any{
		"schema":                    "artifact_bound_side_effect_resolution_followup.v1",
		"admission_kind":            "abpc_recovery_action",
		"abpc_task_class":           "side_effect_foundation",
		"action_kind":               "split_foundation_bucket",
		"successor_key":             "abpc-resolution-successor:old-root",
		"resolution_saga_key":       "abpc-resolution-saga:old-root",
		"decision":                  "split_tension",
		"project_id":                "project-lua",
		"branch_id":                 branchID,
		"branch_name":               branchName,
		"active_task_id":            "task-signal01-lua-root-capability",
		"classification_task_id":    "task-side-effect-classify-old",
		"parent_classifier_task_id": "task-side-effect-classify-old",
		"side_effect_refs":          refs,
		"path_bucket": []string{
			"README.md",
			"cmd/glua/main.go",
			"internal/runner/runner.go",
			"internal/runner/runner_test.go",
		},
	})
	existingTask := map[string]any{
		"task_id":                "task-side-effect-existing-lua",
		"status":                 "PENDING",
		"project_id":             "project-lua",
		"project_lane":           "implementation",
		"task_requirements_json": string(existingRequirements),
		"updated_at":             "2026-06-16T15:46:01Z",
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{existingTask}})
		case 2:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{existingTask}})
		case 3:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{existingTask}})
		case 4:
			if req.Method != "task.close" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_id"); got != "task-side-effect-classify-new" {
				t.Fatalf("expected parent classifier close, got %q", got)
			}
			if got := rpcString(req.Params, "resolution"); got != "RESOLVED" {
				t.Fatalf("expected parent classifier resolved, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"task_id": "task-side-effect-classify-new", "status": "RESOLVED"})
		case 5:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			updatePayload = rpcString(req.Params, "payload_json")
			for _, want := range []string{
				"active_recovery_successor_reused",
				"task-side-effect-existing-lua",
				"existing ABPC recovery successor",
				"resolved_waiting_on_successors",
			} {
				if !strings.Contains(updatePayload, want) {
					t.Fatalf("expected update payload to contain %q, got %s", want, updatePayload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("branch-bucket reuse must not submit a sibling successor; extra call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "zeta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":             "project-lua",
		"decision":               "split_tension",
		"side_effect_refs":       refs[:3],
		"justification":          "same Lua branch bucket was reclassified from the side-effect classifier task",
		"owner_agent_id":         "eta",
		"target_agent_id":        "eta",
		"active_task_id":         "task-side-effect-classify-new",
		"branch_id":              branchID,
		"branch_name":            branchName,
		"dirty_paths":            []string{"cmd/glua/main.go", "internal/runner/runner.go", "internal/runner/runner_test.go"},
		"classification_task_id": "task-side-effect-classify-new",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected existing branch-bucket successor reuse, got %+v", result)
	}
	for _, want := range []string{"task-side-effect-existing-lua", `"followup_created": false`, "existing ABPC recovery successor"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if updatePayload == "" || calls != 5 {
		t.Fatalf("expected active lookup, existing lookup, reconcile lookup, parent close, and update only; calls=%d update=%q", calls, updatePayload)
	}
}

func TestSideEffectResolveToolDoesNotReusePathlessStaleBranchBucketSuccessor(t *testing.T) {
	calls := 0
	var submitted map[string]any
	var updatePayload string
	branchID := "projbranch-lua-delta-old"
	branchName := "agent-delta-p-0df6e988e5-t-dcd047ec19"
	refs := []string{
		"side-effect:ws:project-lua:" + branchID + ":cmd-glua-main.go",
		"side-effect:ws:project-lua:" + branchID + ":internal-lexer-lexer.go",
		"side-effect:ws:project-lua:" + branchID + ":internal-parser-parser.go",
	}
	existingRequirements, _ := json.Marshal(map[string]any{
		"schema":                          "artifact_bound_side_effect_resolution_followup.v1",
		"admission_kind":                  "abpc_recovery_action",
		"abpc_task_class":                 "side_effect_foundation",
		"action_kind":                     "split_foundation_bucket",
		"successor_key":                   "abpc-resolution-successor:old-pathless",
		"resolution_saga_key":             "abpc-resolution-saga:old-pathless",
		"decision":                        "split_tension",
		"project_id":                      "project-lua",
		"branch_id":                       branchID,
		"branch_name":                     branchName,
		"active_task_id":                  "task-old-control-functions",
		"side_effect_refs":                refs,
		"dirty_paths":                     []string{},
		"path_bucket":                     []string{},
		"write_scope_hints":               []string{},
		"write_scope_hints_authoritative": true,
	})
	existingTask := map[string]any{
		"task_id":                "task-side-effect-old-pathless",
		"status":                 "PENDING",
		"project_id":             "project-lua",
		"project_lane":           "implementation",
		"task_kind":              "EXECUTION",
		"task_requirements_json": string(existingRequirements),
		"updated_at":             "2026-06-17T01:30:31Z",
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{existingTask}})
		case 2:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{existingTask}})
		case 3:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			submitted = req.Params
			if got := rpcString(submitted, "task_id"); got == "task-side-effect-old-pathless" {
				t.Fatalf("must create a fresh successor instead of reusing old pathless carrier")
			}
			requirements, _ := submitted["task_requirements"].(map[string]any)
			if got := strings.Join(stringSliceFromAny(requirements["dirty_paths"]), ","); got != "cmd/glua/main.go,internal/lexer/lexer.go" {
				t.Fatalf("expected fresh dirty pathset on successor, got %q requirements=%+v", got, requirements)
			}
			if got := strings.Join(stringSliceFromAny(requirements["write_scope_hints"]), ","); got != "cmd/glua/main.go,internal/lexer/lexer.go" {
				t.Fatalf("expected fresh write scope hints on successor, got %q requirements=%+v", got, requirements)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-side-effect-fresh-pathset",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 4:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{existingTask}})
		case 5:
			if req.Method != "task.close" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_id"); got != "task-side-effect-classify-new" {
				t.Fatalf("expected parent classifier close, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"task_id": "task-side-effect-classify-new", "status": "RESOLVED"})
		case 6:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected sixth method %q", req.Method)
			}
			updatePayload = rpcString(req.Params, "payload_json")
			for _, want := range []string{"task-side-effect-fresh-pathset", "followup_created", "resolved_waiting_on_successors"} {
				if !strings.Contains(updatePayload, want) {
					t.Fatalf("expected update payload to contain %q, got %s", want, updatePayload)
				}
			}
			if strings.Contains(updatePayload, "active_recovery_successor_reused") {
				t.Fatalf("pathless stale successor must not be reported as reused, got %s", updatePayload)
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("pathless stale successor must not add extra calls; call=%d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "zeta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":             "project-lua",
		"decision":               "split_tension",
		"side_effect_refs":       refs[:2],
		"justification":          "current materialization has concrete dirty paths and must not reuse a pathless stale carrier",
		"owner_agent_id":         "delta",
		"target_agent_id":        "delta",
		"active_task_id":         "task-side-effect-classify-new",
		"branch_id":              branchID,
		"branch_name":            branchName,
		"dirty_paths":            []string{"cmd/glua/main.go", "internal/lexer/lexer.go"},
		"classification_task_id": "task-side-effect-classify-new",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected fresh successor creation, got %+v", result)
	}
	for _, want := range []string{"task-side-effect-fresh-pathset", `"followup_created": true`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if submitted == nil || updatePayload == "" || calls != 6 {
		t.Fatalf("expected fresh submit plus parent close/update, calls=%d submitted=%v update=%q", calls, submitted != nil, updatePayload)
	}
}

func TestSideEffectResolveToolBucketSuccessorSupersedesCoveredBroadFollowup(t *testing.T) {
	calls := 0
	closedTaskID := ""
	var updatePayload string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-side-effect-reassign-app",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 3:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []map[string]any{
				sideEffectResolveTestFollowupTask("task-side-effect-broad", "request_verification", "verify_bucket", "2026-05-23T00:00:00Z", []string{"package.json", "public/logo.png", "src/App.tsx"}),
				sideEffectResolveTestFollowupTask("task-side-effect-verify-tooling", "request_verification", "verify_bucket", "2026-05-23T00:00:01Z", []string{"package.json"}),
				sideEffectResolveTestFollowupTask("task-side-effect-revert-assets", "revert", "revert_bucket", "2026-05-23T00:00:02Z", []string{"public/logo.png"}),
			}})
		case 4:
			if req.Method != "task.close" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			closedTaskID = rpcString(req.Params, "task_id")
			if closedTaskID != "task-side-effect-broad" {
				t.Fatalf("expected broad follow-up to be superseded, got %q", closedTaskID)
			}
			if got := rpcString(req.Params, "resolution"); got != "CANCELLED" {
				t.Fatalf("expected superseded task to be cancelled, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"task_id": closedTaskID, "status": "CLOSED"})
		case 5:
			if req.Method != "task.close" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_id"); got != "task-side-effect-classify-gamma" {
				t.Fatalf("expected parent classifier close, got %q", got)
			}
			if got := rpcString(req.Params, "resolution"); got != "RESOLVED" {
				t.Fatalf("expected parent classifier resolved, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"task_id": "task-side-effect-classify-gamma", "status": "RESOLVED"})
		case 6:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected sixth method %q", req.Method)
			}
			updatePayload = rpcString(req.Params, "payload_json")
			for _, want := range []string{"superseded_followup_task_ids", "task-side-effect-broad", "resolved_waiting_on_successors", "side-effect:gamma"} {
				if !strings.Contains(updatePayload, want) {
					t.Fatalf("expected update payload to contain %q, got %s", want, updatePayload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "epsilon", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":             "project-clearpress",
		"decision":               "reassign",
		"side_effect_refs":       []string{"side-effect:gamma"},
		"justification":          "src/App belongs to the app shell owner, while tooling/assets have separate recovery buckets",
		"owner_agent_id":         "gamma",
		"target_agent_id":        "beta",
		"active_task_id":         "task-editor",
		"branch_id":              "branch-gamma",
		"dirty_paths":            []string{"src/App.tsx"},
		"classification_task_id": "task-side-effect-classify-gamma",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected bucketed reassign resolution to succeed, got %+v", result)
	}
	for _, want := range []string{"decision_recorded", "task-side-effect-reassign-app", "task-side-effect-broad", "resolved_waiting_on_successors"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 6 || closedTaskID == "" || updatePayload == "" {
		t.Fatalf("expected lookup, submit, list, supersede close, parent close, and update; calls=%d closed=%q update=%q", calls, closedTaskID, updatePayload)
	}
}

func sideEffectResolveTestFollowupTask(taskID, decision, actionKind, updatedAt string, dirtyPaths []string) map[string]any {
	requirements := map[string]any{
		"schema":                    "artifact_bound_side_effect_resolution_followup.v1",
		"abpc_task_class":           "side_effect_resolution",
		"admission_kind":            "abpc_recovery_action",
		"action_kind":               actionKind,
		"decision":                  decision,
		"project_id":                "project-clearpress",
		"branch_id":                 "branch-gamma",
		"active_task_id":            "task-editor",
		"classification_task_id":    "task-side-effect-classify-gamma",
		"parent_classifier_task_id": "task-side-effect-classify-gamma",
		"side_effect_refs":          []string{"side-effect:gamma"},
		"dirty_paths":               dirtyPaths,
		"path_bucket":               dirtyPaths,
	}
	raw, _ := json.Marshal(requirements)
	return map[string]any{
		"task_id":                taskID,
		"title":                  taskID,
		"status":                 "PENDING",
		"task_kind":              "COORDINATION",
		"project_id":             "project-clearpress",
		"project_lane":           "coordination",
		"task_requirements_json": string(raw),
		"updated_at":             updatedAt,
	}
}

func TestSideEffectResolveToolExpandBoundaryUsesExplicitRoleScopeTransition(t *testing.T) {
	calls := 0
	listCalls := 0
	var assignedScope string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"strategic_lead": map[string]any{
						"agent_id":  "agent-alpha",
						"role_type": "STRATEGIC_LEAD",
						"status":    "ACTIVE",
					},
				},
			})
		case "workspace.tasks.list":
			listCalls++
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case "project.role.assign":
			assignedScope = rpcString(req.Params, "write_scope_json")
			for _, want := range []string{"src/**", "package.json"} {
				if !strings.Contains(assignedScope, want) {
					t.Fatalf("expected assigned expanded scope to contain %q, got %s", want, assignedScope)
				}
			}
			writeRPCResult(w, req, map[string]any{
				"role": map[string]any{
					"role_id":          "role-1",
					"workspace_id":     "ws",
					"project_id":       "project-clearpress",
					"agent_id":         "agent-gamma",
					"role_type":        "IMPLEMENTER",
					"status":           "ACTIVE",
					"write_scope_json": assignedScope,
				},
				"active_claim_rebind": map[string]any{"state": "updated"},
			})
		case "agent.update.post":
			if payload := rpcString(req.Params, "payload_json"); !strings.Contains(payload, "boundary_expansion_requested") || !strings.Contains(payload, "expand_boundary") {
				t.Fatalf("expected boundary expansion decision payload, got %s", payload)
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":               "project-clearpress",
		"decision":                 "expand_boundary",
		"side_effect_refs":         []string{"side-effect:scope"},
		"justification":            "package config is a discovered dependency of the editor lane",
		"owner_agent_id":           "agent-gamma",
		"target_agent_id":          "agent-gamma",
		"active_task_id":           "task-editor",
		"branch_id":                "branch-gamma",
		"dirty_paths":              []string{"package.json"},
		"current_write_scope_json": `{"paths":["src/**"]}`,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected expand_boundary resolution to succeed, got %+v", result)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(result.Output), &payload); err != nil {
		t.Fatalf("decode output: %v\n%s", err, result.Output)
	}
	if payload["boundary_transition"] != "expand_boundary" || payload["transition_executed"] != true {
		t.Fatalf("expected executed boundary transition, got %+v", payload)
	}
	if assignedScope == "" || calls != 4 || listCalls != 1 {
		t.Fatalf("expected receipt lookup, role assignment, and update; calls=%d lists=%d assignedScope=%q", calls, listCalls, assignedScope)
	}
}

func TestSideEffectResolveToolExpandBoundaryRecordsAlreadySatisfiedWithLiveBinding(t *testing.T) {
	calls := 0
	listCalls := 0
	var updatePayload string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch req.Method {
		case "project.coordination.get":
			result := projectRoleAssignCoordinationResultWithRoles("agent-alpha", []map[string]any{{
				"role_id":          "role-gamma",
				"workspace_id":     "ws",
				"project_id":       "project-rq",
				"agent_id":         "agent-gamma",
				"role_type":        "IMPLEMENTER",
				"status":           "ACTIVE",
				"write_scope_json": `{"paths":["cmd/**","go.mod","README.md"]}`,
				"created_at":       "2026-05-04T00:00:00Z",
				"updated_at":       "2026-05-04T00:00:00Z",
			}})
			coordination := result["coordination"].(map[string]any)
			coordination["branches"] = []any{map[string]any{
				"branch_id":        "branch-gamma",
				"agent_id":         "agent-gamma",
				"active_task_id":   "task-cli",
				"status":           "ACTIVE",
				"write_scope_json": `{"paths":["cmd/**","go.mod","README.md"]}`,
			}}
			writeRPCResult(w, req, result)
		case "workspace.tasks.list":
			listCalls++
			writeRPCResult(w, req, map[string]any{"tasks": []any{
				map[string]any{
					"task_id":                "task-cli",
					"status":                 "RUNNING",
					"claim_agent_id":         "agent-gamma",
					"claim_status":           "BLOCKED",
					"claim_branch_id":        "branch-gamma",
					"claim_write_scope_json": `{"paths":["cmd/**","go.mod","README.md"]}`,
				},
			}})
		case "agent.update.post":
			updatePayload = rpcString(req.Params, "payload_json")
			for _, want := range []string{"role_scope_already_satisfied", "already_applied", "expand_boundary"} {
				if !strings.Contains(updatePayload, want) {
					t.Fatalf("expected already-applied boundary payload to contain %q, got %s", want, updatePayload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-gamma", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":                "project-rq",
		"decision":                  "expand_boundary",
		"side_effect_refs":          []string{"side-effect:go-mod"},
		"justification":             "go.mod and README are already in the live branch and claim boundary",
		"owner_agent_id":            "agent-gamma",
		"target_agent_id":           "agent-gamma",
		"active_task_id":            "task-cli",
		"branch_id":                 "branch-gamma",
		"dirty_paths":               []string{"go.mod", "README.md"},
		"current_write_scope_json":  `{"paths":["cmd/**"]}`,
		"expanded_write_scope_json": `{"paths":["cmd/**","go.mod","README.md"]}`,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected already-applied boundary resolution to record decision, got %+v", result)
	}
	for _, want := range []string{"decision_recorded", "role_scope_already_satisfied", "already_applied"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 3 || listCalls != 1 || updatePayload == "" {
		t.Fatalf("expected coordination, binding lookup, and update; calls=%d lists=%d update=%q", calls, listCalls, updatePayload)
	}
}

func TestSideEffectResolveToolExpandBoundaryDoesNotResolveWhenOnlyLeadRequestQueued(t *testing.T) {
	calls := 0
	listCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"strategic_lead": map[string]any{
						"agent_id":  "agent-alpha",
						"role_type": "STRATEGIC_LEAD",
						"status":    "ACTIVE",
					},
				},
			})
		case "workspace.tasks.list":
			listCalls++
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case "task.submit":
			if got := rpcString(req.Params, "title"); !strings.Contains(got, "Resolve project role/scope request") {
				t.Fatalf("expected durable lead repair task, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-role-scope-gamma",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case "agent.request":
			writeRPCResult(w, req, map[string]any{
				"request_id": "areq-role-scope",
				"status":     "PENDING",
			})
		default:
			t.Fatalf("side_effect_resolution update must not be posted for lead-request-only transition; extra method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-gamma", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":               "project-clearpress",
		"decision":                 "expand_boundary",
		"side_effect_refs":         []string{"side-effect:scope"},
		"justification":            "root scaffold is valid foundation work but needs strategic-lead boundary expansion",
		"owner_agent_id":           "agent-gamma",
		"target_agent_id":          "agent-gamma",
		"active_task_id":           "task-foundation",
		"branch_id":                "branch-gamma",
		"dirty_paths":              []string{"package.json"},
		"current_write_scope_json": `{"paths":["src/**"]}`,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected lead-request-only transition to stay blocked, got %+v", result)
	}
	for _, want := range []string{"transition_blocked_decision_not_recorded", "lead_scope_request_pending", "task-role-scope-gamma", "boundary expansion was routed"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 5 || listCalls != 2 {
		t.Fatalf("expected coordination, two receipt lookups, lead task, and lead wake only; calls=%d lists=%d", calls, listCalls)
	}
}

func TestSideEffectResolveToolExpandBoundaryReusesPendingAuthorityTransition(t *testing.T) {
	calls := 0
	listCalls := 0
	var updatePayload string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"strategic_lead": map[string]any{
						"agent_id":  "agent-alpha",
						"role_type": "STRATEGIC_LEAD",
						"status":    "ACTIVE",
					},
				},
			})
		case "workspace.tasks.list":
			listCalls++
			authorityReq, _ := json.Marshal(map[string]any{
				"schema": "project_role_scope_authority_transition.v1",
				"boundary_transition_key": sideEffectBoundaryTransitionKey("ws", sideEffectResolveInput{
					ProjectID:      "project-clearpress",
					Decision:       "expand_boundary",
					SideEffectRefs: []string{"side-effect:scope"},
					OwnerAgentID:   "agent-beta",
					TargetAgentID:  "agent-beta",
					ActiveTaskID:   "task-editor",
					BranchID:       "branch-beta",
				}),
				"side_effect_refs": []string{"side-effect:scope"},
				"active_task_id":   "task-editor",
				"branch_id":        "branch-beta",
				"target_agent_id":  "agent-beta",
				"role_type":        "IMPLEMENTER",
			})
			writeRPCResult(w, req, map[string]any{"tasks": []any{map[string]any{
				"task_id":                "task-role-scope-pending",
				"status":                 "PENDING",
				"task_requirements_json": string(authorityReq),
			}}})
		case "agent.update.post":
			updatePayload = rpcString(req.Params, "payload_json")
			for _, want := range []string{"authority_transition_already_pending", "task-role-scope-", "transition_already_pending"} {
				if !strings.Contains(updatePayload, want) {
					t.Fatalf("expected pending authority transition update to contain %q, got %s", want, updatePayload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("duplicate authority transition must not re-send lead wake request; extra method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":               "project-clearpress",
		"decision":                 "expand_boundary",
		"side_effect_refs":         []string{"side-effect:scope"},
		"justification":            "reuse the existing authority transition for this side effect",
		"owner_agent_id":           "agent-beta",
		"target_agent_id":          "agent-beta",
		"active_task_id":           "task-editor",
		"branch_id":                "branch-beta",
		"dirty_paths":              []string{"package.json"},
		"current_write_scope_json": `{"paths":["src/**"]}`,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected repeated boundary expansion to record pending transition without hard failure, got %+v", result)
	}
	for _, want := range []string{"decision_recorded", "authority_transition_already_pending", "transition_already_pending", "task-role-scope"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 4 || listCalls != 2 || updatePayload == "" {
		t.Fatalf("expected coordination get, two receipt lookups, and durable update only; calls=%d lists=%d update=%q", calls, listCalls, updatePayload)
	}
}

func TestSideEffectResolveToolRecognizesAppliedAuthorityTransitionReceipt(t *testing.T) {
	refs := []string{"side-effect:clearpress", "side-effect:test-setup"}
	inputForKey := sideEffectResolveInput{
		ProjectID:      "project-clearpress",
		Decision:       "expand_boundary",
		SideEffectRefs: refs,
		OwnerAgentID:   "agent-beta",
		TargetAgentID:  "agent-beta",
		ActiveTaskID:   "task-editor",
		BranchID:       "branch-beta",
	}
	boundaryKey := sideEffectBoundaryTransitionKey("ws", inputForKey)
	expandedScope := `{"paths":["src/**","tests/**"]}`
	var updatePayload string
	calls := 0
	listCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"strategic_lead": map[string]any{
						"agent_id":  "agent-alpha",
						"role_type": "STRATEGIC_LEAD",
						"status":    "ACTIVE",
					},
					"branches": []any{map[string]any{
						"branch_id":        "branch-beta",
						"agent_id":         "agent-beta",
						"status":           "READY_FOR_REVIEW",
						"head_sha":         "abc123",
						"review_doc_key":   "task.task-editor.review",
						"write_scope_json": expandedScope,
					}},
				},
			})
		case "workspace.tasks.list":
			listCalls++
			authorityReq, _ := json.Marshal(map[string]any{
				"schema":                  "project_role_scope_authority_transition.v1",
				"boundary_transition_key": boundaryKey,
				"dedup_key":               boundaryKey,
				"side_effect_refs":        refs,
				"active_task_id":          "task-editor",
				"branch_id":               "branch-beta",
				"target_agent_id":         "agent-beta",
				"write_scope_json":        expandedScope,
			})
			writeRPCResult(w, req, map[string]any{"tasks": []any{
				map[string]any{
					"task_id":                "task-role-scope-applied",
					"status":                 "RESOLVED",
					"task_requirements_json": string(authorityReq),
					"claim_agent_id":         "agent-alpha",
					"claim_status":           "COMPLETED",
				},
				map[string]any{
					"task_id":                "task-editor",
					"status":                 "RESOLVED",
					"claim_agent_id":         "agent-beta",
					"claim_status":           "COMPLETED",
					"claim_branch_id":        "branch-beta",
					"claim_write_scope_json": expandedScope,
				},
			}})
		case "agent.update.post":
			updatePayload = rpcString(req.Params, "payload_json")
			for _, want := range []string{"authority_transition_applied", "side-effect:clearpress", "side-effect:test-setup"} {
				if !strings.Contains(updatePayload, want) {
					t.Fatalf("expected applied receipt update to contain %q, got %s", want, updatePayload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("applied authority receipt must not create or wake generic repair work; extra method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-eta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":               "project-clearpress",
		"decision":                 "expand_boundary",
		"side_effect_refs":         refs,
		"justification":            "src support files are now covered by the applied authority transition",
		"owner_agent_id":           "agent-beta",
		"target_agent_id":          "agent-beta",
		"active_task_id":           "task-editor",
		"branch_id":                "branch-beta",
		"dirty_paths":              []string{"src/clearpress.ts", "src/test/setup.ts"},
		"current_write_scope_json": `{"paths":["src/App.*"]}`,
		"classification_task_id":   "task-side-effect-classify-beta",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected applied authority receipt to resolve side effect, got %+v", result)
	}
	for _, want := range []string{"decision_recorded", "authority_transition_applied", "transition_executed", "side-effect:clearpress"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 3 || listCalls != 1 || updatePayload == "" {
		t.Fatalf("expected coordination, task-list receipt lookup, and update; calls=%d lists=%d update=%q", calls, listCalls, updatePayload)
	}
}

func TestSideEffectResolveToolDoesNotTreatUncoveredAuthorityTaskAsApplied(t *testing.T) {
	refs := []string{"side-effect:clearpress", "side-effect:test-setup"}
	inputForKey := sideEffectResolveInput{
		ProjectID:      "project-clearpress",
		Decision:       "expand_boundary",
		SideEffectRefs: refs,
		OwnerAgentID:   "agent-beta",
		TargetAgentID:  "agent-beta",
		ActiveTaskID:   "task-editor",
		BranchID:       "branch-beta",
	}
	boundaryKey := sideEffectBoundaryTransitionKey("ws", inputForKey)
	var updatePayload string
	calls := 0
	listCalls := 0
	submitCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"strategic_lead": map[string]any{"agent_id": "agent-alpha", "role_type": "STRATEGIC_LEAD", "status": "ACTIVE"},
					"branches": []any{map[string]any{
						"branch_id":        "branch-beta",
						"agent_id":         "agent-beta",
						"status":           "ACTIVE",
						"active_task_id":   "task-editor",
						"write_scope_json": `{"paths":["src/App.*"]}`,
					}},
				},
			})
		case "workspace.tasks.list":
			listCalls++
			authorityReq, _ := json.Marshal(map[string]any{
				"schema":                  "project_role_scope_authority_transition.v1",
				"boundary_transition_key": boundaryKey,
				"side_effect_refs":        refs,
				"active_task_id":          "task-editor",
				"branch_id":               "branch-beta",
			})
			writeRPCResult(w, req, map[string]any{"tasks": []any{
				map[string]any{
					"task_id":                "task-role-scope-uncovered",
					"status":                 "RESOLVED",
					"task_requirements_json": string(authorityReq),
					"claim_status":           "COMPLETED",
				},
				map[string]any{
					"task_id":                "task-editor",
					"status":                 "RUNNING",
					"claim_agent_id":         "agent-beta",
					"claim_status":           "BLOCKED",
					"claim_branch_id":        "branch-beta",
					"claim_write_scope_json": `{"paths":["src/App.*"]}`,
				},
			}})
		case "task.submit":
			submitCalls++
			writeRPCError(w, req, -32602, "workspace task already exists")
		case "agent.update.post":
			updatePayload = rpcString(req.Params, "payload_json")
			if strings.Contains(updatePayload, "authority_transition_applied") {
				t.Fatalf("uncovered path bucket must not be treated as applied, got %s", updatePayload)
			}
			for _, want := range []string{"authority_transition_already_pending", "transition_already_pending", "task-role-scope"} {
				if !strings.Contains(updatePayload, want) {
					t.Fatalf("expected pending authority update to contain %q, got %s", want, updatePayload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-eta", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":               "project-clearpress",
		"decision":                 "expand_boundary",
		"side_effect_refs":         refs,
		"justification":            "do not falsely apply incomplete coverage",
		"owner_agent_id":           "agent-beta",
		"target_agent_id":          "agent-beta",
		"active_task_id":           "task-editor",
		"branch_id":                "branch-beta",
		"dirty_paths":              []string{"src/clearpress.ts", "src/test/setup.ts"},
		"current_write_scope_json": `{"paths":["src/App.*"]}`,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected pending transition update to remain non-fatal, got %+v", result)
	}
	if strings.Contains(result.Output, "authority_transition_applied") {
		t.Fatalf("uncovered path bucket must not return applied receipt, got %s", result.Output)
	}
	if listCalls < 2 || submitCalls != 1 || updatePayload == "" {
		t.Fatalf("expected lookup, duplicate task, retry lookup, and update; calls=%d lists=%d submits=%d update=%q", calls, listCalls, submitCalls, updatePayload)
	}
}

func TestSideEffectResolveToolExpandBoundaryRecordsOverlapDenial(t *testing.T) {
	calls := 0
	listCalls := 0
	var updatePayload string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"strategic_lead": map[string]any{
						"agent_id":  "agent-alpha",
						"role_type": "STRATEGIC_LEAD",
						"status":    "ACTIVE",
					},
				},
			})
		case "workspace.tasks.list":
			listCalls++
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case "project.role.assign":
			writeRPCError(w, req, -32000, "project role write scope conflict: write_scope_json overlaps active claim task_id=task-gamma agent_id=gamma branch_id=branch-gamma; request the active owner to commit/publish")
		case "agent.update.post":
			updatePayload = rpcString(req.Params, "payload_json")
			for _, want := range []string{"boundary_expansion_denied_overlap", "overlaps_live_owner_lane", "task-gamma", "gamma"} {
				if !strings.Contains(updatePayload, want) {
					t.Fatalf("expected overlap denial update to contain %q, got %s", want, updatePayload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":               "project-clearpress",
		"decision":                 "expand_boundary",
		"side_effect_refs":         []string{"side-effect:scope"},
		"justification":            "requested root files overlap the live foundation lane",
		"owner_agent_id":           "agent-beta",
		"target_agent_id":          "agent-beta",
		"active_task_id":           "task-editor",
		"branch_id":                "branch-beta",
		"dirty_paths":              []string{"package.json"},
		"current_write_scope_json": `{"paths":["src/editor/**"]}`,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected overlap denial to be recorded as a durable routing state, got %+v", result)
	}
	for _, want := range []string{"decision_recorded", "boundary_expansion_denied_overlap", "overlaps_live_owner_lane", "wait_or_split_existing_owner_lane"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 4 || listCalls != 1 || updatePayload == "" {
		t.Fatalf("expected coordination get, receipt lookup, denied role assign, and durable update; calls=%d lists=%d update=%q", calls, listCalls, updatePayload)
	}
}

func TestSideEffectResolveToolExpandBoundaryRequiresBranchRebindEvidence(t *testing.T) {
	calls := 0
	listCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, map[string]any{
				"coordination": map[string]any{
					"strategic_lead": map[string]any{
						"agent_id":  "agent-alpha",
						"role_type": "STRATEGIC_LEAD",
						"status":    "ACTIVE",
					},
				},
			})
		case "workspace.tasks.list":
			listCalls++
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case "project.role.assign":
			writeRPCResult(w, req, map[string]any{
				"role": map[string]any{
					"role_id":          "role-1",
					"workspace_id":     "ws",
					"project_id":       "project-clearpress",
					"agent_id":         "agent-gamma",
					"role_type":        "IMPLEMENTER",
					"status":           "ACTIVE",
					"write_scope_json": `{"paths":["src/**","package.json"]}`,
				},
			})
		default:
			t.Fatalf("side_effect_resolution update must not be posted without branch rebind evidence; extra method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":               "project-clearpress",
		"decision":                 "expand_boundary",
		"side_effect_refs":         []string{"side-effect:scope"},
		"justification":            "package config is a discovered dependency of the editor lane",
		"owner_agent_id":           "agent-gamma",
		"target_agent_id":          "agent-gamma",
		"active_task_id":           "task-editor",
		"branch_id":                "branch-gamma",
		"dirty_paths":              []string{"package.json"},
		"current_write_scope_json": `{"paths":["src/**"]}`,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected missing branch rebind evidence to keep transition blocked, got %+v", result)
	}
	for _, want := range []string{"transition_blocked_decision_not_recorded", "branch_claim_rebind_missing", "active claim/branch boundary"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 3 || listCalls != 1 {
		t.Fatalf("expected coordination get, receipt lookup, and role assignment only; calls=%d lists=%d", calls, listCalls)
	}
}

func TestSideEffectResolveToolDoesNotRecordDecisionWhenTransitionFails(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"tasks": []any{}})
		case 2:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCError(w, req, -32000, "foundation lane creation failed")
		default:
			t.Fatalf("decision update must not be posted after failed transition; extra method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewSideEffectResolveTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-reviewer", "owner-1")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-clearpress",
		"decision":         "split_tension",
		"side_effect_refs": []string{"side-effect:failed"},
		"justification":    "foundation side effect needs a new lane",
		"owner_agent_id":   "agent-gamma",
		"active_task_id":   "task-auth",
		"branch_id":        "branch-gamma",
		"dirty_paths":      []string{"package.json"},
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected transition failure, got %+v", result)
	}
	for _, want := range []string{"transition_blocked_decision_not_recorded", "pending_classification", "foundation lane creation failed"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 2 {
		t.Fatalf("expected lookup and transition attempt only, got %d calls", calls)
	}
}
