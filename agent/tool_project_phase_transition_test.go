package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProjectPhaseTransitionToolOpensImplementationAfterNonPhaseGates(t *testing.T) {
	transitionCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
				CurrentPhase:            "SPEC",
				LeadAgentID:             "agent-alpha",
				DesignDocID:             "project.project-demo.design_and_plan",
				ImplementationPlanDocID: "project.project-demo.design_and_plan",
				Gates: []map[string]any{
					projectPhaseTransitionGate("design_doc_ready", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_plan_ready", "SATISFIED", true),
					projectPhaseTransitionGate("repo_ready_or_not_required", "SATISFIED", true),
					projectPhaseTransitionGate("repo_materialization_allowed", "SATISFIED", true),
					projectPhaseTransitionGate("strategic_lead_active", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_phase_open", "BLOCKED", true),
				},
			}))
		case "workspace.doc.get":
			writeRPCResult(w, req, projectPhaseTransitionDocResult(rpcString(req.Params, "doc_key"), projectPhaseTransitionValidPlanningDoc()))
		case "project.phase.transition":
			transitionCalls++
			if got := rpcString(req.Params, "to_phase"); got != "IMPLEMENTATION" {
				t.Fatalf("to_phase = %q", got)
			}
			if got := rpcString(req.Params, "actor_id"); got != "agent-alpha" {
				t.Fatalf("actor_id = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-demo",
				"current_phase": "IMPLEMENTATION",
				"repo_required": true,
				"repo_status":   "READY",
				"created_at":    "2026-05-04T00:00:00Z",
				"updated_at":    "2026-05-04T00:01:00Z",
			}})
		default:
			t.Fatalf("unexpected RPC method=%s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPhaseTransitionTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"to_phase":   "implementation",
		"reason":     "Design and implementation plan are ready.",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful phase transition, got %+v", result)
	}
	if !strings.Contains(result.Output, `"from_phase": "SPEC"`) || !strings.Contains(result.Output, `"to_phase": "IMPLEMENTATION"`) {
		t.Fatalf("unexpected output %q", result.Output)
	}
	for _, want := range []string{"implementation_fanout_required", "product_planning_review", "critical_plan_review", "task_submit_then_semantic_self_selection", "task_submit", "autonomous self-selection", "semantic product/code gap", "package.json", "vite.config.*"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected implementation fanout guidance %q in output:\n%s", want, result.Output)
		}
	}
	if transitionCalls != 1 {
		t.Fatalf("expected one transition RPC call, got %d", transitionCalls)
	}
}

func TestProjectPhaseTransitionToolBlocksImplementationWhenDesignGateMissing(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
			CurrentPhase: "SPEC",
			LeadAgentID:  "agent-alpha",
			Gates: []map[string]any{
				projectPhaseTransitionGate("design_doc_ready", "BLOCKED", true),
				projectPhaseTransitionGate("implementation_plan_ready", "SATISFIED", true),
				projectPhaseTransitionGate("repo_ready_or_not_required", "SATISFIED", true),
				projectPhaseTransitionGate("repo_materialization_allowed", "SATISFIED", true),
				projectPhaseTransitionGate("strategic_lead_active", "SATISFIED", true),
				projectPhaseTransitionGate("implementation_phase_open", "BLOCKED", true),
			},
		}))
	}))
	defer server.Close()

	tool := NewProjectPhaseTransitionTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"to_phase":   "IMPLEMENTATION",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "design_doc_ready") {
		t.Fatalf("expected design gate error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected no transition RPC, got %d calls", calls)
	}
}

func TestProjectPhaseTransitionToolBlocksImplementationWhenSourceTraceMissing(t *testing.T) {
	transitionCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
				CurrentPhase:            "SPEC",
				LeadAgentID:             "agent-alpha",
				DesignDocID:             "project.project-demo.design_and_plan",
				ImplementationPlanDocID: "project.project-demo.design_and_plan",
				Gates: []map[string]any{
					projectPhaseTransitionGate("design_doc_ready", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_plan_ready", "SATISFIED", true),
					projectPhaseTransitionGate("repo_ready_or_not_required", "SATISFIED", true),
					projectPhaseTransitionGate("repo_materialization_allowed", "SATISFIED", true),
					projectPhaseTransitionGate("strategic_lead_active", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_phase_open", "BLOCKED", true),
				},
			}))
		case "workspace.doc.get":
			docKey := rpcString(req.Params, "doc_key")
			if strings.HasSuffix(docKey, ".source_refs") {
				writeRPCResult(w, req, projectPhaseTransitionDocResult(docKey, "```rhizome_source_refs_v1\nsource_doc_keys:\n- run.operator-spec\n```"))
				return
			}
			writeRPCResult(w, req, projectPhaseTransitionDocResult(docKey, projectPhaseTransitionValidPlanningDoc()))
		case "project.phase.transition":
			transitionCalls++
			t.Fatalf("phase transition should not be called while source trace is missing")
		default:
			t.Fatalf("unexpected RPC method=%s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPhaseTransitionTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"to_phase":   "implementation",
		"reason":     "Design and implementation plan are ready.",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "source requirements trace missing") {
		t.Fatalf("expected source trace blocker, got %+v", result)
	}
	if transitionCalls != 0 {
		t.Fatalf("expected no transition RPC call, got %d", transitionCalls)
	}
}

func TestProjectPhaseTransitionToolRejectsGenericSourceRequirementsTrace(t *testing.T) {
	transitionCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
				CurrentPhase:            "SPEC",
				LeadAgentID:             "agent-alpha",
				DesignDocID:             "project.project-demo.design_and_plan",
				ImplementationPlanDocID: "project.project-demo.design_and_plan",
				Gates: []map[string]any{
					projectPhaseTransitionGate("design_doc_ready", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_plan_ready", "SATISFIED", true),
					projectPhaseTransitionGate("repo_ready_or_not_required", "SATISFIED", true),
					projectPhaseTransitionGate("repo_materialization_allowed", "SATISFIED", true),
					projectPhaseTransitionGate("strategic_lead_active", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_phase_open", "BLOCKED", true),
				},
			}))
		case "workspace.doc.get":
			docKey := rpcString(req.Params, "doc_key")
			switch {
			case strings.HasSuffix(docKey, ".source_refs"):
				writeRPCResult(w, req, projectPhaseTransitionDocResult(docKey, "```rhizome_source_refs_v1\nsource_doc_keys:\n- run.operator-spec\n```"))
			case strings.HasSuffix(docKey, ".source_requirements_trace"):
				writeRPCResult(w, req, projectPhaseTransitionDocResult(docKey, strings.Join([]string{
					"```rhizome_source_requirements_trace_v1",
					"source_doc_keys:",
					"- run.operator-spec",
					"acceptance_critical_anchors:",
					"- primary flow",
					"acceptance_criteria_refs:",
					"- acceptance criteria",
					"adjacent_wrong_products:",
					"- product",
					"```",
				}, "\n")))
			default:
				writeRPCResult(w, req, projectPhaseTransitionDocResult(docKey, projectPhaseTransitionValidPlanningDoc()))
			}
		case "project.phase.transition":
			transitionCalls++
			t.Fatalf("phase transition should not be called while source trace anchors are generic")
		default:
			t.Fatalf("unexpected RPC method=%s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPhaseTransitionTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"to_phase":   "implementation",
		"reason":     "Design and implementation plan are ready.",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "source requirements trace missing") {
		t.Fatalf("expected generic source trace blocker, got %+v", result)
	}
	if transitionCalls != 0 {
		t.Fatalf("expected no transition RPC call, got %d", transitionCalls)
	}
}

func TestProjectPhaseTransitionToolAcceptsSpecificSourceRequirementsTrace(t *testing.T) {
	transitionCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
				CurrentPhase:            "SPEC",
				LeadAgentID:             "agent-alpha",
				DesignDocID:             "project.project-demo.design_and_plan",
				ImplementationPlanDocID: "project.project-demo.design_and_plan",
				Gates: []map[string]any{
					projectPhaseTransitionGate("design_doc_ready", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_plan_ready", "SATISFIED", true),
					projectPhaseTransitionGate("repo_ready_or_not_required", "SATISFIED", true),
					projectPhaseTransitionGate("repo_materialization_allowed", "SATISFIED", true),
					projectPhaseTransitionGate("strategic_lead_active", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_phase_open", "BLOCKED", true),
				},
			}))
		case "workspace.doc.get":
			docKey := rpcString(req.Params, "doc_key")
			switch {
			case strings.HasSuffix(docKey, ".source_refs"):
				writeRPCResult(w, req, projectPhaseTransitionDocResult(docKey, "```rhizome_source_refs_v1\nsource_doc_keys:\n- run.operator-spec\n```"))
			case strings.HasSuffix(docKey, ".source_requirements_trace"):
				writeRPCResult(w, req, projectPhaseTransitionDocResult(docKey, strings.Join([]string{
					"```rhizome_source_requirements_trace_v1",
					"source_doc_keys:",
					"- run.operator-spec",
					"acceptance_critical_anchors:",
					"- Telegraph-like article editor",
					"- mock auth signed-out/signed-in flow",
					"- editable author profile and avatar",
					"- local persistence for articles and profile",
					"- public read-only article view",
					"acceptance_criteria_refs:",
					"- create edit save and reopen an article without losing state",
					"- toggle quote style and see rendered text change",
					"adjacent_wrong_products:",
					"- marketing landing page",
					"- generic brief generator",
					"```",
				}, "\n")))
			default:
				writeRPCResult(w, req, projectPhaseTransitionDocResult(docKey, projectPhaseTransitionValidPlanningDoc()))
			}
		case "project.phase.transition":
			transitionCalls++
			if got := rpcString(req.Params, "to_phase"); got != "IMPLEMENTATION" {
				t.Fatalf("to_phase = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-demo",
				"current_phase": "IMPLEMENTATION",
				"repo_required": true,
				"repo_status":   "READY",
				"created_at":    "2026-05-04T00:00:00Z",
				"updated_at":    "2026-05-04T00:01:00Z",
			}})
		default:
			t.Fatalf("unexpected RPC method=%s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPhaseTransitionTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"to_phase":   "implementation",
		"reason":     "Design and implementation plan are ready.",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected specific source trace to allow transition, got %+v", result)
	}
	if !strings.Contains(result.Output, `"source_fidelity": true`) {
		t.Fatalf("expected source fidelity true in output:\n%s", result.Output)
	}
	if transitionCalls != 1 {
		t.Fatalf("expected one transition RPC call, got %d", transitionCalls)
	}
}

func TestProjectPhaseTransitionSourceTraceAcceptsIDPrefixedAnchors(t *testing.T) {
	block := strings.Join([]string{
		"source_doc_keys:",
		"- run.operator-spec",
		"acceptance_critical_anchors:",
		"- SF-1: Telegraph-like article editor",
		"- RQ_2: local persistence for article drafts",
		"- AC3: public article view distinct from editing",
		"acceptance_criteria_refs:",
		"- AC-1",
		"adjacent_wrong_products:",
		"- marketing landing page",
	}, "\n")
	if !projectPlanningSourceRequirementsTraceBlockComplete(block, []string{"run.operator-spec"}) {
		t.Fatalf("expected ID-prefixed source anchors to satisfy source requirements trace")
	}
}

func TestProjectPhaseTransitionSourceTraceAcceptsLiveSchemaField(t *testing.T) {
	doc := strings.Join([]string{
		"schema: rhizome_source_requirements_trace_v1",
		"project_id: project-demo",
		"source_doc_keys:",
		"  - run.operator-spec",
		"acceptance_critical_anchors:",
		"  - SF-1: Telegraph-like article editor",
		"  - RQ_2: local persistence for article drafts",
		"acceptance_criteria_refs:",
		"  - AC-1",
		"adjacent_wrong_products:",
		"  - marketing landing page",
	}, "\n")
	if !projectPlanningTextHasSourceRequirementsTrace(doc, []string{"run.operator-spec"}) {
		t.Fatalf("expected live schema-form source trace to satisfy phase-transition source fidelity gate")
	}
}

func TestProjectPhaseTransitionSourceTraceAcceptsSignal01Trace(t *testing.T) {
	doc := strings.Join([]string{
		"# Signal-01 rq Source Requirements Trace",
		"",
		"```rhizome_source_requirements_trace_v1",
		"project_id: project-task-signal01-rq-root-20260601-sidecar-review",
		"source_doc_keys:",
		"  - operator.signal01.rq.spec.v1",
		"acceptance_criteria_refs:",
		"  - AC-01",
		"  - AC-02",
		"  - AC-03",
		"  - AC-04",
		"  - AC-05",
		"  - AC-06",
		"  - AC-07",
		"  - AC-08",
		"  - AC-09",
		"  - AC-10",
		"  - AC-11",
		"  - AC-12",
		"acceptance_critical_anchors:",
		"  - One shared Go repo and one static rq binary; no duplicate full-project scaffolds.",
		"  - Full language surface includes lexer, parser, evaluator, file mode, REPL, built-ins, and lambdas for map/filter.",
		"  - Errors must be lexical, syntax, and runtime distinct with line-column positions and REPL recovery.",
		"  - Edge-case behavior is part of the product contract, not optional polish.",
		"  - README and runnable reviewer path must match actual binary behavior and sample JSON usage.",
		"adjacent_wrong_products:",
		"  - jq-adjacent toy filter without lambdas or REPL.",
		"  - Multi-repo or per-lane disconnected implementations.",
		"  - Parser-only or evaluator-only demo lacking full CLI, README, and test evidence.",
		"non_goals:",
		"  - Broad jq compatibility beyond the operator spec.",
		"```",
	}, "\n")
	if !projectPlanningTextHasSourceRequirementsTrace(doc, []string{"operator.signal01.rq.spec.v1"}) {
		t.Fatalf("expected Signal-01 source trace to satisfy phase-transition source fidelity gate")
	}
}

func TestProjectPhaseTransitionSourceTraceAcceptsRun19NestedAnchors(t *testing.T) {
	doc := strings.Join([]string{
		"```rhizome_source_requirements_trace_v1",
		"project_id: project-task-signal01-rq-root-run19",
		"source_doc_keys:",
		"  - operator.signal01-run19.spec",
		"acceptance_critical_anchors:",
		"  - source_anchor: operator.signal01-run19.spec#expected-semantic-deliverables",
		"    acceptance_criteria_refs: [AC-02, AC-03, AC-04, AC-05, AC-06, AC-07, AC-08, AC-09]",
		"    implementation_requirement: Semantic deliverables must be implemented as separate reviewable lanes and assembled at integration.",
		"  - source_anchor: operator.signal01-run19.spec#signal-target",
		"    acceptance_criteria_refs: [AC-01, AC-08, AC-09]",
		"    implementation_requirement: The project must show claim -> build -> commit -> submit -> independent review -> repair if needed -> integration with runnable evidence.",
		"adjacent_wrong_products:",
		"  - generic JSON formatter or viewer",
		"  - partial query interpreter missing built-ins or lambda behavior from the source spec",
		"non_goals:",
		"  - speculative optimization before correctness and coverage",
		"```",
	}, "\n")
	if !projectPlanningTextHasSourceRequirementsTrace(doc, []string{"operator.signal01-run19.spec"}) {
		t.Fatalf("expected Run19 nested-anchor source trace to satisfy phase-transition source fidelity gate")
	}
}

func TestProjectPhaseTransitionSourceTraceAcceptsSignal01MarkdownHeadings(t *testing.T) {
	doc := strings.Join([]string{
		"# Signal-01 rq source requirements trace",
		"",
		"Schema: `rhizome_source_requirements_trace_v1`",
		"Project: `project-task-signal01-rq-root-20260601-phase-source-fix`",
		"",
		"## source_doc_keys",
		"- `operator.signal01.rq.spec.v1`",
		"",
		"## acceptance_criteria_refs",
		"- `AC-01-build-test-green`",
		"- `AC-02-core-language`",
		"- `AC-05-errors-with-positions`",
		"",
		"## acceptance_critical_anchors",
		"- Spec section 3.5 file mode: `rq \"<expr>\" <file.json>` must load JSON from the named file, evaluate against that context, pretty-print the result to stdout, and exit non-zero on error.",
		"- Spec section 3.5 REPL: `rq` / `rq --repl` must keep a loaded JSON context for the session and continue after evaluation errors.",
		"- Spec section 3.6 error contract: lexical, syntax, and runtime errors must be human-readable with line/column positions when available.",
		"",
		"## adjacent_wrong_products",
		"- Parser-only or evaluator-only demo lacking full CLI, README, and test evidence.",
		"",
		"## non_goals",
		"- Broad jq compatibility beyond the operator spec.",
	}, "\n")
	if !projectPlanningTextHasSourceRequirementsTrace(doc, []string{"operator.signal01.rq.spec.v1"}) {
		t.Fatalf("expected Signal-01 markdown-heading trace to satisfy phase-transition source fidelity gate")
	}
}

func TestProjectPlanningSourceDocKeysIgnoreGeneratedProjectPlanningDocs(t *testing.T) {
	coordination := ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-task-signal01-rq-root-20260601-sidecar-review"},
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID:               "task-signal01-rq-root",
				TaskRequirementsJSON: `{"source_doc_keys":["operator.signal01.rq.spec.v1"],"root_spec_doc_key":"operator.signal01.rq.spec.v1"}`,
			},
			{
				TaskID: "task-ambient-contract-review",
				TaskRequirementsJSON: `{
					"source_doc_keys":[
						"operator.signal01.rq.spec.v1",
						"project.project-task-signal01-rq-root-20260601-sidecar-review.acceptance_criteria"
					],
					"expected_outputs":[
						"project.project-task-signal01-rq-root-20260601-sidecar-review.product_contract"
					]
				}`,
			},
		},
	}
	combinedDocs := "```rhizome_source_refs_v1\nsource_doc_keys:\n- operator.signal01.rq.spec.v1\n```"
	got := projectPlanningSourceDocKeys(coordination, combinedDocs)
	if strings.Join(got, ",") != "operator.signal01.rq.spec.v1" {
		t.Fatalf("source doc keys = %v, want only operator source doc", got)
	}
}

func TestProjectPlanningSourceDocKeysIgnoreGeneratedProjectPlanningDocsWithTypoedProjectID(t *testing.T) {
	coordination := ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-task-signal01-rq-root-run19"},
		Tasks: []WorkspaceTaskRecord{
			{
				TaskID: "task-run19-lexer",
				TaskRequirementsJSON: `{
					"source_doc_keys": [
						"operator.signal01-run19.spec",
						"project.project-task-signal01-rq-root-run19.acceptance_criteria",
						"project.project-task-signal01-run19.source_requirements_trace"
					]
				}`,
			},
		},
	}
	got := projectPlanningSourceDocKeys(coordination, "")
	if strings.Join(got, ",") != "operator.signal01-run19.spec" {
		t.Fatalf("source doc keys = %v, want only operator source doc", got)
	}
}

func TestProjectPhaseTransitionToolTrustFirstStillBlocksMissingDesignGate(t *testing.T) {
	calls := 0
	transitionCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
				CurrentPhase: "SPEC",
				LeadAgentID:  "agent-alpha",
				Gates: []map[string]any{
					projectPhaseTransitionGate("design_doc_ready", "BLOCKED", true),
					projectPhaseTransitionGate("implementation_plan_ready", "BLOCKED", true),
					projectPhaseTransitionGate("repo_ready_or_not_required", "SATISFIED", true),
					projectPhaseTransitionGate("repo_materialization_allowed", "SATISFIED", true),
					projectPhaseTransitionGate("strategic_lead_active", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_phase_open", "BLOCKED", true),
				},
			}))
		case "project.phase.transition":
			transitionCalls++
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("unexpected RPC method=%s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPhaseTransitionTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha").WithCoordinationMode(CoordinationModeTrustFirst)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"to_phase":   "IMPLEMENTATION",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected trust_first missing gate block, got %+v", result)
	}
	for _, want := range []string{"design_doc_ready", "implementation_plan_ready", "project.project-demo.design_and_plan"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, result.Output)
		}
	}
	if transitionCalls != 0 {
		t.Fatalf("expected no transition RPC, got %d", transitionCalls)
	}
	if calls != 1 {
		t.Fatalf("expected one coordination call, got %d", calls)
	}
}

func TestProjectPhaseTransitionToolRequiresExplicitReasonForPlanningReviewWaiver(t *testing.T) {
	tool := NewProjectPhaseTransitionTool(NewRhizomeClient("http://127.0.0.1:1", "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":                      "project-demo",
		"to_phase":                        "IMPLEMENTATION",
		"require_product_planning_review": false,
		"reason":                          "moving quickly",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "requires reason") {
		t.Fatalf("expected missing waiver reason error, got %+v", result)
	}
}

func TestProjectPhaseTransitionToolBlocksImplementationWhenPlanningReviewMissing(t *testing.T) {
	var transitionCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
				CurrentPhase:            "SPEC",
				LeadAgentID:             "agent-alpha",
				DesignDocID:             "project.project-demo.design_and_plan",
				ImplementationPlanDocID: "project.project-demo.design_and_plan",
				Gates: []map[string]any{
					projectPhaseTransitionGate("design_doc_ready", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_plan_ready", "SATISFIED", true),
					projectPhaseTransitionGate("repo_ready_or_not_required", "SATISFIED", true),
					projectPhaseTransitionGate("repo_materialization_allowed", "SATISFIED", true),
					projectPhaseTransitionGate("strategic_lead_active", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_phase_open", "BLOCKED", true),
				},
			}))
		case "workspace.doc.get":
			writeRPCResult(w, req, projectPhaseTransitionDocResult(rpcString(req.Params, "doc_key"), "# Plan\n\nWe will build something eventually."))
		case "project.phase.transition":
			transitionCalls++
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-demo",
				"current_phase": "IMPLEMENTATION",
				"repo_required": true,
				"repo_status":   "READY",
				"created_at":    "2026-05-04T00:00:00Z",
				"updated_at":    "2026-05-04T00:01:00Z",
			}})
		default:
			t.Fatalf("unexpected RPC method=%s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPhaseTransitionTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"to_phase":   "IMPLEMENTATION",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected missing planning review block, got %+v", result)
	}
	for _, want := range []string{"semantic Product Contract missing", "bounded Critical Plan Review missing", "Product Contract", "Critical Plan Review"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected missing planning review output to contain %q, got:\n%s", want, result.Output)
		}
	}
	if transitionCalls != 0 {
		t.Fatalf("expected no transition when planning review missing, got %d", transitionCalls)
	}
}

func TestProjectPhaseTransitionToolTrustFirstWarnsButOpensWhenProductContractMissing(t *testing.T) {
	var transitionCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
				CurrentPhase:            "SPEC",
				LeadAgentID:             "agent-alpha",
				DesignDocID:             "project.project-demo.design_and_plan",
				ImplementationPlanDocID: "project.project-demo.design_and_plan",
				Gates: []map[string]any{
					projectPhaseTransitionGate("design_doc_ready", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_plan_ready", "SATISFIED", true),
					projectPhaseTransitionGate("repo_ready_or_not_required", "SATISFIED", true),
					projectPhaseTransitionGate("repo_materialization_allowed", "SATISFIED", true),
					projectPhaseTransitionGate("strategic_lead_active", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_phase_open", "BLOCKED", true),
				},
			}))
		case "workspace.doc.get":
			writeRPCResult(w, req, projectPhaseTransitionDocResult(rpcString(req.Params, "doc_key"), "# Plan\n\nWe will build something useful for operators."))
		case "project.phase.transition":
			transitionCalls++
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-demo",
				"current_phase": "IMPLEMENTATION",
				"repo_required": true,
				"repo_status":   "READY",
				"created_at":    "2026-05-04T00:00:00Z",
				"updated_at":    "2026-05-04T00:01:00Z",
			}})
		default:
			t.Fatalf("unexpected RPC method=%s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPhaseTransitionTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha").WithCoordinationMode(CoordinationModeTrustFirst)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"to_phase":   "IMPLEMENTATION",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected trust_first transition with product-contract warning, got %+v", result)
	}
	if transitionCalls != 1 {
		t.Fatalf("expected one transition call, got %d", transitionCalls)
	}
	for _, want := range []string{"semantic Product Contract missing", `"product_contract": false`, "trust_first allows implementation to open"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected trust_first product-contract warning output to contain %q, got:\n%s", want, result.Output)
		}
	}
}

func TestProjectPlanningReviewTransitionIssuesTrustFirstMakesMissingProductContractAdvisory(t *testing.T) {
	issues := projectPlanningReviewTransitionIssues(ProjectPlanningReviewCheck{
		CheckedDocKeys:  []string{"project.project-demo.design_and_plan"},
		ProductContract: false,
		CriticalReview:  true,
	}, true)
	if len(issues) != 1 {
		t.Fatalf("expected one missing-contract issue, got %+v", issues)
	}
	if issues[0].Hard {
		t.Fatalf("expected missing product contract to be advisory in trust_first, got %+v", issues[0])
	}
	if !strings.Contains(issues[0].Message, "semantic Product Contract missing") {
		t.Fatalf("expected missing product contract message, got %+v", issues[0])
	}
}

func TestProjectPhaseTransitionToolTrustFirstWarnsButOpensWhenCriticalReviewMissing(t *testing.T) {
	var transitionCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
				CurrentPhase:            "SPEC",
				LeadAgentID:             "agent-alpha",
				DesignDocID:             "project.project-demo.design_and_plan",
				ImplementationPlanDocID: "project.project-demo.design_and_plan",
				Gates: []map[string]any{
					projectPhaseTransitionGate("design_doc_ready", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_plan_ready", "SATISFIED", true),
					projectPhaseTransitionGate("repo_ready_or_not_required", "SATISFIED", true),
					projectPhaseTransitionGate("repo_materialization_allowed", "SATISFIED", true),
					projectPhaseTransitionGate("strategic_lead_active", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_phase_open", "BLOCKED", true),
				},
			}))
		case "workspace.doc.get":
			writeRPCResult(w, req, projectPhaseTransitionDocResult(rpcString(req.Params, "doc_key"), projectPhaseTransitionProductContractOnlyDoc()))
		case "project.phase.transition":
			transitionCalls++
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-demo",
				"current_phase": "IMPLEMENTATION",
				"repo_required": true,
				"repo_status":   "READY",
				"created_at":    "2026-05-04T00:00:00Z",
				"updated_at":    "2026-05-04T00:01:00Z",
			}})
		case "workspace.agents.list":
			writeRPCResult(w, req, map[string]any{"agents": []map[string]any{}})
		default:
			t.Fatalf("unexpected RPC method=%s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPhaseTransitionTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha").WithCoordinationMode(CoordinationModeTrustFirst)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"to_phase":   "IMPLEMENTATION",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected trust_first transition with warning, got %+v", result)
	}
	if transitionCalls != 1 {
		t.Fatalf("expected one transition call, got %d", transitionCalls)
	}
	for _, want := range []string{"planning telemetry", "Critical Plan Review missing", `"critical_plan_review": false`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected trust_first warning output to contain %q, got:\n%s", want, result.Output)
		}
	}
}

func TestProjectPhaseTransitionToolStrictWarnsButOpensWhenCriticalReviewMissing(t *testing.T) {
	var transitionCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
				CurrentPhase:            "SPEC",
				LeadAgentID:             "agent-alpha",
				DesignDocID:             "project.project-demo.design_and_plan",
				ImplementationPlanDocID: "project.project-demo.design_and_plan",
				Gates: []map[string]any{
					projectPhaseTransitionGate("design_doc_ready", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_plan_ready", "SATISFIED", true),
					projectPhaseTransitionGate("repo_ready_or_not_required", "SATISFIED", true),
					projectPhaseTransitionGate("repo_materialization_allowed", "SATISFIED", true),
					projectPhaseTransitionGate("strategic_lead_active", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_phase_open", "BLOCKED", true),
				},
			}))
		case "workspace.doc.get":
			writeRPCResult(w, req, projectPhaseTransitionDocResult(rpcString(req.Params, "doc_key"), projectPhaseTransitionProductContractOnlyDoc()))
		case "project.phase.transition":
			transitionCalls++
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-demo",
				"current_phase": "IMPLEMENTATION",
				"repo_required": true,
				"repo_status":   "READY",
				"created_at":    "2026-05-04T00:00:00Z",
				"updated_at":    "2026-05-04T00:01:00Z",
			}})
		default:
			t.Fatalf("unexpected RPC method=%s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPhaseTransitionTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"to_phase":   "IMPLEMENTATION",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected strict transition with advisory review warning, got %+v", result)
	}
	if transitionCalls != 1 {
		t.Fatalf("expected one transition call, got %d", transitionCalls)
	}
	for _, want := range []string{"planning telemetry", "Critical Plan Review missing", `"critical_plan_review": false`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected strict warning output to contain %q, got:\n%s", want, result.Output)
		}
	}
}

func TestProjectPhaseTransitionToolStrictRejectsOpenBlockingPlanReview(t *testing.T) {
	var transitionCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
				CurrentPhase:            "SPEC",
				LeadAgentID:             "agent-alpha",
				DesignDocID:             "project.project-demo.design_and_plan",
				ImplementationPlanDocID: "project.project-demo.design_and_plan",
				Gates: []map[string]any{
					projectPhaseTransitionGate("design_doc_ready", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_plan_ready", "SATISFIED", true),
					projectPhaseTransitionGate("repo_ready_or_not_required", "SATISFIED", true),
					projectPhaseTransitionGate("repo_materialization_allowed", "SATISFIED", true),
					projectPhaseTransitionGate("strategic_lead_active", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_phase_open", "BLOCKED", true),
				},
			}))
		case "workspace.doc.get":
			writeRPCResult(w, req, projectPhaseTransitionDocResult(rpcString(req.Params, "doc_key"), projectPhaseTransitionOpenBlockingReviewDoc()))
		case "project.phase.transition":
			transitionCalls++
			writeRPCResult(w, req, map[string]any{})
		default:
			t.Fatalf("unexpected RPC method=%s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPhaseTransitionTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"to_phase":   "IMPLEMENTATION",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "unresolved BLOCKING") {
		t.Fatalf("expected unresolved blocking review to reject strict transition, got %+v", result)
	}
	if transitionCalls != 0 {
		t.Fatalf("expected no transition call for unresolved blocking review, got %d", transitionCalls)
	}
}

func TestProjectPhaseTransitionToolDoesNotRecheckPlanningAfterImplementationOpen(t *testing.T) {
	var transitionCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
				CurrentPhase:            "IMPLEMENTATION",
				LeadAgentID:             "agent-alpha",
				DesignDocID:             "project.project-demo.design_and_plan",
				ImplementationPlanDocID: "project.project-demo.design_and_plan",
				Gates: []map[string]any{
					projectPhaseTransitionGate("design_doc_ready", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_plan_ready", "SATISFIED", true),
					projectPhaseTransitionGate("repo_ready_or_not_required", "SATISFIED", true),
					projectPhaseTransitionGate("repo_materialization_allowed", "SATISFIED", true),
					projectPhaseTransitionGate("strategic_lead_active", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_phase_open", "SATISFIED", true),
				},
			}))
		case "project.phase.transition":
			transitionCalls++
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-demo",
				"current_phase": "REVIEW",
				"repo_required": true,
				"repo_status":   "READY",
				"created_at":    "2026-05-04T00:00:00Z",
				"updated_at":    "2026-05-04T00:01:00Z",
			}})
		case "workspace.doc.get":
			t.Fatalf("planning docs should not be rechecked after implementation is already open")
		default:
			t.Fatalf("unexpected RPC method=%s", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPhaseTransitionTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"to_phase":   "REVIEW",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected review phase transition without planning recheck, got %+v", result)
	}
	if transitionCalls != 1 {
		t.Fatalf("expected one transition call, got %d", transitionCalls)
	}
	if strings.Contains(result.Output, `"product_planning_review":`) || strings.Contains(result.Output, "implementation_fanout_required") {
		t.Fatalf("expected no implementation-opening guidance after implementation is already open, got:\n%s", result.Output)
	}
}

func TestProjectPhaseTransitionToolTrustFirstDoesNotSeedAdvisoryRoles(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, projectPhaseTransitionCoordinationResult(projectPhaseTransitionCoordinationInput{
				CurrentPhase:            "SPEC",
				LeadAgentID:             "agent-alpha",
				DesignDocID:             "project.project-demo.design_and_plan",
				ImplementationPlanDocID: "project.project-demo.design_and_plan",
				Gates: []map[string]any{
					projectPhaseTransitionGate("design_doc_ready", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_plan_ready", "SATISFIED", true),
					projectPhaseTransitionGate("repo_ready_or_not_required", "SATISFIED", true),
					projectPhaseTransitionGate("repo_materialization_allowed", "SATISFIED", true),
					projectPhaseTransitionGate("strategic_lead_active", "SATISFIED", true),
					projectPhaseTransitionGate("implementation_phase_open", "BLOCKED", true),
				},
			}))
		case "workspace.doc.get":
			writeRPCResult(w, req, projectPhaseTransitionDocResult(rpcString(req.Params, "doc_key"), projectPhaseTransitionValidPlanningDoc()))
		case "project.phase.transition":
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":  "ws",
				"project_id":    "project-demo",
				"current_phase": "IMPLEMENTATION",
				"repo_required": true,
				"repo_status":   "READY",
				"created_at":    "2026-05-04T00:00:00Z",
				"updated_at":    "2026-05-04T00:01:00Z",
			}})
		case "workspace.agents.list":
			t.Fatalf("trust-first phase transition must not inspect workspace agents to seed roles")
		case "project.role.assign":
			t.Fatalf("trust-first phase transition must not assign project roles: %+v", req.Params)
		default:
			t.Fatalf("unexpected RPC method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPhaseTransitionTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha").WithCoordinationMode(CoordinationModeTrustFirst)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"to_phase":   "IMPLEMENTATION",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful phase transition, got %+v", result)
	}
	if calls < 3 {
		t.Fatalf("expected coordination, doc review, and phase transition calls, got %d", calls)
	}
	if strings.Contains(result.Output, "advisory_role_seed") || strings.Contains(result.Output, `"paths":["**"]`) {
		t.Fatalf("expected no role seed evidence or broad write scope in output:\n%s", result.Output)
	}
	if !strings.Contains(result.Output, `"role_seed_policy": "disabled_in_trust_first"`) || !strings.Contains(result.Output, "autonomous self-selection") {
		t.Fatalf("expected trust-first self-selection guidance in output:\n%s", result.Output)
	}
}

func TestProjectPlanningReviewDetectsOpenBlockingFindingInMixedReviewDoc(t *testing.T) {
	text := strings.ToLower("```rhizome_plan_review_v1\n" +
		"reviewer: alpha\n" +
		"severity: BLOCKING\n" +
		"finding: First issue was product-fidelity drift.\n" +
		"status: resolved\n" +
		"resolved by: plan now names the upload/convert/export flow.\n\n" +
		"```rhizome_plan_review_v1\n" +
		"reviewer: beta\n" +
		"severity: BLOCKING\n" +
		"finding: The implementation plan still includes a manual grid painting editor as the first screen.\n" +
		"status: open\n" +
		"```")

	if !projectPlanningTextHasOpenBlockingReview(text) {
		t.Fatalf("expected mixed review doc to preserve the open blocking finding")
	}
}

func TestProjectPlanningReviewRecognizesMachineReadableMarkers(t *testing.T) {
	doc := strings.ToLower("```rhizome_product_contract_v1\n" +
		"core_user_promise: Upload a source image, convert it into sub-pixel art, preview it, and export PNG.\n" +
		"required_inputs: source image/photo file\n" +
		"required_outputs: preview and exported PNG\n" +
		"primary_user_flow:\n" +
		"  - AC-01: choose image\n" +
		"  - AC-02: tune conversion sliders\n" +
		"non_goals:\n" +
		"  - Manual grid/cell painting editor is an adjacent wrong product.\n" +
		"mvp_acceptance:\n" +
		"  - AC-01 upload/convert/preview/export works\n" +
		"v2_aspiration: excellent UX and fast previews.\n" +
		"```\n\n" +
		"```rhizome_plan_review_v1\n" +
		"reviewer: gamma\n" +
		"severity: NONE\n" +
		"finding: no blocking product-fidelity concerns remain.\n" +
		"status: resolved\n" +
		"```")

	if !projectPlanningTextHasProductContract(doc) {
		t.Fatalf("expected product contract marker block to satisfy semantic product contract detection")
	}
	if !projectPlanningTextHasCriticalReview(doc) {
		t.Fatalf("expected plan review marker block to satisfy critical review detection")
	}
	if projectPlanningTextHasOpenBlockingReview(doc) {
		t.Fatalf("expected resolved NONE review marker not to count as open blocking")
	}
}

func TestProjectPlanningProductContractAcceptsCombinedInputsOutputs(t *testing.T) {
	doc := strings.ToLower("# Product Contract\n\n" +
		"core_user_promise: A player opens the app and immediately gets a complete Minesweeper game.\n\n" +
		"happy_path:\n" +
		"- open app\n" +
		"- choose Beginner, Intermediate, or Expert\n" +
		"- reveal and flag cells until win or loss\n\n" +
		"required_inputs_outputs:\n" +
		"- Inputs: reveal cell, flag cell, reset, switch difficulty.\n" +
		"- Outputs: board state, adjacency numbers, flags, timer, mine counter, win/loss state.\n\n" +
		"non_goals:\n" +
		"- Not a static mockup or landing page.\n\n" +
		"MVP_acceptance:\n" +
		"- Standard Minesweeper gameplay and browser visual evidence for the committed candidate.\n\n" +
		"v2_aspiration: keyboard support and extra polish.")

	if !projectPlanningTextHasProductContract(doc) {
		t.Fatalf("expected combined required_inputs_outputs contract to satisfy semantic product contract detection")
	}
}

func TestProjectPlanningProductContractMarkerAcceptsCombinedInputsOutputs(t *testing.T) {
	doc := strings.ToLower("```rhizome_product_contract_v1\n" +
		"core_user_promise: A player opens the app and immediately gets a complete Minesweeper game.\n" +
		"happy_path: open app -> choose difficulty -> reveal and flag cells -> finish win or loss.\n" +
		"required_inputs_outputs:\n" +
		"  - Inputs: reveal cell, flag cell, reset, switch difficulty.\n" +
		"  - Outputs: board state, adjacency numbers, flags, timer, mine counter, win/loss state.\n" +
		"non_goals: not a static mockup or landing page.\n" +
		"mvp_acceptance: standard Minesweeper gameplay and browser visual evidence for the committed candidate.\n" +
		"v2_aspiration: keyboard support and extra polish.\n" +
		"```")

	if !projectPlanningTextHasProductContract(doc) {
		t.Fatalf("expected marker contract with combined required_inputs_outputs to satisfy detection")
	}
}

func TestProjectPlanningReviewRejectsEmptyProductContractTemplate(t *testing.T) {
	doc := strings.ToLower("```rhizome_product_contract_v1\n" +
		"core_user_promise:\n" +
		"required_inputs:\n" +
		"required_outputs:\n" +
		"primary_user_flow:\n" +
		"non_goals:\n" +
		"mvp_acceptance:\n" +
		"v2_aspiration:\n" +
		"```")

	if projectPlanningTextHasProductContract(doc) {
		t.Fatalf("expected empty marker template not to satisfy product contract detection")
	}
}

func TestProjectPlanningReviewDoesNotTreatMinorOpenAsBlocking(t *testing.T) {
	doc := strings.ToLower("```rhizome_plan_review_v1\n" +
		"reviewer: alpha\n" +
		"severity: MINOR\n" +
		"finding: Button labels could be sharper.\n" +
		"status: open\n" +
		"```\n\n" +
		"```rhizome_plan_review_v1\n" +
		"reviewer: beta\n" +
		"severity: BLOCKING\n" +
		"finding: Earlier product-fidelity drift.\n" +
		"status: accepted_risk\n" +
		"```")

	if projectPlanningTextHasOpenBlockingReview(doc) {
		t.Fatalf("expected minor open plus accepted-risk blocking finding not to count as open blocking")
	}
}

type projectPhaseTransitionCoordinationInput struct {
	CurrentPhase            string
	LeadAgentID             string
	DesignDocID             string
	ImplementationPlanDocID string
	Gates                   []map[string]any
}

func projectPhaseTransitionCoordinationResult(input projectPhaseTransitionCoordinationInput) map[string]any {
	return map[string]any{"coordination": map[string]any{
		"project": map[string]any{
			"workspace_id": "ws",
			"project_id":   "project-demo",
			"title":        "Demo",
			"status":       "ACTIVE",
		},
		"profile": map[string]any{
			"workspace_id":               "ws",
			"project_id":                 "project-demo",
			"current_phase":              input.CurrentPhase,
			"design_doc_id":              input.DesignDocID,
			"implementation_plan_doc_id": input.ImplementationPlanDocID,
			"repo_required":              true,
			"repo_status":                "READY",
			"created_at":                 "2026-05-04T00:00:00Z",
			"updated_at":                 "2026-05-04T00:00:00Z",
		},
		"gate_status": map[string]any{
			"workspace_id":  "ws",
			"project_id":    "project-demo",
			"current_phase": input.CurrentPhase,
			"overall_state": "BLOCKED",
			"gates":         input.Gates,
		},
		"strategic_lead": map[string]any{
			"role_id":          "role-lead",
			"workspace_id":     "ws",
			"project_id":       "project-demo",
			"agent_id":         input.LeadAgentID,
			"role_type":        "STRATEGIC_LEAD",
			"status":           "ACTIVE",
			"created_at":       "2026-05-04T00:00:00Z",
			"updated_at":       "2026-05-04T00:00:00Z",
			"claimed_at":       "2026-05-04T00:00:00Z",
			"write_scope_json": "{}",
		},
	}}
}

func projectPhaseTransitionDocResult(docKey, content string) map[string]any {
	return map[string]any{
		"doc_key":    docKey,
		"title":      "Planning Doc",
		"content":    content,
		"updated_by": "agent-alpha",
		"updated_at": "2026-05-04T00:00:00Z",
		"sha":        "sha-" + sanitizeDocKeySegment(docKey),
	}
}

func projectPhaseTransitionValidPlanningDoc() string {
	return `# Product Contract

- core_user_promise: Upload a source image, convert it into packed RGB sub-pixel art, preview it, and export PNG.
- happy_path: select image -> tune controls -> convert -> inspect preview -> export lossless PNG.
- non_goals / adjacent_wrong_products: not a manual grid painter, not a CLI-only converter, not a static mock.
- MVP acceptance: upload, convert, preview, export, and bounded verification evidence.
- v2_aspiration: simpler UX, sliders for useful controls, better preview and export ergonomics.

# Critical Plan Review

- stance: skeptical product-fidelity reviewer.
- severity: blocking, major, minor, taste.
- finding[blocking]: ensure the implementation cannot become a grid painting tool.
- disposition: resolved by mapping every lane to the upload/convert/preview/export happy path; no blocking findings remain.`
}

func projectPhaseTransitionProductContractOnlyDoc() string {
	return `# Product Contract

- core_user_promise: Upload a source image, convert it into packed RGB sub-pixel art, preview it, and export PNG.
- happy_path: select image -> tune controls -> convert -> inspect preview -> export lossless PNG.
- non_goals / adjacent_wrong_products: not a manual grid painter, not a CLI-only converter, not a static mock.
- MVP acceptance: upload, convert, preview, export, and bounded verification evidence.
- v2_aspiration: simpler UX, sliders for useful controls, better preview and export ergonomics.`
}

func projectPhaseTransitionOpenBlockingReviewDoc() string {
	return projectPhaseTransitionProductContractOnlyDoc() + `

# Critical Plan Review

- reviewer: agent-epsilon
- severity: blocking
- finding: The plan still allows a manual grid/cell painting editor instead of upload/convert/export.
- required_change: Rewrite the implementation lanes around the upload/convert/preview/export flow.
- status: open`
}

func projectPhaseTransitionGate(key, state string, required bool) map[string]any {
	return map[string]any{
		"gate_key":   key,
		"state":      state,
		"required":   required,
		"summary":    key + " summary",
		"updated_at": "2026-05-04T00:00:00Z",
		"source":     "derived",
	}
}
