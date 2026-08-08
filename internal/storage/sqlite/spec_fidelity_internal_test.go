package sqlite

import (
	"strings"
	"testing"
)

func TestSourceRequirementsTraceAcceptsIDPrefixedAnchors(t *testing.T) {
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
	if !sourceRequirementsTraceBlockComplete(block, []string{"run.operator-spec"}) {
		t.Fatalf("expected ID-prefixed source anchors to satisfy source requirements trace")
	}
}

func TestSourceRequirementsTraceAcceptsSignal01MarkdownHeadings(t *testing.T) {
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
	if !sourceRequirementsTraceComplete(doc, []string{"operator.signal01.rq.spec.v1"}) {
		t.Fatalf("expected Signal-01 markdown-heading trace to satisfy source requirements trace")
	}
}

func TestSourceRequirementsTraceAcceptsRun26IndentedMappingAnchors(t *testing.T) {
	doc := strings.Join([]string{
		"schema: rhizome_source_requirements_trace_v1",
		"project_id: project-task-signal01-rq-root-run26",
		"source_doc_keys:",
		"  - operator.signal01-run26.spec",
		"acceptance_critical_anchors:",
		"  core_user_promise: rq is a runnable Go CLI for expression-based transformation of JSON-like input, not a stock hello-world scaffold.",
		"  happy_path: Evaluate an expression from the CLI against stdin JSON and print deterministic output.",
		"  lane_scope_note: This scaffold lane covers shared repo/module/package boundaries only.",
		"acceptance_criteria_refs:",
		"  - AC-LEX-01",
		"  - AC-PARSE-01",
		"  - AC-EVAL-01",
		"adjacent_wrong_products:",
		"  - stock scaffold with no rq-specific structure",
		"  - broad overlapping implementation that preempts sibling lexer/parser/evaluator lanes",
		"non_goals:",
		"  - production jq clone",
	}, "\n")
	if !sourceRequirementsTraceComplete(doc, []string{"operator.signal01-run26.spec"}) {
		t.Fatalf("expected Run26 indented mapping trace to satisfy source requirements trace")
	}
}
