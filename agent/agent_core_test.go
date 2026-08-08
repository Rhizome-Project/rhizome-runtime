package main

import (
	"context"
	"encoding/json"
	"testing"
)

// registryHasTool reports whether the agent's active (post-diet) registry holds
// a tool by name.
func registryHasTool(a *Agent, name string) bool {
	if a == nil || a.registry == nil {
		return false
	}
	_, ok := a.registry.Get(name)
	return ok
}

// definitionsJSONBytes marshals the registry's backend tool definitions exactly
// as they are sent to the LLM and returns the byte size.
func definitionsJSONBytes(t *testing.T, a *Agent) int {
	t.Helper()
	raw, err := json.Marshal(a.registry.Definitions())
	if err != nil {
		t.Fatalf("marshal tool definitions: %v", err)
	}
	return len(raw)
}

// newRoleDietAgent builds an agent with a (non-networked) Rhizome client and
// identity attached so the full Rhizome coordination toolset registers,
// exercising all four role-banned tools. Registration is lazy and does not make
// network calls.
func newRoleDietAgent(t *testing.T, preset string) *Agent {
	t.Helper()
	a := &Agent{
		LLM:         &sequenceLLM{},
		Workdir:     t.TempDir(),
		Client:      NewRhizomeClient("http://127.0.0.1:1", ""),
		WorkspaceID: "ws-diet",
		AgentID:     "agent-diet",
	}
	a.Anatomy.Preset = preset
	a.Init()
	return a
}

// TestRoleToolDietTrimsReviewerWriteTools measures tools_json bytes before
// (generalist, full toolset) and after (reviewer_qa, trimmed toolset) the role
// diet, asserts a nonzero reduction, and asserts required reviewer tools are
// still present while banned write/integrate tools are absent.
func TestRoleToolDietTrimsReviewerWriteTools(t *testing.T) {
	// Baseline: generalist role keeps the full toolset.
	base := newRoleDietAgent(t, "generalist")
	baseBytes := definitionsJSONBytes(t, base)
	baseCount := len(base.registry.Definitions())

	// Reviewer role: write/integrate/apply tools are trimmed at the boundary.
	reviewer := newRoleDietAgent(t, "reviewer_qa")
	reviewerBytes := definitionsJSONBytes(t, reviewer)
	reviewerCount := len(reviewer.registry.Definitions())

	t.Logf("tools_json bytes: generalist=%d (%d tools), reviewer_qa=%d (%d tools), saved=%d bytes / %d tools",
		baseBytes, baseCount, reviewerBytes, reviewerCount, baseBytes-reviewerBytes, baseCount-reviewerCount)

	if reviewerBytes >= baseBytes {
		t.Fatalf("expected reviewer_qa tools_json (%d) to be smaller than generalist (%d)", reviewerBytes, baseBytes)
	}
	if reviewerCount >= baseCount {
		t.Fatalf("expected reviewer_qa tool count (%d) to be smaller than generalist (%d)", reviewerCount, baseCount)
	}

	// Banned tools must be absent from the reviewer registry (so absent from
	// Definitions backend JSON, capability snapshot, prompt projection, and
	// execution).
	for _, banned := range []string{"write_file", "project_branch_commit", "project_patch_queue_integrate", "project_repo_materialize"} {
		if registryHasTool(reviewer, banned) {
			t.Fatalf("reviewer_qa registry should not contain banned tool %q", banned)
		}
	}

	// Required reviewer/inspection tools must remain present.
	for _, required := range []string{"read_file", "list_directory", "project_checkout_materialize", "project_branch_review_ready"} {
		if !registryHasTool(reviewer, required) {
			t.Fatalf("reviewer_qa registry should retain required tool %q", required)
		}
	}

	// Definitions() backend JSON must not name a banned tool anywhere.
	rawJSON, err := json.Marshal(reviewer.registry.Definitions())
	if err != nil {
		t.Fatalf("marshal reviewer definitions: %v", err)
	}
	for _, def := range reviewer.registry.Definitions() {
		if _, banned := roleBannedToolNames("reviewer_qa")[def.Function.Name]; banned {
			t.Fatalf("banned tool %q leaked into reviewer Definitions()", def.Function.Name)
		}
	}
	if len(rawJSON) != reviewerBytes {
		t.Fatalf("definition byte size drifted: %d vs %d", len(rawJSON), reviewerBytes)
	}
}

func TestRuntimeToolDenylistTrimsHarnessTools(t *testing.T) {
	agent := newRoleDietAgent(t, "generalist")
	agent.DisabledToolNames = []string{"agent_request", "project_checkout_materialize"}
	agent.SetDynamicTools(nil)

	for _, banned := range []string{"agent_request", "project_checkout_materialize"} {
		if registryHasTool(agent, banned) {
			t.Fatalf("runtime denylist should remove %q from registry", banned)
		}
	}
	for _, kept := range []string{"read_file", "list_directory", "project_branch_commit"} {
		if !registryHasTool(agent, kept) {
			t.Fatalf("runtime denylist should retain unrelated tool %q", kept)
		}
	}
}

// TestRoleToolDietRejectsBannedExecution proves a filtered tool cannot be
// executed because the role registry never holds it.
func TestRoleToolDietRejectsBannedExecution(t *testing.T) {
	reviewer := newRoleDietAgent(t, "reviewer_qa")

	if _, ok := reviewer.registry.Get("write_file"); ok {
		t.Fatal("reviewer registry must not resolve banned tool write_file for execution")
	}
}

// TestRoleToolDietGeneralistUnchanged confirms the diet is additive: a
// non-reviewer role keeps write/integrate tools.
func TestRoleToolDietGeneralistUnchanged(t *testing.T) {
	gen := newRoleDietAgent(t, "generalist")

	for _, kept := range []string{"write_file", "project_branch_commit", "project_patch_queue_integrate", "project_repo_materialize"} {
		if !registryHasTool(gen, kept) {
			t.Fatalf("generalist registry should retain %q", kept)
		}
	}
}

// TestRoleToolDietReviewerLoopCompletes mirrors the chat-session fake-runtime
// pattern: it exercises a reviewer-role agent through a full ExecuteTask tool
// loop and proves (a) the loop completes and (b) the tool JSON the fake backend
// received excludes the banned tools.
func TestRoleToolDietReviewerLoopCompletes(t *testing.T) {
	llm := &sequenceLLM{
		responses: []*LLMResponse{{Content: "reviewed: no blockers found"}},
	}

	reviewer := &Agent{
		LLM:         llm,
		Workdir:     t.TempDir(),
		Client:      NewRhizomeClient("http://127.0.0.1:1", ""),
		WorkspaceID: "ws-diet",
		AgentID:     "agent-diet",
	}
	reviewer.Anatomy.Preset = "reviewer_qa"
	reviewer.Init()

	out, err := reviewer.ExecuteTask(context.Background(), "Review the candidate artifact and report findings.")
	if err != nil {
		t.Fatalf("ExecuteTask() error = %v", err)
	}
	if out != "reviewed: no blockers found" {
		t.Fatalf("ExecuteTask() = %q", out)
	}

	if len(llm.tools) == 0 {
		t.Fatal("fake LLM observed no tool definitions")
	}
	sentTools := llm.tools[0]
	if len(sentTools) == 0 {
		t.Fatal("reviewer loop sent an empty tool set to the backend")
	}
	banned := roleBannedToolNames("reviewer_qa")
	sawRequired := false
	for _, def := range sentTools {
		if _, isBanned := banned[def.Function.Name]; isBanned {
			t.Fatalf("banned tool %q was sent to the backend during the reviewer loop", def.Function.Name)
		}
		if def.Function.Name == "read_file" {
			sawRequired = true
		}
	}
	if !sawRequired {
		t.Fatal("expected read_file in the reviewer backend tool set")
	}
}
