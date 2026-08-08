package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultAgentAnatomyGeneralistPreservesLegacyProfileShape(t *testing.T) {
	profile := DefaultAgentProfile("iota", "Iota", "generalist implementer")
	anatomy := DefaultAgentAnatomyConfig(profile)
	if err := anatomy.Validate(); err != nil {
		t.Fatalf("default anatomy should validate: %v", err)
	}
	if anatomy.Schema != agentAnatomySchemaV1 {
		t.Fatalf("schema = %q", anatomy.Schema)
	}
	if anatomy.Preset != "generalist" {
		t.Fatalf("preset = %q", anatomy.Preset)
	}
	if _, ok := findTestHeartbeat(anatomy, "active_task_execution"); !ok {
		t.Fatalf("expected active_task_execution heartbeat")
	}
	if _, ok := findTestHeartbeat(anatomy, "loop_self_check"); !ok {
		t.Fatalf("expected loop_self_check heartbeat")
	}
	selfCheck, ok := findTestHeartbeat(anatomy, "loop_self_check")
	if !ok || selfCheck.MaxToolIterations <= 0 || selfCheck.ActiveMemory == nil || selfCheck.WillPolicy == nil || !containsAnatomyTestString(selfCheck.WillPolicy.AllowedActions, "replan_active_work") {
		t.Fatalf("loop_self_check should be a tool-capable local metacognition loop with active memory and will policy, got %+v", selfCheck)
	}
	arbiter, ok := findTestHeartbeat(anatomy, "personal_backlog_arbiter")
	if !ok {
		t.Fatalf("expected personal_backlog_arbiter heartbeat")
	}
	if arbiter.Kind != "backlog_arbiter" || !containsAnatomyTestString(arbiter.ContextSelectors, "local_memory") || containsAnatomyTestString(arbiter.ToolSuites, "bounded_task_submit") {
		t.Fatalf("personal backlog arbiter should be local-only generic triage, got %+v", arbiter)
	}
	promoter, ok := findTestHeartbeat(anatomy, "action_request_promoter")
	if !ok {
		t.Fatalf("expected action_request_promoter heartbeat")
	}
	if promoter.Kind != "action_request_promoter" || !containsAnatomyTestString(promoter.ToolSuites, "bounded_task_submit") || containsAnatomyTestString(promoter.Locks, "local_only") {
		t.Fatalf("action request promoter should be the bounded public bridge from private routes, got %+v", promoter)
	}
	if _, ok := findTestHeartbeat(anatomy, "visual_product_audit"); ok {
		t.Fatalf("generalist should not get UI browser critic by default")
	}
	if _, ok := findTestHeartbeat(anatomy, "global_progress_review"); ok {
		t.Fatalf("generalist implementer should not get global progress review by default")
	}
}

func TestDefaultAgentAnatomyAddsGlobalProgressReviewOnlyForGovernanceRoles(t *testing.T) {
	for _, role := range []string{
		"strategic planner and coordinator",
		"adversarial reviewer and QA critic",
		"UI/UX evil reality critic",
	} {
		anatomy := DefaultAgentAnatomyConfig(DefaultAgentProfile("rho", "Rho", role))
		heartbeat, ok := findTestHeartbeat(anatomy, "global_progress_review")
		if !ok {
			t.Fatalf("role %q missing global_progress_review heartbeat", role)
		}
		if heartbeat.Kind != heartbeatKindGlobalProgressReview ||
			!containsAnatomyTestString(heartbeat.ToolSuites, "project_governance_review") ||
			!strings.Contains(strings.Join(heartbeat.Instructions, "\n"), "action=check reports all strict predicates true") {
			t.Fatalf("global progress review heartbeat lacks governed challenge contract for role %q: %+v", role, heartbeat)
		}
	}

	implementer := DefaultAgentAnatomyConfig(DefaultAgentProfile("beta", "Beta", "frontend implementer"))
	if _, ok := findTestHeartbeat(implementer, "global_progress_review"); ok {
		t.Fatalf("implementation profile should not receive global_progress_review by default")
	}
}

func TestDefaultAgentAnatomyUXCriticAddsGenericEvidenceContract(t *testing.T) {
	profile := DefaultAgentProfile("sigma", "Sigma", "UI/UX evil reality critic")
	anatomy := DefaultAgentAnatomyConfig(profile)
	if err := anatomy.Validate(); err != nil {
		t.Fatalf("UX anatomy should validate: %v", err)
	}
	if anatomy.Preset != "ui_ux_reality_critic" {
		t.Fatalf("preset = %q", anatomy.Preset)
	}
	if anatomy.Concurrency.MaxBrowserSessions != 1 {
		t.Fatalf("expected one browser session, got %d", anatomy.Concurrency.MaxBrowserSessions)
	}
	heartbeat, ok := findTestHeartbeat(anatomy, "visual_product_audit")
	if !ok {
		t.Fatalf("expected visual_product_audit heartbeat")
	}
	if heartbeat.Kind != "browser_critic" {
		t.Fatalf("visual heartbeat kind = %q", heartbeat.Kind)
	}
	if !containsAnatomyTestString(heartbeat.ToolSuites, "browser_unrestricted") || !containsAnatomyTestString(heartbeat.ToolSuites, "browser_read_only") || !containsAnatomyTestString(heartbeat.ToolSuites, "bounded_task_submit") || !containsAnatomyTestString(heartbeat.PromotionSignals, "observed_user_harm") {
		t.Fatalf("visual heartbeat missing browser tools or user-harm promotion: %+v", heartbeat)
	}
	globalReflection, ok := findTestHeartbeat(anatomy, "design_global_reflection")
	if !ok {
		t.Fatalf("expected design_global_reflection heartbeat")
	}
	if globalReflection.Kind != "global_metacognition" || globalReflection.ActiveMemory == nil || globalReflection.ActiveMemory.Lane != "design_sensemaking" || globalReflection.WillPolicy == nil || !containsAnatomyTestString(globalReflection.WillPolicy.AllowedActions, "publish_rhizome_update") {
		t.Fatalf("global UI reflection should carry active design memory and will policy, got %+v", globalReflection)
	}
	if heartbeat.MaxToolIterations != 4 {
		t.Fatalf("visual heartbeat should allow a tiny local installed-tool loop for visual probes: %+v", heartbeat)
	}
	if heartbeat.ActiveMemory == nil || heartbeat.ActiveMemory.Lane != "visual_findings" || heartbeat.WillPolicy == nil || !containsAnatomyTestString(heartbeat.WillPolicy.AllowedActions, "runtime_switch_task") {
		t.Fatalf("visual heartbeat should be able to remember visual findings and steer planner when evidence demands it, got %+v", heartbeat)
	}
	if !strings.Contains(strings.Join(heartbeat.Instructions, " "), "browser_visual_probe") {
		t.Fatalf("visual heartbeat should name the concrete local probe bundle it may use, got %+v", heartbeat.Instructions)
	}
	if !strings.Contains(strings.Join(heartbeat.Instructions, " "), "provisional non-canonical candidate") {
		t.Fatalf("visual heartbeat should allow provisional non-canonical critique when exact evidence is available, got %+v", heartbeat.Instructions)
	}
	if heartbeat.EvidenceContract == nil {
		t.Fatalf("visual heartbeat should carry an explicit generic evidence_contract")
	}
	if len(heartbeat.EvidenceContract.Dimensions) < 2 || len(heartbeat.EvidenceContract.States) < 3 {
		t.Fatalf("visual heartbeat should configure evidence dimension/state matrix, got %+v", heartbeat.EvidenceContract)
	}
	if !containsAnatomyTestString(heartbeat.EvidenceContract.Checks, "performance_symptoms") || !containsAnatomyTestString(heartbeat.EvidenceContract.Checks, "primary_surface_geometry") || !containsAnatomyTestString(heartbeat.EvidenceContract.ArtifactRequirements, "visual_verdict: pass only when no blocking findings remain") || !containsAnatomyTestString(heartbeat.EvidenceContract.ArtifactRequirements, "provisional non-canonical findings must be labeled as critique evidence and cannot satisfy acceptance") || !containsAnatomyTestString(heartbeat.EvidenceContract.ArtifactRequirements, "visible modes/presets/difficulties that change the primary surface are checked or explicitly marked not applicable") {
		t.Fatalf("visual heartbeat should configure harsh real-user checks and artifact gates, got %+v", heartbeat.EvidenceContract)
	}
	if len(heartbeat.EvidenceContract.RequiredToolArtifacts) != 1 || heartbeat.EvidenceContract.RequiredToolArtifacts[0].Tool != "browser_visual_probe" || heartbeat.EvidenceContract.RequiredToolArtifacts[0].When != "runnable_surface_present" {
		t.Fatalf("visual heartbeat should require a concrete browser visual probe artifact, got %+v", heartbeat.EvidenceContract.RequiredToolArtifacts)
	}
}

func TestDefaultAgentAnatomyServiceFactoryAddsScoutAndReadinessLoops(t *testing.T) {
	profile := DefaultAgentProfile("rho", "Rho", "portfolio service scout deploy monetization operator")
	anatomy := DefaultAgentAnatomyConfig(profile)
	if err := anatomy.Validate(); err != nil {
		t.Fatalf("service factory anatomy should validate: %v", err)
	}
	if anatomy.Preset != "service_factory_operator" {
		t.Fatalf("preset = %q", anatomy.Preset)
	}
	for _, lane := range []string{"opportunity_map", "project_sensemaking", "role_backlog"} {
		if !containsAnatomyTestString(anatomy.Memory.Lanes, lane) {
			t.Fatalf("service factory anatomy missing memory lane %q: %+v", lane, anatomy.Memory.Lanes)
		}
	}
	projectInitiative, ok := findTestHeartbeat(anatomy, "project_role_initiative")
	if !ok {
		t.Fatalf("expected project_role_initiative heartbeat")
	}
	if !containsAnatomyTestString(projectInitiative.ContextSelectors, "service_pipeline") {
		t.Fatalf("project initiative heartbeat should see service pipeline context: %+v", projectInitiative)
	}
	scout, ok := findTestHeartbeat(anatomy, "portfolio_scout")
	if !ok {
		t.Fatalf("expected portfolio_scout heartbeat")
	}
	if scout.Kind != "global_metacognition" || !containsAnatomyTestString(scout.ContextSelectors, "service_pipeline") || !containsAnatomyTestString(scout.PromotionSignals, "service_candidate_with_evidence") {
		t.Fatalf("portfolio scout heartbeat lacks service opportunity contract: %+v", scout)
	}
	if !strings.Contains(strings.Join(scout.Instructions, "\n"), "validation plan") {
		t.Fatalf("portfolio scout should require validation planning before promotion: %+v", scout.Instructions)
	}
	readiness, ok := findTestHeartbeat(anatomy, "deploy_monetization_vigilance")
	if !ok {
		t.Fatalf("expected deploy_monetization_vigilance heartbeat")
	}
	if !containsAnatomyTestString(readiness.PromotionSignals, "deploy_smoke_gap") || !containsAnatomyTestString(readiness.PromotionSignals, "policy_review_gap") {
		t.Fatalf("readiness heartbeat should track deploy and policy gaps: %+v", readiness)
	}
	if !strings.Contains(strings.Join(readiness.Instructions, "\n"), "operator-gated blockers") {
		t.Fatalf("readiness heartbeat should preserve operator gates for paid/external actions: %+v", readiness.Instructions)
	}
}

func TestNormalizeAgentAnatomyPresetServiceFactoryAliases(t *testing.T) {
	for _, alias := range []string{"service_scout", "portfolio-steward", "growth scout", "revenue_operator", "deploy_operator"} {
		if got := normalizeAgentAnatomyPreset(alias); got != "service_factory_operator" {
			t.Fatalf("normalizeAgentAnatomyPreset(%q) = %q", alias, got)
		}
	}
}

func TestDefaultAgentAnatomyServiceFactoryInferencePrecedesReviewer(t *testing.T) {
	for _, role := range []string{
		"market scout",
		"service scout",
		"deploy operator",
		"monetization operator",
		"ad monetization compliance reviewer",
	} {
		anatomy := DefaultAgentAnatomyConfig(DefaultAgentProfile("rho", "Rho", role))
		if anatomy.Preset != "service_factory_operator" {
			t.Fatalf("role %q resolved preset %q, want service_factory_operator", role, anatomy.Preset)
		}
		if _, ok := findTestHeartbeat(anatomy, "portfolio_scout"); !ok {
			t.Fatalf("role %q missing portfolio_scout heartbeat", role)
		}
	}
}

func TestDefaultAgentAnatomyIntegratorAddsPatchQueueVigilance(t *testing.T) {
	profile := DefaultAgentProfile("zeta", "Zeta", "patch queue integrator and release captain")
	anatomy := DefaultAgentAnatomyConfig(profile)
	if err := anatomy.Validate(); err != nil {
		t.Fatalf("integrator anatomy should validate: %v", err)
	}
	if anatomy.Preset != "integrator" {
		t.Fatalf("preset = %q", anatomy.Preset)
	}
	heartbeat, ok := findTestHeartbeat(anatomy, "patch_queue_vigilance")
	if !ok {
		t.Fatalf("expected patch_queue_vigilance heartbeat")
	}
	if heartbeat.Kind != "integration_vigilance" || !containsAnatomyTestString(heartbeat.ContextSelectors, "patch_queue") || !containsAnatomyTestString(heartbeat.ToolSuites, "bounded_task_submit") {
		t.Fatalf("patch queue heartbeat lacks integration vigilance authority: %+v", heartbeat)
	}
	if !containsAnatomyTestString(heartbeat.PromotionSignals, "accepted_queue_stale") || !containsAnatomyTestString(heartbeat.PromotionSignals, "verification_gap") {
		t.Fatalf("patch queue heartbeat should carry queue health promotion signals: %+v", heartbeat.PromotionSignals)
	}
}

func TestDefaultAgentAnatomyRoleSignalsBeatBroadMissionText(t *testing.T) {
	integrator := DefaultAgentProfile("zeta", "Zeta", "patch queue integrator and release captain")
	integrator.Mission = "Protect canonical main; require visually accepted evidence before final release."
	if got := DefaultAgentAnatomyConfig(integrator).Preset; got != "integrator" {
		t.Fatalf("visual mission text should not beat explicit integrator role, got %q", got)
	}

	reviewer := DefaultAgentProfile("epsilon", "Epsilon", "adversarial reviewer and QA critic")
	reviewer.Mission = "Review product evidence and mention UI/UX risks when relevant."
	if got := DefaultAgentAnatomyConfig(reviewer).Preset; got != "reviewer_qa" {
		t.Fatalf("generic reviewer with UI-adjacent mission should remain reviewer_qa, got %q", got)
	}

	visualVerifier := DefaultAgentProfile("kappa", "Kappa", "visual acceptance verifier")
	if got := DefaultAgentAnatomyConfig(visualVerifier).Preset; got != "ui_ux_reality_critic" {
		t.Fatalf("explicit visual verifier should remain UI reality critic, got %q", got)
	}

	browserSmoke := DefaultAgentProfile("theta", "Theta", "browser smoke, accessibility, and performance tester")
	if got := DefaultAgentAnatomyConfig(browserSmoke).Preset; got != "ui_ux_reality_critic" {
		t.Fatalf("browser/accessibility tester should get UI reality critic anatomy, got %q", got)
	}

	hyphenatedBrowserSmoke := DefaultAgentProfile("theta", "Theta", "browser-smoke accessibility performance tester")
	if got := DefaultAgentAnatomyConfig(hyphenatedBrowserSmoke).Preset; got != "ui_ux_reality_critic" {
		t.Fatalf("hyphenated browser-smoke tester should get UI reality critic anatomy, got %q", got)
	}

	frontendImplementer := DefaultAgentProfile("beta", "Beta", "frontend implementer; execution-focused React/TypeScript app shell, state management, and responsive layout with local sanity reflection")
	if got := DefaultAgentAnatomyConfig(frontendImplementer).Preset; got != "generalist" {
		t.Fatalf("frontend implementer with responsive layout should keep execution/generalist anatomy, got %q", got)
	}
}

func TestReadAgentAnatomyMergesExplicitWorkdirConfig(t *testing.T) {
	dir := t.TempDir()
	raw := `{
  "schema": "rhizome.agent_anatomy.v1",
  "preset": "custom_critic",
  "concurrency": {"max_parallel_internal_sessions": 4, "max_llm_sessions": 2, "max_browser_sessions": 1},
  "heartbeats": [
    {"id": "active_task_execution", "priority": 77, "tool_suites": ["task_authority"], "locks": ["exclusive_task_mutation"]},
    {"id": "custom_probe", "kind": "metacognition", "cadence": "every_5m", "priority": 60, "locks": ["custom:probe"], "tool_suites": ["custom:probe"]}
  ]
}`
	if err := os.WriteFile(filepath.Join(dir, agentAnatomyFilename), []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	anatomy, err := ReadAgentAnatomyConfig(dir, DefaultAgentProfile("a", "Agent", "generalist"))
	if err != nil {
		t.Fatalf("ReadAgentAnatomyConfig: %v", err)
	}
	if anatomy.Preset != "custom_critic" {
		t.Fatalf("preset = %q", anatomy.Preset)
	}
	active, ok := findTestHeartbeat(anatomy, "active_task_execution")
	if !ok {
		t.Fatalf("active heartbeat missing")
	}
	if active.Priority != 77 {
		t.Fatalf("active priority = %d", active.Priority)
	}
	if _, ok := findTestHeartbeat(anatomy, "loop_self_check"); !ok {
		t.Fatalf("default loop_self_check should be merged into explicit anatomy")
	}
	if _, ok := findTestHeartbeat(anatomy, "custom_probe"); !ok {
		t.Fatalf("custom heartbeat should be preserved")
	}
}

func TestReadAgentAnatomyPreservesExplicitHeartbeatToolBudget(t *testing.T) {
	dir := t.TempDir()
	raw := `{
  "schema": "rhizome.agent_anatomy.v1",
  "heartbeats": [
    {"id": "read_probe", "kind": "metacognition", "cadence": "every_5m", "priority": 60, "tool_suites": ["workspace_docs_read"], "max_tool_iterations": 3}
  ]
}`
	if err := os.WriteFile(filepath.Join(dir, agentAnatomyFilename), []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	anatomy, err := ReadAgentAnatomyConfig(dir, DefaultAgentProfile("a", "Agent", "generalist"))
	if err != nil {
		t.Fatalf("ReadAgentAnatomyConfig: %v", err)
	}
	heartbeat, ok := findTestHeartbeat(anatomy, "read_probe")
	if !ok {
		t.Fatalf("custom read probe missing")
	}
	if heartbeat.MaxToolIterations != 3 {
		t.Fatalf("max_tool_iterations = %d", heartbeat.MaxToolIterations)
	}
}

func TestReadAgentAnatomyPreservesHeartbeatRuntimeAnatomy(t *testing.T) {
	dir := t.TempDir()
	raw := `{
  "schema": "rhizome.agent_anatomy.v1",
  "heartbeats": [
    {
      "id": "portfolio_scout",
      "kind": "market_metacognition",
      "cadence": "every_20m",
      "priority": 58,
      "tool_suites": ["rhizome_read"],
      "context_selectors": ["workspace_state"],
      "objective": "Find one evidence-backed service opportunity without creating public work yet.",
      "instructions": ["score candidate demand", "write private notes before promotion"],
      "memory_lanes": ["opportunity_map", "role_backlog"],
      "notes": ["custom non-UI heartbeat"]
    }
  ]
}`
	if err := os.WriteFile(filepath.Join(dir, agentAnatomyFilename), []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	anatomy, err := ReadAgentAnatomyConfig(dir, DefaultAgentProfile("scout", "Scout", "service scout"))
	if err != nil {
		t.Fatalf("ReadAgentAnatomyConfig: %v", err)
	}
	heartbeat, ok := findTestHeartbeat(anatomy, "portfolio_scout")
	if !ok {
		t.Fatalf("custom heartbeat missing")
	}
	if heartbeat.Objective != "Find one evidence-backed service opportunity without creating public work yet." {
		t.Fatalf("objective not preserved: %+v", heartbeat)
	}
	if !containsAnatomyTestString(heartbeat.Instructions, "score candidate demand") || !containsAnatomyTestString(heartbeat.MemoryLanes, "opportunity_map") || !containsAnatomyTestString(heartbeat.Notes, "custom non-UI heartbeat") {
		t.Fatalf("runtime anatomy instructions/lanes/notes not preserved: %+v", heartbeat)
	}
}

func TestReadAgentAnatomyPreservesVisualAuditContract(t *testing.T) {
	dir := t.TempDir()
	raw := `{
  "schema": "rhizome.agent_anatomy.v1",
  "heartbeats": [
    {
      "id": "visual_product_audit",
      "kind": "browser_critic",
      "cadence": "every_10m",
      "priority": 70,
      "locks": ["read_only_artifact"],
      "tool_suites": ["browser_read_only"],
      "context_selectors": ["runnable_surface"],
      "visual_audit": {
        "viewports": [{"id": "wide", "width": 1600, "height": 1000, "purpose": "wide production viewport"}],
        "scenarios": [{"id": "export_flow", "label": "Export flow", "required_state": "after export", "screenshot_required": true}],
        "checks": ["text_fit", "export_controls"],
        "artifact_requirements": ["wide and export screenshots are locally decodable"]
      }
    }
  ]
}`
	if err := os.WriteFile(filepath.Join(dir, agentAnatomyFilename), []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	anatomy, err := ReadAgentAnatomyConfig(dir, DefaultAgentProfile("iota", "Iota", "UI/UX critic"))
	if err != nil {
		t.Fatalf("ReadAgentAnatomyConfig: %v", err)
	}
	heartbeat, ok := findTestHeartbeat(anatomy, "visual_product_audit")
	if !ok || heartbeat.VisualAudit == nil {
		t.Fatalf("visual audit heartbeat/contract missing: %+v", heartbeat)
	}
	if len(heartbeat.VisualAudit.Viewports) != 1 || heartbeat.VisualAudit.Viewports[0].ID != "wide" || heartbeat.VisualAudit.Viewports[0].Width != 1600 {
		t.Fatalf("unexpected visual audit viewports: %+v", heartbeat.VisualAudit.Viewports)
	}
	if len(heartbeat.VisualAudit.Scenarios) != 1 || heartbeat.VisualAudit.Scenarios[0].ID != "export_flow" || heartbeat.VisualAudit.Scenarios[0].ScreenshotRequired == nil || !*heartbeat.VisualAudit.Scenarios[0].ScreenshotRequired {
		t.Fatalf("unexpected visual audit scenarios: %+v", heartbeat.VisualAudit.Scenarios)
	}
	if !containsAnatomyTestString(heartbeat.VisualAudit.Checks, "text_fit") || !containsAnatomyTestString(heartbeat.VisualAudit.ArtifactRequirements, "wide and export screenshots are locally decodable") {
		t.Fatalf("unexpected visual audit checks/artifacts: %+v", heartbeat.VisualAudit)
	}
}

func TestReadAgentAnatomyBackfillsDefaultRequiredToolArtifacts(t *testing.T) {
	dir := t.TempDir()
	raw := `{
  "schema": "rhizome.agent_anatomy.v1",
  "preset": "ui_ux_reality_critic",
  "heartbeats": [
    {
      "id": "visual_product_audit",
      "kind": "browser_critic",
      "cadence": "every_10m",
      "priority": 65,
      "tool_suites": ["browser_read_only", "screenshot_capture"],
      "evidence_contract": {
        "checks": ["legacy_visual_check"],
        "artifact_requirements": ["legacy screenshot packet"]
      }
    }
  ]
}`
	if err := os.WriteFile(filepath.Join(dir, agentAnatomyFilename), []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	anatomy, err := ReadAgentAnatomyConfig(dir, DefaultAgentProfile("iota", "Iota", "UI/UX critic"))
	if err != nil {
		t.Fatalf("ReadAgentAnatomyConfig: %v", err)
	}
	heartbeat, ok := findTestHeartbeat(anatomy, "visual_product_audit")
	if !ok || heartbeat.EvidenceContract == nil {
		t.Fatalf("visual heartbeat/evidence contract missing: %+v", heartbeat)
	}
	if !containsAnatomyTestString(heartbeat.EvidenceContract.Checks, "legacy_visual_check") {
		t.Fatalf("custom checks should be preserved: %+v", heartbeat.EvidenceContract.Checks)
	}
	if len(heartbeat.EvidenceContract.RequiredToolArtifacts) != 1 || heartbeat.EvidenceContract.RequiredToolArtifacts[0].Tool != "browser_visual_probe" {
		t.Fatalf("default required tool artifact should be backfilled into legacy visual anatomy, got %+v", heartbeat.EvidenceContract.RequiredToolArtifacts)
	}
}

func TestReadAgentAnatomyPreservesGenericEvidenceContract(t *testing.T) {
	dir := t.TempDir()
	raw := `{
  "schema": "rhizome.agent_anatomy.v1",
  "heartbeats": [
    {
      "id": "deploy_smoke",
      "kind": "ops_verification",
      "cadence": "every_15m",
      "priority": 55,
      "tool_suites": ["rhizome_read"],
      "evidence_contract": {
        "dimensions": [{"id": "prod", "kind": "environment", "label": "Production"}],
        "states": [{"id": "healthcheck", "label": "Healthcheck", "required_state": "after deploy", "evidence_required": true, "expected_evidence_kind": "structured probe result"}],
        "checks": ["http_status", "latency"],
        "artifact_requirements": ["structured JSON probe result"],
        "required_tool_artifacts": [{"tool": "http_health_probe", "contract_version": "health_probe/v1", "capability": "http_probe", "tool_suite": "rhizome_read", "when": "always", "purpose": "prove production health"}]
      }
    }
  ]
}`
	if err := os.WriteFile(filepath.Join(dir, agentAnatomyFilename), []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	anatomy, err := ReadAgentAnatomyConfig(dir, DefaultAgentProfile("ops", "Ops", "deploy operator"))
	if err != nil {
		t.Fatalf("ReadAgentAnatomyConfig: %v", err)
	}
	heartbeat, ok := findTestHeartbeat(anatomy, "deploy_smoke")
	if !ok || heartbeat.EvidenceContract == nil {
		t.Fatalf("generic evidence heartbeat/contract missing: %+v", heartbeat)
	}
	if len(heartbeat.EvidenceContract.Dimensions) != 1 || heartbeat.EvidenceContract.Dimensions[0].ID != "prod" || heartbeat.EvidenceContract.Dimensions[0].Kind != "environment" {
		t.Fatalf("unexpected evidence dimensions: %+v", heartbeat.EvidenceContract.Dimensions)
	}
	if len(heartbeat.EvidenceContract.States) != 1 || heartbeat.EvidenceContract.States[0].ID != "healthcheck" || heartbeat.EvidenceContract.States[0].EvidenceRequired == nil || !*heartbeat.EvidenceContract.States[0].EvidenceRequired {
		t.Fatalf("unexpected evidence states: %+v", heartbeat.EvidenceContract.States)
	}
	if !containsAnatomyTestString(heartbeat.EvidenceContract.Checks, "latency") || !containsAnatomyTestString(heartbeat.EvidenceContract.ArtifactRequirements, "structured JSON probe result") {
		t.Fatalf("unexpected generic evidence checks/artifacts: %+v", heartbeat.EvidenceContract)
	}
	if len(heartbeat.EvidenceContract.RequiredToolArtifacts) != 1 || heartbeat.EvidenceContract.RequiredToolArtifacts[0].Tool != "http_health_probe" || heartbeat.EvidenceContract.RequiredToolArtifacts[0].ToolSuite != "rhizome_read" {
		t.Fatalf("unexpected generic required tool artifacts: %+v", heartbeat.EvidenceContract.RequiredToolArtifacts)
	}
}

func TestAgentAnatomyValidationRejectsBadRawConfig(t *testing.T) {
	tests := map[string]string{
		"unknown schema":         `{"schema":"wrong","heartbeats":[{"id":"a"}]}`,
		"duplicate id":           `{"heartbeats":[{"id":"a"},{"id":"a"}]}`,
		"invalid cadence":        `{"heartbeats":[{"id":"a","cadence":"sometimes"}]}`,
		"invalid lock":           `{"heartbeats":[{"id":"a","locks":["mutate_everything"]}]}`,
		"invalid tool":           `{"heartbeats":[{"id":"a","tool_suites":["god_mode"]}]}`,
		"negative tools":         `{"heartbeats":[{"id":"a","max_tool_iterations":-1}]}`,
		"bad viewport":           `{"heartbeats":[{"id":"a","visual_audit":{"viewports":[{"id":"mobile","width":0,"height":844}]}}]}`,
		"bad evidence dimension": `{"heartbeats":[{"id":"a","evidence_contract":{"dimensions":[{"id":"env","width":1280}]}}]}`,
		"bad required tool":      `{"heartbeats":[{"id":"a","evidence_contract":{"required_tool_artifacts":[{"tool_suite":"browser_read_only"}]}}]}`,
		"bad required suite":     `{"heartbeats":[{"id":"a","evidence_contract":{"required_tool_artifacts":[{"tool":"probe","tool_suite":"god_mode"}]}}]}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, agentAnatomyFilename), []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := ReadAgentAnatomyConfig(dir, AgentProfile{})
			if err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestAgentAnatomyDigestIsStableAcrossHeartbeatOrder(t *testing.T) {
	profile := DefaultAgentProfile("a", "Agent", "UI/UX critic")
	left := DefaultAgentAnatomyConfig(profile)
	right := left
	for i, j := 0, len(right.Heartbeats)-1; i < j; i, j = i+1, j-1 {
		right.Heartbeats[i], right.Heartbeats[j] = right.Heartbeats[j], right.Heartbeats[i]
	}
	if AgentAnatomyDigest(left) == "" {
		t.Fatalf("digest should not be empty")
	}
	if AgentAnatomyDigest(left) != AgentAnatomyDigest(right) {
		t.Fatalf("digest should be stable across heartbeat order")
	}
}

func TestSaveAgentAnatomyConfigUsesManagerMaterializationForManagedWorkdir(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	if err := UpsertManagedAgent(ManagedAgentRecord{
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
		WorkspaceID: "rhizome-main",
	}); err != nil {
		t.Fatalf("UpsertManagedAgent() error: %v", err)
	}
	profile := DefaultAgentProfile("sigma", "Sigma", "UI/UX critic")
	if err := SaveAgentAnatomyConfig(workdir, DefaultAgentAnatomyConfig(profile), profile); err != nil {
		t.Fatalf("SaveAgentAnatomyConfig() error: %v", err)
	}
	if pathExists(managerStateMaterializationPath(agentRuntimeConfigRoot())) {
		t.Fatalf("manager materialization journal should be cleared after successful anatomy write")
	}
	got, err := ReadAgentAnatomyConfig(workdir, profile)
	if err != nil {
		t.Fatalf("ReadAgentAnatomyConfig() error: %v", err)
	}
	if got.Preset != "ui_ux_reality_critic" {
		t.Fatalf("expected saved anatomy, got %+v", got)
	}
}

func findTestHeartbeat(anatomy AgentAnatomyConfig, id string) (AgentHeartbeatSpec, bool) {
	for _, heartbeat := range anatomy.Heartbeats {
		if heartbeat.ID == id {
			return heartbeat, true
		}
	}
	return AgentHeartbeatSpec{}, false
}

func containsAnatomyTestString(values []string, want string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}
