package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type ProjectPhaseTransitionTool struct {
	client           *RhizomeClient
	workspaceID      string
	agentID          string
	coordinationMode string
	runtimeBinding   func() AgentRuntimeBinding
}

func NewProjectPhaseTransitionTool(client *RhizomeClient, workspaceID, agentID string) *ProjectPhaseTransitionTool {
	return &ProjectPhaseTransitionTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
	}
}

func (t *ProjectPhaseTransitionTool) WithCoordinationMode(mode string) *ProjectPhaseTransitionTool {
	if t != nil {
		t.coordinationMode = normalizeCoordinationMode(mode)
	}
	return t
}

func (t *ProjectPhaseTransitionTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectPhaseTransitionTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectPhaseTransitionTool) Name() string { return "project_phase_transition" }

func (t *ProjectPhaseTransitionTool) Description() string {
	return "Move a Rhizome Project to the next delivery phase after checking project gates and product-planning sanity. Use IMPLEMENTATION, not PLANNING, to open implementation lanes once non-phase gates are satisfied. Before opening IMPLEMENTATION, broad product/code projects need a semantic product contract and bounded critical plan review in project docs. After opening IMPLEMENTATION, create concrete product/code tasks with task_submit and leave them visible for autonomous self-selection; use delegate_task only as a targeted wake for an exact task. This records coordination evidence only and does not mutate git or local files."
}

func (t *ProjectPhaseTransitionTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id": map[string]any{"type": "string", "description": "Project id."},
			"to_phase": map[string]any{
				"type":        "string",
				"description": "Target phase: SPEC, PLANNING, IMPLEMENTATION, REVIEW, INTEGRATION, VALIDATION, or DONE. IMPLEMENTATION is the phase that opens implementation lanes.",
			},
			"reason": map[string]any{
				"type":        "string",
				"description": "Short factual reason and evidence summary for the phase transition.",
			},
			"require_gates": map[string]any{
				"type":        "boolean",
				"description": "When true, implementation-opening phases require all non-phase implementation gates to be satisfied. Defaults true.",
			},
			"require_product_planning_review": map[string]any{
				"type":        "boolean",
				"description": "When true, implementation-opening phases require project docs to contain a semantic product contract and bounded critical plan review. Defaults true; set false only for explicit tiny/non-code waivers and explain the waiver in reason.",
			},
		},
		"required": []string{"project_id", "to_phase"},
	}
}

func (t *ProjectPhaseTransitionTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_phase_transition is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	projectID, err := resolveProjectToolActiveProjectID("project_phase_transition", t.workspaceID, projectToolRefArg(args, "project_id"), t.runtimeBinding)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	toPhase := normalizeProjectPhaseTransitionToolPhase(stringArg(args, "to_phase"))
	if toPhase == "" {
		return &ToolResult{Output: "to_phase must be one of SPEC, PLANNING, IMPLEMENTATION, REVIEW, INTEGRATION, VALIDATION, or DONE", IsError: true}
	}
	reason := strings.TrimSpace(stringArg(args, "reason"))
	requireGates := true
	if explicit := optionalBoolArg(args, "require_gates"); explicit != nil {
		requireGates = *explicit
	}
	requirePlanningReview := true
	if explicit := optionalBoolArg(args, "require_product_planning_review"); explicit != nil {
		requirePlanningReview = *explicit
	}
	if !requirePlanningReview && !projectPlanningReviewWaiverReason(reason) {
		return &ToolResult{Output: "require_product_planning_review=false requires reason to explicitly mention a tiny/non-code scope, spike, operator override, or planning-review waiver", IsError: true}
	}

	coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_phase_transition failed reading coordination for %s: %v", projectID, err), IsError: true}
	}
	if strings.EqualFold(strings.TrimSpace(coordination.Profile.CurrentPhase), toPhase) {
		return &ToolResult{Output: fmt.Sprintf("project %s is already in phase %s", projectID, toPhase), IsError: true}
	}
	trustFirst := normalizeCoordinationMode(t.coordinationMode) == CoordinationModeTrustFirst
	openingImplementation := projectPhaseTransitionOpeningImplementation(coordination.Profile.CurrentPhase, toPhase)
	var warnings []string
	planningReview := ProjectPlanningReviewCheck{}
	if openingImplementation {
		if coordination.StrategicLead == nil || strings.TrimSpace(coordination.StrategicLead.AgentID) != t.agentID {
			if trustFirst {
				warnings = append(warnings, fmt.Sprintf("strategic_lead_active advisory: this agent does not currently hold the active strategic lead lease for %s", projectID))
			} else {
				return &ToolResult{Output: fmt.Sprintf("project_phase_transition to %s requires this agent to hold the active strategic lead lease for %s", toPhase, projectID), IsError: true}
			}
		}
		if requireGates {
			if missing := projectPhaseTransitionMissingNonPhaseGates(coordination.GateStatus); len(missing) > 0 {
				return &ToolResult{Output: projectPhaseTransitionMissingGateBlockMessage(projectID, missing), IsError: true}
			}
		}
		var reviewErr error
		planningReview, reviewErr = t.checkProjectPlanningReview(ctx, coordination)
		if reviewErr != nil {
			return &ToolResult{Output: fmt.Sprintf("project_phase_transition failed checking product planning review for %s: %v", projectID, reviewErr), IsError: true}
		}
		hardPlanningIssues := []string{}
		for _, issue := range projectPlanningReviewTransitionIssues(planningReview, trustFirst) {
			if issue.Hard && requirePlanningReview {
				hardPlanningIssues = append(hardPlanningIssues, issue.Message)
				continue
			}
			warnings = append(warnings, issue.Message)
		}
		if len(hardPlanningIssues) > 0 {
			planningMessages := append([]string{}, hardPlanningIssues...)
			planningMessages = append(planningMessages, warnings...)
			return &ToolResult{Output: strings.Join(planningMessages, "\n"), IsError: true}
		}
	}

	profile, err := t.client.TransitionProjectPhase(ctx, ProjectPhaseTransitionInput{
		WorkspaceID:      t.workspaceID,
		ProjectID:        projectID,
		ActorID:          t.agentID,
		ToPhase:          toPhase,
		Reason:           reason,
		CoordinationMode: normalizeCoordinationMode(t.coordinationMode),
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_phase_transition failed transitioning %s to %s: %v", projectID, toPhase, err), IsError: true}
	}
	payload := map[string]any{
		"project_id":                      projectID,
		"from_phase":                      coordination.Profile.CurrentPhase,
		"to_phase":                        profile.CurrentPhase,
		"actor_id":                        t.agentID,
		"require_gates":                   requireGates,
		"require_product_planning_review": requirePlanningReview,
		"coordination_mode":               normalizeCoordinationMode(t.coordinationMode),
		"warnings":                        warnings,
		"no_git_mutation":                 true,
		"guidance":                        "Phase transition is durable. In strict mode implementation tasks still require role, write-scope, repo, checkout, and branch admission. In trust_first mode project roles are advisory or repair-only telemetry; task fit, frontier self-selection, write-scope hints, checkout/branch evidence, and published review evidence carry the runtime contract.",
	}
	if openingImplementation {
		payload["product_planning_review"] = planningReview.Payload()
		payload["implementation_fanout_required"] = true
		payload["strategic_lead_next_action"] = "task_submit_then_semantic_self_selection"
		if trustFirst {
			payload["role_seed_policy"] = "disabled_in_trust_first"
		}
		payload["next_required_actions"] = []string{
			"Keep canonical project.<project_id>.design_and_plan synced as the design_doc_id and implementation_plan_doc_id gate artifact; keep product_contract / acceptance_criteria / plan_review visible and update them when evidence changes.",
			"Create bounded product/code tasks with task_submit for each semantic product/code gap visible from the contract, acceptance criteria, docs, active tasks, and frontier.",
			"Map every implementation/review/validation task to the product contract, core user promise, and acceptance-criteria IDs it covers; explicitly list adjacent wrong products that the lane must avoid.",
			"In strict mode each implementation task should include project_id, project_lane=implementation, requires_project_gate=true, and write_scope_hints matching admission requirements. In trust_first mode requires_project_gate should stay false unless a hard gate is genuinely needed.",
			"For frontend/backend/code projects, make shared scaffold/config ownership explicit before coding: package.json, package-lock.json, tsconfig*.json, vite.config.*, index.html, public/**, tests/**, and src/** must be covered by one scaffold/integration task or clearly non-overlapping implementation tasks.",
			"Leave created tasks visible for autonomous self-selection; agents should choose by semantic fit, visible workload, and tool capability rather than by a project-wide role grid. Use agent_request request_kind=delegate_task with an exact task_id only when a suitable agent should be woken for that task.",
			"Use question/review requests only for bounded planning or review; they do not start implementation lanes unless paired with a concrete task_id wake.",
		}
		payload["anti_patterns"] = []string{
			"Do not treat IMPLEMENTATION phase opening as completed implementation.",
			"Do not ask peers to begin coding through a plain question/model.ask request.",
			"Do not keep the root task in coordination-only loops when product implementation lanes are missing.",
		}
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_phase_transition completed for %s -> %s", projectID, profile.CurrentPhase)}
	}
	return &ToolResult{Output: string(raw)}
}

type ProjectPlanningReviewCheck struct {
	CheckedDocKeys     []string `json:"checked_doc_keys"`
	Missing            []string `json:"missing,omitempty"`
	ProductContract    bool     `json:"product_contract"`
	CriticalReview     bool     `json:"critical_plan_review"`
	SourceDocKeys      []string `json:"source_doc_keys,omitempty"`
	SourceFidelity     bool     `json:"source_requirements_trace"`
	BlockingReviewOpen bool     `json:"blocking_review_open"`
}

type projectPlanningReviewTransitionIssue struct {
	Message string
	Hard    bool
}

func (c ProjectPlanningReviewCheck) Payload() map[string]any {
	status := "satisfied"
	if len(c.Missing) > 0 {
		status = "missing"
	}
	return map[string]any{
		"status":               status,
		"checked_doc_keys":     c.CheckedDocKeys,
		"product_contract":     c.ProductContract,
		"critical_plan_review": c.CriticalReview,
		"source_doc_keys":      c.SourceDocKeys,
		"source_fidelity":      c.SourceFidelity,
		"blocking_review_open": c.BlockingReviewOpen,
		"missing":              c.Missing,
		"required_sections": []string{
			"Source Requirements Trace when source_doc_keys exist: rhizome_source_requirements_trace_v1 with source_doc_keys, acceptance-critical anchors, acceptance criteria refs, and adjacent wrong products/non-goals",
			"Product Contract: core_user_promise, happy_path, non_goals/adjacent_wrong_products, MVP acceptance, v2_aspiration",
			"Critical Plan Review: skeptical product-fidelity review with severity labels blocking, major, minor, taste and disposition",
		},
	}
}

func (t *ProjectPhaseTransitionTool) checkProjectPlanningReview(ctx context.Context, coordination ProjectCoordinationRecord) (ProjectPlanningReviewCheck, error) {
	projectID := firstNonEmpty(strings.TrimSpace(coordination.Project.ProjectID), strings.TrimSpace(coordination.Profile.ProjectID))
	keys := uniqueTrimmedCSVStrings([]string{
		strings.TrimSpace(coordination.Profile.DesignDocID),
		strings.TrimSpace(coordination.Profile.ImplementationPlanDocID),
		"project." + projectID + ".source_refs",
		"project." + projectID + ".source_requirements_trace",
		"project." + projectID + ".product_contract",
		"project." + projectID + ".acceptance_criteria",
		"project." + projectID + ".plan_review",
		"project." + projectID + ".planning_review",
	})
	var combined strings.Builder
	checked := []string{}
	for _, key := range keys {
		if strings.TrimSpace(key) == "" || strings.Contains(key, "project..") {
			continue
		}
		doc, ok, err := t.client.GetDoc(ctx, t.workspaceID, key)
		if err != nil {
			return ProjectPlanningReviewCheck{}, err
		}
		if !ok {
			continue
		}
		checked = append(checked, strings.TrimSpace(key))
		combined.WriteString("\n\n# ")
		combined.WriteString(strings.TrimSpace(doc.Title))
		combined.WriteString("\n")
		combined.WriteString(strings.TrimSpace(doc.Content))
	}
	check := ProjectPlanningReviewCheck{CheckedDocKeys: checked}
	text := strings.ToLower(combined.String())
	check.SourceDocKeys = projectPlanningSourceDocKeys(coordination, combined.String())
	check.ProductContract = projectPlanningTextHasProductContract(text)
	check.CriticalReview = projectPlanningTextHasCriticalReview(text)
	check.SourceFidelity = len(check.SourceDocKeys) == 0 || projectPlanningTextHasSourceRequirementsTrace(text, check.SourceDocKeys)
	check.BlockingReviewOpen = projectPlanningTextHasOpenBlockingReview(text)
	if len(checked) == 0 {
		check.Missing = append(check.Missing, "no readable project planning docs found; expected design/implementation plan or project.<project_id>.product_contract / .plan_review docs")
		return check, nil
	}
	if len(check.SourceDocKeys) > 0 && !check.SourceFidelity {
		check.Missing = append(check.Missing, "source requirements trace missing for source_doc_keys")
	}
	if !check.ProductContract {
		check.Missing = append(check.Missing, "semantic product contract missing")
	}
	if !check.CriticalReview {
		check.Missing = append(check.Missing, "bounded critical plan review missing")
	}
	return check, nil
}

func projectPlanningReviewTransitionIssues(check ProjectPlanningReviewCheck, trustFirst bool) []projectPlanningReviewTransitionIssue {
	if len(check.CheckedDocKeys) == 0 {
		return []projectPlanningReviewTransitionIssue{{
			Message: "project_phase_transition blocked; no readable project planning docs found. Required minimum: Product Contract with core_user_promise, happy_path, non_goals/adjacent_wrong_products, MVP acceptance, and v2_aspiration. Critical Plan Review is expected before implementation, and is advisory telemetry in trust_first unless it exposes unresolved BLOCKING findings.",
			Hard:    true,
		}}
	}
	var issues []projectPlanningReviewTransitionIssue
	if !check.ProductContract {
		message := "project_phase_transition blocked; semantic Product Contract missing. Add core_user_promise, happy_path, required inputs/outputs, non_goals or adjacent_wrong_products, MVP acceptance, and v2_aspiration before opening implementation."
		if trustFirst {
			message = "project_phase_transition planning telemetry: semantic Product Contract missing. Add core_user_promise, happy_path, required inputs/outputs, non_goals or adjacent_wrong_products, MVP acceptance, and v2_aspiration; trust_first allows implementation to open while this remains visible advisory telemetry."
		}
		issues = append(issues, projectPlanningReviewTransitionIssue{
			Message: message,
			Hard:    !trustFirst,
		})
	}
	if len(check.SourceDocKeys) > 0 && !check.SourceFidelity {
		issues = append(issues, projectPlanningReviewTransitionIssue{
			Message: "project_phase_transition blocked; source requirements trace missing. Source docs are attached to this project, so planning must preserve non-droppable acceptance-critical anchors in a fenced rhizome_source_requirements_trace_v1 block before implementation opens. Include source_doc_keys, acceptance-critical anchors, acceptance criteria refs, and adjacent wrong products/non-goals.",
			Hard:    true,
		})
	}
	if !check.CriticalReview {
		issues = append(issues, projectPlanningReviewTransitionIssue{
			Message: "project_phase_transition planning telemetry: bounded Critical Plan Review missing. Add a skeptical product-fidelity review with blocking/major/minor/taste severity and disposition; in trust_first this remains advisory telemetry.",
			Hard:    false,
		})
	}
	if check.BlockingReviewOpen {
		issues = append(issues, projectPlanningReviewTransitionIssue{
			Message: "project_phase_transition planning review has unresolved BLOCKING findings. Resolve them or explicitly mark accepted_risk before opening implementation.",
			Hard:    !trustFirst,
		})
	}
	return issues
}

func projectPlanningSourceDocKeys(coordination ProjectCoordinationRecord, combinedDocs string) []string {
	var out []string
	out = append(out, sourceDocKeysFromSourceRefsText(combinedDocs)...)
	for _, task := range coordination.Tasks {
		out = append(out, sourceDocKeysFromTaskRequirementsJSON(task.TaskRequirementsJSON)...)
	}
	projectID := firstNonEmpty(strings.TrimSpace(coordination.Project.ProjectID), strings.TrimSpace(coordination.Profile.ProjectID))
	return projectPlanningCanonicalSourceDocKeys(projectID, out)
}

func projectPlanningCanonicalSourceDocKeys(projectID string, keys []string) []string {
	projectID = sanitizeDocKeySegment(strings.TrimSpace(projectID))
	var out []string
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if projectPlanningGeneratedProjectDocKey(projectID, key) {
			continue
		}
		out = append(out, key)
	}
	return uniqueTrimmedCSVStrings(out)
}

func projectPlanningGeneratedProjectDocKey(projectID, key string) bool {
	projectID = sanitizeDocKeySegment(strings.TrimSpace(projectID))
	key = strings.ToLower(strings.TrimSpace(key))
	if projectID == "" || key == "" {
		return false
	}
	prefix := "project." + strings.ToLower(projectID) + "."
	if strings.HasPrefix(key, prefix) && projectPlanningGeneratedProjectDocSuffix(strings.TrimPrefix(key, prefix)) {
		return true
	}
	if strings.HasPrefix(key, "project.") {
		if idx := strings.LastIndex(key, "."); idx >= 0 && idx+1 < len(key) {
			return projectPlanningGeneratedProjectDocSuffix(key[idx+1:])
		}
	}
	return false
}

func projectPlanningGeneratedProjectDocSuffix(suffix string) bool {
	switch strings.ToLower(strings.TrimSpace(suffix)) {
	case "acceptance_criteria",
		"design",
		"design_and_plan",
		"implementation_plan",
		"intake",
		"plan_review",
		"planning_review",
		"product_contract",
		"quality_reflection",
		"reflection_board",
		"source_refs",
		"source_requirements_trace",
		"strategy_review":
		return true
	default:
		return false
	}
}

func projectPlanningTextHasSourceRequirementsTrace(text string, sourceDocKeys []string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" || len(sourceDocKeys) == 0 {
		return len(sourceDocKeys) == 0
	}
	for _, block := range projectPlanningFencedBlocks(text, "rhizome_source_requirements_trace_v1") {
		if projectPlanningSourceRequirementsTraceBlockComplete(block, sourceDocKeys) {
			return true
		}
	}
	if block, ok := projectPlanningInlineSchemaBlock(text, "rhizome_source_requirements_trace_v1"); ok && projectPlanningSourceRequirementsTraceBlockComplete(block, sourceDocKeys) {
		return true
	}
	return false
}

func projectPlanningInlineSchemaBlock(text, schema string) (string, bool) {
	text = strings.TrimSpace(text)
	schema = strings.ToLower(strings.TrimSpace(schema))
	if text == "" || schema == "" {
		return "", false
	}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(strings.ToLower(line))
		if trimmed == schema || strings.HasPrefix(trimmed, schema+":") || projectPlanningSchemaFieldMatches(trimmed, schema) {
			return strings.Join(lines[i:], "\n"), true
		}
	}
	return "", false
}

func projectPlanningSchemaFieldMatches(line, schema string) bool {
	line = strings.TrimSpace(strings.ToLower(line))
	schema = strings.TrimSpace(strings.ToLower(schema))
	if line == "" || schema == "" {
		return false
	}
	for _, sep := range []string{":", "="} {
		prefix := "schema" + sep
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		value := projectPlanningNormalizeFieldToken(strings.TrimSpace(line[len(prefix):]))
		return value == schema
	}
	return false
}

func projectPlanningSourceRequirementsTraceBlockComplete(block string, sourceDocKeys []string) bool {
	block = strings.ToLower(strings.TrimSpace(block))
	if block == "" {
		return false
	}
	if !projectPlanningFieldContainsAll(block, []string{"source_doc_keys", "source doc keys"}, sourceDocKeys) {
		return false
	}
	hasAnchors := projectPlanningFieldHasSpecificValue(block, []string{
		"acceptance_critical_anchors",
		"acceptance critical anchors",
		"non_droppable_requirements",
		"non-droppable requirements",
		"required_product_identity",
		"required product identity",
	})
	hasAcceptanceRefs := projectPlanningFieldHasSpecificValue(block, []string{
		"acceptance_criteria_refs",
		"acceptance criteria refs",
		"acceptance_criteria",
		"acceptance criteria",
		"mvp_acceptance",
		"mvp acceptance",
	})
	hasWrongProducts := projectPlanningFieldHasSpecificValue(block, []string{
		"adjacent_wrong_products",
		"adjacent wrong products",
		"non_goals",
		"non-goals",
		"negative_constraints",
		"negative constraints",
	})
	return hasAnchors && hasAcceptanceRefs && hasWrongProducts
}

func projectPlanningReviewWaiverReason(reason string) bool {
	return containsAnySignal(reason, []string{
		"waiver",
		"explicit waiver",
		"operator override",
		"tiny",
		"small",
		"non-code",
		"non code",
		"documentation-only",
		"docs-only",
		"spike",
	})
}

func projectPlanningTextHasCriticalReviewLoose(text string) bool {
	if strings.TrimSpace(text) == "" {
		return false
	}
	hasReview := containsAnySignal(text, []string{
		"rhizome_plan_review_v1",
		"plan_review",
		"critical plan review",
		"bounded critical review",
		"product-fidelity review",
		"plan review",
		"adversarial review",
	})
	hasSeverity := containsAnySignal(text, []string{
		"blocking",
		"blocker",
		"major",
		"minor",
		"taste",
		"none",
	}) && containsAnySignal(text, []string{
		"status: open",
		"status=open",
		"disposition",
		"resolved",
		"accepted",
		"rejected",
		"no blocking",
	})
	return hasReview && hasSeverity
}

func projectPlanningTextHasOpenBlockingReviewWindowed(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" || !containsAnySignal(text, []string{"blocking", "blocker"}) {
		return false
	}
	if containsAnySignal(text, []string{"open blocking", "unresolved blocking", "blocking unresolved", "blocking: open"}) {
		return true
	}
	for _, marker := range []string{"finding[blocking]", "severity: blocking", "severity=blocking", "severity: blocker", "severity=blocker"} {
		for _, idx := range projectPlanningSignalIndexes(text, marker) {
			windowEnd := idx + 800
			if windowEnd > len(text) {
				windowEnd = len(text)
			}
			window := text[idx:windowEnd]
			if projectPlanningBlockingFindingIsResolved(window) && !projectPlanningBlockingFindingIsOpen(window) {
				continue
			}
			return true
		}
	}
	return false
}

func projectPlanningSignalIndexes(text, signal string) []int {
	text = strings.ToLower(strings.TrimSpace(text))
	signal = strings.ToLower(strings.TrimSpace(signal))
	if text == "" || signal == "" {
		return nil
	}
	indexes := []int{}
	offset := 0
	for {
		idx := strings.Index(text[offset:], signal)
		if idx < 0 {
			break
		}
		absolute := offset + idx
		indexes = append(indexes, absolute)
		offset = absolute + len(signal)
	}
	return indexes
}

func projectPlanningBlockingFindingIsResolved(text string) bool {
	return containsAnySignal(text, []string{
		"no blocking findings remain",
		"no open blocking",
		"disposition: resolved",
		"disposition=resolved",
		"status: resolved",
		"status=resolved",
		"status: accepted_risk",
		"status=accepted_risk",
		"accepted_risk",
		"accepted risk",
		"resolved by",
	})
}

func projectPlanningBlockingFindingIsOpen(text string) bool {
	return containsAnySignal(text, []string{
		"status: open",
		"status=open",
		"disposition: open",
		"disposition=open",
		"unresolved",
		"not resolved",
		"still blocking",
	})
}

func projectPlanningTextHasProductContract(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	for _, block := range projectPlanningFencedBlocks(text, "rhizome_product_contract_v1") {
		if projectPlanningProductContractBlockComplete(block) {
			return true
		}
	}
	if strings.Contains(text, "rhizome_product_contract_v1") {
		return false
	}
	return projectPlanningProductContractProseComplete(text)
}

func projectPlanningTextHasCriticalReview(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	for _, block := range projectPlanningFencedBlocks(text, "rhizome_plan_review_v1") {
		if projectPlanningReviewSeverityHasValue(block) &&
			(projectPlanningFieldHasValue(block, []string{"status"}) || projectPlanningFieldHasValue(block, []string{"disposition"})) {
			return true
		}
	}
	if strings.Contains(text, "rhizome_plan_review_v1") {
		return false
	}
	return projectPlanningTextHasCriticalReviewLoose(text)
}

func projectPlanningTextHasOpenBlockingReview(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	blocks := projectPlanningFencedBlocks(text, "rhizome_plan_review_v1")
	for _, block := range blocks {
		severity := projectPlanningFieldValue(block, []string{"severity"})
		if !containsAnySignal(severity, []string{"blocking", "blocker"}) {
			continue
		}
		disposition := projectPlanningFieldValue(block, []string{"status", "disposition"})
		if disposition == "" || projectPlanningBlockingFindingIsOpen(disposition) || !projectPlanningBlockingFindingIsResolved(disposition) {
			return true
		}
	}
	if len(blocks) > 0 {
		return false
	}
	return projectPlanningTextHasOpenBlockingReviewWindowed(text)
}

func projectPlanningProductContractBlockComplete(block string) bool {
	return projectPlanningFieldHasValue(block, []string{"core_user_promise", "core user promise"}) &&
		projectPlanningFieldHasValue(block, []string{"happy_path", "happy path", "primary_user_flow", "primary user flow"}) &&
		projectPlanningFieldHasValue(block, []string{"required_inputs", "required inputs", "required_inputs_outputs", "inputs_outputs", "inputs/outputs", "inputs", "input"}) &&
		projectPlanningFieldHasValue(block, []string{"required_outputs", "required outputs", "required_inputs_outputs", "inputs_outputs", "inputs/outputs", "outputs", "output"}) &&
		projectPlanningFieldHasValue(block, []string{"non_goals", "non-goals", "adjacent_wrong_products", "adjacent wrong products"}) &&
		projectPlanningFieldHasValue(block, []string{"mvp_acceptance", "mvp acceptance", "acceptance_criteria_refs", "acceptance criteria refs"}) &&
		projectPlanningFieldHasValue(block, []string{"v2_aspiration", "v2 aspiration", "quality target"})
}

func projectPlanningReviewSeverityHasValue(block string) bool {
	return projectPlanningFieldHasValue(block, []string{"severity"}) ||
		containsAnySignal(block, []string{"severity: none", "severity=none"})
}

func projectPlanningProductContractProseComplete(text string) bool {
	hasContract := containsAnySignal(text, []string{
		"product contract",
		"semantic product contract",
		"core_user_promise",
		"core user promise",
		"core user transformation",
	})
	hasPath := containsAnySignal(text, []string{
		"happy_path",
		"happy path",
		"primary_user_flow",
		"primary user flow",
		"user flow",
	})
	hasInput := containsAnySignal(text, []string{
		"required_inputs",
		"required inputs",
		"inputs_outputs",
		"inputs/outputs",
		"inputs:",
		"input:",
		"upload",
		"source image",
		"source photo",
	})
	hasOutput := containsAnySignal(text, []string{
		"required_outputs",
		"required outputs",
		"inputs_outputs",
		"inputs/outputs",
		"outputs:",
		"output:",
		"export",
		"preview",
		"png",
	})
	hasNonGoals := containsAnySignal(text, []string{
		"non_goals",
		"non-goals",
		"adjacent_wrong_products",
		"adjacent wrong products",
		"wrong product",
		"anti-goals",
		"not a",
	})
	hasMVP := containsAnySignal(text, []string{
		"mvp_acceptance",
		"mvp acceptance",
		"acceptance_criteria_refs",
		"acceptance criteria",
	})
	hasV2 := containsAnySignal(text, []string{
		"v2_aspiration",
		"v2 aspiration",
		"v2 quality",
		"quality target",
	})
	return hasContract && hasPath && hasInput && hasOutput && hasNonGoals && hasMVP && hasV2
}

func projectPlanningFencedBlocks(text, marker string) []string {
	text = strings.ToLower(strings.TrimSpace(text))
	marker = "```" + strings.ToLower(strings.TrimSpace(marker))
	if text == "" || marker == "```" {
		return nil
	}
	var blocks []string
	offset := 0
	for {
		idx := strings.Index(text[offset:], marker)
		if idx < 0 {
			break
		}
		start := offset + idx + len(marker)
		if start < len(text) && text[start] == '\r' {
			start++
		}
		if start < len(text) && text[start] == '\n' {
			start++
		}
		endRel := strings.Index(text[start:], "```")
		end := len(text)
		if endRel >= 0 {
			end = start + endRel
		}
		blocks = append(blocks, strings.TrimSpace(text[start:end]))
		if endRel < 0 {
			break
		}
		offset = end + len("```")
	}
	return blocks
}

func projectPlanningFieldHasValue(block string, fields []string) bool {
	return len(projectPlanningFieldValues(block, fields)) > 0
}

func projectPlanningFieldHasSpecificValue(block string, fields []string) bool {
	for _, value := range projectPlanningFieldValues(block, fields) {
		if projectPlanningTraceValueSpecific(value) {
			return true
		}
	}
	return false
}

func projectPlanningFieldContainsAll(block string, fields []string, required []string) bool {
	values := projectPlanningFieldValues(block, fields)
	if len(values) == 0 {
		return false
	}
	seen := map[string]struct{}{}
	for _, value := range values {
		value = projectPlanningNormalizeFieldToken(value)
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	for _, item := range required {
		item = projectPlanningNormalizeFieldToken(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; !ok {
			return false
		}
	}
	return true
}

func projectPlanningFieldValue(block string, fields []string) string {
	lines := strings.Split(strings.ToLower(block), "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		for _, field := range fields {
			field = strings.ToLower(strings.TrimSpace(field))
			if field == "" {
				continue
			}
			if value, ok := projectPlanningInlineFieldValue(trimmed, field); ok {
				if projectPlanningMeaningfulFieldValue(value) {
					return value
				}
				if listValue := projectPlanningFollowingListValue(lines[i+1:]); listValue != "" {
					return listValue
				}
			}
		}
	}
	return ""
}

func projectPlanningFieldValues(block string, fields []string) []string {
	lines := strings.Split(strings.ToLower(block), "\n")
	var out []string
	for _, field := range fields {
		field = strings.ToLower(strings.TrimSpace(field))
		if field == "" {
			continue
		}
		for i, line := range lines {
			trimmed := strings.TrimSpace(line)
			if projectPlanningMarkdownHeadingMatchesField(trimmed, field) {
				out = append(out, projectPlanningFollowingFieldValues(lines[i+1:])...)
				continue
			}
			for _, sep := range []string{":", "="} {
				token := field + sep
				idx := strings.Index(trimmed, token)
				if idx < 0 {
					continue
				}
				if !strings.HasPrefix(trimmed, token) && !strings.HasPrefix(trimmed, "- "+token) {
					continue
				}
				value := strings.TrimSpace(trimmed[idx+len(token):])
				out = append(out, projectPlanningSplitFieldValues(value)...)
				if projectPlanningMeaningfulFieldValue(value) {
					continue
				}
				out = append(out, projectPlanningFollowingFieldValues(lines[i+1:])...)
			}
		}
	}
	return out
}

func projectPlanningFollowingFieldValues(lines []string) []string {
	var out []string
	for _, next := range lines {
		next = strings.TrimSpace(next)
		if next == "" {
			continue
		}
		if strings.HasPrefix(next, "#") {
			break
		}
		if strings.Contains(next, ":") && !strings.HasPrefix(next, "-") {
			break
		}
		if strings.HasPrefix(next, "-") {
			item := strings.TrimSpace(strings.TrimPrefix(next, "-"))
			if key, value, ok := strings.Cut(item, ":"); ok && projectPlanningNestedTraceFieldAllowed(key) {
				out = append(out, projectPlanningSplitFieldValues(value)...)
				continue
			}
			out = append(out, projectPlanningSplitFieldValues(item)...)
			continue
		}
		out = append(out, projectPlanningSplitFieldValues(next)...)
	}
	return out
}

func projectPlanningMarkdownHeadingMatchesField(line, field string) bool {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "#") {
		return false
	}
	line = strings.TrimSpace(strings.TrimLeft(line, "#"))
	line = strings.TrimSpace(strings.TrimSuffix(line, ":"))
	return projectPlanningCanonicalFieldName(line) == projectPlanningCanonicalFieldName(field)
}

func projectPlanningCanonicalFieldName(value string) string {
	value = projectPlanningNormalizeFieldToken(value)
	value = strings.NewReplacer("_", " ", "-", " ").Replace(value)
	return strings.Join(strings.Fields(value), " ")
}

func projectPlanningInlineFieldValue(line, field string) (string, bool) {
	for _, sep := range []string{":", "="} {
		prefix := field + sep
		if strings.HasPrefix(line, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(line, prefix)), true
		}
	}
	return "", false
}

func projectPlanningFollowingListValue(lines []string) string {
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.Contains(trimmed, ":") && !strings.HasPrefix(trimmed, "-") {
			return ""
		}
		trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "-"))
		if projectPlanningMeaningfulFieldValue(trimmed) {
			return trimmed
		}
	}
	return ""
}

func projectPlanningNestedTraceFieldAllowed(key string) bool {
	switch projectPlanningNormalizeFieldToken(key) {
	case "anchor", "id", "label", "name", "requirement", "criterion", "acceptance_id", "non_goal", "wrong_product":
		return true
	default:
		return projectPlanningTraceListItemIDLabel(key)
	}
}

func projectPlanningTraceListItemIDLabel(key string) bool {
	key = projectPlanningNormalizeFieldToken(key)
	if key == "" {
		return false
	}
	parts := strings.FieldsFunc(key, func(r rune) bool {
		return r == '-' || r == '_' || r == ' ' || r == '.'
	})
	if len(parts) == 1 {
		part := parts[0]
		firstDigit := -1
		for i, r := range part {
			if r >= '0' && r <= '9' {
				firstDigit = i
				break
			}
		}
		if firstDigit <= 0 || firstDigit >= len(part) {
			return false
		}
		parts = []string{part[:firstDigit], part[firstDigit:]}
	}
	if len(parts) != 2 {
		return false
	}
	prefix, suffix := parts[0], parts[1]
	if prefix == "" || suffix == "" || len(prefix) > 12 || len(suffix) > 8 {
		return false
	}
	for _, r := range prefix {
		if r < 'a' || r > 'z' {
			return false
		}
	}
	hasDigit := false
	for _, r := range suffix {
		switch {
		case r >= '0' && r <= '9':
			hasDigit = true
		case r >= 'a' && r <= 'z':
		default:
			return false
		}
	}
	return hasDigit
}

func projectPlanningSplitFieldValues(value string) []string {
	value = strings.TrimSpace(value)
	if !projectPlanningMeaningfulFieldValue(value) {
		return nil
	}
	value = strings.TrimSpace(strings.Trim(value, "[]{}"))
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = projectPlanningNormalizeFieldToken(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func projectPlanningTraceValueSpecific(value string) bool {
	value = projectPlanningNormalizeFieldToken(value)
	if !projectPlanningMeaningfulFieldValue(value) {
		return false
	}
	switch value {
	case "acceptance criteria",
		"acceptance criteria refs",
		"adjacent product",
		"adjacent wrong product",
		"adjacent wrong products",
		"app",
		"application",
		"basic functionality",
		"core experience",
		"core flow",
		"feature",
		"happy path",
		"implementation",
		"main feature",
		"main flow",
		"mvp",
		"primary flow",
		"product",
		"requirements",
		"source requirements",
		"source spec",
		"user experience",
		"user flow":
		return false
	default:
		return true
	}
}

func projectPlanningNormalizeFieldToken(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.TrimSpace(strings.Trim(value, "`'\"[]{}"))
	value = strings.TrimRight(value, ".,;")
	return strings.TrimSpace(value)
}

func projectPlanningMeaningfulFieldValue(value string) bool {
	value = strings.TrimSpace(strings.Trim(value, "`[]"))
	if value == "" {
		return false
	}
	switch value {
	case "todo", "tbd", "none", "n/a", "na", "...", "?", "<todo>", "<fill>", "fill me", "fill in", "placeholder":
		return false
	default:
		return true
	}
}

func normalizeProjectPhaseTransitionToolPhase(value string) string {
	phase := strings.ToUpper(strings.TrimSpace(value))
	phase = strings.ReplaceAll(phase, "-", "_")
	phase = strings.ReplaceAll(phase, " ", "_")
	switch phase {
	case "SPEC", "PLANNING", "IMPLEMENTATION", "REVIEW", "INTEGRATION", "VALIDATION", "DONE":
		return phase
	default:
		return ""
	}
}

func projectPhaseTransitionOpensImplementation(phase string) bool {
	switch strings.ToUpper(strings.TrimSpace(phase)) {
	case "IMPLEMENTATION", "REVIEW", "INTEGRATION", "VALIDATION":
		return true
	default:
		return false
	}
}

func projectPhaseTransitionOpeningImplementation(fromPhase, toPhase string) bool {
	return projectPhaseTransitionOpensImplementation(toPhase) && !projectPhaseTransitionOpensImplementation(fromPhase)
}

func projectPhaseTransitionMissingNonPhaseGates(status ProjectGateStatusRecord) []string {
	missing := []string{}
	for _, gate := range status.Gates {
		if !gate.Required {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(gate.GateKey), "implementation_phase_open") {
			continue
		}
		state := strings.ToUpper(strings.TrimSpace(gate.State))
		if state == "SATISFIED" || state == "WAIVED" {
			continue
		}
		item := strings.TrimSpace(gate.GateKey)
		if gate.Summary != "" {
			item += ": " + strings.TrimSpace(gate.Summary)
		} else if gate.State != "" {
			item += ": " + gate.State
		}
		missing = append(missing, item)
	}
	return missing
}

func projectPhaseTransitionMissingGateBlockMessage(projectID string, missing []string) string {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		projectID = "<project_id>"
	}
	docKey := "project." + sanitizeDocKeySegment(projectID) + ".design_and_plan"
	return "project_phase_transition blocked; satisfy non-phase gates first: " + strings.Join(missing, "; ") +
		". For code/product projects, publish a canonical `" + docKey + "` workspace doc; workspace_doc_put will sync it as both design_doc_id and implementation_plan_doc_id before IMPLEMENTATION can open."
}
