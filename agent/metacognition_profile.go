package main

import (
	"fmt"
	"strings"
)

const (
	reflectionScopeLocal    = "local"
	reflectionScopeArtifact = "artifact"
	reflectionScopeProject  = "project"
	reflectionScopeGlobal   = "global"

	idlePolicySelfCheck              = "self_check"
	idlePolicyReviewArtifact         = "review_artifact"
	idlePolicyJoinExistingReflection = "join_existing_reflection"
	idlePolicyOpenUncoveredDirection = "open_uncovered_direction"
)

type AgentMetacognitionPolicy struct {
	ReflectionScope          string
	IdleActionPolicy         string
	CanOpenReflectionTasks   bool
	MaxNewTasksPerIdleCycle  int
	ScopeDescription         string
	IdleBehaviorDescription  string
	TaskCreationDescription  string
	ReflectionJoinGuidance   string
	StructuredReflectionHint string
	RealityCheckDescription  string
}

func normalizeReflectionScope(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "-", "_")
	normalized = strings.ReplaceAll(normalized, " ", "_")
	switch normalized {
	case reflectionScopeLocal, "task", "own_task", "lane":
		return reflectionScopeLocal
	case reflectionScopeArtifact, "artifact_review", "review", "qa":
		return reflectionScopeArtifact
	case reflectionScopeProject, "project_wide", "projectwide":
		return reflectionScopeProject
	case reflectionScopeGlobal, "workspace", "system", "systemic":
		return reflectionScopeGlobal
	default:
		return ""
	}
}

func normalizeIdleActionPolicy(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "-", "_")
	normalized = strings.ReplaceAll(normalized, " ", "_")
	switch normalized {
	case idlePolicySelfCheck, "heartbeat", "local_heartbeat", "local_self_check":
		return idlePolicySelfCheck
	case idlePolicyReviewArtifact, "artifact_review", "qa", "review":
		return idlePolicyReviewArtifact
	case idlePolicyJoinExistingReflection, "join", "join_existing", "reuse_existing_reflection":
		return idlePolicyJoinExistingReflection
	case idlePolicyOpenUncoveredDirection, "open_uncovered", "create_uncovered_direction", "open_new_direction":
		return idlePolicyOpenUncoveredDirection
	default:
		return ""
	}
}

func normalizeMetacognitionPolicy(profile AgentProfile) AgentMetacognitionPolicy {
	defaults := defaultMetacognitionPolicyForProfile(profile)
	semanticScope := semanticReflectionScopeForProfile(profile)
	scopeFloor := defaults.ReflectionScope
	if reflectionScopeRank(semanticScope) > reflectionScopeRank(scopeFloor) {
		scopeFloor = semanticScope
	}

	scope := normalizeReflectionScope(profile.ReflectionScope)
	if scope == "" {
		scope = scopeFloor
	}
	policy := normalizeIdleActionPolicy(profile.IdleActionPolicy)
	if policy == "" {
		policy = defaults.IdleActionPolicy
	}
	scopeLifted := false
	if reflectionScopeRank(scopeFloor) > reflectionScopeRank(scope) && reflectionScopeRank(scopeFloor) >= reflectionScopeRank(reflectionScopeArtifact) {
		scope = scopeFloor
		scopeLifted = true
	}
	if scopeLifted {
		policy = defaults.IdleActionPolicy
	}
	canOpen := defaults.CanOpenReflectionTasks
	if profile.CanOpenReflectionTasks != nil {
		canOpen = *profile.CanOpenReflectionTasks
	}
	if scopeLifted && defaults.CanOpenReflectionTasks {
		canOpen = true
	}
	maxNew := profile.MaxNewTasksPerIdleCycle
	if maxNew <= 0 {
		maxNew = defaults.MaxNewTasksPerIdleCycle
	}
	if scopeLifted && defaults.MaxNewTasksPerIdleCycle > maxNew {
		maxNew = defaults.MaxNewTasksPerIdleCycle
	}
	if !canOpen {
		maxNew = 0
	}
	if maxNew > 3 {
		maxNew = 3
	}

	qualityBar := ""
	if profileHasUXRealityCriticSignal(profile) {
		qualityBar = uxRealityCriticGuidance()
	}

	return describeMetacognitionPolicy(AgentMetacognitionPolicy{
		ReflectionScope:         scope,
		IdleActionPolicy:        policy,
		CanOpenReflectionTasks:  canOpen,
		MaxNewTasksPerIdleCycle: maxNew,
		RealityCheckDescription: qualityBar,
	})
}

func reflectionScopeRank(scope string) int {
	switch normalizeReflectionScope(scope) {
	case reflectionScopeLocal:
		return 0
	case reflectionScopeArtifact:
		return 1
	case reflectionScopeProject:
		return 2
	case reflectionScopeGlobal:
		return 3
	default:
		return -1
	}
}

func semanticReflectionScopeForProfile(profile AgentProfile) string {
	if profileHasArtifactBoundMetacognitionSignal(profile) {
		return reflectionScopeArtifact
	}
	text := strings.ToLower(strings.Join([]string{
		profile.Role,
		profile.PrimarySpecialization,
		profile.DefaultWorkMode,
	}, " "))
	if text == "" {
		return ""
	}
	switch {
	case containsAnySignal(text, []string{
		"global meta-cognition",
		"global metacognition",
		"global strategy",
		"workspace strategy",
		"workspace steward",
		"system steward",
		"systemic steward",
		"systemic observer",
		"meta-analysis",
		"meta analysis",
		"portfolio",
		"venture",
		"service factory",
		"service-factory",
		"market scout",
		"opportunity",
		"growth",
		"monetization",
		"advertising",
		"revenue",
		"governance",
	}):
		return reflectionScopeGlobal
	case containsAnySignal(text, []string{
		"strategist",
		"strategy",
		"strategic",
		"planner",
		"planning",
		"project lead",
		"lead strategist",
		"coordinator",
		"coordination",
		"architecture",
		"architect",
		"integrator",
		"integration",
		"synthesis",
		"synthesizer",
		"handoff",
		"finalization",
		"release",
		"deploy",
		"deployment",
		"ops",
	}):
		return reflectionScopeProject
	case containsAnySignal(text, []string{
		"ui/ux",
		"ui ux",
		"user experience",
		"ux critic",
		"reviewer",
		"review",
		"qa",
		"tester",
		"testing",
		"verifier",
		"validation",
		"accessibility",
		"usability",
	}):
		return reflectionScopeArtifact
	case containsAnySignal(text, []string{
		"implementer",
		"implementation",
		"builder",
		"worker",
		"frontend",
		"backend",
		"fullstack",
		"full-stack",
		"coder",
	}):
		return reflectionScopeLocal
	default:
		return ""
	}
}

func profileHasArtifactBoundMetacognitionSignal(profile AgentProfile) bool {
	if profileHasUXRealityCriticSignal(profile) {
		return true
	}
	text := strings.ToLower(strings.Join([]string{
		profile.Role,
		profile.PrimarySpecialization,
		profile.DefaultWorkMode,
	}, " "))
	return containsAnySignal(text, []string{
		"reviewer",
		"review",
		"qa",
		"tester",
		"testing",
		"verifier",
		"validation",
		"accessibility",
		"usability",
	})
}

type agentMetacognitionFieldDefaults struct {
	scope   bool
	idle    bool
	canOpen bool
	maxNew  bool
}

func metacognitionFieldsMatchRoleDefaults(profile AgentProfile) agentMetacognitionFieldDefaults {
	defaults := defaultMetacognitionPolicyForProfile(profile)
	scope := normalizeReflectionScope(profile.ReflectionScope)
	idle := normalizeIdleActionPolicy(profile.IdleActionPolicy)
	canOpen := profile.CanOpenReflectionTasks == nil || *profile.CanOpenReflectionTasks == defaults.CanOpenReflectionTasks
	maxNew := profile.MaxNewTasksPerIdleCycle <= 0
	if defaults.MaxNewTasksPerIdleCycle > 0 {
		maxNew = profile.MaxNewTasksPerIdleCycle == defaults.MaxNewTasksPerIdleCycle
	}
	return agentMetacognitionFieldDefaults{
		scope:   profile.ReflectionScope == "" || scope == "" || scope == defaults.ReflectionScope,
		idle:    profile.IdleActionPolicy == "" || idle == "" || idle == defaults.IdleActionPolicy,
		canOpen: canOpen,
		maxNew:  maxNew,
	}
}

func agentProfileForRuntimeConfig(cfg RuntimeConfig) AgentProfile {
	profile := loadAgentProfileRaw(cfg.Workdir)
	roleOverride := strings.TrimSpace(cfg.Role)
	roleChanged := roleOverride != "" && !strings.EqualFold(roleOverride, strings.TrimSpace(profile.Role))
	if roleChanged {
		defaulted := metacognitionFieldsMatchRoleDefaults(profile)
		if defaulted.scope {
			profile.ReflectionScope = ""
		}
		if defaulted.idle {
			profile.IdleActionPolicy = ""
		}
		if defaulted.canOpen {
			profile.CanOpenReflectionTasks = nil
		}
		if defaulted.maxNew {
			profile.MaxNewTasksPerIdleCycle = 0
		}
		if strings.TrimSpace(profile.PrimarySpecialization) == "" || strings.EqualFold(strings.TrimSpace(profile.PrimarySpecialization), strings.TrimSpace(profile.Role)) {
			profile.PrimarySpecialization = ""
		}
		if strings.TrimSpace(profile.DefaultWorkMode) == "" || strings.EqualFold(strings.TrimSpace(profile.DefaultWorkMode), strings.TrimSpace(profile.Role)) {
			profile.DefaultWorkMode = ""
		}
	}
	profile.AgentID = firstNonEmpty(profile.AgentID, cfg.AgentID)
	profile.DisplayName = firstNonEmpty(profile.DisplayName, cfg.DisplayName, cfg.AgentID)
	profile.GroupID = firstNonEmpty(cfg.GroupID, profile.GroupID)
	profile.Role = firstNonEmpty(cfg.Role, profile.Role)
	profile.PrimarySpecialization = firstNonEmpty(profile.PrimarySpecialization, profile.Role)
	profile.DefaultWorkMode = firstNonEmpty(profile.DefaultWorkMode, profile.Role)
	return normalizeAgentProfile(profile)
}

func defaultMetacognitionPolicyForProfile(profile AgentProfile) AgentMetacognitionPolicy {
	if profileHasUXRealityCriticSignal(profile) {
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeArtifact,
			IdleActionPolicy:        idlePolicyReviewArtifact,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 1,
		}
	}
	role := strings.ToLower(strings.TrimSpace(profile.Role))
	switch {
	case containsAnySignal(role, []string{"steward", "observer", "meta-analysis", "meta analysis", "systemic"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeGlobal,
			IdleActionPolicy:        idlePolicyJoinExistingReflection,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 2,
		}
	case containsAnySignal(role, []string{"portfolio", "venture", "service factory", "service-factory", "market", "scout", "opportunity", "growth", "seo", "monetization", "ads", "advertising", "revenue", "telemetry", "analytics", "finance", "resource", "governance"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeGlobal,
			IdleActionPolicy:        idlePolicyOpenUncoveredDirection,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 3,
		}
	case containsAnySignal(role, []string{"deploy", "deployment", "release", "ops", "cloudflare", "vercel"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeProject,
			IdleActionPolicy:        idlePolicyJoinExistingReflection,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 2,
		}
	case containsAnySignal(role, []string{"compliance", "policy", "ad-policy", "safety"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeArtifact,
			IdleActionPolicy:        idlePolicyReviewArtifact,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 1,
		}
	case containsAnySignal(role, []string{"strategist", "strategy", "strategic", "planner", "planning", "lead", "coordinator", "architecture", "architect"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeProject,
			IdleActionPolicy:        idlePolicyOpenUncoveredDirection,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 3,
		}
	case containsAnySignal(role, []string{"integrator", "integration", "synthesis", "synthesizer", "handoff", "finalization"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeProject,
			IdleActionPolicy:        idlePolicyJoinExistingReflection,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 2,
		}
	case containsAnySignal(role, []string{"reviewer", "review", "qa", "tester", "testing", "verifier", "validation", "accessibility", "usability"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeArtifact,
			IdleActionPolicy:        idlePolicyReviewArtifact,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 1,
		}
	case containsAnySignal(role, []string{"implementer", "implementation", "builder", "worker", "frontend", "backend", "fullstack", "full-stack", "coder"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeLocal,
			IdleActionPolicy:        idlePolicySelfCheck,
			CanOpenReflectionTasks:  false,
			MaxNewTasksPerIdleCycle: 0,
		}
	}

	signals := strings.ToLower(strings.Join(append([]string{
		profile.Role,
		profile.PrimarySpecialization,
		profile.DefaultWorkMode,
	}, profile.SecondarySpecializations...), " "))

	switch {
	case containsAnySignal(signals, []string{"steward", "observer", "meta-analysis", "meta analysis", "systemic"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeGlobal,
			IdleActionPolicy:        idlePolicyJoinExistingReflection,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 2,
		}
	case containsAnySignal(signals, []string{"portfolio", "venture", "service factory", "service-factory", "market", "scout", "opportunity", "growth", "seo", "monetization", "ads", "advertising", "revenue", "telemetry", "analytics", "finance", "resource", "governance"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeGlobal,
			IdleActionPolicy:        idlePolicyOpenUncoveredDirection,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 3,
		}
	case containsAnySignal(signals, []string{"deploy", "deployment", "release", "ops", "cloudflare", "vercel"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeProject,
			IdleActionPolicy:        idlePolicyJoinExistingReflection,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 2,
		}
	case containsAnySignal(signals, []string{"compliance", "policy", "ad-policy", "safety"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeArtifact,
			IdleActionPolicy:        idlePolicyReviewArtifact,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 1,
		}
	case containsAnySignal(signals, []string{"strategist", "strategy", "strategic", "planner", "planning", "lead", "coordinator", "architecture", "architect"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeProject,
			IdleActionPolicy:        idlePolicyOpenUncoveredDirection,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 3,
		}
	case containsAnySignal(signals, []string{"integrator", "integration", "synthesis", "synthesizer", "handoff", "finalization"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeProject,
			IdleActionPolicy:        idlePolicyJoinExistingReflection,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 2,
		}
	case containsAnySignal(signals, []string{"reviewer", "review", "qa", "tester", "testing", "verifier", "validation", "accessibility", "usability"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeArtifact,
			IdleActionPolicy:        idlePolicyReviewArtifact,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 1,
		}
	case containsAnySignal(signals, []string{"implementer", "implementation", "builder", "worker", "frontend", "backend", "fullstack", "full-stack", "coder"}):
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeLocal,
			IdleActionPolicy:        idlePolicySelfCheck,
			CanOpenReflectionTasks:  false,
			MaxNewTasksPerIdleCycle: 0,
		}
	default:
		return AgentMetacognitionPolicy{
			ReflectionScope:         reflectionScopeArtifact,
			IdleActionPolicy:        idlePolicyReviewArtifact,
			CanOpenReflectionTasks:  true,
			MaxNewTasksPerIdleCycle: 1,
		}
	}
}

func containsAnySignal(text string, signals []string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	for _, signal := range signals {
		signal = strings.ToLower(strings.TrimSpace(signal))
		if signal != "" && strings.Contains(text, signal) {
			return true
		}
	}
	return false
}

func profileHasUXRealityCriticSignal(profile AgentProfile) bool {
	parts := []string{
		profile.Role,
		profile.PrimarySpecialization,
		profile.DefaultWorkMode,
		profile.Mission,
	}
	parts = append(parts, profile.SecondarySpecializations...)
	parts = append(parts, profile.DomainScope...)
	signals := strings.ToLower(strings.Join(parts, " "))
	phrases := []string{
		"ui/ux",
		"ui ux",
		"user experience",
		"ux review",
		"ux-review",
		"ux qa",
		"ux/qa",
		"ux critic",
		"product critic",
		"real user",
		"real-user",
		"interaction design",
		"visual design",
		"visual polish",
		"responsive ergonomics",
		"usability",
	}
	if containsAnySignal(signals, phrases) {
		return true
	}
	replacer := strings.NewReplacer("/", " ", "-", " ", "_", " ", ",", " ", ";", " ", ":", " ", ".", " ", "\n", " ", "\t", " ")
	tokens := map[string]bool{}
	for _, token := range strings.Fields(replacer.Replace(signals)) {
		tokens[token] = true
	}
	return tokens["ux"] || tokens["usability"]
}

func uxRealityCriticGuidance() string {
	return "UI/UX reality-critic mandate: act as a harsh real-user critic, not a design cheerleader. Exercise real usage scenarios end-to-end in the runnable artifact when possible. Reject superficial acceptance based on screenshots, component presence, nonblank canvas checks, passing builds, plausible intent, or low generic layout-risk scores from probes. Report visible failures with concrete evidence: viewport/device, URL or route, branch/head/checkout, command or browser run, observed behavior, expected user outcome, severity, and the smallest repair direction. A pass for UI-facing work must inspect the first viewport/empty state, primary flow, and post-action/output/result state with state-specific screenshot refs. Check layout overflow, distorted or misleading previews, unreadable text, awkward controls, primary surface geometry, board/grid/canvas/chart/editor density, mode/preset/difficulty-specific fit, missing loading/error/empty states, keyboard/accessibility basics, responsiveness, typography/hierarchy/spacing, and performance symptoms a real user would notice."
}

func describeMetacognitionPolicy(policy AgentMetacognitionPolicy) AgentMetacognitionPolicy {
	switch policy.ReflectionScope {
	case reflectionScopeLocal:
		policy.ScopeDescription = "Local/task scope: inspect your current task, owned files, fresh blocker evidence, and profile-fit next work before looking wider."
	case reflectionScopeArtifact:
		policy.ScopeDescription = "Artifact scope: inspect the runnable artifact, specs, tests, acceptance criteria, and concrete evidence around the product slice."
	case reflectionScopeProject:
		policy.ScopeDescription = "Project scope: inspect project goals, roles, task topology, blockers, acceptance criteria, integration state, and post-MVP improvement lanes."
	case reflectionScopeGlobal:
		policy.ScopeDescription = "Global/workspace scope: inspect cross-project coordination, systemic blockers, missing tools, and long-lived operating patterns."
	default:
		policy.ScopeDescription = "Use the narrowest current scope that still lets you make useful progress."
	}

	switch policy.IdleActionPolicy {
	case idlePolicySelfCheck:
		policy.IdleBehaviorDescription = "On idle heartbeat, self-check for stale work, wasted effort, current blockers, and profile-fit tasks; avoid spawning broad reflection work."
	case idlePolicyReviewArtifact:
		policy.IdleBehaviorDescription = "On idle heartbeat, run a bounded artifact/spec/test/UX review and create or claim only concrete follow-ups tied to observed evidence."
	case idlePolicyJoinExistingReflection:
		policy.IdleBehaviorDescription = "On idle heartbeat, first look for existing active reflection or review threads in scope, including project.<project_id>.reflection_board when project-scoped, and join or extend them before opening another direction."
	case idlePolicyOpenUncoveredDirection:
		policy.IdleBehaviorDescription = "On idle heartbeat, inspect current reflections and the shared reflection board first; if an important direction is uncovered, open a bounded project reflection/improvement lane and ask for critique when it could change product direction."
	default:
		policy.IdleBehaviorDescription = "On idle heartbeat, verify fresh reality and choose the smallest useful next move within your scope."
	}

	if policy.CanOpenReflectionTasks {
		policy.TaskCreationDescription = fmt.Sprintf("May open at most %d new reflection/review/improvement task(s) per idle cycle, after checking for similar active work.", policy.MaxNewTasksPerIdleCycle)
	} else {
		policy.TaskCreationDescription = "Do not open broad reflection tasks automatically; publish concise local evidence and claim or request profile-fit work."
	}
	policy.ReflectionJoinGuidance = "Before creating reflection work, inspect visible tasks/docs/updates and the shared reflection board for related active reflection. Join or extend a solo in-scope discussion when useful; otherwise explore a genuinely uncovered direction in parallel and invite relevant peers. Substantial reflection should invite bounded critique before it becomes product-direction work."
	policy.StructuredReflectionHint = "Structured reflection is still expected for every task cycle; keep it public, concise, and scoped to the policy above."
	return policy
}

func metacognitionPolicyForRuntimeConfig(cfg RuntimeConfig) AgentMetacognitionPolicy {
	return normalizeMetacognitionPolicy(agentProfileForRuntimeConfig(cfg))
}

func renderMetacognitionPromptSection(policy AgentMetacognitionPolicy) string {
	policy = describeMetacognitionPolicy(policy)
	var b strings.Builder
	b.WriteString("## Metacognition And Initiative Profile\n")
	b.WriteString("- reflection_scope: " + policy.ReflectionScope + "\n")
	b.WriteString("- idle_action_policy: " + policy.IdleActionPolicy + "\n")
	b.WriteString(fmt.Sprintf("- can_open_reflection_tasks: %t\n", policy.CanOpenReflectionTasks))
	b.WriteString(fmt.Sprintf("- max_new_tasks_per_idle_cycle: %d\n", policy.MaxNewTasksPerIdleCycle))
	b.WriteString("- scope: " + policy.ScopeDescription + "\n")
	b.WriteString("- idle_behavior: " + policy.IdleBehaviorDescription + "\n")
	b.WriteString("- task_creation: " + policy.TaskCreationDescription + "\n")
	b.WriteString("- autonomy_contract: On every idle heartbeat, produce one concrete outcome: join/extend existing work, create a bounded follow-up task when allowed, or publish fresh evidence explaining why no action is useful now.\n")
	b.WriteString("- service_factory_contract: For service/product work, separate local MVP, integrated product, deployed service, monetized service, and measured outcome; do not collapse these into one done state.\n")
	if strings.TrimSpace(policy.RealityCheckDescription) != "" {
		b.WriteString("- reality_check_quality_bar: " + strings.TrimSpace(policy.RealityCheckDescription) + "\n")
	}
	b.WriteString("- join_policy: " + policy.ReflectionJoinGuidance + "\n")
	b.WriteString("- structured_reflection: " + policy.StructuredReflectionHint + "\n")
	return strings.TrimSpace(b.String())
}

func renderRuntimeMetacognitionPrompt(cfg RuntimeConfig) string {
	return renderMetacognitionPromptSection(metacognitionPolicyForRuntimeConfig(cfg))
}

func renderIdleNextActionForPolicy(policy AgentMetacognitionPolicy) string {
	policy = describeMetacognitionPolicy(policy)
	return policy.IdleBehaviorDescription + " " + policy.TaskCreationDescription
}
