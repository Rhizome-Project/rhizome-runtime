package main

import (
	"fmt"
	"strings"
)

func (r *Runtime) normalizeAutonomyResult(result StructuredTaskResult) StructuredTaskResult {
	result.DecisionType = inferDecisionType(result)
	for idx := range result.BlockedOn {
		result.BlockedOn[idx].Kind = normalizeBlockedKind(result.BlockedOn[idx].Kind, result.BlockedOn[idx].Detail)
		result.BlockedOn[idx].Detail = strings.TrimSpace(result.BlockedOn[idx].Detail)
	}

	requiresHuman := shouldRequireHumanIntervention(result)
	if requiresHuman {
		result.RequiresHuman = true
		result.OwnerAction = firstNonEmpty(result.OwnerAction, defaultOwnerActionForDecision(result.DecisionType))
		result.HumanReason = firstNonEmpty(result.HumanReason, defaultHumanReasonForDecision(result))
	} else if result.RequiresHuman {
		result.RequiresHuman = false
		result.OwnerAction = ""
		result.HumanReason = ""
		result.Details = appendAutonomyNote(result.Details, "routine human escalation suppressed by autonomy policy")
	}

	if normalizeOutcome(result.Outcome) == "blocked" && len(result.BlockedOn) == 0 {
		result.BlockedOn = normalizedBlockedRefs(result.BlockedOn, firstNonEmpty(result.HumanReason, result.Summary))
	}

	return result
}

func shouldSurfaceOperatorQueue(result StructuredTaskResult) bool {
	return shouldRequireHumanIntervention(result)
}

func shouldEndRoutineDependencyBlockedSession(result StructuredTaskResult) bool {
	if normalizeOutcome(result.Outcome) != "blocked" || result.RequiresHuman || shouldSurfaceOperatorQueue(result) {
		return false
	}
	if len(result.BlockedOn) == 0 {
		return false
	}
	for _, item := range result.BlockedOn {
		if !isRoutineSessionEndingBlocker(item) {
			return false
		}
	}
	return true
}

func isRoutineSessionEndingBlocker(item BlockedRef) bool {
	if normalizeBlockedKind(item.Kind, item.Detail) == "dependency" {
		return true
	}
	kind := strings.ToLower(strings.TrimSpace(item.Kind))
	detail := strings.ToLower(strings.TrimSpace(item.Detail))
	if kind != "patch_queue_supersede" && !strings.Contains(detail, "patch queue supersede") && !strings.Contains(detail, "patch_queue_supersede") {
		return false
	}
	return strings.Contains(detail, "parked") ||
		strings.Contains(detail, "do not retry") ||
		strings.Contains(detail, "fresh positive") ||
		strings.Contains(detail, "fresh same-head")
}

func shouldRequireHumanIntervention(result StructuredTaskResult) bool {
	decisionType := inferDecisionType(result)
	switch decisionType {
	case "credential", "payment", "approval":
		return hasConcreteHumanGateEvidence(result, decisionType)
	default:
		return false
	}
}

func inferDecisionType(result StructuredTaskResult) string {
	decision := normalizeDecisionType(result.DecisionType)
	if decision != "" {
		return decision
	}

	for _, item := range result.BlockedOn {
		kind := normalizeBlockedKind(item.Kind, item.Detail)
		switch kind {
		case "credential":
			return "credential"
		case "payment":
			return "payment"
		case "approval":
			return "approval"
		}
	}

	if normalizeOutcome(result.Outcome) != "blocked" && !result.RequiresHuman {
		return ""
	}

	text := strings.ToLower(strings.Join([]string{
		result.Summary,
		result.Details,
		result.NextAction,
		result.OwnerAction,
		result.HumanReason,
		blockedDetails(result.BlockedOn),
	}, "\n"))

	switch {
	case containsAny(text, "policy requires approval", "needs approval", "need approval", "requires approval", "approve "):
		return "approval"
	case containsAny(text, "billing", "payment", "invoice", "card", "checkout", "subscription", "purchase"):
		return "payment"
	case containsAny(text, "oauth", "login", "sign in", "signin", "2fa", "otp", "captcha", "credential", "password", "token", "api key", "authorization", "authorize"):
		return "credential"
	case containsAny(text, "priority"):
		return "priority"
	case containsAny(text, "scope"):
		return "scope"
	case containsAny(text, "review"):
		return "review"
	default:
		return ""
	}
}

func hasConcreteHumanGateEvidence(result StructuredTaskResult, decisionType string) bool {
	decisionType = normalizeDecisionType(decisionType)
	for _, item := range result.BlockedOn {
		if blockedRefMatchesHumanGate(item, decisionType) {
			return true
		}
	}
	if normalizeDecisionType(result.DecisionType) == "approval" && decisionType == "approval" && result.RequiresHuman {
		text := strings.ToLower(strings.Join([]string{
			result.Summary,
			result.Details,
			result.OwnerAction,
			result.HumanReason,
			blockedDetails(result.BlockedOn),
		}, "\n"))
		return containsAny(text, "policy requires approval", "privileged approval", "explicit approval")
	}
	return false
}

func blockedRefMatchesHumanGate(item BlockedRef, decisionType string) bool {
	rawKind := strings.ToLower(strings.TrimSpace(item.Kind))
	kind := normalizeBlockedKind(rawKind, item.Detail)
	detail := strings.ToLower(strings.TrimSpace(item.Detail))
	switch decisionType {
	case "credential":
		return kind == "credential" || containsAny(detail, "oauth", "login", "sign in", "signin", "2fa", "otp", "captcha", "credential", "password", "token", "api key", "authorization", "authorize")
	case "payment":
		if rawKind != "payment" && rawKind != "billing" && kind != "payment" {
			return false
		}
		return containsAny(detail, "billing portal", "checkout page", "invoice", "card", "payment method", "subscription", "purchase")
	case "approval":
		return (rawKind == "approval" || kind == "approval") && containsAny(detail, "policy requires approval", "privileged approval", "explicit approval", "requires approval", "permission")
	default:
		return false
	}
}

func normalizeDecisionType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "approval":
		return "approval"
	case "review":
		return "review"
	case "priority":
		return "priority"
	case "scope":
		return "scope"
	case "credential", "auth", "authorization":
		return "credential"
	case "payment", "billing":
		return "payment"
	case "unknown":
		return "unknown"
	default:
		return ""
	}
}

func normalizeBlockedKind(kind, detail string) string {
	kind = strings.ToLower(strings.TrimSpace(kind))
	switch kind {
	case "dependency":
		return kind
	case "credential", "auth", "authorization":
		return "credential"
	case "payment", "billing":
		return "payment"
	case "approval":
		return "approval"
	}

	text := strings.ToLower(strings.TrimSpace(detail))
	switch kind {
	case "human", "tool", "runtime":
		switch {
		case blockedDetailLooksConcretePaymentGate(text):
			return "payment"
		case containsAny(text, "oauth", "login", "sign in", "2fa", "otp", "captcha", "credential", "password", "token", "api key", "authorization", "authorize"):
			return "credential"
		case blockedDetailLooksConcreteApprovalGate(text):
			return "approval"
		case blockedDetailLooksLocalExecutableFailure(text):
			return kind
		}
		if blockedDetailLooksPeerOwnershipDependency(text) || blockedDetailLooksReviewEvidenceDependency(text) {
			return "dependency"
		}
		return kind
	}
	if blockedDetailLooksPeerDependency(text) {
		return "dependency"
	}
	switch {
	case blockedDetailLooksConcretePaymentGate(text):
		return "payment"
	case containsAny(text, "oauth", "login", "sign in", "2fa", "otp", "captcha", "credential", "password", "token", "api key", "authorization", "authorize"):
		return "credential"
	case blockedDetailLooksConcreteApprovalGate(text):
		return "approval"
	default:
		if kind == "" {
			return "unknown"
		}
		return kind
	}
}

func blockedDetailLooksPeerOwnershipDependency(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	return containsAny(text,
		"ownership conflict",
		"owned by another agent",
		"claimed by another agent",
		"already claimed by",
		"another agent already",
		"peer already",
	)
}

func blockedDetailLooksPeerDependency(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	return blockedDetailLooksPeerOwnershipDependency(text) || containsAny(text,
		"active checkout",
		"active branch",
		"branch evidence",
		"checkout evidence",
		"review-ready branch",
		"waiting on implementation task",
		"waiting for implementation task",
		"wait for implementation task",
		"no permissible runnable lane",
	) || blockedDetailLooksReviewEvidenceDependency(text)
}

func blockedDetailLooksReviewEvidenceDependency(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	return containsAny(text,
		"runtime/browser verification evidence",
		"browser verification evidence",
		"browser smoke evidence",
		"smoke evidence",
		"runnable smoke",
		"smoke check evidence",
		"review packet",
		"review evidence",
		"peer review evidence",
		"reviewer feedback",
		"channel mapping verification",
		"channel-mapping verification",
		"pipeline verification evidence",
		"upstream verification evidence",
		"upstream evidence",
	)
}

func blockedDetailLooksLocalExecutableFailure(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	return containsAny(text,
		"local executable failure",
		"executable failure",
		"executable failed",
		"command failed",
		"tool failed",
		"process failed",
		"process error",
		"exit status",
		"exited with",
		"permission denied",
		"access denied",
	)
}

func blockedDetailLooksConcretePaymentGate(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	return containsAny(text,
		"billing",
		"payment",
		"invoice",
		"credit card",
		"debit card",
		"card payment",
		"payment method",
		"checkout page",
		"payment checkout",
		"subscription",
		"purchase",
	)
}

func blockedDetailLooksConcreteApprovalGate(text string) bool {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return false
	}
	return containsAny(text,
		"policy requires approval",
		"privileged approval",
		"explicit approval",
		"requires approval",
		"approval required",
		"operator approval",
		"human approval",
		"approve privileged",
	)
}

func defaultOwnerActionForDecision(decisionType string) string {
	switch decisionType {
	case "credential":
		return "Complete the required authorization step or provide the needed credential"
	case "payment":
		return "Complete the required payment or billing step"
	case "approval":
		return "Approve the blocked privileged action"
	default:
		return ""
	}
}

func defaultHumanReasonForDecision(result StructuredTaskResult) string {
	switch inferDecisionType(result) {
	case "credential":
		return firstNonEmpty(result.Summary, "the agent is blocked on an authorization or credential gate")
	case "payment":
		return firstNonEmpty(result.Summary, "the agent is blocked on a payment or billing gate")
	case "approval":
		return firstNonEmpty(result.Summary, "the runtime hit a privileged action that requires approval")
	default:
		return firstNonEmpty(result.Summary, "human intervention is required")
	}
}

func appendAutonomyNote(details, note string) string {
	details = strings.TrimSpace(details)
	note = strings.TrimSpace(note)
	switch {
	case details == "":
		return note
	case strings.Contains(strings.ToLower(details), strings.ToLower(note)):
		return details
	default:
		return details + "\n\nAutonomy note: " + note + "."
	}
}

func blockedDetails(items []BlockedRef) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		if detail := strings.TrimSpace(item.Detail); detail != "" {
			parts = append(parts, fmt.Sprintf("%s %s", strings.TrimSpace(item.Kind), detail))
		}
	}
	return strings.Join(parts, "\n")
}

func containsAny(value string, needles ...string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	for _, needle := range needles {
		if strings.Contains(value, strings.ToLower(strings.TrimSpace(needle))) {
			return true
		}
	}
	return false
}
