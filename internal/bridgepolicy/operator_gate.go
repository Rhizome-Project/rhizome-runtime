package bridgepolicy

import (
	"strings"
)

func HasCapability(capabilities []string, want string) bool {
	want = strings.ToLower(strings.TrimSpace(want))
	if want == "" {
		return false
	}
	for _, capability := range capabilities {
		if strings.ToLower(strings.TrimSpace(capability)) == want {
			return true
		}
	}
	return false
}

func RequiresOperatorGate(envelope *PolicyEnvelope, capabilities []string) bool {
	if envelope != nil && envelope.HighRisk && envelope.OperatorControlRequired {
		return true
	}
	return HasCapability(capabilities, "bridge.high_risk") && HasCapability(capabilities, "bridge.operator_control_required")
}

func ApprovalRequestKey(toolID, subjectType, subjectID, capability string) string {
	toolID = normalizeGateKeyPart(toolID)
	subjectType = normalizeGateKeyPart(subjectType)
	subjectID = normalizeGateKeyPart(subjectID)
	capability = normalizeGateKeyPart(capability)
	if capability == "" {
		capability = "tool_call"
	}
	return strings.Join([]string{"high_risk_bridge", toolID, subjectType, subjectID, capability}, ":")
}

func ApprovalTitle(displayName, toolID string) string {
	label := strings.TrimSpace(displayName)
	if label == "" {
		label = strings.TrimSpace(toolID)
	}
	if label == "" {
		label = "high-risk bridge path"
	}
	return "Approve high-risk bridge path: " + label
}

func ApprovalSummary(displayName, toolID, subjectType, subjectID string) string {
	label := strings.TrimSpace(displayName)
	if label == "" {
		label = strings.TrimSpace(toolID)
	}
	subjectLabel := strings.TrimSpace(subjectType)
	if subjectLabel == "" {
		subjectLabel = "subject"
	}
	targetLabel := strings.TrimSpace(subjectID)
	if targetLabel == "" {
		targetLabel = "*"
	}
	return label + " requires explicit operator approval for " + subjectLabel + "/" + targetLabel
}

func ApprovalDetails(displayName, toolID, subjectType, subjectID, capability string, envelope *PolicyEnvelope) string {
	lines := []string{
		"High-risk bridge invocation requires explicit operator approval.",
		"Tool: " + firstNonEmpty(strings.TrimSpace(displayName), strings.TrimSpace(toolID), "unknown"),
		"Subject: " + firstNonEmpty(strings.TrimSpace(subjectType), "subject") + "/" + firstNonEmpty(strings.TrimSpace(subjectID), "*"),
		"Capability: " + firstNonEmpty(strings.TrimSpace(capability), "tool.call"),
	}
	if envelope != nil {
		if tier := strings.TrimSpace(string(envelope.HighestTier)); tier != "" {
			lines = append(lines, "Highest tier: "+tier)
		}
		if posture := strings.TrimSpace(string(envelope.SupportPosture)); posture != "" {
			lines = append(lines, "Support posture: "+posture)
		}
		if !envelope.SupportedArchitecture {
			lines = append(lines, "Supported architecture: false")
		}
	}
	lines = append(lines, "Next action: add an explicit ALLOW capability policy for this subject/tool pair or leave the bridge blocked.")
	return strings.Join(lines, "\n")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func normalizeGateKeyPart(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	normalized := strings.Trim(b.String(), "_")
	if normalized == "" {
		return "unknown"
	}
	return normalized
}
