package sqlite

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

const (
	WorkspaceCoalitionIntegrityCurrent = "CURRENT"
	WorkspaceCoalitionIntegrityDrift   = "DRIFT"
	WorkspaceCoalitionIntegrityUnknown = "UNKNOWN"

	workspaceCoalitionIntegritySeverityHard = "hard"
	workspaceCoalitionIntegritySeveritySoft = "soft"
)

type WorkspaceCoalitionIntegrityIssue struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

type WorkspaceCoalitionIntegrityItem struct {
	WorkspaceID           string                             `json:"workspace_id"`
	TensionID             string                             `json:"tension_id"`
	CanonicalCoalitionID  string                             `json:"canonical_coalition_id,omitempty"`
	ShadowCoalitionIDs    []string                           `json:"shadow_coalition_ids,omitempty"`
	State                 string                             `json:"state"`
	Summary               string                             `json:"summary"`
	LiveCandidateCount    int                                `json:"live_candidate_count"`
	ExpiredCandidateCount int                                `json:"expired_candidate_count,omitempty"`
	MemberCount           int                                `json:"member_count,omitempty"`
	IssueCodes            []string                           `json:"issue_codes,omitempty"`
	Issues                []WorkspaceCoalitionIntegrityIssue `json:"issues,omitempty"`
}

type WorkspaceCoalitionIntegrityReport struct {
	WorkspaceID   string                            `json:"workspace_id"`
	TimeAuthority WorkspaceTimeAuthority            `json:"time_authority"`
	State         string                            `json:"state"`
	Summary       string                            `json:"summary"`
	IssueCodes    []string                          `json:"issue_codes,omitempty"`
	Items         []WorkspaceCoalitionIntegrityItem `json:"items,omitempty"`
}

func (s *Store) EvaluateWorkspaceCoalitionIntegrity(ctx context.Context, workspaceID string) (WorkspaceCoalitionIntegrityReport, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return WorkspaceCoalitionIntegrityReport{}, fmt.Errorf("workspace_id is required")
	}

	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return WorkspaceCoalitionIntegrityReport{}, err
	}

	report := WorkspaceCoalitionIntegrityReport{
		WorkspaceID:   workspaceID,
		TimeAuthority: authority,
		State:         WorkspaceCoalitionIntegrityCurrent,
		Summary:       "workspace coalition integrity invariants hold",
	}

	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT tension_id
		FROM workspace_coalitions
		WHERE workspace_id = ? AND status IN ('FORMING', 'ACTIVE')
		ORDER BY tension_id ASC`, workspaceID)
	if err != nil {
		return WorkspaceCoalitionIntegrityReport{}, fmt.Errorf("query coalition integrity tensions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tensionID string
		if err := rows.Scan(&tensionID); err != nil {
			return WorkspaceCoalitionIntegrityReport{}, fmt.Errorf("scan coalition integrity tension: %w", err)
		}
		item, err := s.evaluateWorkspaceCoalitionIntegrityItem(ctx, workspaceID, tensionID, authority.CurrentEpoch)
		if err != nil {
			return WorkspaceCoalitionIntegrityReport{}, err
		}
		report.Items = append(report.Items, item)
		report.IssueCodes = append(report.IssueCodes, item.IssueCodes...)
	}
	if err := rows.Err(); err != nil {
		return WorkspaceCoalitionIntegrityReport{}, fmt.Errorf("iterate coalition integrity tensions: %w", err)
	}

	report.IssueCodes = dedupeCoalitionIntegrityCodes(report.IssueCodes)
	report.State, report.Summary = classifyWorkspaceCoalitionIntegrityReport(report.Items)
	return report, nil
}

func (s *Store) evaluateWorkspaceCoalitionIntegrityItem(ctx context.Context, workspaceID, tensionID string, currentEpoch int) (WorkspaceCoalitionIntegrityItem, error) {
	canonical, live, expired, err := s.selectLiveCoalitionCandidateByTension(ctx, s.db, workspaceID, tensionID, currentEpoch)
	if err != nil {
		return WorkspaceCoalitionIntegrityItem{}, err
	}

	item := WorkspaceCoalitionIntegrityItem{
		WorkspaceID:           workspaceID,
		TensionID:             tensionID,
		LiveCandidateCount:    len(live),
		ExpiredCandidateCount: len(expired),
		State:                 WorkspaceCoalitionIntegrityUnknown,
		Summary:               "workspace coalition integrity could not be evaluated",
	}

	if canonical != nil {
		item.CanonicalCoalitionID = strings.TrimSpace(canonical.coalition.CoalitionID)
	}
	for _, candidate := range live {
		if strings.TrimSpace(candidate.coalition.CoalitionID) == item.CanonicalCoalitionID {
			continue
		}
		item.ShadowCoalitionIDs = append(item.ShadowCoalitionIDs, strings.TrimSpace(candidate.coalition.CoalitionID))
	}
	for _, candidate := range expired {
		shadowID := strings.TrimSpace(candidate.coalition.CoalitionID)
		if shadowID == "" || shadowID == item.CanonicalCoalitionID {
			continue
		}
		item.ShadowCoalitionIDs = append(item.ShadowCoalitionIDs, shadowID)
	}
	item.ShadowCoalitionIDs = dedupeCoalitionIntegrityCodes(item.ShadowCoalitionIDs)

	if len(live) > 1 {
		item.Issues = append(item.Issues, workspaceCoalitionIntegrityIssue(
			"DUPLICATE_LIVE_COALITIONS",
			workspaceCoalitionIntegritySeverityHard,
			fmt.Sprintf("found %d live coalition rows for tension %s; read surfaces must not silently hide shadow authority", len(live), tensionID),
		))
	}

	tension, err := s.loadTensionRecord(ctx, nil, workspaceID, tensionID)
	switch {
	case err == nil:
		if !coalitionEligibleTension(tension) {
			item.Issues = append(item.Issues, workspaceCoalitionIntegrityIssue(
				"INELIGIBLE_TENSION_LIVE_COALITION",
				workspaceCoalitionIntegritySeverityHard,
				"live coalition rows still exist for a tension that is no longer coalition-eligible",
			))
		}
	case isTensionNotFoundErr(err):
		item.Issues = append(item.Issues, workspaceCoalitionIntegrityIssue(
			"BACKING_TENSION_MISSING",
			workspaceCoalitionIntegritySeverityHard,
			"live coalition rows exist without a backing tension record",
		))
	default:
		return WorkspaceCoalitionIntegrityItem{}, err
	}

	if canonical == nil {
		if len(expired) > 0 {
			item.Issues = append(item.Issues, workspaceCoalitionIntegrityIssue(
				"STALE_LIVE_ROWS_WITHOUT_CANONICAL",
				workspaceCoalitionIntegritySeverityHard,
				"only expired live coalition rows remain; operator surfaces would otherwise hide stale coalition authority",
			))
		}
		item.IssueCodes = coalitionIntegrityIssueCodes(item.Issues)
		item.State, item.Summary = classifyWorkspaceCoalitionIntegrityItem(item.Issues)
		return item, nil
	}

	members, err := s.loadCoalitionMembers(ctx, s.db, workspaceID, canonical.coalition.CoalitionID)
	if err != nil {
		return WorkspaceCoalitionIntegrityItem{}, err
	}
	item.MemberCount = len(members)
	if len(members) == 0 {
		item.Issues = append(item.Issues, workspaceCoalitionIntegrityIssue(
			"EMPTY_LIVE_COALITION",
			workspaceCoalitionIntegritySeverityHard,
			"canonical live coalition has no active members",
		))
	}

	generatorCount := 0
	farReviewerCount := 0
	reviewerCount := 0
	for _, member := range members {
		switch strings.ToUpper(strings.TrimSpace(member.Role)) {
		case "GENERATOR":
			generatorCount++
		case "FAR_REVIEWER":
			farReviewerCount++
			reviewerCount++
		case "NEAR_REVIEWER":
			reviewerCount++
		default:
			item.Issues = append(item.Issues, workspaceCoalitionIntegrityIssue(
				"UNKNOWN_MEMBER_ROLE",
				workspaceCoalitionIntegritySeverityHard,
				fmt.Sprintf("coalition member %s carries unsupported role %q", strings.TrimSpace(member.AgentID), strings.TrimSpace(member.Role)),
			))
		}
	}
	if generatorCount != 1 {
		item.Issues = append(item.Issues, workspaceCoalitionIntegrityIssue(
			"GENERATOR_ROLE_CARDINALITY",
			workspaceCoalitionIntegritySeverityHard,
			fmt.Sprintf("canonical live coalition must have exactly one generator, found %d", generatorCount),
		))
	}
	if farReviewerCount > 1 {
		item.Issues = append(item.Issues, workspaceCoalitionIntegrityIssue(
			"FAR_REVIEWER_ROLE_CARDINALITY",
			workspaceCoalitionIntegritySeverityHard,
			fmt.Sprintf("canonical live coalition must have at most one far reviewer, found %d", farReviewerCount),
		))
	}
	if len(members) > 1 && reviewerCount == 0 {
		item.Issues = append(item.Issues, workspaceCoalitionIntegrityIssue(
			"REVIEWER_ROLE_MISSING",
			workspaceCoalitionIntegritySeverityHard,
			"multi-member coalition lacks any reviewer role after normalization",
		))
	}

	item.IssueCodes = coalitionIntegrityIssueCodes(item.Issues)
	item.State, item.Summary = classifyWorkspaceCoalitionIntegrityItem(item.Issues)
	return item, nil
}

func workspaceCoalitionIntegrityIssue(code, severity, message string) WorkspaceCoalitionIntegrityIssue {
	return WorkspaceCoalitionIntegrityIssue{
		Code:     strings.TrimSpace(code),
		Severity: strings.TrimSpace(severity),
		Message:  strings.TrimSpace(message),
	}
}

func coalitionIntegrityIssueCodes(items []WorkspaceCoalitionIntegrityIssue) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if code := strings.TrimSpace(item.Code); code != "" {
			out = append(out, code)
		}
	}
	return dedupeCoalitionIntegrityCodes(out)
}

func dedupeCoalitionIntegrityCodes(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}

func classifyWorkspaceCoalitionIntegrityItem(issues []WorkspaceCoalitionIntegrityIssue) (string, string) {
	hasHard := false
	hasSoft := false
	for _, issue := range issues {
		switch strings.TrimSpace(issue.Severity) {
		case workspaceCoalitionIntegritySeverityHard:
			hasHard = true
		case workspaceCoalitionIntegritySeveritySoft:
			hasSoft = true
		}
	}
	switch {
	case hasHard:
		return WorkspaceCoalitionIntegrityDrift, "workspace coalition invariants are violated"
	case hasSoft:
		return WorkspaceCoalitionIntegrityUnknown, "workspace coalition invariants are only partially trustworthy"
	default:
		return WorkspaceCoalitionIntegrityCurrent, "workspace coalition invariants hold"
	}
}

func classifyWorkspaceCoalitionIntegrityReport(items []WorkspaceCoalitionIntegrityItem) (string, string) {
	if len(items) == 0 {
		return WorkspaceCoalitionIntegrityCurrent, "no live coalition integrity drift detected"
	}
	hasDrift := false
	hasUnknown := false
	for _, item := range items {
		switch strings.TrimSpace(item.State) {
		case WorkspaceCoalitionIntegrityDrift:
			hasDrift = true
		case WorkspaceCoalitionIntegrityUnknown:
			hasUnknown = true
		}
	}
	switch {
	case hasDrift:
		return WorkspaceCoalitionIntegrityDrift, "workspace coalition integrity drift detected"
	case hasUnknown:
		return WorkspaceCoalitionIntegrityUnknown, "workspace coalition integrity is only partially trustworthy"
	default:
		return WorkspaceCoalitionIntegrityCurrent, "workspace coalition integrity invariants hold"
	}
}
