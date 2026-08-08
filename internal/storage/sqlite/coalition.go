package sqlite

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	ErrCoalitionTargetNotFound  = errors.New("coalition target not found")
	ErrCoalitionTargetAmbiguous = errors.New("coalition target is ambiguous")
	ErrCoalitionActorNotMember  = errors.New("coalition actor is not an active coalition member")
	ErrCoalitionTargetNotMember = errors.New("coalition target is not an active coalition member")
	ErrCoalitionSelfKick        = errors.New("self-removal must use coalition.leave")
)

const (
	coalitionRequestedRoleSemantics = "advisory_system_normalized"
	coalitionKickReasonSemantics    = "operator_note_no_policy_effect"
)

type CoalitionJoinOfferInput struct {
	WorkspaceID                string
	TaskID                     string
	AgentID                    string
	Role                       string
	ActorID                    string
	PromptContextEnvelope      map[string]any
	PromptContextSurface       string
	PromptContextPrincipalType string
	PromptContextPrincipalID   string
}

type CoalitionJoinLeaveInput struct {
	WorkspaceID                string
	CoalitionID                string
	AgentID                    string
	Reason                     string
	ActorID                    string
	PromptContextEnvelope      map[string]any
	PromptContextSurface       string
	PromptContextPrincipalType string
	PromptContextPrincipalID   string
}

type CoalitionSeekQueryInput struct {
	WorkspaceID    string
	TaskID         string
	AgentID        string
	Role           string
	RequiredSkills []string
	Reason         string
	Limit          int
}

type CoalitionInviteEventInput struct {
	WorkspaceID                string
	CoalitionID                string
	AgentID                    string
	InvitedBy                  string
	Role                       string
	PromptContextEnvelope      map[string]any
	PromptContextSurface       string
	PromptContextPrincipalType string
	PromptContextPrincipalID   string
}

type CoalitionKickEventInput struct {
	WorkspaceID                string
	CoalitionID                string
	AgentID                    string
	KickedBy                   string
	Reason                     string
	PromptContextEnvelope      map[string]any
	PromptContextSurface       string
	PromptContextPrincipalType string
	PromptContextPrincipalID   string
}

func coalitionEligibleTension(record TensionRecord) bool {
	switch strings.ToUpper(strings.TrimSpace(record.LifecycleState)) {
	case tensionLifecycleActive, tensionLifecycleEmergent, tensionLifecycleMeta:
		// Coalition participation is only valid for live tensions. META is kept here
		// for legacy compatibility with older rows that predate lifecycle normalization.
	default:
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(record.ReviewStatus)) {
	case "RESOLVED", "DISCARDED":
		return false
	}
	return true
}

func coalitionSuccessCriterion(record TensionRecord) string {
	title := strings.TrimSpace(record.Title)
	if title != "" {
		return "Resolve tension: " + title
	}
	return "Resolve underlying tension"
}

func coalitionTargetIDs(records []TensionRecord) []string {
	ids := make([]string, 0, len(records))
	for _, record := range records {
		if tensionID := strings.TrimSpace(record.TensionID); tensionID != "" {
			ids = append(ids, tensionID)
		}
	}
	return ids
}

func coalitionMemberRecord(coalition *WorkspaceCoalition, agentID string) (*WorkspaceCoalitionMember, bool) {
	if coalition == nil {
		return nil, false
	}
	agentID = strings.TrimSpace(agentID)
	for idx := range coalition.Members {
		if strings.TrimSpace(coalition.Members[idx].AgentID) == agentID {
			return &coalition.Members[idx], true
		}
	}
	return nil, false
}

func (s *Store) loadCanonicalCoalitionForAction(ctx context.Context, workspaceID, coalitionID string) (*WorkspaceCoalition, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	coalitionID = strings.TrimSpace(coalitionID)
	if workspaceID == "" || coalitionID == "" {
		return nil, errors.New("workspace_id and coalition_id are required")
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	coalition, err := s.loadCoalitionRecordByID(ctx, tx, workspaceID, coalitionID)
	if err != nil {
		return nil, err
	}
	if coalition == nil {
		return nil, fmt.Errorf("coalition not found")
	}

	currentEpoch, err := s.currentControlEpoch(ctx, tx, workspaceID)
	if err != nil {
		return nil, err
	}

	canonical, err := s.reconcileLiveCoalitionCandidateByTension(ctx, tx, workspaceID, coalition.TensionID, currentEpoch)
	if err != nil {
		return nil, err
	}
	if canonical == nil || canonical.coalition.CoalitionID != coalitionID {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit coalition authority reconciliation: %w", err)
		}
		return nil, ErrCoalitionExpired
	}

	tension, err := s.loadTensionRecord(ctx, tx, workspaceID, coalition.TensionID)
	switch {
	case err == nil && coalitionEligibleTension(tension):
		// continue
	case err == nil:
		if err := s.disbandLiveCoalitionsForTension(ctx, tx, workspaceID, coalition.TensionID); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit ineligible coalition cleanup: %w", err)
		}
		return nil, ErrCoalitionExpired
	case isTensionNotFoundErr(err):
		if err := s.disbandLiveCoalitionsForTension(ctx, tx, workspaceID, coalition.TensionID); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit missing-tension coalition cleanup: %w", err)
		}
		return nil, ErrCoalitionExpired
	default:
		return nil, err
	}

	members, err := s.loadCoalitionMembers(ctx, tx, workspaceID, coalitionID)
	if err != nil {
		return nil, err
	}
	canonicalCoalition := canonical.coalition
	canonicalCoalition.Members = members
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit coalition action load: %w", err)
	}
	return &canonicalCoalition, nil
}

func (s *Store) disbandCoalitionIfEmpty(ctx context.Context, workspaceID, coalitionID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	coalitionID = strings.TrimSpace(coalitionID)
	if workspaceID == "" || coalitionID == "" {
		return nil
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin empty coalition cleanup: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	coalition, err := s.loadCoalitionRecordByID(ctx, tx, workspaceID, coalitionID)
	if err != nil {
		return err
	}
	if coalition == nil || !coalitionIsActive(coalition.Status) {
		return nil
	}

	members, err := s.loadCoalitionMembers(ctx, tx, workspaceID, coalitionID)
	if err != nil {
		return err
	}
	if len(members) != 0 {
		return nil
	}
	if err := s.disbandCoalition(ctx, tx, coalition); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit empty coalition cleanup: %w", err)
	}
	return nil
}

func isTensionNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "tension not found:")
}

func (s *Store) resolveCoalitionTargetTension(ctx context.Context, workspaceID, taskOrTensionID string) (TensionRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskOrTensionID = strings.TrimSpace(taskOrTensionID)
	if workspaceID == "" || taskOrTensionID == "" {
		return TensionRecord{}, ErrCoalitionTargetNotFound
	}

	detail, err := s.GetTension(ctx, workspaceID, taskOrTensionID)
	switch {
	case err == nil:
		if !coalitionEligibleTension(detail.Tension) {
			return TensionRecord{}, fmt.Errorf("%w: %s is not coalition-eligible", ErrCoalitionTargetNotFound, taskOrTensionID)
		}
		return detail.Tension, nil
	case !isTensionNotFoundErr(err):
		return TensionRecord{}, err
	}

	items, err := s.ListTensions(ctx, TensionFilter{
		WorkspaceID: workspaceID,
		TaskID:      taskOrTensionID,
		Limit:       8,
	})
	if err != nil {
		return TensionRecord{}, err
	}

	candidates := make([]TensionRecord, 0, len(items))
	for _, item := range items {
		if coalitionEligibleTension(item) {
			candidates = append(candidates, item)
		}
	}
	switch len(candidates) {
	case 0:
		return TensionRecord{}, fmt.Errorf("%w: no coalition-eligible tension for %s", ErrCoalitionTargetNotFound, taskOrTensionID)
	case 1:
		return candidates[0], nil
	default:
		return TensionRecord{}, fmt.Errorf("%w: %s resolves to %v", ErrCoalitionTargetAmbiguous, taskOrTensionID, coalitionTargetIDs(candidates))
	}
}

func (s *Store) listWorkspaceCoalitions(ctx context.Context, workspaceID string) ([]WorkspaceCoalition, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}

	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT tension_id
		FROM workspace_coalitions
		WHERE workspace_id = ? AND status IN ('FORMING', 'ACTIVE')
		ORDER BY tension_id ASC`, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("list coalitions: %w", err)
	}
	defer rows.Close()

	coalitions := make([]WorkspaceCoalition, 0)
	for rows.Next() {
		var tensionID string
		if err := rows.Scan(&tensionID); err != nil {
			return nil, fmt.Errorf("scan coalition tension: %w", err)
		}
		coalition, err := s.GetTensionCoalition(ctx, workspaceID, tensionID)
		if err != nil {
			return nil, err
		}
		if coalition == nil {
			continue
		}
		coalitions = append(coalitions, *coalition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate coalitions: %w", err)
	}
	sort.Slice(coalitions, func(i, j int) bool {
		if coalitions[i].CreatedEpoch != coalitions[j].CreatedEpoch {
			return coalitions[i].CreatedEpoch > coalitions[j].CreatedEpoch
		}
		if coalitions[i].UpdatedAt != coalitions[j].UpdatedAt {
			return coalitions[i].UpdatedAt > coalitions[j].UpdatedAt
		}
		return coalitions[i].CoalitionID > coalitions[j].CoalitionID
	})
	return coalitions, nil
}

func (s *Store) CoalitionJoinOffer(ctx context.Context, input CoalitionJoinOfferInput) (map[string]any, error) {
	tension, err := s.resolveCoalitionTargetTension(ctx, input.WorkspaceID, input.TaskID)
	if err != nil {
		return nil, err
	}

	coalition, err := s.CreateCoalition(ctx, input.WorkspaceID, tension.TensionID, coalitionSuccessCriterion(tension))
	if err != nil {
		return nil, err
	}
	factors, err := s.AddCoalitionMemberWithHeuristicFactors(ctx, input.WorkspaceID, coalition.CoalitionID, strings.TrimSpace(input.AgentID))
	if err != nil {
		if len(coalition.Members) == 0 {
			if cleanupErr := s.disbandCoalitionIfEmpty(ctx, input.WorkspaceID, coalition.CoalitionID); cleanupErr != nil {
				return nil, fmt.Errorf("%w (empty coalition cleanup failed: %v)", err, cleanupErr)
			}
		}
		return nil, err
	}
	current, err := s.GetTensionCoalition(ctx, input.WorkspaceID, tension.TensionID)
	if err != nil {
		return nil, err
	}
	decision := evaluateAttachmentDecision(tension, factors)

	return map[string]any{
		"status":                   "joined",
		"coalition_id":             coalition.CoalitionID,
		"tension_id":               tension.TensionID,
		"requested_role":           strings.TrimSpace(input.Role),
		"requested_role_semantics": coalitionRequestedRoleSemantics,
		"assigned_role": func() string {
			if member, ok := coalitionMemberRecord(current, input.AgentID); ok {
				return strings.TrimSpace(member.Role)
			}
			return ""
		}(),
		"heuristic_factors":    factors,
		"attach_decision":      decision,
		"attach_admissibility": "fit_novelty_crowding_envelope",
		"coalition":            current,
	}, nil
}

func (s *Store) CoalitionJoinOfferWithContext(ctx context.Context, input CoalitionJoinOfferInput) (map[string]any, error) {
	tension, err := s.resolveCoalitionTargetTension(ctx, input.WorkspaceID, input.TaskID)
	if err != nil {
		return nil, err
	}
	result, err := s.AttachTensionAgentWithContext(ctx, TensionCoalitionMemberMutationInput{
		WorkspaceID:                input.WorkspaceID,
		TensionID:                  tension.TensionID,
		AgentID:                    input.AgentID,
		ActorID:                    input.ActorID,
		SuccessCriterion:           coalitionSuccessCriterion(tension),
		Reason:                     strings.TrimSpace(input.Role),
		CoalitionAction:            "offered",
		PromptContextEnvelope:      input.PromptContextEnvelope,
		PromptContextSurface:       firstNonEmpty(input.PromptContextSurface, "coalition.offer"),
		PromptContextPrincipalType: input.PromptContextPrincipalType,
		PromptContextPrincipalID:   input.PromptContextPrincipalID,
	})
	if err != nil {
		return nil, err
	}
	return coalitionOfferPayload(input, result), nil
}

func coalitionOfferPayload(input CoalitionJoinOfferInput, result TensionCoalitionMemberMutationResult) map[string]any {
	tension := result.Tension
	if strings.TrimSpace(tension.TensionID) == "" {
		tension = TensionRecord{TensionID: strings.TrimSpace(input.TaskID)}
	}
	payload := map[string]any{
		"success":                  true,
		"changed":                  result.Changed,
		"status":                   "joined",
		"coalition_id":             result.Coalition.CoalitionID,
		"tension_id":               result.Tension.TensionID,
		"requested_role":           strings.TrimSpace(input.Role),
		"requested_role_semantics": coalitionRequestedRoleSemantics,
		"assigned_role": func() string {
			if member, ok := coalitionMemberRecord(&result.Coalition, input.AgentID); ok {
				return strings.TrimSpace(member.Role)
			}
			return ""
		}(),
		"heuristic_factors":    result.Factors,
		"attach_decision":      evaluateAttachmentDecision(tension, result.Factors),
		"attach_admissibility": "fit_novelty_crowding_envelope",
		"coalition":            result.Coalition,
	}
	if result.Changed {
		payload["event"] = result.Event
	}
	return payload
}

func (s *Store) CoalitionJoinLeave(ctx context.Context, input CoalitionJoinLeaveInput) error {
	coalition, err := s.loadCanonicalCoalitionForAction(ctx, input.WorkspaceID, input.CoalitionID)
	if err != nil {
		return err
	}
	if _, ok := coalitionMemberRecord(coalition, input.AgentID); !ok {
		return fmt.Errorf("%w: agent %s", ErrCoalitionActorNotMember, strings.TrimSpace(input.AgentID))
	}
	return s.RemoveCoalitionMember(ctx, input.WorkspaceID, input.CoalitionID, input.AgentID)
}

func (s *Store) CoalitionJoinLeaveWithContext(ctx context.Context, input CoalitionJoinLeaveInput) (map[string]any, error) {
	result, err := s.DetachTensionAgentWithContext(ctx, TensionCoalitionMemberMutationInput{
		WorkspaceID:                input.WorkspaceID,
		CoalitionID:                input.CoalitionID,
		AgentID:                    input.AgentID,
		ActorID:                    input.ActorID,
		Reason:                     input.Reason,
		CoalitionAction:            "left",
		PromptContextEnvelope:      input.PromptContextEnvelope,
		PromptContextSurface:       firstNonEmpty(input.PromptContextSurface, "coalition.leave"),
		PromptContextPrincipalType: input.PromptContextPrincipalType,
		PromptContextPrincipalID:   input.PromptContextPrincipalID,
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"success":      true,
		"changed":      result.Changed,
		"coalition_id": result.Coalition.CoalitionID,
		"agent_id":     strings.TrimSpace(input.AgentID),
		"reason":       strings.TrimSpace(input.Reason),
		"coalition":    result.Coalition,
		"event":        result.Event,
	}, nil
}

func (s *Store) GetCoalitionByWorkspace(ctx context.Context, workspaceID string) (map[string]any, error) {
	coalitions, err := s.listWorkspaceCoalitions(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	integrity, err := s.EvaluateWorkspaceCoalitionIntegrity(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"coalitions":     coalitions,
		"integrity":      integrity,
		"time_authority": integrity.TimeAuthority,
	}, nil
}

func (s *Store) CoalitionSeekQuery(ctx context.Context, input CoalitionSeekQueryInput) (map[string]any, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	agentID := strings.TrimSpace(input.AgentID)
	if workspaceID == "" || agentID == "" {
		return nil, errors.New("workspace_id and agent_id are required")
	}

	limit := input.Limit
	if limit <= 0 {
		limit = 20
	}

	var targetTensionID string
	targetResolution := map[string]any{
		"status": "not_requested",
	}
	if taskID := strings.TrimSpace(input.TaskID); taskID != "" {
		target, err := s.resolveCoalitionTargetTension(ctx, workspaceID, taskID)
		if err != nil {
			targetResolution = map[string]any{
				"status":  "unresolved",
				"task_id": taskID,
				"error":   err.Error(),
			}
			switch {
			case errors.Is(err, ErrCoalitionTargetNotFound):
				targetResolution["status"] = "not_found"
			case errors.Is(err, ErrCoalitionTargetAmbiguous):
				targetResolution["status"] = "ambiguous"
			default:
				return nil, err
			}
		} else {
			targetTensionID = target.TensionID
			targetResolution = map[string]any{
				"status":     "resolved",
				"task_id":    taskID,
				"tension_id": target.TensionID,
			}
		}
	}

	scored, err := s.ListAgentAvailableTensionsScored(ctx, workspaceID, agentID)
	if err != nil {
		return nil, err
	}

	integrityReport, err := s.EvaluateWorkspaceCoalitionIntegrity(ctx, workspaceID)
	if err != nil {
		return nil, err
	}

	matches := make([]map[string]any, 0, minInt(limit, len(scored)))
	for _, item := range scored {
		if targetTensionID != "" && item.TensionID != targetTensionID {
			continue
		}
		coalition, err := s.GetTensionCoalition(ctx, workspaceID, item.TensionID)
		if err != nil {
			return nil, err
		}
		coalitionID := ""
		if coalition != nil {
			coalitionID = coalition.CoalitionID
		}
		matches = append(matches, map[string]any{
			"tension":                  item.TensionRecord,
			"coalition":                coalition,
			"attach_score":             item.AttachScore,
			"attach_prob":              item.AttachProb,
			"attach_factors":           item.AttachFactors,
			"attach_decision":          item.AttachDecision,
			"attach_admissibility":     "fit_novelty_crowding_envelope",
			"requested_role":           strings.TrimSpace(input.Role),
			"requested_role_semantics": coalitionRequestedRoleSemantics,
			"required_skills":          append([]string{}, input.RequiredSkills...),
			"reason":                   strings.TrimSpace(input.Reason),
			"coalition_integrity":      filterWorkspaceCoalitionIntegrityReport(integrityReport, item.TensionID, coalitionID),
		})
		if len(matches) >= limit {
			break
		}
	}

	return map[string]any{
		"matches":                  matches,
		"task_id":                  strings.TrimSpace(input.TaskID),
		"target_resolution":        targetResolution,
		"required_skills":          append([]string{}, input.RequiredSkills...),
		"reason":                   strings.TrimSpace(input.Reason),
		"requested_role_semantics": coalitionRequestedRoleSemantics,
	}, nil
}

func filterWorkspaceCoalitionIntegrityReport(report WorkspaceCoalitionIntegrityReport, tensionID, coalitionID string) WorkspaceCoalitionIntegrityReport {
	filtered := WorkspaceCoalitionIntegrityReport{
		WorkspaceID:   report.WorkspaceID,
		TimeAuthority: report.TimeAuthority,
		State:         WorkspaceCoalitionIntegrityCurrent,
		Summary:       "requested coalition integrity invariants hold",
	}
	tensionID = strings.TrimSpace(tensionID)
	coalitionID = strings.TrimSpace(coalitionID)
	for _, item := range report.Items {
		if tensionID != "" && strings.TrimSpace(item.TensionID) == tensionID {
			filtered.Items = append(filtered.Items, item)
			filtered.IssueCodes = append(filtered.IssueCodes, item.IssueCodes...)
			continue
		}
		if coalitionID != "" && strings.TrimSpace(item.CanonicalCoalitionID) == coalitionID {
			filtered.Items = append(filtered.Items, item)
			filtered.IssueCodes = append(filtered.IssueCodes, item.IssueCodes...)
			continue
		}
		if coalitionID != "" {
			for _, shadowID := range item.ShadowCoalitionIDs {
				if strings.TrimSpace(shadowID) == coalitionID {
					filtered.Items = append(filtered.Items, item)
					filtered.IssueCodes = append(filtered.IssueCodes, item.IssueCodes...)
					break
				}
			}
		}
	}
	filtered.IssueCodes = dedupeCoalitionIntegrityCodes(filtered.IssueCodes)
	if len(filtered.Items) == 0 {
		return filtered
	}
	filtered.State, filtered.Summary = classifyWorkspaceCoalitionIntegrityReport(filtered.Items)
	return filtered
}

func (s *Store) CoalitionInviteEvent(ctx context.Context, input CoalitionInviteEventInput) error {
	coalition, err := s.loadCanonicalCoalitionForAction(ctx, input.WorkspaceID, input.CoalitionID)
	if err != nil {
		return err
	}
	if _, ok := coalitionMemberRecord(coalition, input.InvitedBy); !ok {
		return fmt.Errorf("%w: inviter %s", ErrCoalitionActorNotMember, strings.TrimSpace(input.InvitedBy))
	}
	_, err = s.AddCoalitionMemberWithHeuristicFactors(ctx, input.WorkspaceID, input.CoalitionID, input.AgentID)
	return err
}

func (s *Store) CoalitionInviteEventWithContext(ctx context.Context, input CoalitionInviteEventInput) (map[string]any, error) {
	coalition, err := s.loadCanonicalCoalitionForAction(ctx, input.WorkspaceID, input.CoalitionID)
	if err != nil {
		return nil, err
	}
	if _, ok := coalitionMemberRecord(coalition, input.InvitedBy); !ok {
		return nil, fmt.Errorf("%w: inviter %s", ErrCoalitionActorNotMember, strings.TrimSpace(input.InvitedBy))
	}
	result, err := s.AttachTensionAgentWithContext(ctx, TensionCoalitionMemberMutationInput{
		WorkspaceID:                input.WorkspaceID,
		TensionID:                  coalition.TensionID,
		CoalitionID:                coalition.CoalitionID,
		AgentID:                    input.AgentID,
		ActorID:                    input.InvitedBy,
		SuccessCriterion:           coalition.SuccessCriterion,
		Reason:                     strings.TrimSpace(input.Role),
		RequireActorMembership:     true,
		CoalitionAction:            "invited",
		PromptContextEnvelope:      input.PromptContextEnvelope,
		PromptContextSurface:       firstNonEmpty(input.PromptContextSurface, "coalition.invite"),
		PromptContextPrincipalType: input.PromptContextPrincipalType,
		PromptContextPrincipalID:   input.PromptContextPrincipalID,
	})
	if err != nil {
		return nil, err
	}
	payload := map[string]any{
		"success":                  true,
		"changed":                  result.Changed,
		"coalition_id":             result.Coalition.CoalitionID,
		"target_id":                strings.TrimSpace(input.AgentID),
		"invited_by":               strings.TrimSpace(input.InvitedBy),
		"requested_role":           strings.TrimSpace(input.Role),
		"requested_role_semantics": coalitionRequestedRoleSemantics,
		"membership_effect":        "system_normalized_attach_if_eligible",
		"heuristic_factors":        result.Factors,
		"attach_decision":          evaluateAttachmentDecision(result.Tension, result.Factors),
		"attach_admissibility":     "fit_novelty_crowding_envelope",
		"coalition":                result.Coalition,
	}
	if result.Changed {
		payload["event"] = result.Event
	}
	return payload, nil
}

func (s *Store) CoalitionKickEvent(ctx context.Context, input CoalitionKickEventInput) error {
	coalition, err := s.loadCanonicalCoalitionForAction(ctx, input.WorkspaceID, input.CoalitionID)
	if err != nil {
		return err
	}
	if _, ok := coalitionMemberRecord(coalition, input.KickedBy); !ok {
		return fmt.Errorf("%w: kicker %s", ErrCoalitionActorNotMember, strings.TrimSpace(input.KickedBy))
	}
	if strings.TrimSpace(input.KickedBy) == strings.TrimSpace(input.AgentID) {
		return ErrCoalitionSelfKick
	}
	if _, ok := coalitionMemberRecord(coalition, input.AgentID); !ok {
		return fmt.Errorf("%w: target %s", ErrCoalitionTargetNotMember, strings.TrimSpace(input.AgentID))
	}
	return s.RemoveCoalitionMember(ctx, input.WorkspaceID, input.CoalitionID, input.AgentID)
}

func (s *Store) CoalitionKickEventWithContext(ctx context.Context, input CoalitionKickEventInput) (map[string]any, error) {
	if strings.TrimSpace(input.KickedBy) == strings.TrimSpace(input.AgentID) {
		return nil, ErrCoalitionSelfKick
	}
	result, err := s.DetachTensionAgentWithContext(ctx, TensionCoalitionMemberMutationInput{
		WorkspaceID:                input.WorkspaceID,
		CoalitionID:                input.CoalitionID,
		AgentID:                    input.AgentID,
		ActorID:                    input.KickedBy,
		Reason:                     input.Reason,
		RequireActorMembership:     true,
		RejectActorSelfMutation:    true,
		CoalitionAction:            "kicked",
		PromptContextEnvelope:      input.PromptContextEnvelope,
		PromptContextSurface:       firstNonEmpty(input.PromptContextSurface, "coalition.kick"),
		PromptContextPrincipalType: input.PromptContextPrincipalType,
		PromptContextPrincipalID:   input.PromptContextPrincipalID,
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"success":           true,
		"changed":           result.Changed,
		"coalition_id":      result.Coalition.CoalitionID,
		"target_id":         strings.TrimSpace(input.AgentID),
		"kicked_by":         strings.TrimSpace(input.KickedBy),
		"reason":            strings.TrimSpace(input.Reason),
		"reason_semantics":  coalitionKickReasonSemantics,
		"membership_effect": "remove_active_member_only",
		"coalition":         result.Coalition,
		"event":             result.Event,
	}, nil
}

func (s *Store) CoalitionFormationSweep(ctx context.Context, workspaceID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}

	rows, err := s.db.QueryContext(ctx, `SELECT tension_id, COALESCE(NULLIF(title, ''), 'Resolve underlying tension')
		FROM workspace_tensions
		WHERE workspace_id = ?
		  AND UPPER(TRIM(COALESCE(lifecycle_state, ''))) IN ('ACTIVE', 'EMERGENT', 'META')
		  AND UPPER(TRIM(COALESCE(review_status, ''))) NOT IN ('RESOLVED', 'DISCARDED')
		ORDER BY updated_at DESC, tension_id ASC`, workspaceID)
	if err != nil {
		return fmt.Errorf("query coalition formation sweep targets: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tensionID string
		var successCriterion string
		if err := rows.Scan(&tensionID, &successCriterion); err != nil {
			return fmt.Errorf("scan coalition formation sweep target: %w", err)
		}
		if _, err := s.CreateCoalition(ctx, workspaceID, tensionID, strings.TrimSpace(successCriterion)); err != nil {
			if errors.Is(err, ErrCoalitionTargetNotFound) {
				continue
			}
			return fmt.Errorf("sweep coalition %s: %w", tensionID, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate coalition formation sweep targets: %w", err)
	}
	return nil
}
