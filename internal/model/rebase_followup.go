package model

import (
	"sort"
	"strings"
)

const (
	RebaseFollowupQueueKeyPrefix        = "tension_rebase_followup:"
	RebaseRollbackFailureQueueKeyPrefix = "rebase_rollback_failure:"
	RebaseRollbackFailureKind           = "rebase_rollback_failure"

	RebaseNextActionAttempt = "attempt_rebase"
	RebaseNextActionHard    = "hard_fork"
	RebaseNextActionMerge   = "merge_bundle"

	RebaseWorkflowStateClaimed    = "claimed"
	RebaseWorkflowStateInProgress = "in_progress"
	RebaseWorkflowStateCompleted  = "completed"
	RebaseWorkflowStateFailed     = "failed"

	RebaseWorkflowStepAwaitResolution = "await_action_resolution"
	RebaseWorkflowStepAwaitRestart    = "await_action_restart"
	RebaseWorkflowStepOperatorClaimed = "operator_claimed"
	RebaseWorkflowStepActionResolved  = "action_resolved"

	ActionStatusPending   = "PENDING"
	ActionStatusCompleted = "COMPLETED"
	ActionStatusFailed    = "FAILED"
)

type RebaseRollbackFailurePayload struct {
	Kind                           string   `json:"kind,omitempty"`
	FailureScope                   string   `json:"failure_scope,omitempty"`
	FailureTrigger                 string   `json:"failure_trigger,omitempty"`
	FailureMessage                 string   `json:"failure_message,omitempty"`
	EventID                        string   `json:"event_id,omitempty"`
	RootCauseID                    string   `json:"root_cause_id,omitempty"`
	ProvenanceGroupID              string   `json:"provenance_group_id,omitempty"`
	ParentRefsJSON                 []string `json:"parent_refs_json,omitempty"`
	RunID                          string   `json:"run_id,omitempty"`
	StepID                         string   `json:"step_id,omitempty"`
	EntityID                       string   `json:"entity_id,omitempty"`
	Family                         string   `json:"family,omitempty"`
	TaskID                         string   `json:"task_id,omitempty"`
	SessionID                      string   `json:"session_id,omitempty"`
	AgentID                        string   `json:"agent_id,omitempty"`
	ActionID                       string   `json:"action_id,omitempty"`
	SourceQueueID                  string   `json:"source_queue_id,omitempty"`
	SourceQueueKey                 string   `json:"source_queue_key,omitempty"`
	RepairTensionID                string   `json:"repair_tension_id,omitempty"`
	FollowupActionID               string   `json:"followup_action_id,omitempty"`
	FollowupActionQueueKey         string   `json:"followup_action_queue_key,omitempty"`
	FollowupActionStatus           string   `json:"followup_action_status,omitempty"`
	LastFailedFollowupActionID     string   `json:"last_failed_followup_action_id,omitempty"`
	LastFailedFollowupActionStatus string   `json:"last_failed_followup_action_status,omitempty"`
}

func (p *RebaseRollbackFailurePayload) Normalize() {
	if p == nil {
		return
	}
	p.Kind = strings.TrimSpace(p.Kind)
	p.FailureScope = strings.TrimSpace(p.FailureScope)
	p.FailureTrigger = strings.TrimSpace(p.FailureTrigger)
	p.FailureMessage = strings.TrimSpace(p.FailureMessage)
	p.EventID = strings.TrimSpace(p.EventID)
	p.RootCauseID = strings.TrimSpace(p.RootCauseID)
	p.ProvenanceGroupID = strings.TrimSpace(p.ProvenanceGroupID)
	p.ParentRefsJSON = normalizeLineageRefs(p.ParentRefsJSON)
	p.RunID = strings.TrimSpace(p.RunID)
	p.StepID = strings.TrimSpace(p.StepID)
	p.EntityID = strings.TrimSpace(p.EntityID)
	p.Family = strings.TrimSpace(p.Family)
	p.TaskID = strings.TrimSpace(p.TaskID)
	p.SessionID = strings.TrimSpace(p.SessionID)
	p.AgentID = strings.TrimSpace(p.AgentID)
	p.ActionID = strings.TrimSpace(p.ActionID)
	p.SourceQueueID = strings.TrimSpace(p.SourceQueueID)
	p.SourceQueueKey = strings.TrimSpace(p.SourceQueueKey)
	p.RepairTensionID = strings.TrimSpace(p.RepairTensionID)
	p.FollowupActionID = strings.TrimSpace(p.FollowupActionID)
	p.FollowupActionQueueKey = strings.TrimSpace(p.FollowupActionQueueKey)
	p.FollowupActionStatus = strings.TrimSpace(p.FollowupActionStatus)
	p.LastFailedFollowupActionID = strings.TrimSpace(p.LastFailedFollowupActionID)
	p.LastFailedFollowupActionStatus = strings.TrimSpace(p.LastFailedFollowupActionStatus)
}

func (p RebaseRollbackFailurePayload) IsRollbackFailure(queueKey string) bool {
	queueKey = strings.TrimSpace(strings.ToLower(queueKey))
	return strings.HasPrefix(queueKey, RebaseRollbackFailureQueueKeyPrefix) ||
		strings.TrimSpace(strings.ToLower(p.Kind)) == RebaseRollbackFailureKind
}

type RebaseFollowupPayload struct {
	CoalitionID          string   `json:"coalition_id,omitempty"`
	ForkTensionID        string   `json:"fork_tension_id,omitempty"`
	RepairTensionID      string   `json:"repair_tension_id,omitempty"`
	StewardLeaseRequired bool     `json:"steward_lease_required,omitempty"`
	NextAction           string   `json:"next_action,omitempty"`
	RebasePlanClass      string   `json:"rebase_plan_class,omitempty"`
	RebaseReason         string   `json:"rebase_reason,omitempty"`
	ConflictSafeClass    string   `json:"conflict_safe_class,omitempty"`
	DecisionReason       string   `json:"decision_reason,omitempty"`
	DecisionDetail       string   `json:"decision_detail,omitempty"`
	AlternativePatch     string   `json:"alternative_patch,omitempty"`
	TaskID               string   `json:"task_id,omitempty"`
	TaskIDs              []string `json:"task_ids,omitempty"`
	ActionID             string   `json:"action_id,omitempty"`
	ActionQueueKey       string   `json:"action_queue_key,omitempty"`
	ActionStatus         string   `json:"action_status,omitempty"`
	SourceQueueID        string   `json:"source_queue_id,omitempty"`
	SourceQueueKey       string   `json:"source_queue_key,omitempty"`
	ActionTitle          string   `json:"action_title,omitempty"`
	ActionAssignedTo     string   `json:"action_assigned_to,omitempty"`
	ActionBlocking       bool     `json:"action_blocking"`
	ActionStartedBy      string   `json:"action_started_by,omitempty"`
	ActionStartedComment string   `json:"action_started_comment,omitempty"`
	ActionPausedBy       string   `json:"action_paused_by,omitempty"`
	ActionPauseComment   string   `json:"action_pause_comment,omitempty"`
	LastFailedActionID   string   `json:"last_failed_action_id,omitempty"`
	LastFailedActionKey  string   `json:"last_failed_action_key,omitempty"`
	LastFailedStatus     string   `json:"last_failed_status,omitempty"`
	RollbackReason       string   `json:"rollback_reason,omitempty"`
	RootCauseID          string   `json:"root_cause_id,omitempty"`
	ProvenanceGroupID    string   `json:"provenance_group_id,omitempty"`
	ParentRefsJSON       []string `json:"parent_refs_json,omitempty"`
	RebaseWorkflowState  string   `json:"rebase_workflow_state,omitempty"`
	RebaseWorkflowStep   string   `json:"rebase_workflow_step,omitempty"`
}

func (p *RebaseFollowupPayload) Normalize() {
	if p == nil {
		return
	}
	p.CoalitionID = strings.TrimSpace(p.CoalitionID)
	p.ForkTensionID = strings.TrimSpace(p.ForkTensionID)
	p.RepairTensionID = strings.TrimSpace(p.RepairTensionID)
	p.NextAction = strings.TrimSpace(p.NextAction)
	p.RebasePlanClass = strings.TrimSpace(p.RebasePlanClass)
	p.RebaseReason = strings.TrimSpace(p.RebaseReason)
	p.ConflictSafeClass = strings.TrimSpace(p.ConflictSafeClass)
	p.DecisionReason = strings.TrimSpace(p.DecisionReason)
	p.DecisionDetail = strings.TrimSpace(p.DecisionDetail)
	p.AlternativePatch = strings.TrimSpace(p.AlternativePatch)
	p.TaskID = strings.TrimSpace(p.TaskID)
	p.ActionID = strings.TrimSpace(p.ActionID)
	p.ActionQueueKey = strings.TrimSpace(p.ActionQueueKey)
	p.ActionStatus = strings.TrimSpace(p.ActionStatus)
	p.SourceQueueID = strings.TrimSpace(p.SourceQueueID)
	p.SourceQueueKey = strings.TrimSpace(p.SourceQueueKey)
	p.ActionTitle = strings.TrimSpace(p.ActionTitle)
	p.ActionAssignedTo = strings.TrimSpace(p.ActionAssignedTo)
	p.ActionStartedBy = strings.TrimSpace(p.ActionStartedBy)
	p.ActionStartedComment = strings.TrimSpace(p.ActionStartedComment)
	p.ActionPausedBy = strings.TrimSpace(p.ActionPausedBy)
	p.ActionPauseComment = strings.TrimSpace(p.ActionPauseComment)
	p.LastFailedActionID = strings.TrimSpace(p.LastFailedActionID)
	p.LastFailedActionKey = strings.TrimSpace(p.LastFailedActionKey)
	p.LastFailedStatus = strings.TrimSpace(p.LastFailedStatus)
	p.RollbackReason = strings.TrimSpace(p.RollbackReason)
	p.RootCauseID = strings.TrimSpace(p.RootCauseID)
	p.ProvenanceGroupID = strings.TrimSpace(p.ProvenanceGroupID)
	p.ParentRefsJSON = normalizeLineageRefs(p.ParentRefsJSON)
	p.RebaseWorkflowState = strings.TrimSpace(p.RebaseWorkflowState)
	p.RebaseWorkflowStep = strings.TrimSpace(p.RebaseWorkflowStep)
	if len(p.TaskIDs) == 0 {
		return
	}
	seen := make(map[string]struct{}, len(p.TaskIDs))
	normalized := make([]string, 0, len(p.TaskIDs))
	for _, taskID := range p.TaskIDs {
		trimmed := strings.TrimSpace(taskID)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}
	sort.Strings(normalized)
	p.TaskIDs = normalized
}

func (p RebaseFollowupPayload) IsRebaseFollowup(queueKey string) bool {
	queueKey = strings.TrimSpace(strings.ToLower(queueKey))
	if strings.HasPrefix(queueKey, RebaseFollowupQueueKeyPrefix) {
		return true
	}
	return strings.TrimSpace(strings.ToLower(p.NextAction)) == RebaseNextActionAttempt ||
		strings.TrimSpace(p.RepairTensionID) != "" ||
		strings.TrimSpace(p.ForkTensionID) != ""
}

func (p RebaseFollowupPayload) LinkedActionExists() bool {
	return strings.TrimSpace(p.ActionID) != ""
}

func normalizeLineageRefs(refs []string) []string {
	if len(refs) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(refs))
	normalized := make([]string, 0, len(refs))
	for _, ref := range refs {
		trimmed := strings.TrimSpace(ref)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}
	sort.Strings(normalized)
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}
