package model

import (
	"errors"
	"strings"
)

const (
	SessionStatusActive          = "ACTIVE"
	SessionStatusBlocked         = "BLOCKED"
	SessionStatusWaitingDecision = "WAITING_DECISION"
	SessionStatusHandoffPending  = "HANDOFF_PENDING"
	SessionStatusEnded           = "ENDED"

	SessionEventStart          = "session.start"
	SessionEventStatus         = "session.status"
	SessionEventBlocked        = "session.blocked"
	SessionEventDecisionNeeded = "session.decision_needed"
	SessionEventKeepalive      = "session.keepalive"
	SessionEventEnd            = "session.end"
)

var validSessionStatuses = map[string]struct{}{
	SessionStatusActive:          {},
	SessionStatusBlocked:         {},
	SessionStatusWaitingDecision: {},
	SessionStatusHandoffPending:  {},
	SessionStatusEnded:           {},
}

var validSessionEventTypes = map[string]struct{}{
	SessionEventStart:          {},
	SessionEventStatus:         {},
	SessionEventBlocked:        {},
	SessionEventDecisionNeeded: {},
	SessionEventKeepalive:      {},
	SessionEventEnd:            {},
}

type SessionCoordinationPayloadV1 struct {
	SessionID           string                   `json:"session_id"`
	TaskID              string                   `json:"task_id,omitempty"`
	Status              string                   `json:"status"`
	Summary             string                   `json:"summary"`
	OwnerScope          string                   `json:"owner_scope,omitempty"`
	BlockedOn           []AgentUpdateBlockedRef  `json:"blocked_on,omitempty"`
	DecisionNeededFrom  string                   `json:"decision_needed_from,omitempty"`
	DecisionType        string                   `json:"decision_type,omitempty"`
	KeepSessionActive   *bool                    `json:"keep_session_active,omitempty"`
	HandoffTo           string                   `json:"handoff_to,omitempty"`
	RelatedDocKeys      []string                 `json:"related_doc_keys,omitempty"`
	RelatedArtifactRefs []AgentUpdateArtifactRef `json:"related_artifact_refs,omitempty"`
	UpdatedAt           string                   `json:"updated_at"`
}

func NormalizeSessionEventType(eventType string) string {
	return strings.ToLower(strings.TrimSpace(eventType))
}

func DefaultSessionStatusForEvent(eventType string) string {
	switch NormalizeSessionEventType(eventType) {
	case SessionEventBlocked:
		return SessionStatusBlocked
	case SessionEventDecisionNeeded:
		return SessionStatusWaitingDecision
	case SessionEventEnd:
		return SessionStatusEnded
	default:
		return SessionStatusActive
	}
}

func DefaultKeepSessionActiveForEvent(eventType string) bool {
	switch NormalizeSessionEventType(eventType) {
	case SessionEventEnd:
		return false
	default:
		return true
	}
}

func ValidSessionStatus(status string) bool {
	_, ok := validSessionStatuses[NormalizeStatus(status)]
	return ok
}

func ValidSessionEventType(eventType string) bool {
	_, ok := validSessionEventTypes[NormalizeSessionEventType(eventType)]
	return ok
}

func IsSessionStatusActive(status string) bool {
	switch NormalizeStatus(status) {
	case SessionStatusActive, SessionStatusBlocked, SessionStatusWaitingDecision, SessionStatusHandoffPending, "RUNNING":
		return true
	default:
		return false
	}
}

func (p *SessionCoordinationPayloadV1) Normalize() {
	p.SessionID = strings.TrimSpace(p.SessionID)
	p.TaskID = strings.TrimSpace(p.TaskID)
	p.Status = NormalizeStatus(p.Status)
	p.Summary = strings.TrimSpace(p.Summary)
	p.OwnerScope = strings.TrimSpace(p.OwnerScope)
	p.DecisionNeededFrom = strings.TrimSpace(p.DecisionNeededFrom)
	p.DecisionType = strings.TrimSpace(p.DecisionType)
	p.HandoffTo = strings.TrimSpace(p.HandoffTo)
	p.RelatedDocKeys = normalizeStringList(p.RelatedDocKeys)
	p.UpdatedAt = strings.TrimSpace(p.UpdatedAt)

	artifacts := make([]AgentUpdateArtifactRef, 0, len(p.RelatedArtifactRefs))
	for _, artifact := range p.RelatedArtifactRefs {
		artifact.Ref = strings.TrimSpace(artifact.Ref)
		artifact.Kind = strings.TrimSpace(artifact.Kind)
		artifact.ContentType = strings.TrimSpace(artifact.ContentType)
		if artifact.Ref == "" {
			continue
		}
		artifacts = append(artifacts, artifact)
	}
	p.RelatedArtifactRefs = artifacts

	blocked := make([]AgentUpdateBlockedRef, 0, len(p.BlockedOn))
	for _, item := range p.BlockedOn {
		item.Kind = strings.TrimSpace(item.Kind)
		item.Detail = strings.TrimSpace(item.Detail)
		if item.Kind == "" && item.Detail == "" {
			continue
		}
		blocked = append(blocked, item)
	}
	p.BlockedOn = blocked
}

func (p SessionCoordinationPayloadV1) ValidateForEvent(eventType string) error {
	if !ValidSessionEventType(eventType) {
		return errors.New("invalid session event type")
	}
	if p.SessionID == "" {
		return errors.New("session payload requires session_id")
	}
	if p.Summary == "" {
		return errors.New("session payload requires summary")
	}
	if p.Status == "" {
		return errors.New("session payload requires status")
	}
	if !ValidSessionStatus(p.Status) {
		return errors.New("session payload has invalid status")
	}
	if strings.TrimSpace(p.UpdatedAt) == "" {
		return errors.New("session payload requires updated_at")
	}
	if p.KeepSessionActive == nil {
		return errors.New("session payload requires keep_session_active")
	}
	for _, item := range p.BlockedOn {
		if item.Kind == "" {
			return errors.New("session payload blocked_on.kind is required")
		}
		if item.Detail == "" {
			return errors.New("session payload blocked_on.detail is required")
		}
	}

	switch NormalizeSessionEventType(eventType) {
	case SessionEventStart:
		if !*p.KeepSessionActive {
			return errors.New("session.start requires keep_session_active=true")
		}
	case SessionEventBlocked:
		if len(p.BlockedOn) == 0 {
			return errors.New("session.blocked requires blocked_on")
		}
	case SessionEventDecisionNeeded:
		if p.DecisionNeededFrom == "" {
			return errors.New("session.decision_needed requires decision_needed_from")
		}
	case SessionEventEnd:
		if *p.KeepSessionActive {
			return errors.New("session.end requires keep_session_active=false")
		}
	}
	return nil
}
