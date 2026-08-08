package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type MemoryInvalidationRecord struct {
	TimeAuthority               WorkspaceTimeAuthority        `json:"time_authority"`
	InvalidationID              string                        `json:"invalidation_id"`
	WorkspaceID                 string                        `json:"workspace_id"`
	AgentID                     string                        `json:"agent_id"`
	SessionID                   string                        `json:"session_id,omitempty"`
	ReportScope                 string                        `json:"report_scope"`
	ReportID                    string                        `json:"report_id,omitempty"`
	ResidencyTier               string                        `json:"residency_tier,omitempty"`
	ReplicaKind                 string                        `json:"replica_kind,omitempty"`
	CoherenceClass              string                        `json:"coherence_class,omitempty"`
	CanonicalMemoryID           string                        `json:"canonical_memory_id,omitempty"`
	CacheKey                    string                        `json:"cache_key,omitempty"`
	RefKind                     string                        `json:"ref_kind"`
	RefID                       string                        `json:"ref_id"`
	PreviousVersionToken        string                        `json:"previous_version_token,omitempty"`
	CurrentVersionToken         string                        `json:"current_version_token,omitempty"`
	DependencyRevisionVector    []MemoryResidencyVersionGuard `json:"dependency_revision_vector,omitempty"`
	DependencyVectorMalformed   bool                          `json:"dependency_revision_vector_malformed,omitempty"`
	Reason                      string                        `json:"reason"`
	TriggerCause                string                        `json:"trigger_cause,omitempty"`
	RecoveredFromInvalidationID string                        `json:"recovered_from_invalidation_id,omitempty"`
	RecoveredAt                 string                        `json:"recovered_at,omitempty"`
	RecoveryCause               string                        `json:"recovery_cause,omitempty"`
	State                       string                        `json:"state"`
	Metadata                    map[string]any                `json:"metadata,omitempty"`
	DeliveredAt                 string                        `json:"delivered_at,omitempty"`
	DeliveredNow                bool                          `json:"delivered_now,omitempty"`
	DeliveryAttemptCount        int                           `json:"delivery_attempt_count"`
	LastDeliveryAttemptAt       string                        `json:"last_delivery_attempt_at,omitempty"`
	LeaseExpiresAt              string                        `json:"lease_expires_at,omitempty"`
	NextDeliveryAt              string                        `json:"next_delivery_at,omitempty"`
	AcknowledgedAt              string                        `json:"acknowledged_at,omitempty"`
	FailureCount                int                           `json:"failure_count"`
	LastFailureAt               string                        `json:"last_failure_at,omitempty"`
	LastFailureReason           string                        `json:"last_failure_reason,omitempty"`
	DeadLetteredAt              string                        `json:"dead_lettered_at,omitempty"`
	CreatedAt                   string                        `json:"created_at"`
	UpdatedAt                   string                        `json:"updated_at"`
	TemporalContracts           []TemporalHorizonContract     `json:"temporal_contracts,omitempty"`
}

type MemoryInvalidationPollFilter struct {
	WorkspaceID   string
	AgentID       string
	SessionID     string
	IncludeAcked  bool
	Limit         int
	MarkDelivered bool
}

type MemoryInvalidationAckInput struct {
	WorkspaceID     string
	AgentID         string
	InvalidationIDs []string
}

type MemoryInvalidationFailInput struct {
	WorkspaceID     string
	AgentID         string
	InvalidationIDs []string
	FailureReason   string
}

type MemoryInvalidationRequeueInput struct {
	WorkspaceID     string
	AgentID         string
	InvalidationIDs []string
}

type MemoryInvalidationListFilter struct {
	WorkspaceID       string
	AgentID           string
	SessionID         string
	IncludeAcked      bool
	IncludeDeadLetter bool
	Limit             int
}

type MemoryInvalidationCursorRecord struct {
	TimeAuthority                  WorkspaceTimeAuthority `json:"time_authority"`
	WorkspaceID                    string                 `json:"workspace_id"`
	AgentID                        string                 `json:"agent_id"`
	SessionID                      string                 `json:"session_id,omitempty"`
	LastPolledAt                   string                 `json:"last_polled_at,omitempty"`
	LastDeliveredAt                string                 `json:"last_delivered_at,omitempty"`
	LastDeliveredInvalidationID    string                 `json:"last_delivered_invalidation_id,omitempty"`
	LastAcknowledgedAt             string                 `json:"last_acknowledged_at,omitempty"`
	LastAcknowledgedInvalidationID string                 `json:"last_acknowledged_invalidation_id,omitempty"`
	LastPollCount                  int                    `json:"last_poll_count"`
	UpdatedAt                      string                 `json:"updated_at"`
}

type memoryInvalidationTarget struct {
	WorkspaceID       string
	AgentID           string
	SessionID         string
	ReportScope       string
	ReportID          string
	ResidencyTier     string
	ReplicaKind       string
	CoherenceClass    string
	CanonicalMemoryID string
	CacheKey          string
	VersionGuards     []MemoryResidencyVersionGuard
}

type memoryInvalidationRefChange struct {
	RefKind             string
	RefID               string
	CurrentVersionToken string
	Cause               string
}

const (
	memoryInvalidationDeadLetterThreshold = 3
	memoryInvalidationDeliveryLease       = 30 * time.Second
)

func (s *Store) PollMemoryInvalidations(ctx context.Context, filter MemoryInvalidationPollFilter) ([]MemoryInvalidationRecord, error) {
	items, _, err := s.PollMemoryInvalidationsWithEvents(ctx, filter)
	return items, err
}

func (s *Store) PollMemoryInvalidationsWithEvents(ctx context.Context, filter MemoryInvalidationPollFilter) ([]MemoryInvalidationRecord, []RuntimeEventRecord, error) {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	if filter.WorkspaceID == "" {
		return nil, nil, errors.New("workspace_id is required")
	}
	filter.AgentID = strings.TrimSpace(filter.AgentID)
	if filter.AgentID == "" {
		return nil, nil, errors.New("agent_id is required")
	}
	filter.SessionID = strings.TrimSpace(filter.SessionID)
	if filter.Limit <= 0 {
		filter.Limit = 50
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	referenceAt, err := s.workspaceReferenceTimestamp(ctx, filter.WorkspaceID, now)
	if err != nil {
		return nil, nil, err
	}
	fenceInput := WorkspaceAuthorityFenceInput{
		WorkspaceID: filter.WorkspaceID,
		Scope:       authorityScopeWorkspace,
		ReferenceAt: referenceAt,
	}
	if filter.MarkDelivered {
		fenceInput, err = s.currentLocalWorkspaceAuthorityFenceInput(ctx, filter.WorkspaceID, authorityScopeWorkspace, referenceAt)
		if err != nil {
			return nil, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
		}
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("begin memory invalidation poll tx: %w", err)
	}
	items, err := listMemoryInvalidationsTx(ctx, tx, filter, referenceAt)
	if err != nil {
		_ = tx.Rollback()
		return nil, nil, err
	}
	deliveredEvents := make([]RuntimeEventRecord, 0, len(items))
	if filter.MarkDelivered {
		if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
			leaseUntil := memoryInvalidationTimestampAdd(referenceAt, memoryInvalidationDeliveryLease)
			firstDeliveryIDs := make([]string, 0, len(items))
			attemptIDs := make([]string, 0, len(items))
			delivered := make([]MemoryInvalidationRecord, 0, len(items))
			for _, item := range items {
				if item.State == "OPEN" {
					attemptIDs = append(attemptIDs, item.InvalidationID)
					item.DeliveryAttemptCount++
					item.LastDeliveryAttemptAt = now
					item.LeaseExpiresAt = leaseUntil
					item.NextDeliveryAt = ""
					item.UpdatedAt = now
					if strings.TrimSpace(item.DeliveredAt) == "" {
						firstDeliveryIDs = append(firstDeliveryIDs, item.InvalidationID)
						item.DeliveredAt = now
						item.DeliveredNow = true
						delivered = append(delivered, item)
					}
				}
			}
			if err := markMemoryInvalidationsDeliveredTx(ctx, tx, attemptIDs, firstDeliveryIDs, now, leaseUntil); err != nil {
				return err
			}
			for _, item := range delivered {
				event, err := s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
					EventID:     nextID("rtev"),
					WorkspaceID: item.WorkspaceID,
					EventType:   "memory.invalidation_delivered",
					EntityType:  "memory_invalidation",
					EntityID:    item.InvalidationID,
					ActorType:   "agent",
					ActorID:     item.AgentID,
					AgentID:     item.AgentID,
					SessionID:   item.SessionID,
					PayloadJSON: mustJSON(memoryInvalidationRuntimeEventPayload(item, "MEMORY_INVALIDATION_DELIVERED")),
					CreatedAt:   now,
				})
				if err != nil {
					return err
				}
				deliveredEvents = append(deliveredEvents, event)
			}
			for idx := range items {
				if items[idx].State == "OPEN" && strings.TrimSpace(items[idx].DeliveredAt) == "" {
					items[idx].DeliveredAt = now
					items[idx].UpdatedAt = now
					items[idx].DeliveredNow = true
				}
				if items[idx].State == "OPEN" {
					items[idx].DeliveryAttemptCount++
					items[idx].LastDeliveryAttemptAt = now
					items[idx].LeaseExpiresAt = leaseUntil
					items[idx].UpdatedAt = now
				}
			}
			if err := upsertMemoryInvalidationCursorTx(ctx, tx, MemoryInvalidationCursorRecord{
				WorkspaceID:                 filter.WorkspaceID,
				AgentID:                     filter.AgentID,
				SessionID:                   filter.SessionID,
				LastPolledAt:                now,
				LastDeliveredAt:             cursorLastDeliveredAt(delivered),
				LastDeliveredInvalidationID: cursorLastDeliveredID(delivered),
				LastPollCount:               len(items),
				UpdatedAt:                   now,
			}); err != nil {
				return err
			}
			if filter.SessionID == "" {
				sessionDelivered := make(map[string][]MemoryInvalidationRecord)
				for _, item := range delivered {
					sessionID := strings.TrimSpace(item.SessionID)
					if sessionID == "" {
						continue
					}
					sessionDelivered[sessionID] = append(sessionDelivered[sessionID], item)
				}
				for sessionID, sessionItems := range sessionDelivered {
					if err := upsertMemoryInvalidationCursorTx(ctx, tx, MemoryInvalidationCursorRecord{
						WorkspaceID:                 filter.WorkspaceID,
						AgentID:                     filter.AgentID,
						SessionID:                   sessionID,
						LastPolledAt:                now,
						LastDeliveredAt:             cursorLastDeliveredAt(sessionItems),
						LastDeliveredInvalidationID: cursorLastDeliveredID(sessionItems),
						LastPollCount:               len(sessionItems),
						UpdatedAt:                   now,
					}); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			_ = tx.Rollback()
			return nil, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
		}
	} else {
		if err := upsertMemoryInvalidationCursorTx(ctx, tx, MemoryInvalidationCursorRecord{
			WorkspaceID:   filter.WorkspaceID,
			AgentID:       filter.AgentID,
			SessionID:     filter.SessionID,
			LastPolledAt:  now,
			LastPollCount: len(items),
			UpdatedAt:     now,
		}); err != nil {
			_ = tx.Rollback()
			return nil, nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("commit memory invalidation poll tx: %w", err)
	}
	items, err = s.withMemoryInvalidationTimeAuthority(ctx, filter.WorkspaceID, items)
	if err != nil {
		return nil, nil, err
	}
	return items, deliveredEvents, nil
}

func (s *Store) AckMemoryInvalidations(ctx context.Context, input MemoryInvalidationAckInput) ([]MemoryInvalidationRecord, error) {
	items, _, err := s.AckMemoryInvalidationsWithEvents(ctx, input)
	return items, err
}

func (s *Store) AckMemoryInvalidationsWithEvents(ctx context.Context, input MemoryInvalidationAckInput) ([]MemoryInvalidationRecord, []RuntimeEventRecord, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	if input.WorkspaceID == "" {
		return nil, nil, errors.New("workspace_id is required")
	}
	input.AgentID = strings.TrimSpace(input.AgentID)
	if input.AgentID == "" {
		return nil, nil, errors.New("agent_id is required")
	}
	input.InvalidationIDs = uniqueTrimmedStrings(input.InvalidationIDs)
	if len(input.InvalidationIDs) == 0 {
		return nil, nil, errors.New("invalidation_ids is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	referenceAt, err := s.workspaceReferenceTimestamp(ctx, input.WorkspaceID, now)
	if err != nil {
		return nil, nil, err
	}
	fenceInput := WorkspaceAuthorityFenceInput{
		WorkspaceID: input.WorkspaceID,
		Scope:       authorityScopeWorkspace,
		ReferenceAt: referenceAt,
	}
	fenceInput, err = s.currentLocalWorkspaceAuthorityFenceInput(ctx, input.WorkspaceID, authorityScopeWorkspace, referenceAt)
	if err != nil {
		return nil, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("begin memory invalidation ack tx: %w", err)
	}
	out := make([]MemoryInvalidationRecord, 0, len(input.InvalidationIDs))
	events := make([]RuntimeEventRecord, 0, len(input.InvalidationIDs))
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		for _, invalidationID := range input.InvalidationIDs {
			record, event, changed, err := s.ackMemoryInvalidationTx(ctx, tx, authority, input.WorkspaceID, input.AgentID, invalidationID, referenceAt)
			if err != nil {
				return err
			}
			if !changed {
				continue
			}
			if err := upsertMemoryInvalidationCursorTx(ctx, tx, MemoryInvalidationCursorRecord{
				WorkspaceID:                    input.WorkspaceID,
				AgentID:                        input.AgentID,
				SessionID:                      "",
				LastAcknowledgedAt:             now,
				LastAcknowledgedInvalidationID: record.InvalidationID,
				UpdatedAt:                      now,
			}); err != nil {
				return err
			}
			if strings.TrimSpace(record.SessionID) != "" {
				if err := upsertMemoryInvalidationCursorTx(ctx, tx, MemoryInvalidationCursorRecord{
					WorkspaceID:                    input.WorkspaceID,
					AgentID:                        input.AgentID,
					SessionID:                      record.SessionID,
					LastAcknowledgedAt:             now,
					LastAcknowledgedInvalidationID: record.InvalidationID,
					UpdatedAt:                      now,
				}); err != nil {
					return err
				}
			}
			out = append(out, record)
			events = append(events, event)
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return nil, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("commit memory invalidation ack tx: %w", err)
	}
	out, err = s.withMemoryInvalidationTimeAuthority(ctx, input.WorkspaceID, out)
	if err != nil {
		return nil, nil, err
	}
	return out, events, nil
}

func (s *Store) FailMemoryInvalidations(ctx context.Context, input MemoryInvalidationFailInput) ([]MemoryInvalidationRecord, error) {
	items, _, err := s.FailMemoryInvalidationsWithEvents(ctx, input)
	return items, err
}

func (s *Store) FailMemoryInvalidationsWithEvents(ctx context.Context, input MemoryInvalidationFailInput) ([]MemoryInvalidationRecord, []RuntimeEventRecord, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	if input.WorkspaceID == "" {
		return nil, nil, errors.New("workspace_id is required")
	}
	input.AgentID = strings.TrimSpace(input.AgentID)
	if input.AgentID == "" {
		return nil, nil, errors.New("agent_id is required")
	}
	input.InvalidationIDs = uniqueTrimmedStrings(input.InvalidationIDs)
	if len(input.InvalidationIDs) == 0 {
		return nil, nil, errors.New("invalidation_ids is required")
	}
	input.FailureReason = normalizeMemoryInvalidationFailureReason(input.FailureReason)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	referenceAt, err := s.workspaceReferenceTimestamp(ctx, input.WorkspaceID, now)
	if err != nil {
		return nil, nil, err
	}
	fenceInput := WorkspaceAuthorityFenceInput{
		WorkspaceID: input.WorkspaceID,
		Scope:       authorityScopeWorkspace,
		ReferenceAt: referenceAt,
	}
	fenceInput, err = s.currentLocalWorkspaceAuthorityFenceInput(ctx, input.WorkspaceID, authorityScopeWorkspace, referenceAt)
	if err != nil {
		return nil, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("begin memory invalidation fail tx: %w", err)
	}
	out := make([]MemoryInvalidationRecord, 0, len(input.InvalidationIDs))
	events := make([]RuntimeEventRecord, 0, len(input.InvalidationIDs))
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		for _, invalidationID := range input.InvalidationIDs {
			record, event, changed, err := s.failMemoryInvalidationTx(ctx, tx, authority, input.WorkspaceID, input.AgentID, invalidationID, input.FailureReason, referenceAt)
			if err != nil {
				return err
			}
			if !changed {
				continue
			}
			out = append(out, record)
			events = append(events, event)
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return nil, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("commit memory invalidation fail tx: %w", err)
	}
	out, err = s.withMemoryInvalidationTimeAuthority(ctx, input.WorkspaceID, out)
	if err != nil {
		return nil, nil, err
	}
	return out, events, nil
}

func (s *Store) RequeueMemoryInvalidations(ctx context.Context, input MemoryInvalidationRequeueInput) ([]MemoryInvalidationRecord, error) {
	items, _, err := s.RequeueMemoryInvalidationsWithEvents(ctx, input)
	return items, err
}

func (s *Store) RequeueMemoryInvalidationsWithEvents(ctx context.Context, input MemoryInvalidationRequeueInput) ([]MemoryInvalidationRecord, []RuntimeEventRecord, error) {
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	if input.WorkspaceID == "" {
		return nil, nil, errors.New("workspace_id is required")
	}
	input.AgentID = strings.TrimSpace(input.AgentID)
	if input.AgentID == "" {
		return nil, nil, errors.New("agent_id is required")
	}
	input.InvalidationIDs = uniqueTrimmedStrings(input.InvalidationIDs)
	if len(input.InvalidationIDs) == 0 {
		return nil, nil, errors.New("invalidation_ids is required")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	referenceAt, err := s.workspaceReferenceTimestamp(ctx, input.WorkspaceID, now)
	if err != nil {
		return nil, nil, err
	}
	fenceInput := WorkspaceAuthorityFenceInput{
		WorkspaceID: input.WorkspaceID,
		Scope:       authorityScopeWorkspace,
		ReferenceAt: referenceAt,
	}
	fenceInput, err = s.currentLocalWorkspaceAuthorityFenceInput(ctx, input.WorkspaceID, authorityScopeWorkspace, referenceAt)
	if err != nil {
		return nil, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("begin memory invalidation requeue tx: %w", err)
	}
	out := make([]MemoryInvalidationRecord, 0, len(input.InvalidationIDs))
	events := make([]RuntimeEventRecord, 0, len(input.InvalidationIDs))
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		for _, invalidationID := range input.InvalidationIDs {
			record, event, changed, err := s.requeueMemoryInvalidationTx(ctx, tx, authority, input.WorkspaceID, input.AgentID, invalidationID, referenceAt)
			if err != nil {
				return err
			}
			if !changed {
				continue
			}
			out = append(out, record)
			events = append(events, event)
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return nil, nil, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("commit memory invalidation requeue tx: %w", err)
	}
	out, err = s.withMemoryInvalidationTimeAuthority(ctx, input.WorkspaceID, out)
	if err != nil {
		return nil, nil, err
	}
	return out, events, nil
}

func (s *Store) ListMemoryInvalidations(ctx context.Context, filter MemoryInvalidationListFilter) ([]MemoryInvalidationRecord, error) {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	if filter.WorkspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	filter.AgentID = strings.TrimSpace(filter.AgentID)
	if filter.AgentID == "" {
		return nil, errors.New("agent_id is required")
	}
	filter.SessionID = strings.TrimSpace(filter.SessionID)
	if filter.Limit <= 0 {
		filter.Limit = 50
	}

	rows, err := s.db.QueryContext(
		ctx,
		buildMemoryInvalidationListQuery(filter),
		buildMemoryInvalidationListArgs(filter)...,
	)
	if err != nil {
		return nil, fmt.Errorf("list memory invalidations: %w", err)
	}
	defer rows.Close()
	items, err := collectMemoryInvalidationRows(rows)
	if err != nil {
		return nil, err
	}
	return s.withMemoryInvalidationTimeAuthority(ctx, filter.WorkspaceID, items)
}

func (s *Store) GetMemoryInvalidation(ctx context.Context, workspaceID, agentID, invalidationID string) (MemoryInvalidationRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return MemoryInvalidationRecord{}, errors.New("workspace_id is required")
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return MemoryInvalidationRecord{}, errors.New("agent_id is required")
	}
	invalidationID = strings.TrimSpace(invalidationID)
	if invalidationID == "" {
		return MemoryInvalidationRecord{}, errors.New("invalidation_id is required")
	}
	row := s.db.QueryRowContext(
		ctx,
		`SELECT invalidation_id, workspace_id, agent_id, session_id, report_scope, report_id,
		        residency_tier, replica_kind, coherence_class, canonical_memory_id, cache_key,
		        ref_kind, ref_id, previous_version_token, current_version_token, reason, state,
		        metadata_json, delivered_at, delivery_attempt_count, last_delivery_attempt_at,
		        lease_expires_at, next_delivery_at, acknowledged_at,
		        failure_count, last_failure_at, last_failure_reason, dead_lettered_at,
		        created_at, updated_at
		   FROM memory_invalidation_queue
		  WHERE workspace_id = ? AND agent_id = ? AND invalidation_id = ?`,
		workspaceID,
		agentID,
		invalidationID,
	)
	record, err := scanMemoryInvalidationRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return MemoryInvalidationRecord{}, fmt.Errorf("memory invalidation not found: %s/%s/%s", workspaceID, agentID, invalidationID)
		}
		return MemoryInvalidationRecord{}, err
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return MemoryInvalidationRecord{}, err
	}
	record.TimeAuthority = authority
	return record, nil
}

func (s *Store) GetMemoryInvalidationCursor(ctx context.Context, workspaceID, agentID, sessionID string) (MemoryInvalidationCursorRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return MemoryInvalidationCursorRecord{}, errors.New("workspace_id is required")
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return MemoryInvalidationCursorRecord{}, errors.New("agent_id is required")
	}
	sessionID = strings.TrimSpace(sessionID)
	row := s.db.QueryRowContext(
		ctx,
		`SELECT workspace_id, agent_id, session_id,
		        last_polled_at, last_delivered_at, last_delivered_invalidation_id,
		        last_acknowledged_at, last_acknowledged_invalidation_id, last_poll_count, updated_at
		   FROM memory_invalidation_cursors
		  WHERE workspace_id = ? AND agent_id = ? AND session_id = ?`,
		workspaceID,
		agentID,
		sessionID,
	)
	record, err := scanMemoryInvalidationCursorRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return MemoryInvalidationCursorRecord{}, fmt.Errorf("memory invalidation cursor not found: %s/%s/%s", workspaceID, agentID, sessionID)
		}
		return MemoryInvalidationCursorRecord{}, err
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return MemoryInvalidationCursorRecord{}, err
	}
	record.TimeAuthority = authority
	return record, nil
}

func (s *Store) withMemoryInvalidationTimeAuthority(ctx context.Context, workspaceID string, items []MemoryInvalidationRecord) ([]MemoryInvalidationRecord, error) {
	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	for idx := range items {
		items[idx].TimeAuthority = authority
		applyMemoryInvalidationTemporalContracts(&items[idx])
	}
	return items, nil
}

func (s *Store) enqueueMemoryInvalidationsForResidencyReportTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, input MemoryResidencyReportInput) ([]MemoryInvalidationRecord, []RuntimeEventRecord, error) {
	if len(input.Replicas) == 0 {
		return nil, nil, nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	out := make([]MemoryInvalidationRecord, 0)
	events := make([]RuntimeEventRecord, 0)
	for _, replica := range input.Replicas {
		if !shouldAutoEnqueueMemoryInvalidation(replica.ResidencyTier, replica.CoherenceClass) || normalizeMemoryReplicaLifecycleState(replica.State) == "EVICTED" {
			continue
		}
		target := memoryInvalidationTarget{
			WorkspaceID:       strings.TrimSpace(input.WorkspaceID),
			AgentID:           strings.TrimSpace(input.AgentID),
			SessionID:         strings.TrimSpace(input.SessionID),
			ReportScope:       strings.TrimSpace(input.ReportScope),
			ReportID:          strings.TrimSpace(input.ReportID),
			ResidencyTier:     strings.TrimSpace(replica.ResidencyTier),
			ReplicaKind:       strings.TrimSpace(replica.ReplicaKind),
			CoherenceClass:    strings.TrimSpace(replica.CoherenceClass),
			CanonicalMemoryID: strings.TrimSpace(replica.CanonicalMemoryID),
			CacheKey:          strings.TrimSpace(replica.CacheKey),
			VersionGuards:     replica.VersionGuards,
		}
		for _, guard := range dedupeMemoryVersionGuards(strings.TrimSpace(input.WorkspaceID), replica.VersionGuards) {
			currentToken, reason, err := s.resolveCurrentInvalidationVersionGuardTx(ctx, tx, strings.TrimSpace(input.WorkspaceID), guard)
			if err != nil {
				return nil, nil, err
			}
			if reason == "" {
				continue
			}
			record, event, inserted, err := s.enqueueMemoryInvalidationTx(ctx, tx, authority, target, guard, currentToken, reason, now, map[string]any{
				"cause": "residency_report",
			})
			if err != nil {
				return nil, nil, err
			}
			if event.EventID != "" {
				events = append(events, event)
			}
			if inserted {
				out = append(out, record)
			}
		}
	}
	return out, events, nil
}

func (s *Store) enqueueMemoryInvalidationsForRefChangeTx(ctx context.Context, tx *sql.Tx, workspaceID string, change memoryInvalidationRefChange) ([]MemoryInvalidationRecord, error) {
	records, _, err := s.enqueueMemoryInvalidationsForRefChangeTxWithEvents(ctx, tx, workspaceID, change)
	return records, err
}

func (s *Store) enqueueMemoryInvalidationsForRefChangeTxWithEvents(ctx context.Context, tx *sql.Tx, workspaceID string, change memoryInvalidationRefChange) ([]MemoryInvalidationRecord, []RuntimeEventRecord, error) {
	return s.enqueueMemoryInvalidationsForRefChangeWithAuthorityTxWithEvents(ctx, tx, WorkspaceAuthorityRecord{}, workspaceID, change)
}

func (s *Store) enqueueMemoryInvalidationsForRefChangeWithAuthorityTxWithEvents(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, workspaceID string, change memoryInvalidationRefChange) ([]MemoryInvalidationRecord, []RuntimeEventRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	change.RefKind = normalizeMemoryInvalidationRefKind(change.RefKind)
	change.RefID = strings.TrimSpace(change.RefID)
	change.CurrentVersionToken = strings.TrimSpace(change.CurrentVersionToken)
	change.Cause = strings.TrimSpace(change.Cause)
	if workspaceID == "" || change.RefKind == "" || change.RefID == "" {
		return nil, nil, nil
	}
	targets, err := listLatestMemoryInvalidationTargetsTx(ctx, tx, workspaceID)
	if err != nil {
		return nil, nil, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	out := make([]MemoryInvalidationRecord, 0)
	events := make([]RuntimeEventRecord, 0)
	for _, target := range targets {
		if !shouldAutoEnqueueMemoryInvalidation(target.ResidencyTier, target.CoherenceClass) {
			continue
		}
		for _, guard := range dedupeMemoryVersionGuards(workspaceID, target.VersionGuards) {
			if normalizeMemoryInvalidationRefKind(guard.RefKind) != change.RefKind || strings.TrimSpace(guard.RefID) != change.RefID {
				continue
			}
			reason := "VERSION_CHANGED"
			if change.CurrentVersionToken == "" {
				reason = "SOURCE_MISSING"
			} else if strings.TrimSpace(guard.VersionToken) == change.CurrentVersionToken {
				continue
			}
			record, event, inserted, err := s.enqueueMemoryInvalidationTx(ctx, tx, authority, target, guard, change.CurrentVersionToken, reason, now, map[string]any{
				"cause": change.Cause,
			})
			if err != nil {
				return nil, nil, err
			}
			if event.EventID != "" {
				events = append(events, event)
			}
			if inserted {
				out = append(out, record)
			}
		}
	}
	return out, events, nil
}

func (s *Store) ackMemoryInvalidationTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, workspaceID, agentID, invalidationID, referenceAt string) (MemoryInvalidationRecord, RuntimeEventRecord, bool, error) {
	current, err := getMemoryInvalidationTx(ctx, tx, workspaceID, invalidationID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, nil
		}
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, err
	}
	if current.AgentID != agentID || current.State != "OPEN" || !memoryInvalidationLeaseActiveAt(current, referenceAt) {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, nil
	}
	result, err := tx.ExecContext(
		ctx,
		`UPDATE memory_invalidation_queue
		    SET state = 'ACKED',
		        acknowledged_at = ?,
		        lease_expires_at = '',
		        next_delivery_at = '',
		        updated_at = ?,
		        invalidation_key = invalidation_key || '|ACKED|' || invalidation_id
		  WHERE workspace_id = ? AND agent_id = ? AND invalidation_id = ? AND state = 'OPEN'
		    AND delivered_at <> '' AND lease_expires_at <> '' AND lease_expires_at > ?`,
		referenceAt,
		referenceAt,
		workspaceID,
		agentID,
		invalidationID,
		referenceAt,
	)
	if err != nil {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, fmt.Errorf("ack memory invalidation: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, nil
	}
	record, err := getMemoryInvalidationTx(ctx, tx, workspaceID, invalidationID)
	if err != nil {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, err
	}
	event, err := s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_acked",
		EntityType:  "memory_invalidation",
		EntityID:    invalidationID,
		ActorType:   "agent",
		ActorID:     agentID,
		AgentID:     agentID,
		SessionID:   record.SessionID,
		PayloadJSON: mustJSON(memoryInvalidationRuntimeEventPayload(record, "MEMORY_INVALIDATION_ACK")),
		CreatedAt:   referenceAt,
	})
	if err != nil {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, err
	}
	return record, event, true, nil
}

func (s *Store) failMemoryInvalidationTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, workspaceID, agentID, invalidationID, failureReason, referenceAt string) (MemoryInvalidationRecord, RuntimeEventRecord, bool, error) {
	current, err := getMemoryInvalidationTx(ctx, tx, workspaceID, invalidationID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, nil
		}
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, err
	}
	if current.AgentID != agentID || current.State != "OPEN" || !memoryInvalidationLeaseActiveAt(current, referenceAt) {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, nil
	}
	nextFailureCount := current.FailureCount + 1
	nextDeliveryAt := ""
	if nextFailureCount < memoryInvalidationDeadLetterThreshold {
		nextDeliveryAt = memoryInvalidationTimestampAdd(referenceAt, memoryInvalidationRetryDelay(nextFailureCount))
	}
	result, err := tx.ExecContext(
		ctx,
		`UPDATE memory_invalidation_queue
		    SET failure_count = failure_count + 1,
		        last_failure_at = ?,
		        last_failure_reason = ?,
		        lease_expires_at = '',
		        next_delivery_at = CASE
		            WHEN failure_count + 1 >= ? THEN ''
		            ELSE ?
		        END,
		        dead_lettered_at = CASE WHEN failure_count + 1 >= ? THEN ? ELSE dead_lettered_at END,
		        state = CASE WHEN failure_count + 1 >= ? THEN 'DEAD_LETTER' ELSE 'OPEN' END,
		        invalidation_key = CASE WHEN failure_count + 1 >= ? THEN invalidation_key || '|DEAD|' || invalidation_id ELSE invalidation_key END,
		        updated_at = ?
		  WHERE workspace_id = ? AND agent_id = ? AND invalidation_id = ? AND state = 'OPEN'
		    AND delivered_at <> '' AND lease_expires_at <> '' AND lease_expires_at > ?`,
		referenceAt,
		failureReason,
		memoryInvalidationDeadLetterThreshold,
		nextDeliveryAt,
		memoryInvalidationDeadLetterThreshold,
		referenceAt,
		memoryInvalidationDeadLetterThreshold,
		memoryInvalidationDeadLetterThreshold,
		referenceAt,
		workspaceID,
		agentID,
		invalidationID,
		referenceAt,
	)
	if err != nil {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, fmt.Errorf("fail memory invalidation: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, nil
	}
	record, err := getMemoryInvalidationTx(ctx, tx, workspaceID, invalidationID)
	if err != nil {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, err
	}
	eventType := "memory.invalidation_failed"
	typedEventType := "MEMORY_INVALIDATION_FAILURE"
	if record.State == "DEAD_LETTER" {
		eventType = "memory.invalidation_dead_lettered"
		typedEventType = "MEMORY_INVALIDATION_DEAD_LETTER"
	}
	event, err := s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "memory_invalidation",
		EntityID:    invalidationID,
		ActorType:   "agent",
		ActorID:     agentID,
		AgentID:     agentID,
		SessionID:   record.SessionID,
		PayloadJSON: mustJSON(memoryInvalidationRuntimeEventPayload(record, typedEventType)),
		CreatedAt:   referenceAt,
	})
	if err != nil {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, err
	}
	return record, event, true, nil
}

func (s *Store) enqueueMemoryInvalidationTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, target memoryInvalidationTarget, guard MemoryResidencyVersionGuard, currentVersionToken, reason, now string, metadata map[string]any) (MemoryInvalidationRecord, RuntimeEventRecord, bool, error) {
	metadata = memoryInvalidationMetadataWithDependencyVector(strings.TrimSpace(target.WorkspaceID), target.VersionGuards, metadata)
	record := MemoryInvalidationRecord{
		InvalidationID:       nextID("meminv"),
		WorkspaceID:          strings.TrimSpace(target.WorkspaceID),
		AgentID:              strings.TrimSpace(target.AgentID),
		SessionID:            strings.TrimSpace(target.SessionID),
		ReportScope:          firstNonEmpty(strings.TrimSpace(target.ReportScope), "AGENT"),
		ReportID:             strings.TrimSpace(target.ReportID),
		ResidencyTier:        strings.TrimSpace(target.ResidencyTier),
		ReplicaKind:          strings.TrimSpace(target.ReplicaKind),
		CoherenceClass:       strings.TrimSpace(target.CoherenceClass),
		CanonicalMemoryID:    strings.TrimSpace(target.CanonicalMemoryID),
		CacheKey:             strings.TrimSpace(target.CacheKey),
		RefKind:              normalizeMemoryInvalidationRefKind(guard.RefKind),
		RefID:                strings.TrimSpace(guard.RefID),
		PreviousVersionToken: strings.TrimSpace(guard.VersionToken),
		CurrentVersionToken:  strings.TrimSpace(currentVersionToken),
		Reason:               normalizeMemoryInvalidationReason(reason),
		State:                "OPEN",
		Metadata:             metadata,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	record.DependencyRevisionVector, record.DependencyVectorMalformed = memoryInvalidationDependencyRevisionVector(record.WorkspaceID, record.Metadata)
	record.TriggerCause = memoryInvalidationMetadataString(record.Metadata, "cause")
	record.RecoveredFromInvalidationID = memoryInvalidationMetadataString(record.Metadata, "recovered_from_invalidation_id")
	record.RecoveredAt = memoryInvalidationMetadataString(record.Metadata, "recovered_at")
	record.RecoveryCause = memoryInvalidationMetadataString(record.Metadata, "recovery_cause")
	if record.DependencyVectorMalformed && len(record.DependencyRevisionVector) == 0 {
		if record.Metadata == nil {
			record.Metadata = map[string]any{}
		}
		record.Metadata["dependency_revision_vector_malformed"] = true
	}
	recordKey := memoryInvalidationRecordKey(record)
	suppressed, err := hasEquivalentAckedMemoryInvalidationTx(ctx, tx, record)
	if err != nil {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, err
	}
	if suppressed {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, nil
	}
	if current, handled, refreshed, err := s.refreshEquivalentOpenMemoryInvalidationTx(ctx, tx, record, recordKey, now); err != nil {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, err
	} else if handled {
		if refreshed {
			event, err := s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
				EventID:     nextID("rtev"),
				WorkspaceID: current.WorkspaceID,
				EventType:   "memory.invalidation_refreshed",
				EntityType:  "memory_invalidation",
				EntityID:    current.InvalidationID,
				ActorType:   "system",
				ActorID:     "memory_coherence",
				AgentID:     current.AgentID,
				SessionID:   current.SessionID,
				PayloadJSON: mustJSON(memoryInvalidationRuntimeEventPayload(current, "MEMORY_INVALIDATION_REFRESHED")),
				CreatedAt:   now,
			})
			if err != nil {
				return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, err
			}
			return current, event, false, nil
		}
		return current, RuntimeEventRecord{}, false, nil
	}
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO memory_invalidation_queue(
		    invalidation_id, invalidation_key, workspace_id, agent_id, session_id, report_scope, report_id,
		    residency_tier, replica_kind, coherence_class, canonical_memory_id, cache_key,
		    ref_kind, ref_id, previous_version_token, current_version_token, reason, state,
		    metadata_json, delivered_at, delivery_attempt_count, last_delivery_attempt_at, acknowledged_at,
		    lease_expires_at, next_delivery_at, failure_count, last_failure_at, last_failure_reason, dead_lettered_at, created_at, updated_at
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', 0, '', '', '', '', 0, '', '', '', ?, ?)
		  ON CONFLICT(invalidation_key) DO NOTHING`,
		record.InvalidationID,
		recordKey,
		record.WorkspaceID,
		record.AgentID,
		record.SessionID,
		record.ReportScope,
		record.ReportID,
		record.ResidencyTier,
		record.ReplicaKind,
		record.CoherenceClass,
		record.CanonicalMemoryID,
		record.CacheKey,
		record.RefKind,
		record.RefID,
		record.PreviousVersionToken,
		record.CurrentVersionToken,
		record.Reason,
		record.State,
		encodeMemoryResidencyJSONMap(record.Metadata),
		record.CreatedAt,
		record.UpdatedAt,
	)
	if err != nil {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, fmt.Errorf("insert memory invalidation: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, nil
	}
	event, err := s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: record.WorkspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		EntityID:    record.InvalidationID,
		ActorType:   "system",
		ActorID:     "memory_coherence",
		AgentID:     record.AgentID,
		SessionID:   record.SessionID,
		PayloadJSON: mustJSON(memoryInvalidationRuntimeEventPayload(record, "MEMORY_INVALIDATION")),
		CreatedAt:   now,
	})
	if err != nil {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, err
	}
	return record, event, true, nil
}

func (s *Store) requeueMemoryInvalidationTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, workspaceID, agentID, invalidationID, now string) (MemoryInvalidationRecord, RuntimeEventRecord, bool, error) {
	current, err := getMemoryInvalidationTx(ctx, tx, workspaceID, invalidationID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, nil
		}
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, err
	}
	if current.AgentID != agentID || current.State != "DEAD_LETTER" {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, nil
	}

	metadata := make(map[string]any, len(current.Metadata)+3)
	for k, v := range current.Metadata {
		metadata[k] = v
	}
	metadata["recovered_from_invalidation_id"] = current.InvalidationID
	metadata["recovered_at"] = now
	metadata["recovery_cause"] = "dead_letter_requeue"

	record := current
	record.InvalidationID = nextID("meminv")
	record.State = "OPEN"
	record.Metadata = metadata
	record.DependencyRevisionVector, record.DependencyVectorMalformed = memoryInvalidationDependencyRevisionVector(record.WorkspaceID, record.Metadata)
	record.TriggerCause = memoryInvalidationMetadataString(record.Metadata, "cause")
	record.RecoveredFromInvalidationID = memoryInvalidationMetadataString(record.Metadata, "recovered_from_invalidation_id")
	record.RecoveredAt = memoryInvalidationMetadataString(record.Metadata, "recovered_at")
	record.RecoveryCause = memoryInvalidationMetadataString(record.Metadata, "recovery_cause")
	if record.DependencyVectorMalformed && len(record.DependencyRevisionVector) == 0 {
		if record.Metadata == nil {
			record.Metadata = map[string]any{}
		}
		record.Metadata["dependency_revision_vector_malformed"] = true
	}
	record.DeliveredAt = ""
	record.DeliveryAttemptCount = 0
	record.LastDeliveryAttemptAt = ""
	record.LeaseExpiresAt = ""
	record.NextDeliveryAt = ""
	record.AcknowledgedAt = ""
	record.FailureCount = 0
	record.LastFailureAt = ""
	record.LastFailureReason = ""
	record.DeadLetteredAt = ""
	record.CreatedAt = now
	record.UpdatedAt = now

	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO memory_invalidation_queue(
		    invalidation_id, invalidation_key, workspace_id, agent_id, session_id, report_scope, report_id,
		    residency_tier, replica_kind, coherence_class, canonical_memory_id, cache_key,
		    ref_kind, ref_id, previous_version_token, current_version_token, reason, state,
		    metadata_json, delivered_at, delivery_attempt_count, last_delivery_attempt_at, acknowledged_at,
		    lease_expires_at, next_delivery_at, failure_count, last_failure_at, last_failure_reason, dead_lettered_at, created_at, updated_at
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', 0, '', '', '', '', 0, '', '', '', ?, ?)
		  ON CONFLICT(invalidation_key) DO NOTHING`,
		record.InvalidationID,
		memoryInvalidationRecordKey(record),
		record.WorkspaceID,
		record.AgentID,
		record.SessionID,
		record.ReportScope,
		record.ReportID,
		record.ResidencyTier,
		record.ReplicaKind,
		record.CoherenceClass,
		record.CanonicalMemoryID,
		record.CacheKey,
		record.RefKind,
		record.RefID,
		record.PreviousVersionToken,
		record.CurrentVersionToken,
		record.Reason,
		record.State,
		encodeMemoryResidencyJSONMap(record.Metadata),
		record.CreatedAt,
		record.UpdatedAt,
	)
	if err != nil {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, fmt.Errorf("requeue memory invalidation: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, nil
	}
	payload := memoryInvalidationRuntimeEventPayload(record, "MEMORY_INVALIDATION_REQUEUE")
	payload["recovered_from_invalidation_id"] = current.InvalidationID
	event, err := s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     nextID("rtev"),
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_requeued",
		EntityType:  "memory_invalidation",
		EntityID:    record.InvalidationID,
		ActorType:   "agent",
		ActorID:     agentID,
		AgentID:     agentID,
		SessionID:   record.SessionID,
		PayloadJSON: mustJSON(payload),
		CreatedAt:   now,
	})
	if err != nil {
		return MemoryInvalidationRecord{}, RuntimeEventRecord{}, false, err
	}
	return record, event, true, nil
}

func listMemoryInvalidationsTx(ctx context.Context, tx *sql.Tx, filter MemoryInvalidationPollFilter, now string) ([]MemoryInvalidationRecord, error) {
	rows, err := tx.QueryContext(ctx, buildMemoryInvalidationPollQuery(filter), buildMemoryInvalidationPollArgs(filter, now)...)
	if err != nil {
		return nil, fmt.Errorf("list memory invalidations: %w", err)
	}
	defer rows.Close()
	return collectMemoryInvalidationRows(rows)
}

func memoryInvalidationTimestampAdd(base string, delta time.Duration) string {
	if parsed, ok := controlParseTimestamp(base); ok {
		return parsed.Add(delta).UTC().Format(time.RFC3339Nano)
	}
	return time.Now().UTC().Add(delta).Format(time.RFC3339Nano)
}

func memoryInvalidationLeaseActiveAt(record MemoryInvalidationRecord, referenceAt string) bool {
	if strings.TrimSpace(record.DeliveredAt) == "" || strings.TrimSpace(record.LeaseExpiresAt) == "" {
		return false
	}
	referenceTS, ok := controlParseTimestamp(referenceAt)
	if !ok {
		return false
	}
	leaseTS, ok := controlParseTimestamp(record.LeaseExpiresAt)
	if !ok {
		return false
	}
	return leaseTS.After(referenceTS)
}

func getMemoryInvalidationTx(ctx context.Context, tx *sql.Tx, workspaceID, invalidationID string) (MemoryInvalidationRecord, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT invalidation_id, workspace_id, agent_id, session_id, report_scope, report_id,
		        residency_tier, replica_kind, coherence_class, canonical_memory_id, cache_key,
		        ref_kind, ref_id, previous_version_token, current_version_token, reason, state,
		        metadata_json, delivered_at, delivery_attempt_count, last_delivery_attempt_at,
		        lease_expires_at, next_delivery_at, acknowledged_at,
		        failure_count, last_failure_at, last_failure_reason, dead_lettered_at,
		        created_at, updated_at
		   FROM memory_invalidation_queue
		  WHERE workspace_id = ? AND invalidation_id = ?`,
		workspaceID,
		invalidationID,
	)
	record, err := scanMemoryInvalidationRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return MemoryInvalidationRecord{}, fmt.Errorf("memory invalidation not found: %s/%s", workspaceID, invalidationID)
		}
		return MemoryInvalidationRecord{}, err
	}
	return record, nil
}

func listLatestMemoryInvalidationTargetsTx(ctx context.Context, tx *sql.Tx, workspaceID string) ([]memoryInvalidationTarget, error) {
	rows, err := tx.QueryContext(
		ctx,
		`WITH ranked_reports AS (
		    SELECT report_id, workspace_id, agent_id, session_id, report_scope,
		           ROW_NUMBER() OVER (
		               PARTITION BY workspace_id, agent_id, session_id, report_scope
		               ORDER BY updated_at DESC, report_id DESC
		           ) AS rn
		      FROM memory_residency_reports
		     WHERE workspace_id = ?
		  ),
		  latest_reports AS (
		    SELECT report_id, workspace_id, agent_id, session_id, report_scope
		      FROM ranked_reports
		     WHERE rn = 1
		  )
		  SELECT lr.workspace_id, lr.agent_id, lr.session_id, lr.report_scope, lr.report_id,
		         rs.residency_tier, rs.replica_kind, rs.coherence_class, rs.canonical_memory_id, rs.cache_key,
		         rs.version_guard_json
		    FROM latest_reports lr
		    JOIN memory_replica_states rs ON rs.report_id = lr.report_id
		   WHERE rs.state <> 'EVICTED'
		   ORDER BY lr.agent_id, lr.session_id, lr.report_scope, rs.residency_tier, rs.replica_kind, rs.replica_state_id`,
		workspaceID,
	)
	if err != nil {
		return nil, fmt.Errorf("list latest memory invalidation targets: %w", err)
	}
	defer rows.Close()
	out := make([]memoryInvalidationTarget, 0)
	for rows.Next() {
		var (
			target           memoryInvalidationTarget
			versionGuardJSON string
		)
		if err := rows.Scan(
			&target.WorkspaceID,
			&target.AgentID,
			&target.SessionID,
			&target.ReportScope,
			&target.ReportID,
			&target.ResidencyTier,
			&target.ReplicaKind,
			&target.CoherenceClass,
			&target.CanonicalMemoryID,
			&target.CacheKey,
			&versionGuardJSON,
		); err != nil {
			return nil, fmt.Errorf("scan latest memory invalidation target: %w", err)
		}
		target.VersionGuards = decodeMemoryResidencyVersionGuards(versionGuardJSON)
		out = append(out, target)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate latest memory invalidation targets: %w", err)
	}
	return out, nil
}

func collectMemoryInvalidationRows(rows *sql.Rows) ([]MemoryInvalidationRecord, error) {
	out := make([]MemoryInvalidationRecord, 0)
	for rows.Next() {
		record, err := scanMemoryInvalidationRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memory invalidations: %w", err)
	}
	return out, nil
}

func scanMemoryInvalidationCursorRecord(scanner interface{ Scan(dest ...any) error }) (MemoryInvalidationCursorRecord, error) {
	var record MemoryInvalidationCursorRecord
	if err := scanner.Scan(
		&record.WorkspaceID,
		&record.AgentID,
		&record.SessionID,
		&record.LastPolledAt,
		&record.LastDeliveredAt,
		&record.LastDeliveredInvalidationID,
		&record.LastAcknowledgedAt,
		&record.LastAcknowledgedInvalidationID,
		&record.LastPollCount,
		&record.UpdatedAt,
	); err != nil {
		return MemoryInvalidationCursorRecord{}, fmt.Errorf("scan memory invalidation cursor: %w", err)
	}
	return record, nil
}

func scanMemoryInvalidationRecord(scanner interface{ Scan(dest ...any) error }) (MemoryInvalidationRecord, error) {
	var (
		record       MemoryInvalidationRecord
		metadataJSON string
	)
	if err := scanner.Scan(
		&record.InvalidationID,
		&record.WorkspaceID,
		&record.AgentID,
		&record.SessionID,
		&record.ReportScope,
		&record.ReportID,
		&record.ResidencyTier,
		&record.ReplicaKind,
		&record.CoherenceClass,
		&record.CanonicalMemoryID,
		&record.CacheKey,
		&record.RefKind,
		&record.RefID,
		&record.PreviousVersionToken,
		&record.CurrentVersionToken,
		&record.Reason,
		&record.State,
		&metadataJSON,
		&record.DeliveredAt,
		&record.DeliveryAttemptCount,
		&record.LastDeliveryAttemptAt,
		&record.LeaseExpiresAt,
		&record.NextDeliveryAt,
		&record.AcknowledgedAt,
		&record.FailureCount,
		&record.LastFailureAt,
		&record.LastFailureReason,
		&record.DeadLetteredAt,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return MemoryInvalidationRecord{}, err
	}
	record.Metadata = decodeMemoryResidencyJSONMap(metadataJSON)
	record.DependencyRevisionVector, record.DependencyVectorMalformed = memoryInvalidationDependencyRevisionVector(record.WorkspaceID, record.Metadata)
	if len(record.DependencyRevisionVector) > 0 {
		if record.Metadata == nil {
			record.Metadata = map[string]any{}
		}
		record.Metadata["dependency_revision_vector"] = record.DependencyRevisionVector
		delete(record.Metadata, "dependency_revision_vector_malformed")
	} else if record.DependencyVectorMalformed {
		if record.Metadata == nil {
			record.Metadata = map[string]any{}
		}
		record.Metadata["dependency_revision_vector_malformed"] = true
	}
	record.TriggerCause = memoryInvalidationMetadataString(record.Metadata, "cause")
	record.RecoveredFromInvalidationID = memoryInvalidationMetadataString(record.Metadata, "recovered_from_invalidation_id")
	record.RecoveredAt = memoryInvalidationMetadataString(record.Metadata, "recovered_at")
	record.RecoveryCause = memoryInvalidationMetadataString(record.Metadata, "recovery_cause")
	return record, nil
}

func markMemoryInvalidationsDeliveredTx(ctx context.Context, tx *sql.Tx, attemptIDs, firstDeliveryIDs []string, now, leaseUntil string) error {
	if len(attemptIDs) == 0 {
		return nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(attemptIDs)), ",")
	args := make([]any, 0, 4+len(attemptIDs))
	args = append(args, now, leaseUntil, "", now)
	for _, id := range attemptIDs {
		args = append(args, id)
	}
	query := fmt.Sprintf(`UPDATE memory_invalidation_queue
	    SET delivery_attempt_count = delivery_attempt_count + 1,
	        last_delivery_attempt_at = ?,
	        lease_expires_at = ?,
	        next_delivery_at = ?,
	        updated_at = ?
	  WHERE invalidation_id IN (%s) AND state = 'OPEN'`, placeholders)
	_, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("mark memory invalidations delivered: %w", err)
	}
	if len(firstDeliveryIDs) == 0 {
		return nil
	}
	firstPlaceholders := strings.TrimSuffix(strings.Repeat("?,", len(firstDeliveryIDs)), ",")
	firstArgs := make([]any, 0, 2+len(firstDeliveryIDs))
	firstArgs = append(firstArgs, now, now)
	for _, id := range firstDeliveryIDs {
		firstArgs = append(firstArgs, id)
	}
	firstQuery := fmt.Sprintf(`UPDATE memory_invalidation_queue
	    SET delivered_at = ?, updated_at = ?
	  WHERE invalidation_id IN (%s) AND delivered_at = ''`, firstPlaceholders)
	if _, err := tx.ExecContext(ctx, firstQuery, firstArgs...); err != nil {
		return fmt.Errorf("set first delivered_at on memory invalidations: %w", err)
	}
	return nil
}

func upsertMemoryInvalidationCursorTx(ctx context.Context, tx *sql.Tx, record MemoryInvalidationCursorRecord) error {
	if strings.TrimSpace(record.WorkspaceID) == "" || strings.TrimSpace(record.AgentID) == "" {
		return errors.New("workspace_id and agent_id are required for invalidation cursor")
	}
	if record.UpdatedAt == "" {
		record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO memory_invalidation_cursors(
		    workspace_id, agent_id, session_id,
		    last_polled_at, last_delivered_at, last_delivered_invalidation_id,
		    last_acknowledged_at, last_acknowledged_invalidation_id, last_poll_count, updated_at
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		  ON CONFLICT(workspace_id, agent_id, session_id) DO UPDATE SET
		    last_polled_at = CASE WHEN excluded.last_polled_at <> '' THEN excluded.last_polled_at ELSE memory_invalidation_cursors.last_polled_at END,
		    last_delivered_at = CASE WHEN excluded.last_delivered_at <> '' THEN excluded.last_delivered_at ELSE memory_invalidation_cursors.last_delivered_at END,
		    last_delivered_invalidation_id = CASE WHEN excluded.last_delivered_invalidation_id <> '' THEN excluded.last_delivered_invalidation_id ELSE memory_invalidation_cursors.last_delivered_invalidation_id END,
		    last_acknowledged_at = CASE WHEN excluded.last_acknowledged_at <> '' THEN excluded.last_acknowledged_at ELSE memory_invalidation_cursors.last_acknowledged_at END,
		    last_acknowledged_invalidation_id = CASE WHEN excluded.last_acknowledged_invalidation_id <> '' THEN excluded.last_acknowledged_invalidation_id ELSE memory_invalidation_cursors.last_acknowledged_invalidation_id END,
		    last_poll_count = CASE WHEN excluded.last_polled_at <> '' THEN excluded.last_poll_count ELSE memory_invalidation_cursors.last_poll_count END,
		    updated_at = excluded.updated_at`,
		record.WorkspaceID,
		record.AgentID,
		record.SessionID,
		record.LastPolledAt,
		record.LastDeliveredAt,
		record.LastDeliveredInvalidationID,
		record.LastAcknowledgedAt,
		record.LastAcknowledgedInvalidationID,
		record.LastPollCount,
		record.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert memory invalidation cursor: %w", err)
	}
	return nil
}

func buildMemoryInvalidationListQuery(filter MemoryInvalidationListFilter) string {
	query := strings.Builder{}
	query.WriteString(`SELECT invalidation_id, workspace_id, agent_id, session_id, report_scope, report_id,
	        residency_tier, replica_kind, coherence_class, canonical_memory_id, cache_key,
	        ref_kind, ref_id, previous_version_token, current_version_token, reason, state,
	        metadata_json, delivered_at, delivery_attempt_count, last_delivery_attempt_at,
	        lease_expires_at, next_delivery_at, acknowledged_at,
	        failure_count, last_failure_at, last_failure_reason, dead_lettered_at,
	        created_at, updated_at
	   FROM memory_invalidation_queue
	  WHERE workspace_id = ? AND agent_id = ?`)
	if filter.SessionID != "" {
		query.WriteString(` AND session_id = ?`)
	}
	if !filter.IncludeAcked && !filter.IncludeDeadLetter {
		query.WriteString(` AND state = 'OPEN'`)
	} else if filter.IncludeAcked && !filter.IncludeDeadLetter {
		query.WriteString(` AND state <> 'DEAD_LETTER'`)
	} else if !filter.IncludeAcked && filter.IncludeDeadLetter {
		query.WriteString(` AND state <> 'ACKED'`)
	}
	query.WriteString(` ORDER BY CASE WHEN state = 'OPEN' AND delivered_at <> '' THEN 1 ELSE 0 END, created_at ASC, invalidation_id ASC LIMIT ?`)
	return query.String()
}

func buildMemoryInvalidationListArgs(filter MemoryInvalidationListFilter) []any {
	args := []any{filter.WorkspaceID, filter.AgentID}
	if filter.SessionID != "" {
		args = append(args, filter.SessionID)
	}
	args = append(args, filter.Limit)
	return args
}

func buildMemoryInvalidationPollQuery(filter MemoryInvalidationPollFilter) string {
	query := strings.Builder{}
	query.WriteString(`SELECT invalidation_id, workspace_id, agent_id, session_id, report_scope, report_id,
	        residency_tier, replica_kind, coherence_class, canonical_memory_id, cache_key,
	        ref_kind, ref_id, previous_version_token, current_version_token, reason, state,
	        metadata_json, delivered_at, delivery_attempt_count, last_delivery_attempt_at,
	        lease_expires_at, next_delivery_at, acknowledged_at,
	        failure_count, last_failure_at, last_failure_reason, dead_lettered_at,
	        created_at, updated_at
	   FROM memory_invalidation_queue
	  WHERE workspace_id = ? AND agent_id = ?`)
	if filter.SessionID != "" {
		query.WriteString(` AND session_id = ?`)
	}
	query.WriteString(` AND ((state = 'OPEN' AND (lease_expires_at = '' OR lease_expires_at <= ?) AND (next_delivery_at = '' OR next_delivery_at <= ?))`)
	if filter.IncludeAcked {
		query.WriteString(` OR state = 'ACKED'`)
	}
	query.WriteString(`) ORDER BY CASE WHEN state = 'OPEN' AND delivered_at <> '' THEN 1 ELSE 0 END, created_at ASC, invalidation_id ASC LIMIT ?`)
	return query.String()
}

func buildMemoryInvalidationPollArgs(filter MemoryInvalidationPollFilter, now string) []any {
	args := []any{filter.WorkspaceID, filter.AgentID}
	if filter.SessionID != "" {
		args = append(args, filter.SessionID)
	}
	args = append(args, now, now, filter.Limit)
	return args
}

func (s *Store) resolveCurrentInvalidationVersionGuardTx(ctx context.Context, tx *sql.Tx, workspaceID string, guard MemoryResidencyVersionGuard) (string, string, error) {
	refKind := normalizeMemoryInvalidationRefKind(guard.RefKind)
	refID := strings.TrimSpace(guard.RefID)
	if refKind == "" || refID == "" {
		return "", "", nil
	}
	switch refKind {
	case "workspace_doc":
		token, ok, err := s.currentWorkspaceDocVersionTokenTx(ctx, tx, workspaceID, refID)
		if err != nil {
			return "", "", err
		}
		if !ok {
			return "", "SOURCE_MISSING", nil
		}
		if token == strings.TrimSpace(guard.VersionToken) {
			return token, "", nil
		}
		return token, "VERSION_CHANGED", nil
	case "artifact_ref":
		token, ok, err := s.currentWorkspaceArtifactVersionTokenTx(ctx, tx, workspaceID, refID)
		if err != nil {
			return "", "", err
		}
		if !ok {
			return "", "SOURCE_MISSING", nil
		}
		if token == strings.TrimSpace(guard.VersionToken) {
			return token, "", nil
		}
		return token, "VERSION_CHANGED", nil
	case "workspace_memory":
		token, ok, err := s.currentWorkspaceMemoryVersionTokenTx(ctx, tx, workspaceID, refID)
		if err != nil {
			return "", "", err
		}
		if !ok {
			return "", "SOURCE_MISSING", nil
		}
		if token == strings.TrimSpace(guard.VersionToken) {
			return token, "", nil
		}
		return token, "VERSION_CHANGED", nil
	case "knowledge_claim":
		token, ok, err := s.currentKnowledgeClaimVersionTokenTx(ctx, tx, workspaceID, refID)
		if err != nil {
			return "", "", err
		}
		if !ok {
			return "", "SOURCE_MISSING", nil
		}
		if token == strings.TrimSpace(guard.VersionToken) {
			return token, "", nil
		}
		return token, "VERSION_CHANGED", nil
	case "knowledge_claim_relation":
		token, ok, err := s.currentKnowledgeClaimRelationVersionTokenTx(ctx, tx, workspaceID, refID)
		if err != nil {
			return "", "", err
		}
		if !ok {
			return "", "SOURCE_MISSING", nil
		}
		if token == strings.TrimSpace(guard.VersionToken) {
			return token, "", nil
		}
		return token, "VERSION_CHANGED", nil
	case "segment_ref":
		token, ok, err := s.currentSegmentVersionTokenTx(ctx, tx, workspaceID, refID)
		if err != nil {
			return "", "", err
		}
		if !ok {
			return "", "SOURCE_MISSING", nil
		}
		if token == strings.TrimSpace(guard.VersionToken) {
			return token, "", nil
		}
		return token, "VERSION_CHANGED", nil
	default:
		return "", "", nil
	}
}

func memoryInvalidationRuntimeEventPayload(record MemoryInvalidationRecord, typedEventType string) map[string]any {
	return map[string]any{
		"invalidation_id":                      record.InvalidationID,
		"agent_id":                             record.AgentID,
		"session_id":                           record.SessionID,
		"report_scope":                         record.ReportScope,
		"report_id":                            record.ReportID,
		"residency_tier":                       record.ResidencyTier,
		"replica_kind":                         record.ReplicaKind,
		"coherence_class":                      record.CoherenceClass,
		"canonical_memory_id":                  record.CanonicalMemoryID,
		"cache_key":                            record.CacheKey,
		"ref_kind":                             record.RefKind,
		"ref_id":                               record.RefID,
		"previous_version_token":               record.PreviousVersionToken,
		"current_version_token":                record.CurrentVersionToken,
		"dependency_revision_vector":           record.DependencyRevisionVector,
		"dependency_revision_vector_malformed": record.DependencyVectorMalformed,
		"reason":                               record.Reason,
		"trigger_cause":                        record.TriggerCause,
		"recovered_from_invalidation_id":       record.RecoveredFromInvalidationID,
		"recovered_at":                         record.RecoveredAt,
		"recovery_cause":                       record.RecoveryCause,
		"state":                                record.State,
		"delivery_attempt_count":               record.DeliveryAttemptCount,
		"last_delivery_attempt_at":             record.LastDeliveryAttemptAt,
		"lease_expires_at":                     record.LeaseExpiresAt,
		"next_delivery_at":                     record.NextDeliveryAt,
		"failure_count":                        record.FailureCount,
		"last_failure_at":                      record.LastFailureAt,
		"last_failure_reason":                  record.LastFailureReason,
		"dead_lettered_at":                     record.DeadLetteredAt,
		"typed_event_type":                     typedEventType,
		"summary": fmt.Sprintf(
			"memory invalidation %s for %s %s on %s/%s",
			strings.ToLower(record.Reason),
			strings.ToLower(record.ReplicaKind),
			firstNonEmpty(record.CanonicalMemoryID, record.CacheKey, record.ReportID),
			record.RefKind,
			record.RefID,
		),
	}
}

func memoryInvalidationMetadataWithDependencyVector(workspaceID string, versionGuards []MemoryResidencyVersionGuard, metadata map[string]any) map[string]any {
	out := make(map[string]any, len(metadata)+1)
	for k, v := range metadata {
		out[k] = v
	}
	guards, malformed := memoryInvalidationDependencyRevisionVector(strings.TrimSpace(workspaceID), out)
	if len(guards) == 0 {
		guards = dedupeMemoryVersionGuards(strings.TrimSpace(workspaceID), versionGuards)
	}
	if len(guards) > 0 {
		out["dependency_revision_vector"] = guards
		delete(out, "dependency_revision_vector_malformed")
	} else if malformed {
		out["dependency_revision_vector_malformed"] = true
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func memoryInvalidationDependencyRevisionVector(workspaceID string, metadata map[string]any) ([]MemoryResidencyVersionGuard, bool) {
	if len(metadata) == 0 {
		return nil, false
	}
	raw, ok := metadata["dependency_revision_vector"]
	if !ok || raw == nil {
		return nil, false
	}
	guards, ok := decodeMemoryInvalidationDependencyRevisionVector(raw)
	if !ok {
		return nil, true
	}
	return dedupeMemoryVersionGuards(strings.TrimSpace(workspaceID), guards), false
}

func decodeMemoryInvalidationDependencyRevisionVector(raw any) ([]MemoryResidencyVersionGuard, bool) {
	switch typed := raw.(type) {
	case nil:
		return nil, false
	case []MemoryResidencyVersionGuard:
		return append([]MemoryResidencyVersionGuard(nil), typed...), true
	case string:
		return decodeMemoryInvalidationDependencyRevisionVectorString(typed)
	case []byte:
		return decodeMemoryInvalidationDependencyRevisionVectorString(string(typed))
	case json.RawMessage:
		return decodeMemoryInvalidationDependencyRevisionVectorString(string(typed))
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, false
	}
	if guards, ok := decodeMemoryInvalidationDependencyRevisionVectorJSON(encoded); ok {
		return guards, true
	}
	return nil, false
}

func decodeMemoryInvalidationDependencyRevisionVectorString(raw string) ([]MemoryResidencyVersionGuard, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, true
	}
	return decodeMemoryInvalidationDependencyRevisionVectorJSON([]byte(trimmed))
}

func decodeMemoryInvalidationDependencyRevisionVectorJSON(raw []byte) ([]MemoryResidencyVersionGuard, bool) {
	var guards []MemoryResidencyVersionGuard
	if err := json.Unmarshal(raw, &guards); err == nil {
		return guards, true
	}
	var single MemoryResidencyVersionGuard
	if err := json.Unmarshal(raw, &single); err == nil {
		if strings.TrimSpace(single.RefKind) == "" && strings.TrimSpace(single.RefID) == "" {
			return nil, false
		}
		return []MemoryResidencyVersionGuard{single}, true
	}
	return nil, false
}

func memoryInvalidationMetadataString(metadata map[string]any, key string) string {
	if len(metadata) == 0 {
		return ""
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func hasEquivalentAckedMemoryInvalidationTx(ctx context.Context, tx *sql.Tx, record MemoryInvalidationRecord) (bool, error) {
	targetID := strings.TrimSpace(record.CanonicalMemoryID)
	targetColumn := "canonical_memory_id"
	if targetID == "" {
		targetID = strings.TrimSpace(record.CacheKey)
		targetColumn = "cache_key"
	}
	if targetID == "" {
		return false, nil
	}
	rows, err := tx.QueryContext(
		ctx,
		fmt.Sprintf(`SELECT invalidation_id
		   FROM memory_invalidation_queue
		  WHERE workspace_id = ?
		    AND agent_id = ?
		    AND session_id = ?
		    AND report_scope = ?
		    AND report_id = ?
		    AND residency_tier = ?
		    AND replica_kind = ?
		    AND %s = ?
		    AND ref_kind = ?
		    AND ref_id = ?
		    AND previous_version_token = ?
		    AND current_version_token = ?
		    AND reason = ?
		    AND state = 'ACKED'
		  ORDER BY created_at DESC, invalidation_id DESC
		  LIMIT 16`, targetColumn),
		record.WorkspaceID,
		record.AgentID,
		record.SessionID,
		record.ReportScope,
		record.ReportID,
		record.ResidencyTier,
		record.ReplicaKind,
		targetID,
		record.RefKind,
		record.RefID,
		record.PreviousVersionToken,
		record.CurrentVersionToken,
		record.Reason,
	)
	if err != nil {
		return false, fmt.Errorf("check equivalent acked memory invalidation: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var invalidationID string
		if err := rows.Scan(&invalidationID); err != nil {
			return false, fmt.Errorf("scan equivalent acked memory invalidation: %w", err)
		}
		current, err := getMemoryInvalidationTx(ctx, tx, record.WorkspaceID, invalidationID)
		if err != nil {
			return false, err
		}
		if memoryInvalidationCanonicalDependencyEqual(current, record) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate equivalent acked memory invalidations: %w", err)
	}
	return false, nil
}

func memoryInvalidationRecordKey(record MemoryInvalidationRecord) string {
	return strings.Join([]string{
		record.WorkspaceID,
		record.AgentID,
		record.SessionID,
		record.ReportScope,
		record.ReportID,
		record.ResidencyTier,
		record.ReplicaKind,
		record.CanonicalMemoryID,
		record.CacheKey,
		record.RefKind,
		record.RefID,
		record.PreviousVersionToken,
		record.CurrentVersionToken,
		record.Reason,
	}, "|")
}

func memoryInvalidationCanonicalDependencyEqual(left, right MemoryInvalidationRecord) bool {
	if left.DependencyVectorMalformed || right.DependencyVectorMalformed {
		return left.DependencyVectorMalformed == right.DependencyVectorMalformed &&
			len(left.DependencyRevisionVector) == 0 &&
			len(right.DependencyRevisionVector) == 0
	}
	return memoryInvalidationVersionGuardsEqual(left.DependencyRevisionVector, right.DependencyRevisionVector)
}

func memoryInvalidationVersionGuardsEqual(left, right []MemoryResidencyVersionGuard) bool {
	if len(left) != len(right) {
		return false
	}
	for idx := range left {
		if left[idx].RefKind != right[idx].RefKind ||
			left[idx].RefID != right[idx].RefID ||
			left[idx].VersionToken != right[idx].VersionToken ||
			left[idx].Weight != right[idx].Weight ||
			left[idx].State != right[idx].State {
			return false
		}
	}
	return true
}

func (s *Store) refreshEquivalentOpenMemoryInvalidationTx(ctx context.Context, tx *sql.Tx, record MemoryInvalidationRecord, recordKey, now string) (MemoryInvalidationRecord, bool, bool, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT invalidation_id
		   FROM memory_invalidation_queue
		  WHERE workspace_id = ? AND invalidation_key = ? AND state = 'OPEN'
		  LIMIT 1`,
		record.WorkspaceID,
		recordKey,
	)
	var invalidationID string
	if err := row.Scan(&invalidationID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return MemoryInvalidationRecord{}, false, false, nil
		}
		return MemoryInvalidationRecord{}, false, false, fmt.Errorf("lookup open memory invalidation by key: %w", err)
	}
	current, err := getMemoryInvalidationTx(ctx, tx, record.WorkspaceID, invalidationID)
	if err != nil {
		return MemoryInvalidationRecord{}, false, false, err
	}
	if memoryInvalidationCanonicalDependencyEqual(current, record) {
		return current, true, false, nil
	}
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE memory_invalidation_queue
		    SET metadata_json = ?,
		        updated_at = ?
		  WHERE workspace_id = ? AND invalidation_id = ? AND state = 'OPEN'`,
		encodeMemoryResidencyJSONMap(record.Metadata),
		now,
		record.WorkspaceID,
		invalidationID,
	); err != nil {
		return MemoryInvalidationRecord{}, false, false, fmt.Errorf("refresh open memory invalidation metadata: %w", err)
	}
	refreshed, err := getMemoryInvalidationTx(ctx, tx, record.WorkspaceID, invalidationID)
	if err != nil {
		return MemoryInvalidationRecord{}, false, false, err
	}
	return refreshed, true, true, nil
}

func normalizeMemoryInvalidationRefKind(raw string) string {
	switch strings.TrimSpace(raw) {
	case "workspace_doc", "artifact_ref", "workspace_memory", "knowledge_claim", "knowledge_claim_relation", "segment_ref":
		return strings.TrimSpace(raw)
	default:
		return ""
	}
}

func normalizeMemoryInvalidationReason(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "SOURCE_MISSING":
		return "SOURCE_MISSING"
	default:
		return "VERSION_CHANGED"
	}
}

func normalizeMemoryInvalidationFailureReason(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "APPLY_FAILED"
	}
	return raw
}

func memoryInvalidationRetryDelay(failureCount int) time.Duration {
	switch {
	case failureCount <= 1:
		return 15 * time.Second
	case failureCount == 2:
		return 60 * time.Second
	default:
		return 5 * time.Minute
	}
}

func cursorLastDeliveredAt(items []MemoryInvalidationRecord) string {
	if len(items) == 0 {
		return ""
	}
	return items[len(items)-1].DeliveredAt
}

func cursorLastDeliveredID(items []MemoryInvalidationRecord) string {
	if len(items) == 0 {
		return ""
	}
	return items[len(items)-1].InvalidationID
}

func dedupeMemoryVersionGuards(workspaceID string, items []MemoryResidencyVersionGuard) []MemoryResidencyVersionGuard {
	if len(items) == 0 {
		return nil
	}
	index := make(map[string]MemoryResidencyVersionGuard, len(items))
	for _, item := range items {
		refKind := normalizeMemoryInvalidationRefKind(item.RefKind)
		refID := strings.TrimSpace(item.RefID)
		if refKind == "" || refID == "" {
			continue
		}
		normalized := item
		normalized.RefKind = refKind
		normalized.RefID = refID
		normalized.Weight = clampUnitInterval(item.Weight)
		key := refKind + "|" + refID
		existing, ok := index[key]
		if !ok || normalized.Weight > existing.Weight {
			index[key] = normalized
		}
	}
	aliased := map[string]struct{}{}
	for key, item := range index {
		sourceKind, sourceRef, ok := memoryRootSegmentAliasTarget(workspaceID, item.RefKind, item.RefID)
		if !ok {
			continue
		}
		if _, exists := index[sourceKind+"|"+sourceRef]; exists {
			aliased[key] = struct{}{}
		}
	}
	out := make([]MemoryResidencyVersionGuard, 0, len(index)-len(aliased))
	for key, item := range index {
		if _, skip := aliased[key]; skip {
			continue
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RefKind != out[j].RefKind {
			return out[i].RefKind < out[j].RefKind
		}
		return out[i].RefID < out[j].RefID
	})
	return out
}

func shouldAutoEnqueueMemoryInvalidation(residencyTier, coherenceClass string) bool {
	switch normalizeMemoryResidencyTier(residencyTier) {
	case "P1", "P2":
		return normalizeMemoryCoherenceClass(coherenceClass) == "A"
	default:
		return false
	}
}
