package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	ServiceDirectionStatusDraft    = "DRAFT"
	ServiceDirectionStatusActive   = "ACTIVE"
	ServiceDirectionStatusPaused   = "PAUSED"
	ServiceDirectionStatusArchived = "ARCHIVED"

	ServiceCandidateStatusProposed = "PROPOSED"
	ServiceCandidateStatusSelected = "SELECTED"
	ServiceCandidateStatusRejected = "REJECTED"
	ServiceCandidateStatusParked   = "PARKED"

	ServiceRunStatusPlanned   = "PLANNED"
	ServiceRunStatusActive    = "ACTIVE"
	ServiceRunStatusBlocked   = "BLOCKED"
	ServiceRunStatusDeployed  = "DEPLOYED"
	ServiceRunStatusMeasuring = "MEASURING"
	ServiceRunStatusCompleted = "COMPLETED"
	ServiceRunStatusKilled    = "KILLED"
	ServiceRunStatusCancelled = "CANCELLED"

	ServiceCredentialPolicyPendingApproval = "PENDING_APPROVAL"
	ServiceCredentialPolicyFreeTierOnly    = "FREE_TIER_ONLY"
	ServiceCredentialPolicyApproved        = "APPROVED"

	ServiceApprovalStatusPending  = "PENDING"
	ServiceApprovalStatusApproved = "APPROVED"
	ServiceApprovalStatusRejected = "REJECTED"
	ServiceApprovalStatusExpired  = "EXPIRED"
	ServiceApprovalStatusRevoked  = "REVOKED"

	ServiceResourceStatusPendingApproval = "PENDING_APPROVAL"
	ServiceResourceStatusProvisioned     = "PROVISIONED"
	ServiceResourceStatusActive          = "ACTIVE"
	ServiceResourceStatusRevoked         = "REVOKED"
	ServiceResourceStatusFailed          = "FAILED"

	ServiceDeployHealthUnknown = "UNKNOWN"
	ServiceDeployHealthPass    = "PASS"
	ServiceDeployHealthFail    = "FAIL"
	ServiceDeployHealthWaived  = "WAIVED"

	ServiceOutcomeDecisionContinue = "CONTINUE"
	ServiceOutcomeDecisionIterate  = "ITERATE"
	ServiceOutcomeDecisionKill     = "KILL"
	ServiceOutcomeDecisionBlocked  = "BLOCKED"
	ServiceOutcomeDecisionHold     = "HOLD"
)

var ErrServiceVentureInvalid = errors.New("service venture invalid")

type ServiceDirectionBriefRecord struct {
	DirectionID     string `json:"direction_id"`
	IdempotencyKey  string `json:"idempotency_key"`
	WorkspaceID     string `json:"workspace_id"`
	Title           string `json:"title"`
	Description     string `json:"description,omitempty"`
	ConstraintsJSON string `json:"constraints_json"`
	BudgetCapMicros int64  `json:"budget_cap_micros,omitempty"`
	Status          string `json:"status"`
	CreatedBy       string `json:"created_by"`
	UpdatedBy       string `json:"updated_by"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

type ServiceCandidateRecord struct {
	CandidateID        string `json:"candidate_id"`
	IdempotencyKey     string `json:"idempotency_key"`
	WorkspaceID        string `json:"workspace_id"`
	DirectionID        string `json:"direction_id"`
	Title              string `json:"title"`
	TargetUser         string `json:"target_user,omitempty"`
	UserPain           string `json:"user_pain,omitempty"`
	SolutionSummary    string `json:"solution_summary,omitempty"`
	Distribution       string `json:"distribution,omitempty"`
	Monetization       string `json:"monetization,omitempty"`
	ImplementationSize string `json:"implementation_size,omitempty"`
	RiskLevel          string `json:"risk_level,omitempty"`
	Score              int    `json:"score,omitempty"`
	EvidencePlanJSON   string `json:"evidence_plan_json"`
	Status             string `json:"status"`
	SelectedBy         string `json:"selected_by,omitempty"`
	SelectedAt         string `json:"selected_at,omitempty"`
	CreatedBy          string `json:"created_by"`
	UpdatedBy          string `json:"updated_by"`
	CreatedAt          string `json:"created_at"`
	UpdatedAt          string `json:"updated_at"`
}

type ServiceRunRecord struct {
	RunID            string `json:"run_id"`
	IdempotencyKey   string `json:"idempotency_key"`
	WorkspaceID      string `json:"workspace_id"`
	CandidateID      string `json:"candidate_id"`
	ProjectID        string `json:"project_id"`
	Title            string `json:"title"`
	DeployTarget     string `json:"deploy_target,omitempty"`
	PublicURL        string `json:"public_url,omitempty"`
	HealthCheckURL   string `json:"health_check_url,omitempty"`
	BudgetAccountID  string `json:"budget_account_id,omitempty"`
	BudgetCapMicros  int64  `json:"budget_cap_micros,omitempty"`
	CredentialPolicy string `json:"credential_policy"`
	Status           string `json:"status"`
	StartedBy        string `json:"started_by"`
	UpdatedBy        string `json:"updated_by"`
	StartedAt        string `json:"started_at"`
	UpdatedAt        string `json:"updated_at"`
	CompletedAt      string `json:"completed_at,omitempty"`
}

type ServiceApprovalGrantRecord struct {
	GrantID        string `json:"grant_id"`
	IdempotencyKey string `json:"idempotency_key"`
	WorkspaceID    string `json:"workspace_id"`
	RunID          string `json:"run_id"`
	GrantType      string `json:"grant_type"`
	ScopeJSON      string `json:"scope_json"`
	ApprovalRef    string `json:"approval_ref,omitempty"`
	Status         string `json:"status"`
	ApprovedBy     string `json:"approved_by,omitempty"`
	ExpiresAt      string `json:"expires_at,omitempty"`
	CreatedBy      string `json:"created_by"`
	UpdatedBy      string `json:"updated_by"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

type ServiceProviderResourceRecord struct {
	ResourceID             string `json:"resource_id"`
	IdempotencyKey         string `json:"idempotency_key"`
	WorkspaceID            string `json:"workspace_id"`
	RunID                  string `json:"run_id"`
	Provider               string `json:"provider"`
	ResourceType           string `json:"resource_type"`
	ResourceRef            string `json:"resource_ref,omitempty"`
	CredentialVaultEntryID string `json:"credential_vault_entry_id,omitempty"`
	ApprovalGrantID        string `json:"approval_grant_id,omitempty"`
	Paid                   bool   `json:"paid,omitempty"`
	CostCapMicros          int64  `json:"cost_cap_micros,omitempty"`
	Status                 string `json:"status"`
	TTLExpiresAt           string `json:"ttl_expires_at,omitempty"`
	CreatedBy              string `json:"created_by"`
	UpdatedBy              string `json:"updated_by"`
	CreatedAt              string `json:"created_at"`
	UpdatedAt              string `json:"updated_at"`
}

type ServiceSpendReceiptRecord struct {
	ReceiptID          string `json:"receipt_id"`
	IdempotencyKey     string `json:"idempotency_key"`
	WorkspaceID        string `json:"workspace_id"`
	RunID              string `json:"run_id"`
	ProviderResourceID string `json:"provider_resource_id,omitempty"`
	LedgerEntryID      string `json:"ledger_entry_id,omitempty"`
	AmountMicros       int64  `json:"amount_micros"`
	Currency           string `json:"currency"`
	ExternalReceiptRef string `json:"external_receipt_ref,omitempty"`
	EvidenceRef        string `json:"evidence_ref"`
	RecordedBy         string `json:"recorded_by"`
	RecordedAt         string `json:"recorded_at"`
}

type ServiceRevenueObservationRecord struct {
	ObservationID      string `json:"observation_id"`
	IdempotencyKey     string `json:"idempotency_key"`
	WorkspaceID        string `json:"workspace_id"`
	RunID              string `json:"run_id"`
	AmountMicros       int64  `json:"amount_micros"`
	Currency           string `json:"currency"`
	Source             string `json:"source"`
	ExternalReceiptRef string `json:"external_receipt_ref,omitempty"`
	EvidenceRef        string `json:"evidence_ref"`
	ObservedAt         string `json:"observed_at"`
	RecordedBy         string `json:"recorded_by"`
	CreatedAt          string `json:"created_at"`
}

type ServiceOutcomeRecord struct {
	OutcomeID            string `json:"outcome_id"`
	IdempotencyKey       string `json:"idempotency_key"`
	WorkspaceID          string `json:"workspace_id"`
	RunID                string `json:"run_id"`
	PublicURL            string `json:"public_url,omitempty"`
	DeployHealthStatus   string `json:"deploy_health_status"`
	DeployEvidenceRef    string `json:"deploy_evidence_ref,omitempty"`
	AnalyticsJSON        string `json:"analytics_json"`
	AnalyticsEvidenceRef string `json:"analytics_evidence_ref,omitempty"`
	SpendMicros          int64  `json:"spend_micros"`
	SpendEvidenceRef     string `json:"spend_evidence_ref,omitempty"`
	RevenueMicros        int64  `json:"revenue_micros"`
	RevenueEvidenceRef   string `json:"revenue_evidence_ref,omitempty"`
	QualityScore         int    `json:"quality_score,omitempty"`
	Decision             string `json:"decision"`
	DecisionReason       string `json:"decision_reason"`
	EvidenceRefsJSON     string `json:"evidence_refs_json"`
	RecordedBy           string `json:"recorded_by"`
	RecordedAt           string `json:"recorded_at"`
}

type ServiceDirectionBriefInput struct {
	DirectionID           string
	IdempotencyKey        string
	WorkspaceID           string
	Title                 string
	Description           string
	ConstraintsJSON       string
	BudgetCapMicros       int64
	Status                string
	ActorID               string
	ActorType             string
	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ServiceCandidateInput struct {
	CandidateID           string
	IdempotencyKey        string
	WorkspaceID           string
	DirectionID           string
	Title                 string
	TargetUser            string
	UserPain              string
	SolutionSummary       string
	Distribution          string
	Monetization          string
	ImplementationSize    string
	RiskLevel             string
	Score                 int
	EvidencePlanJSON      string
	Status                string
	ActorID               string
	ActorType             string
	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ServiceRunInput struct {
	RunID                 string
	IdempotencyKey        string
	WorkspaceID           string
	CandidateID           string
	ProjectID             string
	Title                 string
	DeployTarget          string
	PublicURL             string
	HealthCheckURL        string
	BudgetAccountID       string
	BudgetCapMicros       int64
	CredentialPolicy      string
	Status                string
	ActorID               string
	ActorType             string
	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ServiceApprovalGrantInput struct {
	GrantID               string
	IdempotencyKey        string
	WorkspaceID           string
	RunID                 string
	GrantType             string
	ScopeJSON             string
	ApprovalRef           string
	Status                string
	ApprovedBy            string
	ExpiresAt             string
	ActorID               string
	ActorType             string
	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ServiceProviderResourceInput struct {
	ResourceID             string
	IdempotencyKey         string
	WorkspaceID            string
	RunID                  string
	Provider               string
	ResourceType           string
	ResourceRef            string
	CredentialVaultEntryID string
	ApprovalGrantID        string
	Paid                   bool
	CostCapMicros          int64
	Status                 string
	TTLExpiresAt           string
	ActorID                string
	ActorType              string
	PromptContextEnvelope  map[string]any
	PromptContextSurface   string
}

type ServiceSpendReceiptInput struct {
	ReceiptID             string
	IdempotencyKey        string
	WorkspaceID           string
	RunID                 string
	ProviderResourceID    string
	LedgerEntryID         string
	AmountMicros          int64
	Currency              string
	ExternalReceiptRef    string
	EvidenceRef           string
	ActorID               string
	ActorType             string
	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ServiceRevenueObservationInput struct {
	ObservationID         string
	IdempotencyKey        string
	WorkspaceID           string
	RunID                 string
	AmountMicros          int64
	Currency              string
	Source                string
	ExternalReceiptRef    string
	EvidenceRef           string
	ObservedAt            string
	ActorID               string
	ActorType             string
	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ServiceOutcomeInput struct {
	OutcomeID             string
	IdempotencyKey        string
	WorkspaceID           string
	RunID                 string
	PublicURL             string
	DeployHealthStatus    string
	DeployEvidenceRef     string
	AnalyticsJSON         string
	AnalyticsEvidenceRef  string
	SpendMicros           int64
	SpendEvidenceRef      string
	RevenueMicros         int64
	RevenueEvidenceRef    string
	QualityScore          int
	Decision              string
	DecisionReason        string
	EvidenceRefsJSON      string
	ActorID               string
	ActorType             string
	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ServicePortfolioFilter struct {
	WorkspaceID string
	DirectionID string
	CandidateID string
	RunID       string
	ProjectID   string
	Status      string
	Limit       int
}

type ServiceCoordinationRecord struct {
	SnapshotAt          string                            `json:"snapshot_at"`
	CoordinationVersion string                            `json:"coordination_version"`
	Direction           ServiceDirectionBriefRecord       `json:"direction,omitempty"`
	Candidate           ServiceCandidateRecord            `json:"candidate,omitempty"`
	Run                 ServiceRunRecord                  `json:"run,omitempty"`
	Outcomes            []ServiceOutcomeRecord            `json:"outcomes,omitempty"`
	ApprovalGrants      []ServiceApprovalGrantRecord      `json:"approval_grants,omitempty"`
	ProviderResources   []ServiceProviderResourceRecord   `json:"provider_resources,omitempty"`
	SpendReceipts       []ServiceSpendReceiptRecord       `json:"spend_receipts,omitempty"`
	RevenueObservations []ServiceRevenueObservationRecord `json:"revenue_observations,omitempty"`
	Project             *ProjectCoordinationRecord        `json:"project,omitempty"`
}

func (s *Store) UpsertServiceDirectionBriefWithEvent(ctx context.Context, input ServiceDirectionBriefInput) (ServiceDirectionBriefRecord, RuntimeEventRecord, error) {
	explicitDirectionID := strings.TrimSpace(input.DirectionID) != ""
	record, err := normalizeServiceDirectionBriefInput(input)
	if err != nil {
		return ServiceDirectionBriefRecord{}, RuntimeEventRecord{}, err
	}
	if input.PromptContextEnvelope == nil {
		return ServiceDirectionBriefRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ServiceDirectionBriefRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ServiceDirectionBriefRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin service direction tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		directionID, err := resolveServiceEntityIdentityTx(ctx, tx, "service_direction_briefs", "direction_id", record.WorkspaceID, record.DirectionID, record.IdempotencyKey, explicitDirectionID)
		if err != nil {
			return err
		}
		record.DirectionID = directionID
		if err := s.upsertServiceDirectionBriefTx(ctx, tx, record, now); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, record.WorkspaceID, now, "service direction upsert"); err != nil {
			return err
		}
		appended, err := s.appendServiceVentureEventTx(ctx, tx, authority, input.PromptContextEnvelope, firstNonEmpty(input.PromptContextSurface, "service.direction.upsert"), RuntimeEventInput{
			WorkspaceID: record.WorkspaceID,
			EventType:   "service.direction.upserted",
			EntityType:  "service_direction",
			EntityID:    record.DirectionID,
			ActorType:   firstNonEmpty(input.ActorType, "agent"),
			ActorID:     record.UpdatedBy,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id":       record.WorkspaceID,
				"direction_id":       record.DirectionID,
				"status":             record.Status,
				"title":              record.Title,
				"mutation_operation": "upsert",
			}),
			CreatedAt: now,
		})
		if err != nil {
			return err
		}
		event = appended
		return nil
	}); err != nil {
		return ServiceDirectionBriefRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ServiceDirectionBriefRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit service direction tx: %w", err)
	}
	updated, err := s.GetServiceDirectionBrief(ctx, record.WorkspaceID, record.DirectionID)
	if err != nil {
		return ServiceDirectionBriefRecord{}, RuntimeEventRecord{}, err
	}
	return updated, event, nil
}

func (s *Store) UpsertServiceCandidateWithEvent(ctx context.Context, input ServiceCandidateInput) (ServiceCandidateRecord, RuntimeEventRecord, error) {
	explicitCandidateID := strings.TrimSpace(input.CandidateID) != ""
	record, err := normalizeServiceCandidateInput(input)
	if err != nil {
		return ServiceCandidateRecord{}, RuntimeEventRecord{}, err
	}
	if input.PromptContextEnvelope == nil {
		return ServiceCandidateRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ServiceCandidateRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ServiceCandidateRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin service candidate tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		candidateID, err := resolveServiceEntityIdentityTx(ctx, tx, "service_candidates", "candidate_id", record.WorkspaceID, record.CandidateID, record.IdempotencyKey, explicitCandidateID)
		if err != nil {
			return err
		}
		record.CandidateID = candidateID
		if _, err := getServiceDirectionBriefTx(ctx, tx, record.WorkspaceID, record.DirectionID); err != nil {
			return err
		}
		if record.Status == ServiceCandidateStatusSelected {
			record.SelectedBy = record.UpdatedBy
			record.SelectedAt = now
		}
		if err := s.upsertServiceCandidateTx(ctx, tx, record, now); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, record.WorkspaceID, now, "service candidate upsert"); err != nil {
			return err
		}
		appended, err := s.appendServiceVentureEventTx(ctx, tx, authority, input.PromptContextEnvelope, firstNonEmpty(input.PromptContextSurface, "service.candidate.upsert"), RuntimeEventInput{
			WorkspaceID: record.WorkspaceID,
			EventType:   "service.candidate.upserted",
			EntityType:  "service_candidate",
			EntityID:    record.CandidateID,
			ActorType:   firstNonEmpty(input.ActorType, "agent"),
			ActorID:     record.UpdatedBy,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id":       record.WorkspaceID,
				"direction_id":       record.DirectionID,
				"candidate_id":       record.CandidateID,
				"status":             record.Status,
				"score":              record.Score,
				"mutation_operation": "upsert",
			}),
			CreatedAt: now,
		})
		if err != nil {
			return err
		}
		event = appended
		return nil
	}); err != nil {
		return ServiceCandidateRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ServiceCandidateRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit service candidate tx: %w", err)
	}
	updated, err := s.GetServiceCandidate(ctx, record.WorkspaceID, record.CandidateID)
	if err != nil {
		return ServiceCandidateRecord{}, RuntimeEventRecord{}, err
	}
	return updated, event, nil
}

func (s *Store) UpsertServiceRunWithEvent(ctx context.Context, input ServiceRunInput) (ServiceRunRecord, RuntimeEventRecord, error) {
	explicitRunID := strings.TrimSpace(input.RunID) != ""
	record, err := normalizeServiceRunInput(input)
	if err != nil {
		return ServiceRunRecord{}, RuntimeEventRecord{}, err
	}
	if input.PromptContextEnvelope == nil {
		return ServiceRunRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	if existing, err := getServiceRunTx(ctx, s.DB(), record.WorkspaceID, record.RunID); err == nil && serviceRunTerminal(existing.Status) {
		if !sameServiceRunMutation(existing, record) {
			return ServiceRunRecord{}, RuntimeEventRecord{}, fmt.Errorf("%w: terminal service run %s cannot be mutated", ErrServiceVentureInvalid, record.RunID)
		}
		return existing, RuntimeEventRecord{}, nil
	} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return ServiceRunRecord{}, RuntimeEventRecord{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ServiceRunRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ServiceRunRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin service run tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		runID, err := resolveServiceEntityIdentityTx(ctx, tx, "service_runs", "run_id", record.WorkspaceID, record.RunID, record.IdempotencyKey, explicitRunID)
		if err != nil {
			return err
		}
		record.RunID = runID
		candidate, err := getServiceCandidateTx(ctx, tx, record.WorkspaceID, record.CandidateID)
		if err != nil {
			return err
		}
		if candidate.Status != ServiceCandidateStatusSelected {
			return fmt.Errorf("%w: service run requires SELECTED candidate, got %s", ErrServiceVentureInvalid, candidate.Status)
		}
		if _, err := s.getProjectTx(ctx, tx, record.WorkspaceID, record.ProjectID); err != nil {
			return err
		}
		if existing, err := getServiceRunTx(ctx, tx, record.WorkspaceID, record.RunID); err == nil {
			if record.CandidateID != existing.CandidateID {
				return fmt.Errorf("%w: service run candidate_id is immutable (%s)", ErrServiceVentureInvalid, existing.CandidateID)
			}
			if record.ProjectID != existing.ProjectID {
				return fmt.Errorf("%w: service run project_id is immutable (%s)", ErrServiceVentureInvalid, existing.ProjectID)
			}
			if err := validateServiceRunTransition(existing.Status, record.Status); err != nil {
				return err
			}
			if record.CompletedAt == "" {
				record.CompletedAt = existing.CompletedAt
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := s.upsertServiceRunTx(ctx, tx, record, now); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, record.WorkspaceID, now, "service run upsert"); err != nil {
			return err
		}
		appended, err := s.appendServiceVentureEventTx(ctx, tx, authority, input.PromptContextEnvelope, firstNonEmpty(input.PromptContextSurface, "service.run.start"), RuntimeEventInput{
			WorkspaceID: record.WorkspaceID,
			EventType:   "service.run.upserted",
			EntityType:  "service_run",
			EntityID:    record.RunID,
			ActorType:   firstNonEmpty(input.ActorType, "agent"),
			ActorID:     record.UpdatedBy,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id":       record.WorkspaceID,
				"candidate_id":       record.CandidateID,
				"run_id":             record.RunID,
				"project_id":         record.ProjectID,
				"status":             record.Status,
				"mutation_operation": "upsert",
			}),
			CreatedAt: now,
		})
		if err != nil {
			return err
		}
		event = appended
		return nil
	}); err != nil {
		return ServiceRunRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ServiceRunRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit service run tx: %w", err)
	}
	updated, err := s.GetServiceRun(ctx, record.WorkspaceID, record.RunID)
	if err != nil {
		return ServiceRunRecord{}, RuntimeEventRecord{}, err
	}
	return updated, event, nil
}

func (s *Store) UpsertServiceApprovalGrantWithEvent(ctx context.Context, input ServiceApprovalGrantInput) (ServiceApprovalGrantRecord, RuntimeEventRecord, error) {
	explicitGrantID := strings.TrimSpace(input.GrantID) != ""
	record, err := normalizeServiceApprovalGrantInput(input)
	if err != nil {
		return ServiceApprovalGrantRecord{}, RuntimeEventRecord{}, err
	}
	if input.PromptContextEnvelope == nil {
		return ServiceApprovalGrantRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ServiceApprovalGrantRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ServiceApprovalGrantRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin service approval tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		grantID, err := resolveServiceEntityIdentityTx(ctx, tx, "service_approval_grants", "grant_id", record.WorkspaceID, record.GrantID, record.IdempotencyKey, explicitGrantID)
		if err != nil {
			return err
		}
		record.GrantID = grantID
		run, err := getServiceRunTx(ctx, tx, record.WorkspaceID, record.RunID)
		if err != nil {
			return err
		}
		if serviceRunTerminal(run.Status) {
			if existing, err := getServiceApprovalGrantTx(ctx, tx, record.WorkspaceID, record.GrantID); err == nil {
				if sameServiceApprovalGrantMutation(existing, record) {
					record = existing
					return nil
				}
				return fmt.Errorf("%w: terminal service run %s cannot accept approval grant mutations", ErrServiceVentureInvalid, record.RunID)
			} else if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: terminal service run %s cannot accept new approval grants", ErrServiceVentureInvalid, record.RunID)
			} else {
				return err
			}
		}
		if err := validateServiceApprovalGrantUsable(record, now); err != nil {
			return err
		}
		if err := s.upsertServiceApprovalGrantTx(ctx, tx, record, now); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, record.WorkspaceID, now, "service approval grant upsert"); err != nil {
			return err
		}
		appended, err := s.appendServiceVentureEventTx(ctx, tx, authority, input.PromptContextEnvelope, firstNonEmpty(input.PromptContextSurface, "service.approval.grant"), RuntimeEventInput{
			WorkspaceID: record.WorkspaceID,
			EventType:   "service.approval.granted",
			EntityType:  "service_approval_grant",
			EntityID:    record.GrantID,
			ActorType:   firstNonEmpty(input.ActorType, "agent"),
			ActorID:     record.UpdatedBy,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id": record.WorkspaceID,
				"run_id":       record.RunID,
				"grant_id":     record.GrantID,
				"grant_type":   record.GrantType,
				"status":       record.Status,
			}),
			CreatedAt: now,
		})
		if err != nil {
			return err
		}
		event = appended
		return nil
	}); err != nil {
		return ServiceApprovalGrantRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ServiceApprovalGrantRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit service approval tx: %w", err)
	}
	updated, err := s.GetServiceApprovalGrant(ctx, record.WorkspaceID, record.GrantID)
	if err != nil {
		return ServiceApprovalGrantRecord{}, RuntimeEventRecord{}, err
	}
	return updated, event, nil
}

func (s *Store) UpsertServiceProviderResourceWithEvent(ctx context.Context, input ServiceProviderResourceInput) (ServiceProviderResourceRecord, RuntimeEventRecord, error) {
	explicitResourceID := strings.TrimSpace(input.ResourceID) != ""
	record, err := normalizeServiceProviderResourceInput(input)
	if err != nil {
		return ServiceProviderResourceRecord{}, RuntimeEventRecord{}, err
	}
	if input.PromptContextEnvelope == nil {
		return ServiceProviderResourceRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ServiceProviderResourceRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ServiceProviderResourceRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin service resource tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		resourceID, err := resolveServiceEntityIdentityTx(ctx, tx, "service_provider_resources", "resource_id", record.WorkspaceID, record.ResourceID, record.IdempotencyKey, explicitResourceID)
		if err != nil {
			return err
		}
		record.ResourceID = resourceID
		run, err := getServiceRunTx(ctx, tx, record.WorkspaceID, record.RunID)
		if err != nil {
			return err
		}
		if serviceRunTerminal(run.Status) {
			if existing, err := getServiceProviderResourceTx(ctx, tx, record.WorkspaceID, record.ResourceID); err == nil {
				if sameServiceProviderResourceMutation(existing, record) {
					record = existing
					return nil
				}
				return fmt.Errorf("%w: terminal service run %s cannot accept provider resource mutations", ErrServiceVentureInvalid, record.RunID)
			} else if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: terminal service run %s cannot accept new provider resources", ErrServiceVentureInvalid, record.RunID)
			} else {
				return err
			}
		}
		if err := s.validateServiceProviderResourceGrantTx(ctx, tx, run, record, now); err != nil {
			return err
		}
		if err := s.upsertServiceProviderResourceTx(ctx, tx, record, now); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, record.WorkspaceID, now, "service provider resource upsert"); err != nil {
			return err
		}
		appended, err := s.appendServiceVentureEventTx(ctx, tx, authority, input.PromptContextEnvelope, firstNonEmpty(input.PromptContextSurface, "service.resource.record"), RuntimeEventInput{
			WorkspaceID: record.WorkspaceID,
			EventType:   "service.resource.recorded",
			EntityType:  "service_provider_resource",
			EntityID:    record.ResourceID,
			ActorType:   firstNonEmpty(input.ActorType, "agent"),
			ActorID:     record.UpdatedBy,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id":  record.WorkspaceID,
				"run_id":        record.RunID,
				"resource_id":   record.ResourceID,
				"provider":      record.Provider,
				"resource_type": record.ResourceType,
				"status":        record.Status,
				"paid":          record.Paid,
			}),
			CreatedAt: now,
		})
		if err != nil {
			return err
		}
		event = appended
		return nil
	}); err != nil {
		return ServiceProviderResourceRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ServiceProviderResourceRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit service resource tx: %w", err)
	}
	updated, err := s.GetServiceProviderResource(ctx, record.WorkspaceID, record.ResourceID)
	if err != nil {
		return ServiceProviderResourceRecord{}, RuntimeEventRecord{}, err
	}
	return updated, event, nil
}

func (s *Store) RecordServiceSpendReceiptWithEvent(ctx context.Context, input ServiceSpendReceiptInput) (ServiceSpendReceiptRecord, RuntimeEventRecord, error) {
	explicitReceiptID := strings.TrimSpace(input.ReceiptID) != ""
	record, err := normalizeServiceSpendReceiptInput(input)
	if err != nil {
		return ServiceSpendReceiptRecord{}, RuntimeEventRecord{}, err
	}
	if input.PromptContextEnvelope == nil {
		return ServiceSpendReceiptRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	if existing, ok, err := s.lookupReplayServiceSpendReceipt(ctx, record, strings.TrimSpace(input.IdempotencyKey) != ""); err != nil {
		return ServiceSpendReceiptRecord{}, RuntimeEventRecord{}, err
	} else if ok {
		if !sameServiceSpendReceiptMutation(existing, record) {
			return ServiceSpendReceiptRecord{}, RuntimeEventRecord{}, fmt.Errorf("%w: idempotent spend receipt replay payload mismatch", ErrServiceVentureInvalid)
		}
		return existing, RuntimeEventRecord{}, nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if record.RecordedAt == "" {
		record.RecordedAt = now
	}
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ServiceSpendReceiptRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ServiceSpendReceiptRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin service spend tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		receiptID, err := resolveServiceEntityIdentityTx(ctx, tx, "service_spend_receipts", "receipt_id", record.WorkspaceID, record.ReceiptID, record.IdempotencyKey, explicitReceiptID)
		if err != nil {
			return err
		}
		record.ReceiptID = receiptID
		run, err := getServiceRunTx(ctx, tx, record.WorkspaceID, record.RunID)
		if err != nil {
			return err
		}
		if serviceRunTerminal(run.Status) {
			return fmt.Errorf("%w: terminal service run %s cannot accept new spend receipts", ErrServiceVentureInvalid, record.RunID)
		}
		if err := s.validateServiceSpendReceiptTx(ctx, tx, run, record); err != nil {
			return err
		}
		if err := s.insertServiceSpendReceiptTx(ctx, tx, record); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, record.WorkspaceID, now, "service spend receipt record"); err != nil {
			return err
		}
		appended, err := s.appendServiceVentureEventTx(ctx, tx, authority, input.PromptContextEnvelope, firstNonEmpty(input.PromptContextSurface, "service.spend.record"), RuntimeEventInput{
			WorkspaceID: record.WorkspaceID,
			EventType:   "service.spend.recorded",
			EntityType:  "service_spend_receipt",
			EntityID:    record.ReceiptID,
			ActorType:   firstNonEmpty(input.ActorType, "agent"),
			ActorID:     record.RecordedBy,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id":  record.WorkspaceID,
				"run_id":        record.RunID,
				"receipt_id":    record.ReceiptID,
				"amount_micros": record.AmountMicros,
				"currency":      record.Currency,
				"evidence_ref":  record.EvidenceRef,
			}),
			CreatedAt: now,
		})
		if err != nil {
			return err
		}
		event = appended
		return nil
	}); err != nil {
		return ServiceSpendReceiptRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ServiceSpendReceiptRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit service spend tx: %w", err)
	}
	updated, err := s.GetServiceSpendReceipt(ctx, record.WorkspaceID, record.ReceiptID)
	if err != nil {
		return ServiceSpendReceiptRecord{}, RuntimeEventRecord{}, err
	}
	return updated, event, nil
}

func (s *Store) RecordServiceRevenueObservationWithEvent(ctx context.Context, input ServiceRevenueObservationInput) (ServiceRevenueObservationRecord, RuntimeEventRecord, error) {
	explicitObservationID := strings.TrimSpace(input.ObservationID) != ""
	record, err := normalizeServiceRevenueObservationInput(input)
	if err != nil {
		return ServiceRevenueObservationRecord{}, RuntimeEventRecord{}, err
	}
	if input.PromptContextEnvelope == nil {
		return ServiceRevenueObservationRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	if existing, ok, err := s.lookupReplayServiceRevenueObservation(ctx, record, strings.TrimSpace(input.IdempotencyKey) != ""); err != nil {
		return ServiceRevenueObservationRecord{}, RuntimeEventRecord{}, err
	} else if ok {
		if !sameServiceRevenueObservationMutation(existing, record) {
			return ServiceRevenueObservationRecord{}, RuntimeEventRecord{}, fmt.Errorf("%w: idempotent revenue observation replay payload mismatch", ErrServiceVentureInvalid)
		}
		return existing, RuntimeEventRecord{}, nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if record.ObservedAt == "" {
		record.ObservedAt = now
	}
	record.CreatedAt = now
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ServiceRevenueObservationRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ServiceRevenueObservationRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin service revenue tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		observationID, err := resolveServiceEntityIdentityTx(ctx, tx, "service_revenue_observations", "observation_id", record.WorkspaceID, record.ObservationID, record.IdempotencyKey, explicitObservationID)
		if err != nil {
			return err
		}
		record.ObservationID = observationID
		run, err := getServiceRunTx(ctx, tx, record.WorkspaceID, record.RunID)
		if err != nil {
			return err
		}
		if serviceRunTerminal(run.Status) {
			return fmt.Errorf("%w: terminal service run %s cannot accept new revenue observations", ErrServiceVentureInvalid, record.RunID)
		}
		if err := validateServiceEvidenceRefsExistTx(ctx, tx, record.WorkspaceID, []string{record.EvidenceRef}); err != nil {
			return err
		}
		if err := s.insertServiceRevenueObservationTx(ctx, tx, record); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, record.WorkspaceID, now, "service revenue observation record"); err != nil {
			return err
		}
		appended, err := s.appendServiceVentureEventTx(ctx, tx, authority, input.PromptContextEnvelope, firstNonEmpty(input.PromptContextSurface, "service.revenue.record"), RuntimeEventInput{
			WorkspaceID: record.WorkspaceID,
			EventType:   "service.revenue.recorded",
			EntityType:  "service_revenue_observation",
			EntityID:    record.ObservationID,
			ActorType:   firstNonEmpty(input.ActorType, "agent"),
			ActorID:     record.RecordedBy,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id":   record.WorkspaceID,
				"run_id":         record.RunID,
				"observation_id": record.ObservationID,
				"amount_micros":  record.AmountMicros,
				"currency":       record.Currency,
				"source":         record.Source,
				"evidence_ref":   record.EvidenceRef,
			}),
			CreatedAt: now,
		})
		if err != nil {
			return err
		}
		event = appended
		return nil
	}); err != nil {
		return ServiceRevenueObservationRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ServiceRevenueObservationRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit service revenue tx: %w", err)
	}
	updated, err := s.GetServiceRevenueObservation(ctx, record.WorkspaceID, record.ObservationID)
	if err != nil {
		return ServiceRevenueObservationRecord{}, RuntimeEventRecord{}, err
	}
	return updated, event, nil
}

func (s *Store) RecordServiceOutcomeWithEvent(ctx context.Context, input ServiceOutcomeInput) (ServiceOutcomeRecord, RuntimeEventRecord, error) {
	explicitOutcomeID := strings.TrimSpace(input.OutcomeID) != ""
	record, err := normalizeServiceOutcomeInput(input)
	if err != nil {
		return ServiceOutcomeRecord{}, RuntimeEventRecord{}, err
	}
	if input.PromptContextEnvelope == nil {
		return ServiceOutcomeRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	if existing, ok, err := s.lookupReplayServiceOutcome(ctx, record, strings.TrimSpace(input.IdempotencyKey) != ""); err != nil {
		return ServiceOutcomeRecord{}, RuntimeEventRecord{}, err
	} else if ok {
		if !sameServiceOutcomeMutation(existing, record) {
			return ServiceOutcomeRecord{}, RuntimeEventRecord{}, fmt.Errorf("%w: idempotent outcome replay payload mismatch", ErrServiceVentureInvalid)
		}
		return existing, RuntimeEventRecord{}, nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if record.RecordedAt == "" {
		record.RecordedAt = now
	}
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ServiceOutcomeRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ServiceOutcomeRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin service outcome tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		outcomeID, err := resolveServiceEntityIdentityTx(ctx, tx, "service_outcomes", "outcome_id", record.WorkspaceID, record.OutcomeID, record.IdempotencyKey, explicitOutcomeID)
		if err != nil {
			return err
		}
		record.OutcomeID = outcomeID
		run, err := getServiceRunTx(ctx, tx, record.WorkspaceID, record.RunID)
		if err != nil {
			return err
		}
		if err := s.validateServiceOutcomeAgainstRunTx(ctx, tx, run, record); err != nil {
			return err
		}
		if err := s.insertServiceOutcomeTx(ctx, tx, record); err != nil {
			return err
		}
		if err := s.updateServiceRunForOutcomeTx(ctx, tx, run, record, now); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, record.WorkspaceID, now, "service outcome record"); err != nil {
			return err
		}
		appended, err := s.appendServiceVentureEventTx(ctx, tx, authority, input.PromptContextEnvelope, firstNonEmpty(input.PromptContextSurface, "service.outcome.record"), RuntimeEventInput{
			WorkspaceID: record.WorkspaceID,
			EventType:   "service.outcome.recorded",
			EntityType:  "service_outcome",
			EntityID:    record.OutcomeID,
			ActorType:   firstNonEmpty(input.ActorType, "agent"),
			ActorID:     record.RecordedBy,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id":         record.WorkspaceID,
				"run_id":               record.RunID,
				"outcome_id":           record.OutcomeID,
				"decision":             record.Decision,
				"deploy_health_status": record.DeployHealthStatus,
				"public_url":           record.PublicURL,
				"spend_micros":         record.SpendMicros,
				"revenue_micros":       record.RevenueMicros,
			}),
			CreatedAt: now,
		})
		if err != nil {
			return err
		}
		event = appended
		return nil
	}); err != nil {
		return ServiceOutcomeRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ServiceOutcomeRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit service outcome tx: %w", err)
	}
	updated, err := s.GetServiceOutcome(ctx, record.WorkspaceID, record.OutcomeID)
	if err != nil {
		return ServiceOutcomeRecord{}, RuntimeEventRecord{}, err
	}
	return updated, event, nil
}

func (s *Store) GetServiceDirectionBrief(ctx context.Context, workspaceID, directionID string) (ServiceDirectionBriefRecord, error) {
	return getServiceDirectionBriefTx(ctx, s.db, strings.TrimSpace(workspaceID), strings.TrimSpace(directionID))
}

func (s *Store) GetServiceCandidate(ctx context.Context, workspaceID, candidateID string) (ServiceCandidateRecord, error) {
	return getServiceCandidateTx(ctx, s.db, strings.TrimSpace(workspaceID), strings.TrimSpace(candidateID))
}

func (s *Store) GetServiceRun(ctx context.Context, workspaceID, runID string) (ServiceRunRecord, error) {
	return getServiceRunTx(ctx, s.db, strings.TrimSpace(workspaceID), strings.TrimSpace(runID))
}

func (s *Store) GetServiceApprovalGrant(ctx context.Context, workspaceID, grantID string) (ServiceApprovalGrantRecord, error) {
	return getServiceApprovalGrantTx(ctx, s.db, strings.TrimSpace(workspaceID), strings.TrimSpace(grantID))
}

func (s *Store) GetServiceProviderResource(ctx context.Context, workspaceID, resourceID string) (ServiceProviderResourceRecord, error) {
	return getServiceProviderResourceTx(ctx, s.db, strings.TrimSpace(workspaceID), strings.TrimSpace(resourceID))
}

func (s *Store) GetServiceSpendReceipt(ctx context.Context, workspaceID, receiptID string) (ServiceSpendReceiptRecord, error) {
	return getServiceSpendReceiptTx(ctx, s.db, strings.TrimSpace(workspaceID), strings.TrimSpace(receiptID))
}

func (s *Store) GetServiceRevenueObservation(ctx context.Context, workspaceID, observationID string) (ServiceRevenueObservationRecord, error) {
	return getServiceRevenueObservationTx(ctx, s.db, strings.TrimSpace(workspaceID), strings.TrimSpace(observationID))
}

func (s *Store) GetServiceOutcome(ctx context.Context, workspaceID, outcomeID string) (ServiceOutcomeRecord, error) {
	return getServiceOutcomeTx(ctx, s.db, strings.TrimSpace(workspaceID), strings.TrimSpace(outcomeID))
}

func (s *Store) ListServiceDirectionBriefs(ctx context.Context, filter ServicePortfolioFilter) ([]ServiceDirectionBriefRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	limit := normalizeServiceListLimit(filter.Limit)
	args := []any{workspaceID}
	query := `SELECT direction_id, idempotency_key, workspace_id, title, description, constraints_json, budget_cap_micros, status, created_by, updated_by, created_at, updated_at
	          FROM service_direction_briefs WHERE workspace_id = ?`
	if status := normalizeServiceOptionalUpper(filter.Status); status != "" {
		query += ` AND status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY updated_at DESC, direction_id ASC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list service directions: %w", err)
	}
	defer rows.Close()
	var out []ServiceDirectionBriefRecord
	for rows.Next() {
		record, err := scanServiceDirectionBrief(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func (s *Store) ListServiceCandidates(ctx context.Context, filter ServicePortfolioFilter) ([]ServiceCandidateRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	limit := normalizeServiceListLimit(filter.Limit)
	args := []any{workspaceID}
	query := `SELECT candidate_id, idempotency_key, workspace_id, direction_id, title, target_user, user_pain, solution_summary, distribution, monetization, implementation_size, risk_level, score, evidence_plan_json, status, selected_by, selected_at, created_by, updated_by, created_at, updated_at
	          FROM service_candidates WHERE workspace_id = ?`
	if directionID := strings.TrimSpace(filter.DirectionID); directionID != "" {
		query += ` AND direction_id = ?`
		args = append(args, directionID)
	}
	if status := normalizeServiceOptionalUpper(filter.Status); status != "" {
		query += ` AND status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY score DESC, updated_at DESC, candidate_id ASC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list service candidates: %w", err)
	}
	defer rows.Close()
	var out []ServiceCandidateRecord
	for rows.Next() {
		record, err := scanServiceCandidate(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func (s *Store) ListServiceRuns(ctx context.Context, filter ServicePortfolioFilter) ([]ServiceRunRecord, error) {
	return s.listServiceRunsWithQuerier(ctx, s.db, filter)
}

func (s *Store) listServiceRunsWithQuerier(ctx context.Context, q sqlReadQuerier, filter ServicePortfolioFilter) ([]ServiceRunRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	limit := normalizeServiceListLimit(filter.Limit)
	args := []any{workspaceID}
	query := `SELECT run_id, idempotency_key, workspace_id, candidate_id, project_id, title, deploy_target, public_url, health_check_url, budget_account_id, budget_cap_micros, credential_policy, status, started_by, updated_by, started_at, updated_at, completed_at
	          FROM service_runs WHERE workspace_id = ?`
	if candidateID := strings.TrimSpace(filter.CandidateID); candidateID != "" {
		query += ` AND candidate_id = ?`
		args = append(args, candidateID)
	}
	if projectID := strings.TrimSpace(filter.ProjectID); projectID != "" {
		query += ` AND project_id = ?`
		args = append(args, projectID)
	}
	if runID := strings.TrimSpace(filter.RunID); runID != "" {
		query += ` AND run_id = ?`
		args = append(args, runID)
	}
	if status := normalizeServiceOptionalUpper(filter.Status); status != "" {
		query += ` AND status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY updated_at DESC, run_id ASC LIMIT ?`
	args = append(args, limit)
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list service runs: %w", err)
	}
	defer rows.Close()
	var out []ServiceRunRecord
	for rows.Next() {
		record, err := scanServiceRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func (s *Store) GetServiceCoordination(ctx context.Context, workspaceID, runID string) (ServiceCoordinationRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	runID = strings.TrimSpace(runID)
	if workspaceID == "" || runID == "" {
		return ServiceCoordinationRecord{}, errors.New("workspace_id and run_id are required")
	}
	tx, err := s.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ServiceCoordinationRecord{}, fmt.Errorf("begin service coordination tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	run, err := getServiceRunTx(ctx, tx, workspaceID, runID)
	if err != nil {
		return ServiceCoordinationRecord{}, err
	}
	candidate, err := getServiceCandidateTx(ctx, tx, workspaceID, run.CandidateID)
	if err != nil {
		return ServiceCoordinationRecord{}, err
	}
	direction, err := getServiceDirectionBriefTx(ctx, tx, workspaceID, candidate.DirectionID)
	if err != nil {
		return ServiceCoordinationRecord{}, err
	}
	outcomes, err := listServiceOutcomesForRunTx(ctx, tx, workspaceID, runID)
	if err != nil {
		return ServiceCoordinationRecord{}, err
	}
	grants, err := listServiceApprovalGrantsForRunTx(ctx, tx, workspaceID, runID)
	if err != nil {
		return ServiceCoordinationRecord{}, err
	}
	resources, err := listServiceProviderResourcesForRunTx(ctx, tx, workspaceID, runID)
	if err != nil {
		return ServiceCoordinationRecord{}, err
	}
	spend, err := listServiceSpendReceiptsForRunTx(ctx, tx, workspaceID, runID)
	if err != nil {
		return ServiceCoordinationRecord{}, err
	}
	revenue, err := listServiceRevenueObservationsForRunTx(ctx, tx, workspaceID, runID)
	if err != nil {
		return ServiceCoordinationRecord{}, err
	}
	project, err := s.getProjectCoordinationTx(ctx, tx, workspaceID, run.ProjectID)
	if err != nil {
		return ServiceCoordinationRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return ServiceCoordinationRecord{}, fmt.Errorf("commit service coordination tx: %w", err)
	}
	snapshotAt := time.Now().UTC().Format(time.RFC3339Nano)
	return ServiceCoordinationRecord{
		SnapshotAt:          snapshotAt,
		CoordinationVersion: serviceCoordinationVersion(run, candidate, direction, outcomes, grants, resources, spend, revenue),
		Direction:           direction,
		Candidate:           candidate,
		Run:                 run,
		Outcomes:            outcomes,
		ApprovalGrants:      grants,
		ProviderResources:   resources,
		SpendReceipts:       spend,
		RevenueObservations: revenue,
		Project:             &project,
	}, nil
}

func (s *Store) appendServiceVentureEventTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, envelope map[string]any, surface string, input RuntimeEventInput) (RuntimeEventRecord, error) {
	var payload map[string]any
	if strings.TrimSpace(input.PayloadJSON) != "" {
		if err := json.Unmarshal([]byte(input.PayloadJSON), &payload); err != nil {
			return RuntimeEventRecord{}, fmt.Errorf("decode service event payload: %w", err)
		}
	}
	if payload == nil {
		payload = map[string]any{}
	}
	if envelope == nil {
		return RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	envelope["surface"] = strings.TrimSpace(surface)
	payload, err := AttachServiceVenturePromptContextEnvelope(payload, envelope)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	input.PayloadJSON = mustJSON(payload)
	return s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, input)
}

func normalizeServiceDirectionBriefInput(input ServiceDirectionBriefInput) (ServiceDirectionBriefRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	title := strings.TrimSpace(input.Title)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || title == "" || actorID == "" {
		return ServiceDirectionBriefRecord{}, errors.New("workspace_id, title, and actor_id are required")
	}
	if input.BudgetCapMicros < 0 {
		return ServiceDirectionBriefRecord{}, errors.New("budget_cap_micros cannot be negative")
	}
	constraints, err := normalizeServiceJSONObject(input.ConstraintsJSON, "constraints_json")
	if err != nil {
		return ServiceDirectionBriefRecord{}, err
	}
	status, err := normalizeServiceDirectionStatus(input.Status)
	if err != nil {
		return ServiceDirectionBriefRecord{}, err
	}
	directionID := strings.TrimSpace(input.DirectionID)
	if directionID == "" {
		directionID = nextID("svcdir")
	}
	return ServiceDirectionBriefRecord{
		DirectionID:     directionID,
		IdempotencyKey:  firstNonEmpty(strings.TrimSpace(input.IdempotencyKey), directionID),
		WorkspaceID:     workspaceID,
		Title:           title,
		Description:     strings.TrimSpace(input.Description),
		ConstraintsJSON: constraints,
		BudgetCapMicros: input.BudgetCapMicros,
		Status:          status,
		CreatedBy:       actorID,
		UpdatedBy:       actorID,
	}, nil
}

func normalizeServiceCandidateInput(input ServiceCandidateInput) (ServiceCandidateRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	directionID := strings.TrimSpace(input.DirectionID)
	title := strings.TrimSpace(input.Title)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || directionID == "" || title == "" || actorID == "" {
		return ServiceCandidateRecord{}, errors.New("workspace_id, direction_id, title, and actor_id are required")
	}
	evidencePlan, err := normalizeServiceJSONObject(input.EvidencePlanJSON, "evidence_plan_json")
	if err != nil {
		return ServiceCandidateRecord{}, err
	}
	status, err := normalizeServiceCandidateStatus(input.Status)
	if err != nil {
		return ServiceCandidateRecord{}, err
	}
	candidateID := strings.TrimSpace(input.CandidateID)
	if candidateID == "" {
		candidateID = nextID("svccand")
	}
	return ServiceCandidateRecord{
		CandidateID:        candidateID,
		IdempotencyKey:     firstNonEmpty(strings.TrimSpace(input.IdempotencyKey), candidateID),
		WorkspaceID:        workspaceID,
		DirectionID:        directionID,
		Title:              title,
		TargetUser:         strings.TrimSpace(input.TargetUser),
		UserPain:           strings.TrimSpace(input.UserPain),
		SolutionSummary:    strings.TrimSpace(input.SolutionSummary),
		Distribution:       strings.TrimSpace(input.Distribution),
		Monetization:       strings.TrimSpace(input.Monetization),
		ImplementationSize: strings.TrimSpace(input.ImplementationSize),
		RiskLevel:          strings.TrimSpace(input.RiskLevel),
		Score:              input.Score,
		EvidencePlanJSON:   evidencePlan,
		Status:             status,
		CreatedBy:          actorID,
		UpdatedBy:          actorID,
	}, nil
}

func normalizeServiceRunInput(input ServiceRunInput) (ServiceRunRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	candidateID := strings.TrimSpace(input.CandidateID)
	projectID := strings.TrimSpace(input.ProjectID)
	title := strings.TrimSpace(input.Title)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || candidateID == "" || projectID == "" || title == "" || actorID == "" {
		return ServiceRunRecord{}, errors.New("workspace_id, candidate_id, project_id, title, and actor_id are required")
	}
	if input.BudgetCapMicros < 0 {
		return ServiceRunRecord{}, errors.New("budget_cap_micros cannot be negative")
	}
	status, err := normalizeServiceRunStatus(firstNonEmpty(input.Status, ServiceRunStatusActive))
	if err != nil {
		return ServiceRunRecord{}, err
	}
	credentialPolicy, err := normalizeServiceCredentialPolicy(input.CredentialPolicy)
	if err != nil {
		return ServiceRunRecord{}, err
	}
	runID := strings.TrimSpace(input.RunID)
	if runID == "" {
		runID = nextID("svcrun")
	}
	return ServiceRunRecord{
		RunID:            runID,
		IdempotencyKey:   firstNonEmpty(strings.TrimSpace(input.IdempotencyKey), runID),
		WorkspaceID:      workspaceID,
		CandidateID:      candidateID,
		ProjectID:        projectID,
		Title:            title,
		DeployTarget:     strings.TrimSpace(input.DeployTarget),
		PublicURL:        strings.TrimSpace(input.PublicURL),
		HealthCheckURL:   strings.TrimSpace(input.HealthCheckURL),
		BudgetAccountID:  strings.TrimSpace(input.BudgetAccountID),
		BudgetCapMicros:  input.BudgetCapMicros,
		CredentialPolicy: credentialPolicy,
		Status:           status,
		StartedBy:        actorID,
		UpdatedBy:        actorID,
	}, nil
}

func normalizeServiceApprovalGrantInput(input ServiceApprovalGrantInput) (ServiceApprovalGrantRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	runID := strings.TrimSpace(input.RunID)
	grantType := strings.TrimSpace(input.GrantType)
	actorID := strings.TrimSpace(input.ActorID)
	actorType := strings.ToLower(strings.TrimSpace(firstNonEmpty(input.ActorType, "agent")))
	if workspaceID == "" || runID == "" || grantType == "" || actorID == "" {
		return ServiceApprovalGrantRecord{}, errors.New("workspace_id, run_id, grant_type, and actor_id are required")
	}
	scopeJSON, err := normalizeServiceJSONObject(input.ScopeJSON, "scope_json")
	if err != nil {
		return ServiceApprovalGrantRecord{}, err
	}
	status, err := normalizeServiceApprovalStatus(input.Status)
	if err != nil {
		return ServiceApprovalGrantRecord{}, err
	}
	grantID := strings.TrimSpace(input.GrantID)
	if grantID == "" {
		grantID = nextID("svcgrant")
	}
	approvalRef := strings.TrimSpace(input.ApprovalRef)
	approvedBy := strings.TrimSpace(input.ApprovedBy)
	if status == ServiceApprovalStatusApproved {
		if actorType == "agent" {
			return ServiceApprovalGrantRecord{}, fmt.Errorf("%w: agents cannot self-approve service approval grants", ErrServiceVentureInvalid)
		}
		if approvalRef == "" {
			return ServiceApprovalGrantRecord{}, fmt.Errorf("%w: approved service approval grant requires approval_ref", ErrServiceVentureInvalid)
		}
		if approvedBy == "" {
			approvedBy = actorID
		}
	}
	return ServiceApprovalGrantRecord{
		GrantID:        grantID,
		IdempotencyKey: firstNonEmpty(strings.TrimSpace(input.IdempotencyKey), grantID),
		WorkspaceID:    workspaceID,
		RunID:          runID,
		GrantType:      grantType,
		ScopeJSON:      scopeJSON,
		ApprovalRef:    approvalRef,
		Status:         status,
		ApprovedBy:     approvedBy,
		ExpiresAt:      strings.TrimSpace(input.ExpiresAt),
		CreatedBy:      actorID,
		UpdatedBy:      actorID,
	}, nil
}

func normalizeServiceProviderResourceInput(input ServiceProviderResourceInput) (ServiceProviderResourceRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	runID := strings.TrimSpace(input.RunID)
	provider := strings.TrimSpace(input.Provider)
	resourceType := strings.TrimSpace(input.ResourceType)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || runID == "" || provider == "" || resourceType == "" || actorID == "" {
		return ServiceProviderResourceRecord{}, errors.New("workspace_id, run_id, provider, resource_type, and actor_id are required")
	}
	if input.CostCapMicros < 0 {
		return ServiceProviderResourceRecord{}, errors.New("cost_cap_micros cannot be negative")
	}
	credentialRef := strings.TrimSpace(input.CredentialVaultEntryID)
	if looksLikeCredentialMaterial(credentialRef) {
		return ServiceProviderResourceRecord{}, fmt.Errorf("%w: credential_vault_entry_id must be a vault reference, not credential material", ErrServiceVentureInvalid)
	}
	status, err := normalizeServiceResourceStatus(input.Status)
	if err != nil {
		return ServiceProviderResourceRecord{}, err
	}
	resourceID := strings.TrimSpace(input.ResourceID)
	if resourceID == "" {
		resourceID = nextID("svcres")
	}
	return ServiceProviderResourceRecord{
		ResourceID:             resourceID,
		IdempotencyKey:         firstNonEmpty(strings.TrimSpace(input.IdempotencyKey), resourceID),
		WorkspaceID:            workspaceID,
		RunID:                  runID,
		Provider:               provider,
		ResourceType:           resourceType,
		ResourceRef:            strings.TrimSpace(input.ResourceRef),
		CredentialVaultEntryID: credentialRef,
		ApprovalGrantID:        strings.TrimSpace(input.ApprovalGrantID),
		Paid:                   input.Paid,
		CostCapMicros:          input.CostCapMicros,
		Status:                 status,
		TTLExpiresAt:           strings.TrimSpace(input.TTLExpiresAt),
		CreatedBy:              actorID,
		UpdatedBy:              actorID,
	}, nil
}

func normalizeServiceSpendReceiptInput(input ServiceSpendReceiptInput) (ServiceSpendReceiptRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	runID := strings.TrimSpace(input.RunID)
	evidenceRef := strings.TrimSpace(input.EvidenceRef)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || runID == "" || evidenceRef == "" || actorID == "" {
		return ServiceSpendReceiptRecord{}, errors.New("workspace_id, run_id, evidence_ref, and actor_id are required")
	}
	if input.AmountMicros < 0 {
		return ServiceSpendReceiptRecord{}, errors.New("amount_micros cannot be negative")
	}
	receiptID := strings.TrimSpace(input.ReceiptID)
	if receiptID == "" {
		receiptID = nextID("svcspend")
	}
	return ServiceSpendReceiptRecord{
		ReceiptID:          receiptID,
		IdempotencyKey:     firstNonEmpty(strings.TrimSpace(input.IdempotencyKey), receiptID),
		WorkspaceID:        workspaceID,
		RunID:              runID,
		ProviderResourceID: strings.TrimSpace(input.ProviderResourceID),
		LedgerEntryID:      strings.TrimSpace(input.LedgerEntryID),
		AmountMicros:       input.AmountMicros,
		Currency:           normalizeServiceCurrency(input.Currency),
		ExternalReceiptRef: strings.TrimSpace(input.ExternalReceiptRef),
		EvidenceRef:        evidenceRef,
		RecordedBy:         actorID,
	}, nil
}

func normalizeServiceRevenueObservationInput(input ServiceRevenueObservationInput) (ServiceRevenueObservationRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	runID := strings.TrimSpace(input.RunID)
	source := strings.TrimSpace(input.Source)
	evidenceRef := strings.TrimSpace(input.EvidenceRef)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || runID == "" || source == "" || evidenceRef == "" || actorID == "" {
		return ServiceRevenueObservationRecord{}, errors.New("workspace_id, run_id, source, evidence_ref, and actor_id are required")
	}
	if input.AmountMicros < 0 {
		return ServiceRevenueObservationRecord{}, errors.New("amount_micros cannot be negative")
	}
	observationID := strings.TrimSpace(input.ObservationID)
	if observationID == "" {
		observationID = nextID("svcrev")
	}
	return ServiceRevenueObservationRecord{
		ObservationID:      observationID,
		IdempotencyKey:     firstNonEmpty(strings.TrimSpace(input.IdempotencyKey), observationID),
		WorkspaceID:        workspaceID,
		RunID:              runID,
		AmountMicros:       input.AmountMicros,
		Currency:           normalizeServiceCurrency(input.Currency),
		Source:             source,
		ExternalReceiptRef: strings.TrimSpace(input.ExternalReceiptRef),
		EvidenceRef:        evidenceRef,
		ObservedAt:         strings.TrimSpace(input.ObservedAt),
		RecordedBy:         actorID,
	}, nil
}

func normalizeServiceOutcomeInput(input ServiceOutcomeInput) (ServiceOutcomeRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	runID := strings.TrimSpace(input.RunID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || runID == "" || actorID == "" {
		return ServiceOutcomeRecord{}, errors.New("workspace_id, run_id, and actor_id are required")
	}
	if input.SpendMicros < 0 || input.RevenueMicros < 0 {
		return ServiceOutcomeRecord{}, errors.New("spend_micros and revenue_micros cannot be negative")
	}
	analyticsJSON, err := normalizeServiceJSONObject(input.AnalyticsJSON, "analytics_json")
	if err != nil {
		return ServiceOutcomeRecord{}, err
	}
	evidenceRefsJSON, err := normalizeServiceEvidenceRefs(input.EvidenceRefsJSON)
	if err != nil {
		return ServiceOutcomeRecord{}, err
	}
	health, err := normalizeServiceDeployHealth(input.DeployHealthStatus)
	if err != nil {
		return ServiceOutcomeRecord{}, err
	}
	decision, err := normalizeServiceOutcomeDecision(input.Decision)
	if err != nil {
		return ServiceOutcomeRecord{}, err
	}
	outcomeID := strings.TrimSpace(input.OutcomeID)
	if outcomeID == "" {
		outcomeID = nextID("svcout")
	}
	return ServiceOutcomeRecord{
		OutcomeID:            outcomeID,
		IdempotencyKey:       firstNonEmpty(strings.TrimSpace(input.IdempotencyKey), outcomeID),
		WorkspaceID:          workspaceID,
		RunID:                runID,
		PublicURL:            strings.TrimSpace(input.PublicURL),
		DeployHealthStatus:   health,
		DeployEvidenceRef:    strings.TrimSpace(input.DeployEvidenceRef),
		AnalyticsJSON:        analyticsJSON,
		AnalyticsEvidenceRef: strings.TrimSpace(input.AnalyticsEvidenceRef),
		SpendMicros:          input.SpendMicros,
		SpendEvidenceRef:     strings.TrimSpace(input.SpendEvidenceRef),
		RevenueMicros:        input.RevenueMicros,
		RevenueEvidenceRef:   strings.TrimSpace(input.RevenueEvidenceRef),
		QualityScore:         input.QualityScore,
		Decision:             decision,
		DecisionReason:       strings.TrimSpace(input.DecisionReason),
		EvidenceRefsJSON:     evidenceRefsJSON,
		RecordedBy:           actorID,
	}, nil
}

func (s *Store) upsertServiceDirectionBriefTx(ctx context.Context, tx *sql.Tx, record ServiceDirectionBriefRecord, now string) error {
	if err := s.ensureWorkspaceExistsTx(ctx, tx, record.WorkspaceID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO service_direction_briefs(direction_id, idempotency_key, workspace_id, title, description, constraints_json, budget_cap_micros, status, created_by, updated_by, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(direction_id) DO UPDATE SET
  title = excluded.title,
  description = excluded.description,
  constraints_json = excluded.constraints_json,
  budget_cap_micros = excluded.budget_cap_micros,
  status = excluded.status,
  updated_by = excluded.updated_by,
  updated_at = excluded.updated_at`,
		record.DirectionID, record.IdempotencyKey, record.WorkspaceID, record.Title, record.Description, record.ConstraintsJSON, record.BudgetCapMicros, record.Status, record.CreatedBy, record.UpdatedBy, now, now)
	if err != nil {
		return fmt.Errorf("upsert service direction: %w", err)
	}
	return nil
}

func (s *Store) upsertServiceCandidateTx(ctx context.Context, tx *sql.Tx, record ServiceCandidateRecord, now string) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO service_candidates(candidate_id, idempotency_key, workspace_id, direction_id, title, target_user, user_pain, solution_summary, distribution, monetization, implementation_size, risk_level, score, evidence_plan_json, status, selected_by, selected_at, created_by, updated_by, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(candidate_id) DO UPDATE SET
  title = excluded.title,
  target_user = excluded.target_user,
  user_pain = excluded.user_pain,
  solution_summary = excluded.solution_summary,
  distribution = excluded.distribution,
  monetization = excluded.monetization,
  implementation_size = excluded.implementation_size,
  risk_level = excluded.risk_level,
  score = excluded.score,
  evidence_plan_json = excluded.evidence_plan_json,
  status = excluded.status,
  selected_by = CASE WHEN excluded.status = 'SELECTED' THEN excluded.selected_by ELSE service_candidates.selected_by END,
  selected_at = CASE WHEN excluded.status = 'SELECTED' THEN excluded.selected_at ELSE service_candidates.selected_at END,
  updated_by = excluded.updated_by,
  updated_at = excluded.updated_at`,
		record.CandidateID, record.IdempotencyKey, record.WorkspaceID, record.DirectionID, record.Title, record.TargetUser, record.UserPain, record.SolutionSummary, record.Distribution, record.Monetization, record.ImplementationSize, record.RiskLevel, record.Score, record.EvidencePlanJSON, record.Status, record.SelectedBy, record.SelectedAt, record.CreatedBy, record.UpdatedBy, now, now)
	if err != nil {
		return fmt.Errorf("upsert service candidate: %w", err)
	}
	return nil
}

func (s *Store) upsertServiceRunTx(ctx context.Context, tx *sql.Tx, record ServiceRunRecord, now string) error {
	completedAt := record.CompletedAt
	if serviceRunTerminal(record.Status) && completedAt == "" {
		completedAt = now
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO service_runs(run_id, idempotency_key, workspace_id, candidate_id, project_id, title, deploy_target, public_url, health_check_url, budget_account_id, budget_cap_micros, credential_policy, status, started_by, updated_by, started_at, updated_at, completed_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(run_id) DO UPDATE SET
  title = excluded.title,
  deploy_target = excluded.deploy_target,
  public_url = excluded.public_url,
  health_check_url = excluded.health_check_url,
  budget_account_id = excluded.budget_account_id,
  budget_cap_micros = excluded.budget_cap_micros,
  credential_policy = excluded.credential_policy,
  status = excluded.status,
  updated_by = excluded.updated_by,
  updated_at = excluded.updated_at,
  completed_at = CASE WHEN excluded.completed_at != '' THEN excluded.completed_at ELSE service_runs.completed_at END`,
		record.RunID, record.IdempotencyKey, record.WorkspaceID, record.CandidateID, record.ProjectID, record.Title, record.DeployTarget, record.PublicURL, record.HealthCheckURL, record.BudgetAccountID, record.BudgetCapMicros, record.CredentialPolicy, record.Status, record.StartedBy, record.UpdatedBy, now, now, completedAt)
	if err != nil {
		return fmt.Errorf("upsert service run: %w", err)
	}
	return nil
}

func (s *Store) upsertServiceApprovalGrantTx(ctx context.Context, tx *sql.Tx, record ServiceApprovalGrantRecord, now string) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO service_approval_grants(grant_id, idempotency_key, workspace_id, run_id, grant_type, scope_json, approval_ref, status, approved_by, expires_at, created_by, updated_by, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(grant_id) DO UPDATE SET
  grant_type = excluded.grant_type,
  scope_json = excluded.scope_json,
  approval_ref = excluded.approval_ref,
  status = excluded.status,
  approved_by = excluded.approved_by,
  expires_at = excluded.expires_at,
  updated_by = excluded.updated_by,
  updated_at = excluded.updated_at`,
		record.GrantID, record.IdempotencyKey, record.WorkspaceID, record.RunID, record.GrantType, record.ScopeJSON, record.ApprovalRef, record.Status, record.ApprovedBy, record.ExpiresAt, record.CreatedBy, record.UpdatedBy, now, now)
	if err != nil {
		return fmt.Errorf("upsert service approval grant: %w", err)
	}
	return nil
}

func (s *Store) upsertServiceProviderResourceTx(ctx context.Context, tx *sql.Tx, record ServiceProviderResourceRecord, now string) error {
	paid := 0
	if record.Paid {
		paid = 1
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO service_provider_resources(resource_id, idempotency_key, workspace_id, run_id, provider, resource_type, resource_ref, credential_vault_entry_id, approval_grant_id, paid, cost_cap_micros, status, ttl_expires_at, created_by, updated_by, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(resource_id) DO UPDATE SET
  provider = excluded.provider,
  resource_type = excluded.resource_type,
  resource_ref = excluded.resource_ref,
  credential_vault_entry_id = excluded.credential_vault_entry_id,
  approval_grant_id = excluded.approval_grant_id,
  paid = excluded.paid,
  cost_cap_micros = excluded.cost_cap_micros,
  status = excluded.status,
  ttl_expires_at = excluded.ttl_expires_at,
  updated_by = excluded.updated_by,
  updated_at = excluded.updated_at`,
		record.ResourceID, record.IdempotencyKey, record.WorkspaceID, record.RunID, record.Provider, record.ResourceType, record.ResourceRef, record.CredentialVaultEntryID, record.ApprovalGrantID, paid, record.CostCapMicros, record.Status, record.TTLExpiresAt, record.CreatedBy, record.UpdatedBy, now, now)
	if err != nil {
		return fmt.Errorf("upsert service provider resource: %w", err)
	}
	return nil
}

func (s *Store) insertServiceSpendReceiptTx(ctx context.Context, tx *sql.Tx, record ServiceSpendReceiptRecord) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO service_spend_receipts(receipt_id, idempotency_key, workspace_id, run_id, provider_resource_id, ledger_entry_id, amount_micros, currency, external_receipt_ref, evidence_ref, recorded_by, recorded_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(receipt_id) DO NOTHING`,
		record.ReceiptID, record.IdempotencyKey, record.WorkspaceID, record.RunID, record.ProviderResourceID, record.LedgerEntryID, record.AmountMicros, record.Currency, record.ExternalReceiptRef, record.EvidenceRef, record.RecordedBy, record.RecordedAt)
	if err != nil {
		return fmt.Errorf("insert service spend receipt: %w", err)
	}
	return nil
}

func (s *Store) insertServiceRevenueObservationTx(ctx context.Context, tx *sql.Tx, record ServiceRevenueObservationRecord) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO service_revenue_observations(observation_id, idempotency_key, workspace_id, run_id, amount_micros, currency, source, external_receipt_ref, evidence_ref, observed_at, recorded_by, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(observation_id) DO NOTHING`,
		record.ObservationID, record.IdempotencyKey, record.WorkspaceID, record.RunID, record.AmountMicros, record.Currency, record.Source, record.ExternalReceiptRef, record.EvidenceRef, record.ObservedAt, record.RecordedBy, record.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert service revenue observation: %w", err)
	}
	return nil
}

func (s *Store) insertServiceOutcomeTx(ctx context.Context, tx *sql.Tx, record ServiceOutcomeRecord) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO service_outcomes(outcome_id, idempotency_key, workspace_id, run_id, public_url, deploy_health_status, deploy_evidence_ref, analytics_json, analytics_evidence_ref, spend_micros, spend_evidence_ref, revenue_micros, revenue_evidence_ref, quality_score, decision, decision_reason, evidence_refs_json, recorded_by, recorded_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(outcome_id) DO NOTHING`,
		record.OutcomeID, record.IdempotencyKey, record.WorkspaceID, record.RunID, record.PublicURL, record.DeployHealthStatus, record.DeployEvidenceRef, record.AnalyticsJSON, record.AnalyticsEvidenceRef, record.SpendMicros, record.SpendEvidenceRef, record.RevenueMicros, record.RevenueEvidenceRef, record.QualityScore, record.Decision, record.DecisionReason, record.EvidenceRefsJSON, record.RecordedBy, record.RecordedAt)
	if err != nil {
		return fmt.Errorf("insert service outcome: %w", err)
	}
	return nil
}

func (s *Store) updateServiceRunForOutcomeTx(ctx context.Context, tx *sql.Tx, run ServiceRunRecord, outcome ServiceOutcomeRecord, now string) error {
	nextStatus := run.Status
	completedAt := run.CompletedAt
	switch outcome.Decision {
	case ServiceOutcomeDecisionContinue, ServiceOutcomeDecisionIterate:
		if outcome.Decision == ServiceOutcomeDecisionContinue {
			nextStatus = ServiceRunStatusMeasuring
		} else {
			nextStatus = ServiceRunStatusActive
		}
	case ServiceOutcomeDecisionKill:
		nextStatus = ServiceRunStatusKilled
		completedAt = now
	case ServiceOutcomeDecisionBlocked:
		nextStatus = ServiceRunStatusBlocked
	}
	_, err := tx.ExecContext(ctx, `
UPDATE service_runs
SET status = ?, public_url = CASE WHEN ? != '' THEN ? ELSE public_url END, updated_by = ?, updated_at = ?, completed_at = CASE WHEN ? != '' THEN ? ELSE completed_at END
WHERE workspace_id = ? AND run_id = ?`,
		nextStatus, outcome.PublicURL, outcome.PublicURL, outcome.RecordedBy, now, completedAt, completedAt, run.WorkspaceID, run.RunID)
	if err != nil {
		return fmt.Errorf("update service run outcome status: %w", err)
	}
	return nil
}

func getServiceDirectionBriefTx(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, workspaceID, directionID string) (ServiceDirectionBriefRecord, error) {
	if workspaceID == "" || directionID == "" {
		return ServiceDirectionBriefRecord{}, errors.New("workspace_id and direction_id are required")
	}
	row := q.QueryRowContext(ctx, `SELECT direction_id, idempotency_key, workspace_id, title, description, constraints_json, budget_cap_micros, status, created_by, updated_by, created_at, updated_at FROM service_direction_briefs WHERE workspace_id = ? AND direction_id = ?`, workspaceID, directionID)
	return scanServiceDirectionBrief(row)
}

func getServiceCandidateTx(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, workspaceID, candidateID string) (ServiceCandidateRecord, error) {
	if workspaceID == "" || candidateID == "" {
		return ServiceCandidateRecord{}, errors.New("workspace_id and candidate_id are required")
	}
	row := q.QueryRowContext(ctx, `SELECT candidate_id, idempotency_key, workspace_id, direction_id, title, target_user, user_pain, solution_summary, distribution, monetization, implementation_size, risk_level, score, evidence_plan_json, status, selected_by, selected_at, created_by, updated_by, created_at, updated_at FROM service_candidates WHERE workspace_id = ? AND candidate_id = ?`, workspaceID, candidateID)
	return scanServiceCandidate(row)
}

func getServiceRunTx(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, workspaceID, runID string) (ServiceRunRecord, error) {
	if workspaceID == "" || runID == "" {
		return ServiceRunRecord{}, errors.New("workspace_id and run_id are required")
	}
	row := q.QueryRowContext(ctx, `SELECT run_id, idempotency_key, workspace_id, candidate_id, project_id, title, deploy_target, public_url, health_check_url, budget_account_id, budget_cap_micros, credential_policy, status, started_by, updated_by, started_at, updated_at, completed_at FROM service_runs WHERE workspace_id = ? AND run_id = ?`, workspaceID, runID)
	return scanServiceRun(row)
}

func getServiceApprovalGrantTx(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, workspaceID, grantID string) (ServiceApprovalGrantRecord, error) {
	if workspaceID == "" || grantID == "" {
		return ServiceApprovalGrantRecord{}, errors.New("workspace_id and grant_id are required")
	}
	row := q.QueryRowContext(ctx, `SELECT grant_id, idempotency_key, workspace_id, run_id, grant_type, scope_json, approval_ref, status, approved_by, expires_at, created_by, updated_by, created_at, updated_at FROM service_approval_grants WHERE workspace_id = ? AND grant_id = ?`, workspaceID, grantID)
	return scanServiceApprovalGrant(row)
}

func getServiceProviderResourceTx(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, workspaceID, resourceID string) (ServiceProviderResourceRecord, error) {
	if workspaceID == "" || resourceID == "" {
		return ServiceProviderResourceRecord{}, errors.New("workspace_id and resource_id are required")
	}
	row := q.QueryRowContext(ctx, `SELECT resource_id, idempotency_key, workspace_id, run_id, provider, resource_type, resource_ref, credential_vault_entry_id, approval_grant_id, paid, cost_cap_micros, status, ttl_expires_at, created_by, updated_by, created_at, updated_at FROM service_provider_resources WHERE workspace_id = ? AND resource_id = ?`, workspaceID, resourceID)
	return scanServiceProviderResource(row)
}

func getServiceSpendReceiptTx(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, workspaceID, receiptID string) (ServiceSpendReceiptRecord, error) {
	row := q.QueryRowContext(ctx, `SELECT receipt_id, idempotency_key, workspace_id, run_id, provider_resource_id, ledger_entry_id, amount_micros, currency, external_receipt_ref, evidence_ref, recorded_by, recorded_at FROM service_spend_receipts WHERE workspace_id = ? AND receipt_id = ?`, workspaceID, receiptID)
	return scanServiceSpendReceipt(row)
}

func getServiceRevenueObservationTx(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, workspaceID, observationID string) (ServiceRevenueObservationRecord, error) {
	row := q.QueryRowContext(ctx, `SELECT observation_id, idempotency_key, workspace_id, run_id, amount_micros, currency, source, external_receipt_ref, evidence_ref, observed_at, recorded_by, created_at FROM service_revenue_observations WHERE workspace_id = ? AND observation_id = ?`, workspaceID, observationID)
	return scanServiceRevenueObservation(row)
}

func getServiceOutcomeTx(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, workspaceID, outcomeID string) (ServiceOutcomeRecord, error) {
	row := q.QueryRowContext(ctx, `SELECT outcome_id, idempotency_key, workspace_id, run_id, public_url, deploy_health_status, deploy_evidence_ref, analytics_json, analytics_evidence_ref, spend_micros, spend_evidence_ref, revenue_micros, revenue_evidence_ref, quality_score, decision, decision_reason, evidence_refs_json, recorded_by, recorded_at FROM service_outcomes WHERE workspace_id = ? AND outcome_id = ?`, workspaceID, outcomeID)
	return scanServiceOutcome(row)
}

func getServiceSpendReceiptByIdempotencyKeyTx(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, workspaceID, idempotencyKey string) (ServiceSpendReceiptRecord, error) {
	row := q.QueryRowContext(ctx, `SELECT receipt_id, idempotency_key, workspace_id, run_id, provider_resource_id, ledger_entry_id, amount_micros, currency, external_receipt_ref, evidence_ref, recorded_by, recorded_at FROM service_spend_receipts WHERE workspace_id = ? AND idempotency_key = ?`, workspaceID, idempotencyKey)
	return scanServiceSpendReceipt(row)
}

func getServiceRevenueObservationByIdempotencyKeyTx(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, workspaceID, idempotencyKey string) (ServiceRevenueObservationRecord, error) {
	row := q.QueryRowContext(ctx, `SELECT observation_id, idempotency_key, workspace_id, run_id, amount_micros, currency, source, external_receipt_ref, evidence_ref, observed_at, recorded_by, created_at FROM service_revenue_observations WHERE workspace_id = ? AND idempotency_key = ?`, workspaceID, idempotencyKey)
	return scanServiceRevenueObservation(row)
}

func getServiceOutcomeByIdempotencyKeyTx(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, workspaceID, idempotencyKey string) (ServiceOutcomeRecord, error) {
	row := q.QueryRowContext(ctx, `SELECT outcome_id, idempotency_key, workspace_id, run_id, public_url, deploy_health_status, deploy_evidence_ref, analytics_json, analytics_evidence_ref, spend_micros, spend_evidence_ref, revenue_micros, revenue_evidence_ref, quality_score, decision, decision_reason, evidence_refs_json, recorded_by, recorded_at FROM service_outcomes WHERE workspace_id = ? AND idempotency_key = ?`, workspaceID, idempotencyKey)
	return scanServiceOutcome(row)
}

func resolveServiceEntityIdentityTx(ctx context.Context, tx *sql.Tx, table, idColumn, workspaceID, entityID, idempotencyKey string, explicitEntityID bool) (string, error) {
	table = strings.TrimSpace(table)
	idColumn = strings.TrimSpace(idColumn)
	workspaceID = strings.TrimSpace(workspaceID)
	entityID = strings.TrimSpace(entityID)
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if table == "" || idColumn == "" || workspaceID == "" || entityID == "" {
		return entityID, errors.New("service entity identity guard requires table, id column, workspace_id, and entity id")
	}
	var existingWorkspace string
	err := tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT workspace_id FROM %s WHERE %s = ?`, table, idColumn), entityID).Scan(&existingWorkspace)
	if err == nil && strings.TrimSpace(existingWorkspace) != workspaceID {
		return entityID, fmt.Errorf("%w: service %s %s already belongs to workspace %s", ErrServiceVentureInvalid, idColumn, entityID, existingWorkspace)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return entityID, err
	}
	if idempotencyKey == "" {
		return entityID, nil
	}
	var idempotentID, idempotentWorkspace string
	err = tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT %s, workspace_id FROM %s WHERE workspace_id = ? AND idempotency_key = ?`, idColumn, table), workspaceID, idempotencyKey).Scan(&idempotentID, &idempotentWorkspace)
	if errors.Is(err, sql.ErrNoRows) {
		return entityID, nil
	}
	if err != nil {
		return entityID, err
	}
	if strings.TrimSpace(idempotentWorkspace) != workspaceID {
		return entityID, fmt.Errorf("%w: service idempotency_key %s already belongs to workspace %s", ErrServiceVentureInvalid, idempotencyKey, idempotentWorkspace)
	}
	idempotentID = strings.TrimSpace(idempotentID)
	if idempotentID != "" && idempotentID != entityID {
		if explicitEntityID {
			return entityID, fmt.Errorf("%w: service idempotency_key %s already bound to %s", ErrServiceVentureInvalid, idempotencyKey, idempotentID)
		}
		return idempotentID, nil
	}
	return entityID, nil
}

func (s *Store) lookupReplayServiceSpendReceipt(ctx context.Context, record ServiceSpendReceiptRecord, explicitIdempotencyKey bool) (ServiceSpendReceiptRecord, bool, error) {
	if explicitIdempotencyKey {
		existing, err := getServiceSpendReceiptByIdempotencyKeyTx(ctx, s.DB(), record.WorkspaceID, record.IdempotencyKey)
		if err == nil {
			return existing, true, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return ServiceSpendReceiptRecord{}, false, err
		}
	}
	existing, err := getServiceSpendReceiptTx(ctx, s.DB(), record.WorkspaceID, record.ReceiptID)
	if err == nil {
		return existing, true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return ServiceSpendReceiptRecord{}, false, nil
	}
	return ServiceSpendReceiptRecord{}, false, err
}

func (s *Store) lookupReplayServiceRevenueObservation(ctx context.Context, record ServiceRevenueObservationRecord, explicitIdempotencyKey bool) (ServiceRevenueObservationRecord, bool, error) {
	if explicitIdempotencyKey {
		existing, err := getServiceRevenueObservationByIdempotencyKeyTx(ctx, s.DB(), record.WorkspaceID, record.IdempotencyKey)
		if err == nil {
			return existing, true, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return ServiceRevenueObservationRecord{}, false, err
		}
	}
	existing, err := getServiceRevenueObservationTx(ctx, s.DB(), record.WorkspaceID, record.ObservationID)
	if err == nil {
		return existing, true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return ServiceRevenueObservationRecord{}, false, nil
	}
	return ServiceRevenueObservationRecord{}, false, err
}

func (s *Store) lookupReplayServiceOutcome(ctx context.Context, record ServiceOutcomeRecord, explicitIdempotencyKey bool) (ServiceOutcomeRecord, bool, error) {
	if explicitIdempotencyKey {
		existing, err := getServiceOutcomeByIdempotencyKeyTx(ctx, s.DB(), record.WorkspaceID, record.IdempotencyKey)
		if err == nil {
			return existing, true, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return ServiceOutcomeRecord{}, false, err
		}
	}
	existing, err := getServiceOutcomeTx(ctx, s.DB(), record.WorkspaceID, record.OutcomeID)
	if err == nil {
		return existing, true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return ServiceOutcomeRecord{}, false, nil
	}
	return ServiceOutcomeRecord{}, false, err
}

func sameServiceSpendReceiptMutation(existing, replay ServiceSpendReceiptRecord) bool {
	return existing.WorkspaceID == replay.WorkspaceID &&
		existing.RunID == replay.RunID &&
		existing.ProviderResourceID == replay.ProviderResourceID &&
		existing.LedgerEntryID == replay.LedgerEntryID &&
		existing.AmountMicros == replay.AmountMicros &&
		existing.Currency == replay.Currency &&
		existing.ExternalReceiptRef == replay.ExternalReceiptRef &&
		existing.EvidenceRef == replay.EvidenceRef &&
		existing.RecordedBy == replay.RecordedBy
}

func sameServiceRevenueObservationMutation(existing, replay ServiceRevenueObservationRecord) bool {
	return existing.WorkspaceID == replay.WorkspaceID &&
		existing.RunID == replay.RunID &&
		existing.AmountMicros == replay.AmountMicros &&
		existing.Currency == replay.Currency &&
		existing.Source == replay.Source &&
		existing.ExternalReceiptRef == replay.ExternalReceiptRef &&
		existing.EvidenceRef == replay.EvidenceRef &&
		existing.RecordedBy == replay.RecordedBy
}

func sameServiceOutcomeMutation(existing, replay ServiceOutcomeRecord) bool {
	return existing.WorkspaceID == replay.WorkspaceID &&
		existing.RunID == replay.RunID &&
		existing.PublicURL == replay.PublicURL &&
		existing.DeployHealthStatus == replay.DeployHealthStatus &&
		existing.DeployEvidenceRef == replay.DeployEvidenceRef &&
		existing.AnalyticsJSON == replay.AnalyticsJSON &&
		existing.AnalyticsEvidenceRef == replay.AnalyticsEvidenceRef &&
		existing.SpendMicros == replay.SpendMicros &&
		existing.SpendEvidenceRef == replay.SpendEvidenceRef &&
		existing.RevenueMicros == replay.RevenueMicros &&
		existing.RevenueEvidenceRef == replay.RevenueEvidenceRef &&
		existing.QualityScore == replay.QualityScore &&
		existing.Decision == replay.Decision &&
		existing.DecisionReason == replay.DecisionReason &&
		existing.EvidenceRefsJSON == replay.EvidenceRefsJSON &&
		existing.RecordedBy == replay.RecordedBy
}

func sameServiceRunMutation(existing, replay ServiceRunRecord) bool {
	return existing.WorkspaceID == replay.WorkspaceID &&
		existing.RunID == replay.RunID &&
		existing.CandidateID == replay.CandidateID &&
		existing.ProjectID == replay.ProjectID &&
		existing.Title == replay.Title &&
		existing.DeployTarget == replay.DeployTarget &&
		existing.PublicURL == replay.PublicURL &&
		existing.HealthCheckURL == replay.HealthCheckURL &&
		existing.BudgetAccountID == replay.BudgetAccountID &&
		existing.BudgetCapMicros == replay.BudgetCapMicros &&
		existing.CredentialPolicy == replay.CredentialPolicy &&
		existing.Status == replay.Status &&
		existing.StartedBy == replay.StartedBy
}

func sameServiceApprovalGrantMutation(existing, replay ServiceApprovalGrantRecord) bool {
	return existing.WorkspaceID == replay.WorkspaceID &&
		existing.RunID == replay.RunID &&
		existing.GrantID == replay.GrantID &&
		existing.GrantType == replay.GrantType &&
		existing.ScopeJSON == replay.ScopeJSON &&
		existing.ApprovalRef == replay.ApprovalRef &&
		existing.Status == replay.Status &&
		existing.ApprovedBy == replay.ApprovedBy &&
		existing.ExpiresAt == replay.ExpiresAt &&
		existing.CreatedBy == replay.CreatedBy
}

func sameServiceProviderResourceMutation(existing, replay ServiceProviderResourceRecord) bool {
	return existing.WorkspaceID == replay.WorkspaceID &&
		existing.RunID == replay.RunID &&
		existing.ResourceID == replay.ResourceID &&
		existing.Provider == replay.Provider &&
		existing.ResourceType == replay.ResourceType &&
		existing.ResourceRef == replay.ResourceRef &&
		existing.CredentialVaultEntryID == replay.CredentialVaultEntryID &&
		existing.ApprovalGrantID == replay.ApprovalGrantID &&
		existing.Paid == replay.Paid &&
		existing.CostCapMicros == replay.CostCapMicros &&
		existing.Status == replay.Status &&
		existing.TTLExpiresAt == replay.TTLExpiresAt &&
		existing.CreatedBy == replay.CreatedBy
}

type serviceRowScanner interface {
	Scan(dest ...any) error
}

func scanServiceDirectionBrief(row serviceRowScanner) (ServiceDirectionBriefRecord, error) {
	var r ServiceDirectionBriefRecord
	if err := row.Scan(&r.DirectionID, &r.IdempotencyKey, &r.WorkspaceID, &r.Title, &r.Description, &r.ConstraintsJSON, &r.BudgetCapMicros, &r.Status, &r.CreatedBy, &r.UpdatedBy, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return ServiceDirectionBriefRecord{}, err
	}
	return r, nil
}

func scanServiceCandidate(row serviceRowScanner) (ServiceCandidateRecord, error) {
	var r ServiceCandidateRecord
	if err := row.Scan(&r.CandidateID, &r.IdempotencyKey, &r.WorkspaceID, &r.DirectionID, &r.Title, &r.TargetUser, &r.UserPain, &r.SolutionSummary, &r.Distribution, &r.Monetization, &r.ImplementationSize, &r.RiskLevel, &r.Score, &r.EvidencePlanJSON, &r.Status, &r.SelectedBy, &r.SelectedAt, &r.CreatedBy, &r.UpdatedBy, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return ServiceCandidateRecord{}, err
	}
	return r, nil
}

func scanServiceRun(row serviceRowScanner) (ServiceRunRecord, error) {
	var r ServiceRunRecord
	if err := row.Scan(&r.RunID, &r.IdempotencyKey, &r.WorkspaceID, &r.CandidateID, &r.ProjectID, &r.Title, &r.DeployTarget, &r.PublicURL, &r.HealthCheckURL, &r.BudgetAccountID, &r.BudgetCapMicros, &r.CredentialPolicy, &r.Status, &r.StartedBy, &r.UpdatedBy, &r.StartedAt, &r.UpdatedAt, &r.CompletedAt); err != nil {
		return ServiceRunRecord{}, err
	}
	return r, nil
}

func scanServiceApprovalGrant(row serviceRowScanner) (ServiceApprovalGrantRecord, error) {
	var r ServiceApprovalGrantRecord
	if err := row.Scan(&r.GrantID, &r.IdempotencyKey, &r.WorkspaceID, &r.RunID, &r.GrantType, &r.ScopeJSON, &r.ApprovalRef, &r.Status, &r.ApprovedBy, &r.ExpiresAt, &r.CreatedBy, &r.UpdatedBy, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return ServiceApprovalGrantRecord{}, err
	}
	return r, nil
}

func scanServiceProviderResource(row serviceRowScanner) (ServiceProviderResourceRecord, error) {
	var r ServiceProviderResourceRecord
	var paid int
	if err := row.Scan(&r.ResourceID, &r.IdempotencyKey, &r.WorkspaceID, &r.RunID, &r.Provider, &r.ResourceType, &r.ResourceRef, &r.CredentialVaultEntryID, &r.ApprovalGrantID, &paid, &r.CostCapMicros, &r.Status, &r.TTLExpiresAt, &r.CreatedBy, &r.UpdatedBy, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return ServiceProviderResourceRecord{}, err
	}
	r.Paid = paid != 0
	return r, nil
}

func scanServiceSpendReceipt(row serviceRowScanner) (ServiceSpendReceiptRecord, error) {
	var r ServiceSpendReceiptRecord
	if err := row.Scan(&r.ReceiptID, &r.IdempotencyKey, &r.WorkspaceID, &r.RunID, &r.ProviderResourceID, &r.LedgerEntryID, &r.AmountMicros, &r.Currency, &r.ExternalReceiptRef, &r.EvidenceRef, &r.RecordedBy, &r.RecordedAt); err != nil {
		return ServiceSpendReceiptRecord{}, err
	}
	return r, nil
}

func scanServiceRevenueObservation(row serviceRowScanner) (ServiceRevenueObservationRecord, error) {
	var r ServiceRevenueObservationRecord
	if err := row.Scan(&r.ObservationID, &r.IdempotencyKey, &r.WorkspaceID, &r.RunID, &r.AmountMicros, &r.Currency, &r.Source, &r.ExternalReceiptRef, &r.EvidenceRef, &r.ObservedAt, &r.RecordedBy, &r.CreatedAt); err != nil {
		return ServiceRevenueObservationRecord{}, err
	}
	return r, nil
}

func scanServiceOutcome(row serviceRowScanner) (ServiceOutcomeRecord, error) {
	var r ServiceOutcomeRecord
	if err := row.Scan(&r.OutcomeID, &r.IdempotencyKey, &r.WorkspaceID, &r.RunID, &r.PublicURL, &r.DeployHealthStatus, &r.DeployEvidenceRef, &r.AnalyticsJSON, &r.AnalyticsEvidenceRef, &r.SpendMicros, &r.SpendEvidenceRef, &r.RevenueMicros, &r.RevenueEvidenceRef, &r.QualityScore, &r.Decision, &r.DecisionReason, &r.EvidenceRefsJSON, &r.RecordedBy, &r.RecordedAt); err != nil {
		return ServiceOutcomeRecord{}, err
	}
	return r, nil
}

func listServiceOutcomesForRunTx(ctx context.Context, q sqlReadQuerier, workspaceID, runID string) ([]ServiceOutcomeRecord, error) {
	rows, err := q.QueryContext(ctx, `SELECT outcome_id, idempotency_key, workspace_id, run_id, public_url, deploy_health_status, deploy_evidence_ref, analytics_json, analytics_evidence_ref, spend_micros, spend_evidence_ref, revenue_micros, revenue_evidence_ref, quality_score, decision, decision_reason, evidence_refs_json, recorded_by, recorded_at FROM service_outcomes WHERE workspace_id = ? AND run_id = ? ORDER BY recorded_at DESC, outcome_id ASC`, workspaceID, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ServiceOutcomeRecord
	for rows.Next() {
		record, err := scanServiceOutcome(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func listServiceApprovalGrantsForRunTx(ctx context.Context, q sqlReadQuerier, workspaceID, runID string) ([]ServiceApprovalGrantRecord, error) {
	rows, err := q.QueryContext(ctx, `SELECT grant_id, idempotency_key, workspace_id, run_id, grant_type, scope_json, approval_ref, status, approved_by, expires_at, created_by, updated_by, created_at, updated_at FROM service_approval_grants WHERE workspace_id = ? AND run_id = ? ORDER BY updated_at DESC, grant_id ASC`, workspaceID, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ServiceApprovalGrantRecord
	for rows.Next() {
		record, err := scanServiceApprovalGrant(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func listServiceProviderResourcesForRunTx(ctx context.Context, q sqlReadQuerier, workspaceID, runID string) ([]ServiceProviderResourceRecord, error) {
	rows, err := q.QueryContext(ctx, `SELECT resource_id, idempotency_key, workspace_id, run_id, provider, resource_type, resource_ref, credential_vault_entry_id, approval_grant_id, paid, cost_cap_micros, status, ttl_expires_at, created_by, updated_by, created_at, updated_at FROM service_provider_resources WHERE workspace_id = ? AND run_id = ? ORDER BY updated_at DESC, resource_id ASC`, workspaceID, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ServiceProviderResourceRecord
	for rows.Next() {
		record, err := scanServiceProviderResource(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func listServiceSpendReceiptsForRunTx(ctx context.Context, q sqlReadQuerier, workspaceID, runID string) ([]ServiceSpendReceiptRecord, error) {
	rows, err := q.QueryContext(ctx, `SELECT receipt_id, idempotency_key, workspace_id, run_id, provider_resource_id, ledger_entry_id, amount_micros, currency, external_receipt_ref, evidence_ref, recorded_by, recorded_at FROM service_spend_receipts WHERE workspace_id = ? AND run_id = ? ORDER BY recorded_at DESC, receipt_id ASC`, workspaceID, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ServiceSpendReceiptRecord
	for rows.Next() {
		record, err := scanServiceSpendReceipt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func listServiceRevenueObservationsForRunTx(ctx context.Context, q sqlReadQuerier, workspaceID, runID string) ([]ServiceRevenueObservationRecord, error) {
	rows, err := q.QueryContext(ctx, `SELECT observation_id, idempotency_key, workspace_id, run_id, amount_micros, currency, source, external_receipt_ref, evidence_ref, observed_at, recorded_by, created_at FROM service_revenue_observations WHERE workspace_id = ? AND run_id = ? ORDER BY observed_at DESC, observation_id ASC`, workspaceID, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ServiceRevenueObservationRecord
	for rows.Next() {
		record, err := scanServiceRevenueObservation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func listServiceRunsForCoordinationTx(ctx context.Context, q sqlReadQuerier, workspaceID, projectID string) ([]ServiceRunRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	projectID = strings.TrimSpace(projectID)
	if workspaceID == "" || projectID == "" {
		return nil, errors.New("workspace_id and project_id are required")
	}
	rows, err := q.QueryContext(ctx, `SELECT run_id, idempotency_key, workspace_id, candidate_id, project_id, title, deploy_target, public_url, health_check_url, budget_account_id, budget_cap_micros, credential_policy, status, started_by, updated_by, started_at, updated_at, completed_at
	          FROM service_runs
	          WHERE workspace_id = ? AND project_id = ?
	          ORDER BY CASE status WHEN 'COMPLETED' THEN 1 WHEN 'KILLED' THEN 1 WHEN 'CANCELLED' THEN 1 ELSE 0 END,
	                   updated_at DESC, run_id ASC`, workspaceID, projectID)
	if err != nil {
		return nil, fmt.Errorf("list service runs for coordination: %w", err)
	}
	defer rows.Close()
	var out []ServiceRunRecord
	for rows.Next() {
		record, err := scanServiceRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func (s *Store) validateServiceProviderResourceGrantTx(ctx context.Context, tx *sql.Tx, run ServiceRunRecord, resource ServiceProviderResourceRecord, referenceAt string) error {
	if !resource.Paid && strings.TrimSpace(resource.CredentialVaultEntryID) == "" {
		return nil
	}
	switch run.CredentialPolicy {
	case ServiceCredentialPolicyApproved:
	case ServiceCredentialPolicyFreeTierOnly:
		return fmt.Errorf("%w: service run credential_policy FREE_TIER_ONLY forbids paid or credentialed resources", ErrServiceVentureInvalid)
	default:
		return fmt.Errorf("%w: service run credential_policy %s requires explicit approval before paid or credentialed resources", ErrServiceVentureInvalid, run.CredentialPolicy)
	}
	if strings.TrimSpace(resource.ApprovalGrantID) == "" {
		return fmt.Errorf("%w: paid or credentialed provider resource requires approved service_approval_grant", ErrServiceVentureInvalid)
	}
	grant, err := getServiceApprovalGrantTx(ctx, tx, resource.WorkspaceID, resource.ApprovalGrantID)
	if err != nil {
		return err
	}
	if grant.RunID != resource.RunID {
		return fmt.Errorf("%w: approval grant belongs to run %s, resource belongs to run %s", ErrServiceVentureInvalid, grant.RunID, resource.RunID)
	}
	if grant.Status != ServiceApprovalStatusApproved {
		return fmt.Errorf("%w: approval grant %s is %s", ErrServiceVentureInvalid, grant.GrantID, grant.Status)
	}
	if err := validateServiceApprovalGrantUsable(grant, referenceAt); err != nil {
		return err
	}
	return validateServiceApprovalGrantScope(grant, resource)
}

func (s *Store) validateServiceSpendReceiptTx(ctx context.Context, tx *sql.Tx, run ServiceRunRecord, receipt ServiceSpendReceiptRecord) error {
	if err := validateServiceEvidenceRefsExistTx(ctx, tx, receipt.WorkspaceID, []string{receipt.EvidenceRef}); err != nil {
		return err
	}
	if receipt.ProviderResourceID != "" {
		resource, err := getServiceProviderResourceTx(ctx, tx, receipt.WorkspaceID, receipt.ProviderResourceID)
		if err != nil {
			return err
		}
		if resource.RunID != receipt.RunID {
			return fmt.Errorf("%w: spend receipt provider resource belongs to run %s, receipt belongs to run %s", ErrServiceVentureInvalid, resource.RunID, receipt.RunID)
		}
		if resource.Paid {
			if err := s.validateServiceProviderResourceGrantTx(ctx, tx, run, resource, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
		}
	}
	if receipt.AmountMicros > 0 && strings.TrimSpace(receipt.LedgerEntryID) == "" {
		return fmt.Errorf("%w: positive service spend requires ledger_entry_id", ErrServiceVentureInvalid)
	}
	if receipt.LedgerEntryID != "" {
		entry, err := s.getBudgetLedgerEntryTx(ctx, tx, receipt.LedgerEntryID)
		if err != nil {
			return fmt.Errorf("budget ledger entry %s: %w", receipt.LedgerEntryID, err)
		}
		if entry.EntryType != BudgetLedgerEntrySpend {
			return fmt.Errorf("%w: service spend receipt ledger_entry_id must reference a SPEND entry, got %s", ErrServiceVentureInvalid, entry.EntryType)
		}
		if entry.WorkspaceID != receipt.WorkspaceID {
			return fmt.Errorf("%w: budget ledger entry workspace %s does not match receipt workspace %s", ErrServiceVentureInvalid, entry.WorkspaceID, receipt.WorkspaceID)
		}
		if strings.TrimSpace(entry.RunID) != receipt.RunID {
			return fmt.Errorf("%w: budget ledger entry must be bound to run %s, got %s", ErrServiceVentureInvalid, receipt.RunID, entry.RunID)
		}
		if strings.TrimSpace(run.BudgetAccountID) == "" {
			return fmt.Errorf("%w: service run must declare budget_account_id before spend receipts can bind ledger entries", ErrServiceVentureInvalid)
		}
		if strings.TrimSpace(entry.AccountID) != run.BudgetAccountID {
			return fmt.Errorf("%w: budget ledger entry account %s does not match service run budget account %s", ErrServiceVentureInvalid, entry.AccountID, run.BudgetAccountID)
		}
		if entry.AmountMicros != receipt.AmountMicros {
			return fmt.Errorf("%w: budget ledger entry amount %d does not match receipt amount %d", ErrServiceVentureInvalid, entry.AmountMicros, receipt.AmountMicros)
		}
		if strings.TrimSpace(entry.Currency) != strings.TrimSpace(receipt.Currency) {
			return fmt.Errorf("%w: budget ledger entry currency %s does not match receipt currency %s", ErrServiceVentureInvalid, entry.Currency, receipt.Currency)
		}
	}
	if run.BudgetCapMicros > 0 {
		var current sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(amount_micros), 0) FROM service_spend_receipts WHERE workspace_id = ? AND run_id = ? AND receipt_id != ?`, receipt.WorkspaceID, receipt.RunID, receipt.ReceiptID).Scan(&current); err != nil {
			return err
		}
		if current.Int64+receipt.AmountMicros > run.BudgetCapMicros {
			return fmt.Errorf("%w: service run budget cap exceeded (current=%d receipt=%d cap=%d)", ErrBudgetExceeded, current.Int64, receipt.AmountMicros, run.BudgetCapMicros)
		}
	}
	candidate, err := getServiceCandidateTx(ctx, tx, run.WorkspaceID, run.CandidateID)
	if err != nil {
		return err
	}
	direction, err := getServiceDirectionBriefTx(ctx, tx, candidate.WorkspaceID, candidate.DirectionID)
	if err != nil {
		return err
	}
	if direction.BudgetCapMicros > 0 {
		var current sql.NullInt64
		if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(SUM(r.amount_micros), 0)
  FROM service_spend_receipts r
  JOIN service_runs sr ON sr.workspace_id = r.workspace_id AND sr.run_id = r.run_id
  JOIN service_candidates c ON c.workspace_id = sr.workspace_id AND c.candidate_id = sr.candidate_id
 WHERE r.workspace_id = ? AND c.direction_id = ? AND r.receipt_id != ?`,
			receipt.WorkspaceID, direction.DirectionID, receipt.ReceiptID).Scan(&current); err != nil {
			return err
		}
		if current.Int64+receipt.AmountMicros > direction.BudgetCapMicros {
			return fmt.Errorf("%w: service direction budget cap exceeded (current=%d receipt=%d cap=%d)", ErrBudgetExceeded, current.Int64, receipt.AmountMicros, direction.BudgetCapMicros)
		}
	}
	return nil
}

func validateServiceApprovalGrantUsable(grant ServiceApprovalGrantRecord, referenceAt string) error {
	if grant.Status != ServiceApprovalStatusApproved {
		return nil
	}
	if strings.TrimSpace(grant.ApprovedBy) == "" {
		return fmt.Errorf("%w: approved grant requires approved_by", ErrServiceVentureInvalid)
	}
	if strings.TrimSpace(grant.ExpiresAt) == "" {
		return nil
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, grant.ExpiresAt)
	if err != nil {
		return fmt.Errorf("%w: invalid grant expires_at: %v", ErrServiceVentureInvalid, err)
	}
	ref, err := time.Parse(time.RFC3339Nano, referenceAt)
	if err != nil {
		ref = time.Now().UTC()
	}
	if !expiresAt.After(ref) {
		return fmt.Errorf("%w: approval grant %s is expired", ErrServiceVentureInvalid, grant.GrantID)
	}
	return nil
}

func validateServiceApprovalGrantScope(grant ServiceApprovalGrantRecord, resource ServiceProviderResourceRecord) error {
	var scope map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(grant.ScopeJSON)), &scope); err != nil {
		return fmt.Errorf("%w: approval grant scope_json must be an object", ErrServiceVentureInvalid)
	}
	if provider := serviceScopeString(scope, "provider"); provider != "" && !strings.EqualFold(provider, resource.Provider) {
		return fmt.Errorf("%w: approval grant provider %s does not allow resource provider %s", ErrServiceVentureInvalid, provider, resource.Provider)
	}
	if resourceType := serviceScopeString(scope, "resource_type"); resourceType != "" && !strings.EqualFold(resourceType, resource.ResourceType) {
		return fmt.Errorf("%w: approval grant resource_type %s does not allow resource type %s", ErrServiceVentureInvalid, resourceType, resource.ResourceType)
	}
	if resourceRef := serviceScopeString(scope, "resource_ref"); resourceRef != "" && strings.TrimSpace(resourceRef) != strings.TrimSpace(resource.ResourceRef) {
		return fmt.Errorf("%w: approval grant resource_ref %s does not allow resource ref %s", ErrServiceVentureInvalid, resourceRef, resource.ResourceRef)
	}
	if capMicros := firstPositiveServiceScopeMicros(scope, "cap_micros", "cost_cap_micros", "max_cost_micros", "budget_cap_micros"); capMicros > 0 {
		if resource.CostCapMicros <= 0 {
			return fmt.Errorf("%w: scoped approval grant requires provider resource cost_cap_micros", ErrServiceVentureInvalid)
		}
		if resource.CostCapMicros > capMicros {
			return fmt.Errorf("%w: provider resource cost cap %d exceeds approval grant cap %d", ErrServiceVentureInvalid, resource.CostCapMicros, capMicros)
		}
	}
	return nil
}

func (s *Store) validateServiceOutcomeAgainstRunTx(ctx context.Context, tx *sql.Tx, run ServiceRunRecord, outcome ServiceOutcomeRecord) error {
	if serviceRunTerminal(run.Status) {
		return fmt.Errorf("%w: service run %s is terminal (%s)", ErrServiceVentureInvalid, run.RunID, run.Status)
	}
	if strings.TrimSpace(outcome.DecisionReason) == "" {
		return fmt.Errorf("%w: service outcome decision_reason is required", ErrServiceVentureInvalid)
	}
	refs, err := serviceEvidenceRefs(outcome.EvidenceRefsJSON)
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		return fmt.Errorf("%w: service outcome requires evidence_refs", ErrServiceVentureInvalid)
	}
	evidenceRefs := []string{outcome.DeployEvidenceRef, outcome.AnalyticsEvidenceRef, outcome.SpendEvidenceRef, outcome.RevenueEvidenceRef}
	evidenceRefs = append(evidenceRefs, refs...)
	switch outcome.Decision {
	case ServiceOutcomeDecisionContinue, ServiceOutcomeDecisionIterate:
		if !isPublicServiceURL(outcome.PublicURL) {
			return fmt.Errorf("%w: continue/iterate outcome requires non-local public http(s) URL", ErrServiceVentureInvalid)
		}
		if outcome.DeployHealthStatus != ServiceDeployHealthPass {
			return fmt.Errorf("%w: continue/iterate outcome requires PASS deploy health", ErrServiceVentureInvalid)
		}
		if strings.TrimSpace(outcome.DeployEvidenceRef) == "" || strings.TrimSpace(outcome.AnalyticsEvidenceRef) == "" || strings.TrimSpace(outcome.SpendEvidenceRef) == "" {
			return fmt.Errorf("%w: continue/iterate outcome requires deploy, analytics, and spend evidence refs", ErrServiceVentureInvalid)
		}
		if serviceJSONObjectEmpty(outcome.AnalyticsJSON) {
			return fmt.Errorf("%w: continue/iterate outcome requires non-empty analytics_json", ErrServiceVentureInvalid)
		}
	case ServiceOutcomeDecisionKill:
		if strings.TrimSpace(outcome.DeployEvidenceRef) == "" && strings.TrimSpace(outcome.AnalyticsEvidenceRef) == "" && strings.TrimSpace(outcome.SpendEvidenceRef) == "" {
			return fmt.Errorf("%w: kill outcome requires deploy, analytics, or spend evidence", ErrServiceVentureInvalid)
		}
	}
	return validateServiceEvidenceRefsExistTx(ctx, tx, outcome.WorkspaceID, evidenceRefs)
}

func validateServiceRunTransition(from, to string) error {
	if serviceRunTerminal(from) && from != to {
		return fmt.Errorf("%w: cannot transition terminal service run from %s to %s", ErrServiceVentureInvalid, from, to)
	}
	return nil
}

func serviceRunTerminal(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case ServiceRunStatusCompleted, ServiceRunStatusKilled, ServiceRunStatusCancelled:
		return true
	default:
		return false
	}
}

func normalizeServiceDirectionStatus(value string) (string, error) {
	status := strings.ToUpper(strings.TrimSpace(value))
	if status == "" {
		status = ServiceDirectionStatusActive
	}
	switch status {
	case ServiceDirectionStatusDraft, ServiceDirectionStatusActive, ServiceDirectionStatusPaused, ServiceDirectionStatusArchived:
		return status, nil
	default:
		return "", fmt.Errorf("invalid service direction status: %s", value)
	}
}

func normalizeServiceCandidateStatus(value string) (string, error) {
	status := strings.ToUpper(strings.TrimSpace(value))
	if status == "" {
		status = ServiceCandidateStatusProposed
	}
	switch status {
	case ServiceCandidateStatusProposed, ServiceCandidateStatusSelected, ServiceCandidateStatusRejected, ServiceCandidateStatusParked:
		return status, nil
	default:
		return "", fmt.Errorf("invalid service candidate status: %s", value)
	}
}

func normalizeServiceRunStatus(value string) (string, error) {
	status := strings.ToUpper(strings.TrimSpace(value))
	if status == "" {
		status = ServiceRunStatusActive
	}
	switch status {
	case ServiceRunStatusPlanned, ServiceRunStatusActive, ServiceRunStatusBlocked, ServiceRunStatusDeployed, ServiceRunStatusMeasuring, ServiceRunStatusCompleted, ServiceRunStatusKilled, ServiceRunStatusCancelled:
		return status, nil
	default:
		return "", fmt.Errorf("invalid service run status: %s", value)
	}
}

func normalizeServiceCredentialPolicy(value string) (string, error) {
	policy := strings.ToUpper(strings.TrimSpace(value))
	if policy == "" {
		policy = ServiceCredentialPolicyPendingApproval
	}
	switch policy {
	case ServiceCredentialPolicyPendingApproval, ServiceCredentialPolicyFreeTierOnly, ServiceCredentialPolicyApproved:
		return policy, nil
	default:
		return "", fmt.Errorf("invalid service credential policy: %s", value)
	}
}

func normalizeServiceApprovalStatus(value string) (string, error) {
	status := strings.ToUpper(strings.TrimSpace(value))
	if status == "" {
		status = ServiceApprovalStatusPending
	}
	switch status {
	case ServiceApprovalStatusPending, ServiceApprovalStatusApproved, ServiceApprovalStatusRejected, ServiceApprovalStatusExpired, ServiceApprovalStatusRevoked:
		return status, nil
	default:
		return "", fmt.Errorf("invalid service approval status: %s", value)
	}
}

func normalizeServiceResourceStatus(value string) (string, error) {
	status := strings.ToUpper(strings.TrimSpace(value))
	if status == "" {
		status = ServiceResourceStatusPendingApproval
	}
	switch status {
	case ServiceResourceStatusPendingApproval, ServiceResourceStatusProvisioned, ServiceResourceStatusActive, ServiceResourceStatusRevoked, ServiceResourceStatusFailed:
		return status, nil
	default:
		return "", fmt.Errorf("invalid service resource status: %s", value)
	}
}

func normalizeServiceDeployHealth(value string) (string, error) {
	health := strings.ToUpper(strings.TrimSpace(value))
	if health == "" {
		health = ServiceDeployHealthUnknown
	}
	switch health {
	case ServiceDeployHealthUnknown, ServiceDeployHealthPass, ServiceDeployHealthFail, ServiceDeployHealthWaived:
		return health, nil
	default:
		return "", fmt.Errorf("invalid service deploy health: %s", value)
	}
}

func normalizeServiceOutcomeDecision(value string) (string, error) {
	decision := strings.ToUpper(strings.TrimSpace(value))
	switch decision {
	case ServiceOutcomeDecisionContinue, ServiceOutcomeDecisionIterate, ServiceOutcomeDecisionKill, ServiceOutcomeDecisionBlocked, ServiceOutcomeDecisionHold:
		return decision, nil
	default:
		return "", fmt.Errorf("invalid service outcome decision: %s", value)
	}
}

func normalizeServiceJSONObject(raw, field string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "{}"
	}
	var value map[string]any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return "", fmt.Errorf("%s must be a JSON object: %w", field, err)
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal %s: %w", field, err)
	}
	return string(normalized), nil
}

func normalizeServiceEvidenceRefs(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "[]"
	}
	refs, err := serviceEvidenceRefs(raw)
	if err != nil {
		return "", err
	}
	normalized, err := json.Marshal(refs)
	if err != nil {
		return "", err
	}
	return string(normalized), nil
}

func serviceEvidenceRefs(raw string) ([]string, error) {
	var refs []string
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &refs); err != nil {
		return nil, fmt.Errorf("evidence_refs_json must be a JSON string array: %w", err)
	}
	out := refs[:0]
	seen := map[string]struct{}{}
	for _, ref := range refs {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	return out, nil
}

func serviceJSONObjectEmpty(raw string) bool {
	var value map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &value); err != nil {
		return true
	}
	return len(value) == 0
}

func serviceEvidenceRefExistsTx(ctx context.Context, tx *sql.Tx, workspaceID, ref string) (bool, error) {
	ref = strings.TrimSpace(ref)
	workspaceID = strings.TrimSpace(workspaceID)
	if ref == "" {
		return true, nil
	}
	type evidenceTarget struct {
		table  string
		column string
		value  string
	}
	candidates := []evidenceTarget{}
	switch {
	case strings.HasPrefix(ref, "doc:"):
		candidates = append(candidates, evidenceTarget{"workspace_docs", "doc_key", strings.TrimPrefix(ref, "doc:")})
	case strings.HasPrefix(ref, "artifact:"):
		candidates = append(candidates, evidenceTarget{"workspace_artifacts", "artifact_ref", strings.TrimPrefix(ref, "artifact:")})
	case strings.HasPrefix(ref, "event:"):
		candidates = append(candidates, evidenceTarget{"runtime_events", "event_id", strings.TrimPrefix(ref, "event:")})
	case strings.HasPrefix(ref, "runtime_event:"):
		candidates = append(candidates, evidenceTarget{"runtime_events", "event_id", strings.TrimPrefix(ref, "runtime_event:")})
	default:
		candidates = append(candidates,
			evidenceTarget{"workspace_docs", "doc_key", ref},
			evidenceTarget{"workspace_artifacts", "artifact_ref", ref},
			evidenceTarget{"runtime_events", "event_id", ref},
		)
	}
	for _, candidate := range candidates {
		value := strings.TrimSpace(candidate.value)
		if value == "" {
			continue
		}
		if candidate.table == "workspace_docs" {
			var content string
			query := fmt.Sprintf("SELECT content FROM %s WHERE workspace_id = ? AND %s = ?", candidate.table, candidate.column)
			if err := tx.QueryRowContext(ctx, query, workspaceID, value).Scan(&content); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				return false, err
			}
			if serviceEvidenceDocContentValid(content) {
				return true, nil
			}
			continue
		}
		var count int
		query := fmt.Sprintf("SELECT COUNT(1) FROM %s WHERE workspace_id = ? AND %s = ?", candidate.table, candidate.column)
		if err := tx.QueryRowContext(ctx, query, workspaceID, value).Scan(&count); err != nil {
			return false, err
		}
		if count > 0 {
			return true, nil
		}
	}
	return false, nil
}

func validateServiceEvidenceRefsExistTx(ctx context.Context, tx *sql.Tx, workspaceID string, refs []string) error {
	for _, ref := range uniqueTrimmedServiceStrings(refs) {
		if ok, err := serviceEvidenceRefExistsTx(ctx, tx, workspaceID, ref); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("%w: evidence ref %q does not resolve to a structured workspace evidence doc, artifact, or runtime event", ErrServiceVentureInvalid, ref)
		}
	}
	return nil
}

func serviceEvidenceDocContentValid(content string) bool {
	content = strings.ToLower(strings.TrimSpace(content))
	return strings.Contains(content, "rhizome_service_evidence_v1")
}

func serviceScopeString(scope map[string]any, key string) string {
	if scope == nil {
		return ""
	}
	value, ok := scope[key]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func firstPositiveServiceScopeMicros(scope map[string]any, keys ...string) int64 {
	for _, key := range keys {
		if value := serviceScopeMicros(scope, key); value > 0 {
			return value
		}
	}
	return 0
}

func serviceScopeMicros(scope map[string]any, key string) int64 {
	if scope == nil {
		return 0
	}
	value, ok := scope[key]
	if !ok {
		return 0
	}
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case float64:
		return int64(typed)
	case json.Number:
		n, _ := typed.Int64()
		return n
	case string:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		return parsed
	default:
		return 0
	}
}

func uniqueTrimmedServiceStrings(values []string) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func normalizeServiceCurrency(value string) string {
	currency := strings.ToUpper(strings.TrimSpace(value))
	if currency == "" {
		return BudgetCurrencyUSD
	}
	return currency
}

func normalizeServiceOptionalUpper(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

func normalizeServiceListLimit(limit int) int {
	if limit <= 0 {
		return 50
	}
	if limit > 200 {
		return 200
	}
	return limit
}

func looksLikeCredentialMaterial(value string) bool {
	lower := strings.ToLower(strings.TrimSpace(value))
	return strings.Contains(lower, "sk-") ||
		strings.Contains(lower, "api_key=") ||
		strings.Contains(lower, "secret") ||
		strings.Contains(value, "-----BEGIN") ||
		strings.ContainsAny(value, "\r\n\t ")
}

func isPublicServiceURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed == nil {
		return false
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if host == "" || host == "localhost" || strings.HasSuffix(host, ".local") {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return !(ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified())
	}
	return true
}

func serviceCoordinationVersion(run ServiceRunRecord, candidate ServiceCandidateRecord, direction ServiceDirectionBriefRecord, outcomes []ServiceOutcomeRecord, grants []ServiceApprovalGrantRecord, resources []ServiceProviderResourceRecord, spend []ServiceSpendReceiptRecord, revenue []ServiceRevenueObservationRecord) string {
	parts := []string{
		"direction:" + direction.UpdatedAt,
		"candidate:" + candidate.UpdatedAt,
		"run:" + run.UpdatedAt,
		serviceOutcomeVersionSegment(outcomes),
		serviceApprovalGrantVersionSegment(grants),
		serviceProviderResourceVersionSegment(resources),
		serviceSpendVersionSegment(spend),
		serviceRevenueVersionSegment(revenue),
	}
	return strings.Join(parts, "|")
}

func serviceOutcomeVersionSegment(items []ServiceOutcomeRecord) string {
	ids := make([]string, 0, len(items))
	maxAt := ""
	for _, item := range items {
		ids = append(ids, strings.TrimSpace(item.OutcomeID)+":"+strings.TrimSpace(item.RecordedAt))
		if item.RecordedAt > maxAt {
			maxAt = item.RecordedAt
		}
	}
	sort.Strings(ids)
	return fmt.Sprintf("outcomes:%d:%s:%s", len(items), maxAt, strings.Join(ids, ","))
}

func serviceApprovalGrantVersionSegment(items []ServiceApprovalGrantRecord) string {
	ids := make([]string, 0, len(items))
	maxAt := ""
	for _, item := range items {
		ids = append(ids, strings.TrimSpace(item.GrantID)+":"+strings.TrimSpace(item.UpdatedAt))
		if item.UpdatedAt > maxAt {
			maxAt = item.UpdatedAt
		}
	}
	sort.Strings(ids)
	return fmt.Sprintf("grants:%d:%s:%s", len(items), maxAt, strings.Join(ids, ","))
}

func serviceProviderResourceVersionSegment(items []ServiceProviderResourceRecord) string {
	ids := make([]string, 0, len(items))
	maxAt := ""
	for _, item := range items {
		ids = append(ids, strings.TrimSpace(item.ResourceID)+":"+strings.TrimSpace(item.UpdatedAt))
		if item.UpdatedAt > maxAt {
			maxAt = item.UpdatedAt
		}
	}
	sort.Strings(ids)
	return fmt.Sprintf("resources:%d:%s:%s", len(items), maxAt, strings.Join(ids, ","))
}

func serviceSpendVersionSegment(items []ServiceSpendReceiptRecord) string {
	ids := make([]string, 0, len(items))
	maxAt := ""
	for _, item := range items {
		ids = append(ids, strings.TrimSpace(item.ReceiptID)+":"+strings.TrimSpace(item.RecordedAt))
		if item.RecordedAt > maxAt {
			maxAt = item.RecordedAt
		}
	}
	sort.Strings(ids)
	return fmt.Sprintf("spend:%d:%s:%s", len(items), maxAt, strings.Join(ids, ","))
}

func serviceRevenueVersionSegment(items []ServiceRevenueObservationRecord) string {
	ids := make([]string, 0, len(items))
	maxAt := ""
	for _, item := range items {
		stamp := firstNonEmpty(strings.TrimSpace(item.CreatedAt), strings.TrimSpace(item.ObservedAt))
		ids = append(ids, strings.TrimSpace(item.ObservationID)+":"+stamp)
		if stamp > maxAt {
			maxAt = stamp
		}
	}
	sort.Strings(ids)
	return fmt.Sprintf("revenue:%d:%s:%s", len(items), maxAt, strings.Join(ids, ","))
}
