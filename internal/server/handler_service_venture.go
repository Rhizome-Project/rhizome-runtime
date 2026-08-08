package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type serviceDirectionUpsertParams struct {
	WorkspaceID     string `json:"workspace_id"`
	DirectionID     string `json:"direction_id"`
	IdempotencyKey  string `json:"idempotency_key"`
	ActorID         string `json:"actor_id"`
	Title           string `json:"title"`
	Description     string `json:"description"`
	ConstraintsJSON string `json:"constraints_json"`
	BudgetCapMicros int64  `json:"budget_cap_micros"`
	Status          string `json:"status"`
}

type serviceDirectionGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	DirectionID string `json:"direction_id"`
}

type serviceDirectionListParams struct {
	WorkspaceID string `json:"workspace_id"`
	Status      string `json:"status"`
	Limit       int    `json:"limit"`
}

type serviceCandidateUpsertParams struct {
	WorkspaceID        string `json:"workspace_id"`
	CandidateID        string `json:"candidate_id"`
	IdempotencyKey     string `json:"idempotency_key"`
	ActorID            string `json:"actor_id"`
	DirectionID        string `json:"direction_id"`
	Title              string `json:"title"`
	TargetUser         string `json:"target_user"`
	UserPain           string `json:"user_pain"`
	SolutionSummary    string `json:"solution_summary"`
	Distribution       string `json:"distribution"`
	Monetization       string `json:"monetization"`
	ImplementationSize string `json:"implementation_size"`
	RiskLevel          string `json:"risk_level"`
	Score              int    `json:"score"`
	EvidencePlanJSON   string `json:"evidence_plan_json"`
	Status             string `json:"status"`
}

type serviceCandidateGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	CandidateID string `json:"candidate_id"`
}

type serviceCandidateListParams struct {
	WorkspaceID string `json:"workspace_id"`
	DirectionID string `json:"direction_id"`
	Status      string `json:"status"`
	Limit       int    `json:"limit"`
}

type serviceRunUpsertParams struct {
	WorkspaceID      string `json:"workspace_id"`
	RunID            string `json:"run_id"`
	IdempotencyKey   string `json:"idempotency_key,omitempty"`
	ActorID          string `json:"actor_id"`
	CandidateID      string `json:"candidate_id,omitempty"`
	ProjectID        string `json:"project_id,omitempty"`
	Title            string `json:"title,omitempty"`
	DeployTarget     string `json:"deploy_target,omitempty"`
	PublicURL        string `json:"public_url,omitempty"`
	HealthCheckURL   string `json:"health_check_url,omitempty"`
	BudgetAccountID  string `json:"budget_account_id,omitempty"`
	BudgetCapMicros  int64  `json:"budget_cap_micros,omitempty"`
	CredentialPolicy string `json:"credential_policy,omitempty"`
	Status           string `json:"status,omitempty"`
}

type serviceRunGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	RunID       string `json:"run_id"`
}

type serviceRunListParams struct {
	WorkspaceID string `json:"workspace_id"`
	CandidateID string `json:"candidate_id"`
	ProjectID   string `json:"project_id"`
	Status      string `json:"status"`
	Limit       int    `json:"limit"`
}

type serviceApprovalGrantParams struct {
	WorkspaceID    string `json:"workspace_id"`
	GrantID        string `json:"grant_id"`
	IdempotencyKey string `json:"idempotency_key"`
	ActorID        string `json:"actor_id"`
	RunID          string `json:"run_id"`
	GrantType      string `json:"grant_type"`
	ScopeJSON      string `json:"scope_json"`
	ApprovalRef    string `json:"approval_ref"`
	Status         string `json:"status"`
	ApprovedBy     string `json:"approved_by"`
	ExpiresAt      string `json:"expires_at"`
}

type serviceResourceRecordParams struct {
	WorkspaceID            string `json:"workspace_id"`
	ResourceID             string `json:"resource_id"`
	IdempotencyKey         string `json:"idempotency_key"`
	ActorID                string `json:"actor_id"`
	RunID                  string `json:"run_id"`
	Provider               string `json:"provider"`
	ResourceType           string `json:"resource_type"`
	ResourceRef            string `json:"resource_ref"`
	CredentialVaultEntryID string `json:"credential_vault_entry_id"`
	ApprovalGrantID        string `json:"approval_grant_id"`
	Paid                   bool   `json:"paid"`
	CostCapMicros          int64  `json:"cost_cap_micros"`
	Status                 string `json:"status"`
	TTLExpiresAt           string `json:"ttl_expires_at"`
}

type serviceSpendRecordParams struct {
	WorkspaceID        string `json:"workspace_id"`
	ReceiptID          string `json:"receipt_id"`
	IdempotencyKey     string `json:"idempotency_key"`
	ActorID            string `json:"actor_id"`
	RunID              string `json:"run_id"`
	ProviderResourceID string `json:"provider_resource_id"`
	LedgerEntryID      string `json:"ledger_entry_id"`
	AmountMicros       int64  `json:"amount_micros"`
	Currency           string `json:"currency"`
	ExternalReceiptRef string `json:"external_receipt_ref"`
	EvidenceRef        string `json:"evidence_ref"`
}

type serviceRevenueRecordParams struct {
	WorkspaceID        string `json:"workspace_id"`
	ObservationID      string `json:"observation_id"`
	IdempotencyKey     string `json:"idempotency_key"`
	ActorID            string `json:"actor_id"`
	RunID              string `json:"run_id"`
	AmountMicros       int64  `json:"amount_micros"`
	Currency           string `json:"currency"`
	Source             string `json:"source"`
	ExternalReceiptRef string `json:"external_receipt_ref"`
	EvidenceRef        string `json:"evidence_ref"`
	ObservedAt         string `json:"observed_at"`
}

type serviceOutcomeRecordParams struct {
	WorkspaceID          string `json:"workspace_id"`
	OutcomeID            string `json:"outcome_id"`
	IdempotencyKey       string `json:"idempotency_key"`
	ActorID              string `json:"actor_id"`
	RunID                string `json:"run_id"`
	PublicURL            string `json:"public_url"`
	DeployHealthStatus   string `json:"deploy_health_status"`
	DeployEvidenceRef    string `json:"deploy_evidence_ref"`
	AnalyticsJSON        string `json:"analytics_json"`
	AnalyticsEvidenceRef string `json:"analytics_evidence_ref"`
	SpendMicros          int64  `json:"spend_micros"`
	SpendEvidenceRef     string `json:"spend_evidence_ref"`
	RevenueMicros        int64  `json:"revenue_micros"`
	RevenueEvidenceRef   string `json:"revenue_evidence_ref"`
	QualityScore         int    `json:"quality_score"`
	Decision             string `json:"decision"`
	DecisionReason       string `json:"decision_reason"`
	EvidenceRefsJSON     string `json:"evidence_refs_json"`
}

type serviceCoordinationGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	RunID       string `json:"run_id"`
}

func (h *Handler) serviceDirectionUpsert(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p serviceDirectionUpsertParams
	principal, rpcErr := decodeServiceWorkspaceActor(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	direction, event, err := h.store.UpsertServiceDirectionBriefWithEvent(ctx, sqlite.ServiceDirectionBriefInput{
		DirectionID:           p.DirectionID,
		IdempotencyKey:        p.IdempotencyKey,
		WorkspaceID:           p.WorkspaceID,
		Title:                 p.Title,
		Description:           p.Description,
		ConstraintsJSON:       p.ConstraintsJSON,
		BudgetCapMicros:       p.BudgetCapMicros,
		Status:                p.Status,
		ActorID:               p.ActorID,
		ActorType:             principal.PrincipalType,
		PromptContextEnvelope: h.serviceVenturePromptContextEnvelope(ctx, p.WorkspaceID, "service.direction.upsert", map[string]string{"actor_id": p.ActorID, "direction_id": p.DirectionID}),
		PromptContextSurface:  "service.direction.upsert",
	})
	if err != nil {
		return nil, rpcErrorFromServiceVentureErr(err, "service.direction.upsert")
	}
	h.publishServiceVentureEvent(event, direction.DirectionID)
	return map[string]any{"direction": direction}, nil
}

func (h *Handler) serviceDirectionList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p serviceDirectionListParams
	workspaceID, rpcErr := decodeServiceWorkspaceRead(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListServiceDirectionBriefs(ctx, sqlite.ServicePortfolioFilter{WorkspaceID: workspaceID, Status: p.Status, Limit: p.Limit})
	if err != nil {
		return nil, rpcErrorFromServiceVentureErr(err, "service.direction.list")
	}
	return map[string]any{"directions": items, "count": len(items)}, nil
}

func (h *Handler) serviceDirectionGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p serviceDirectionGetParams
	workspaceID, rpcErr := decodeServiceWorkspaceRead(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	item, err := h.store.GetServiceDirectionBrief(ctx, workspaceID, strings.TrimSpace(p.DirectionID))
	if err != nil {
		return nil, rpcErrorFromServiceVentureErr(err, "service.direction.get")
	}
	return map[string]any{"direction": item}, nil
}

func (h *Handler) serviceCandidateUpsert(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p serviceCandidateUpsertParams
	principal, rpcErr := decodeServiceWorkspaceActor(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	candidate, event, err := h.store.UpsertServiceCandidateWithEvent(ctx, sqlite.ServiceCandidateInput{
		CandidateID:           p.CandidateID,
		IdempotencyKey:        p.IdempotencyKey,
		WorkspaceID:           p.WorkspaceID,
		DirectionID:           p.DirectionID,
		Title:                 p.Title,
		TargetUser:            p.TargetUser,
		UserPain:              p.UserPain,
		SolutionSummary:       p.SolutionSummary,
		Distribution:          p.Distribution,
		Monetization:          p.Monetization,
		ImplementationSize:    p.ImplementationSize,
		RiskLevel:             p.RiskLevel,
		Score:                 p.Score,
		EvidencePlanJSON:      p.EvidencePlanJSON,
		Status:                p.Status,
		ActorID:               p.ActorID,
		ActorType:             principal.PrincipalType,
		PromptContextEnvelope: h.serviceVenturePromptContextEnvelope(ctx, p.WorkspaceID, "service.candidate.upsert", map[string]string{"actor_id": p.ActorID, "candidate_id": p.CandidateID, "direction_id": p.DirectionID}),
		PromptContextSurface:  "service.candidate.upsert",
	})
	if err != nil {
		return nil, rpcErrorFromServiceVentureErr(err, "service.candidate.upsert")
	}
	h.publishServiceVentureEvent(event, candidate.CandidateID)
	return map[string]any{"candidate": candidate}, nil
}

func (h *Handler) serviceCandidateList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p serviceCandidateListParams
	workspaceID, rpcErr := decodeServiceWorkspaceRead(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListServiceCandidates(ctx, sqlite.ServicePortfolioFilter{WorkspaceID: workspaceID, DirectionID: p.DirectionID, Status: p.Status, Limit: p.Limit})
	if err != nil {
		return nil, rpcErrorFromServiceVentureErr(err, "service.candidate.list")
	}
	return map[string]any{"candidates": items, "count": len(items)}, nil
}

func (h *Handler) serviceCandidateGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p serviceCandidateGetParams
	workspaceID, rpcErr := decodeServiceWorkspaceRead(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	item, err := h.store.GetServiceCandidate(ctx, workspaceID, strings.TrimSpace(p.CandidateID))
	if err != nil {
		return nil, rpcErrorFromServiceVentureErr(err, "service.candidate.get")
	}
	return map[string]any{"candidate": item}, nil
}

func (h *Handler) serviceRunStart(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.serviceRunUpsert(ctx, raw, "service.run.start")
}

func (h *Handler) serviceRunUpdate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	return h.serviceRunUpsert(ctx, raw, "service.run.update")
}

func (h *Handler) serviceRunUpsert(ctx context.Context, raw json.RawMessage, surface string) (any, *RPCError) {
	var p serviceRunUpsertParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	var rawFields map[string]json.RawMessage
	if strings.EqualFold(surface, "service.run.update") {
		if err := json.Unmarshal(raw, &rawFields); err != nil {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
	}
	var (
		workspaceID string
		actorID     string
		principal   AuthPrincipal
	)
	if strings.EqualFold(surface, "service.run.update") {
		workspaceID = strings.TrimSpace(p.WorkspaceID)
		actorID = strings.TrimSpace(p.ActorID)
		if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
			return nil, rpcErr
		}
		if rpcErr := requireTrimmedParam(actorID, "actor_id"); rpcErr != nil {
			return nil, rpcErr
		}
		if rpcErr := requireTrimmedParam(strings.TrimSpace(p.RunID), "run_id"); rpcErr != nil {
			return nil, rpcErr
		}
		var rpcErr *RPCError
		principal, rpcErr = requireWorkspaceActorPrincipal(ctx, workspaceID, actorID, "actor_id")
		if rpcErr != nil {
			return nil, rpcErr
		}
		existing, err := h.store.GetServiceRun(ctx, workspaceID, p.RunID)
		if err != nil {
			return nil, rpcErrorFromServiceVentureErr(err, surface)
		}
		if rpcErr := h.requireServiceRunMutationPrincipal(ctx, workspaceID, p.RunID, actorID, principal, surface); rpcErr != nil {
			return nil, rpcErr
		}
		if strings.TrimSpace(p.CandidateID) != "" && strings.TrimSpace(p.CandidateID) != strings.TrimSpace(existing.CandidateID) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: "service.run.update candidate_id does not match existing run"}
		}
		if strings.TrimSpace(p.ProjectID) != "" && strings.TrimSpace(p.ProjectID) != strings.TrimSpace(existing.ProjectID) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: "service.run.update project_id does not match existing run"}
		}
		p.CandidateID = existing.CandidateID
		p.ProjectID = existing.ProjectID
		if !serviceRunPatchFieldPresent(rawFields, "title") || strings.TrimSpace(p.Title) == "" {
			p.Title = existing.Title
		}
		if !serviceRunPatchFieldPresent(rawFields, "deploy_target") {
			p.DeployTarget = existing.DeployTarget
		}
		if !serviceRunPatchFieldPresent(rawFields, "public_url") {
			p.PublicURL = existing.PublicURL
		}
		if !serviceRunPatchFieldPresent(rawFields, "health_check_url") {
			p.HealthCheckURL = existing.HealthCheckURL
		}
		if !serviceRunPatchFieldPresent(rawFields, "budget_account_id") {
			p.BudgetAccountID = existing.BudgetAccountID
		}
		if !serviceRunPatchFieldPresent(rawFields, "budget_cap_micros") {
			p.BudgetCapMicros = existing.BudgetCapMicros
		}
		if !serviceRunPatchFieldPresent(rawFields, "credential_policy") || strings.TrimSpace(p.CredentialPolicy) == "" {
			p.CredentialPolicy = existing.CredentialPolicy
		}
		if !serviceRunPatchFieldPresent(rawFields, "status") || strings.TrimSpace(p.Status) == "" {
			p.Status = existing.Status
		}
	} else {
		var rpcErr *RPCError
		workspaceID, _, actorID, principal, rpcErr = h.validateProjectActor(ctx, p.WorkspaceID, p.ProjectID, p.ActorID)
		if rpcErr != nil {
			return nil, rpcErr
		}
		runID := strings.TrimSpace(p.RunID)
		if runID != "" {
			if existing, err := h.store.GetServiceRun(ctx, workspaceID, runID); err == nil && strings.TrimSpace(existing.RunID) != "" {
				if rpcErr := h.requireServiceRunMutationPrincipal(ctx, workspaceID, runID, actorID, principal, surface); rpcErr != nil {
					return nil, rpcErr
				}
			} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, rpcErrorFromServiceVentureErr(err, surface)
			} else if rpcErr := h.requireServiceRunStartPrincipal(ctx, workspaceID, p.ProjectID, actorID, principal); rpcErr != nil {
				return nil, rpcErr
			}
		} else if rpcErr := h.requireServiceRunStartPrincipal(ctx, workspaceID, p.ProjectID, actorID, principal); rpcErr != nil {
			return nil, rpcErr
		}
	}
	if rpcErr := h.requireServiceRunBudgetAccountPrincipal(ctx, workspaceID, p.ProjectID, p.RunID, p.BudgetAccountID, actorID, principal); rpcErr != nil {
		return nil, rpcErr
	}
	run, event, err := h.store.UpsertServiceRunWithEvent(ctx, sqlite.ServiceRunInput{
		RunID:                 p.RunID,
		IdempotencyKey:        p.IdempotencyKey,
		WorkspaceID:           workspaceID,
		CandidateID:           p.CandidateID,
		ProjectID:             p.ProjectID,
		Title:                 p.Title,
		DeployTarget:          p.DeployTarget,
		PublicURL:             p.PublicURL,
		HealthCheckURL:        p.HealthCheckURL,
		BudgetAccountID:       p.BudgetAccountID,
		BudgetCapMicros:       p.BudgetCapMicros,
		CredentialPolicy:      p.CredentialPolicy,
		Status:                p.Status,
		ActorID:               actorID,
		ActorType:             principal.PrincipalType,
		PromptContextEnvelope: h.serviceVenturePromptContextEnvelope(ctx, workspaceID, surface, map[string]string{"actor_id": actorID, "run_id": p.RunID, "candidate_id": p.CandidateID, "project_id": p.ProjectID}),
		PromptContextSurface:  surface,
	})
	if err != nil {
		return nil, rpcErrorFromServiceVentureErr(err, surface)
	}
	h.publishServiceVentureEvent(event, run.RunID, run.ProjectID)
	return map[string]any{"run": run}, nil
}

func serviceRunPatchFieldPresent(fields map[string]json.RawMessage, name string) bool {
	if len(fields) == 0 {
		return false
	}
	_, ok := fields[name]
	return ok
}

func (h *Handler) serviceRunList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p serviceRunListParams
	workspaceID, rpcErr := decodeServiceWorkspaceRead(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	items, err := h.store.ListServiceRuns(ctx, sqlite.ServicePortfolioFilter{WorkspaceID: workspaceID, CandidateID: p.CandidateID, ProjectID: p.ProjectID, Status: p.Status, Limit: p.Limit})
	if err != nil {
		return nil, rpcErrorFromServiceVentureErr(err, "service.run.list")
	}
	return map[string]any{"runs": items, "count": len(items)}, nil
}

func (h *Handler) serviceRunGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p serviceRunGetParams
	workspaceID, rpcErr := decodeServiceWorkspaceRead(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	item, err := h.store.GetServiceRun(ctx, workspaceID, strings.TrimSpace(p.RunID))
	if err != nil {
		return nil, rpcErrorFromServiceVentureErr(err, "service.run.get")
	}
	return map[string]any{"run": item}, nil
}

func (h *Handler) serviceApprovalGrant(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p serviceApprovalGrantParams
	principal, rpcErr := decodeServiceWorkspaceActor(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := h.requireServiceRunMutationPrincipal(ctx, p.WorkspaceID, p.RunID, p.ActorID, principal, "service.approval.grant"); rpcErr != nil {
		return nil, rpcErr
	}
	record, event, err := h.store.UpsertServiceApprovalGrantWithEvent(ctx, sqlite.ServiceApprovalGrantInput{
		GrantID:               p.GrantID,
		IdempotencyKey:        p.IdempotencyKey,
		WorkspaceID:           p.WorkspaceID,
		RunID:                 p.RunID,
		GrantType:             p.GrantType,
		ScopeJSON:             p.ScopeJSON,
		ApprovalRef:           p.ApprovalRef,
		Status:                p.Status,
		ApprovedBy:            p.ApprovedBy,
		ExpiresAt:             p.ExpiresAt,
		ActorID:               p.ActorID,
		ActorType:             principal.PrincipalType,
		PromptContextEnvelope: h.serviceVenturePromptContextEnvelope(ctx, p.WorkspaceID, "service.approval.grant", map[string]string{"actor_id": p.ActorID, "run_id": p.RunID, "grant_id": p.GrantID}),
		PromptContextSurface:  "service.approval.grant",
	})
	if err != nil {
		return nil, rpcErrorFromServiceVentureErr(err, "service.approval.grant")
	}
	h.publishServiceVentureEvent(event, record.RunID, record.GrantID)
	return map[string]any{"approval_grant": record}, nil
}

func (h *Handler) serviceResourceRecord(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p serviceResourceRecordParams
	principal, rpcErr := decodeServiceWorkspaceActor(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := h.requireServiceRunMutationPrincipal(ctx, p.WorkspaceID, p.RunID, p.ActorID, principal, "service.resource.record"); rpcErr != nil {
		return nil, rpcErr
	}
	record, event, err := h.store.UpsertServiceProviderResourceWithEvent(ctx, sqlite.ServiceProviderResourceInput{
		ResourceID:             p.ResourceID,
		IdempotencyKey:         p.IdempotencyKey,
		WorkspaceID:            p.WorkspaceID,
		RunID:                  p.RunID,
		Provider:               p.Provider,
		ResourceType:           p.ResourceType,
		ResourceRef:            p.ResourceRef,
		CredentialVaultEntryID: p.CredentialVaultEntryID,
		ApprovalGrantID:        p.ApprovalGrantID,
		Paid:                   p.Paid,
		CostCapMicros:          p.CostCapMicros,
		Status:                 p.Status,
		TTLExpiresAt:           p.TTLExpiresAt,
		ActorID:                p.ActorID,
		ActorType:              principal.PrincipalType,
		PromptContextEnvelope:  h.serviceVenturePromptContextEnvelope(ctx, p.WorkspaceID, "service.resource.record", map[string]string{"actor_id": p.ActorID, "run_id": p.RunID, "resource_id": p.ResourceID}),
		PromptContextSurface:   "service.resource.record",
	})
	if err != nil {
		return nil, rpcErrorFromServiceVentureErr(err, "service.resource.record")
	}
	h.publishServiceVentureEvent(event, record.RunID, record.ResourceID)
	return map[string]any{"provider_resource": record}, nil
}

func (h *Handler) serviceSpendRecord(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p serviceSpendRecordParams
	principal, rpcErr := decodeServiceWorkspaceActor(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := h.requireServiceRunMutationPrincipal(ctx, p.WorkspaceID, p.RunID, p.ActorID, principal, "service.spend.record"); rpcErr != nil {
		return nil, rpcErr
	}
	record, event, err := h.store.RecordServiceSpendReceiptWithEvent(ctx, sqlite.ServiceSpendReceiptInput{
		ReceiptID:             p.ReceiptID,
		IdempotencyKey:        p.IdempotencyKey,
		WorkspaceID:           p.WorkspaceID,
		RunID:                 p.RunID,
		ProviderResourceID:    p.ProviderResourceID,
		LedgerEntryID:         p.LedgerEntryID,
		AmountMicros:          p.AmountMicros,
		Currency:              p.Currency,
		ExternalReceiptRef:    p.ExternalReceiptRef,
		EvidenceRef:           p.EvidenceRef,
		ActorID:               p.ActorID,
		ActorType:             principal.PrincipalType,
		PromptContextEnvelope: h.serviceVenturePromptContextEnvelope(ctx, p.WorkspaceID, "service.spend.record", map[string]string{"actor_id": p.ActorID, "run_id": p.RunID, "receipt_id": p.ReceiptID}),
		PromptContextSurface:  "service.spend.record",
	})
	if err != nil {
		return nil, rpcErrorFromServiceVentureErr(err, "service.spend.record")
	}
	h.publishServiceVentureEvent(event, record.RunID, record.ReceiptID)
	return map[string]any{"spend_receipt": record}, nil
}

func (h *Handler) serviceRevenueRecord(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p serviceRevenueRecordParams
	principal, rpcErr := decodeServiceWorkspaceActor(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := h.requireServiceRunMutationPrincipal(ctx, p.WorkspaceID, p.RunID, p.ActorID, principal, "service.revenue.record"); rpcErr != nil {
		return nil, rpcErr
	}
	record, event, err := h.store.RecordServiceRevenueObservationWithEvent(ctx, sqlite.ServiceRevenueObservationInput{
		ObservationID:         p.ObservationID,
		IdempotencyKey:        p.IdempotencyKey,
		WorkspaceID:           p.WorkspaceID,
		RunID:                 p.RunID,
		AmountMicros:          p.AmountMicros,
		Currency:              p.Currency,
		Source:                p.Source,
		ExternalReceiptRef:    p.ExternalReceiptRef,
		EvidenceRef:           p.EvidenceRef,
		ObservedAt:            p.ObservedAt,
		ActorID:               p.ActorID,
		ActorType:             principal.PrincipalType,
		PromptContextEnvelope: h.serviceVenturePromptContextEnvelope(ctx, p.WorkspaceID, "service.revenue.record", map[string]string{"actor_id": p.ActorID, "run_id": p.RunID, "observation_id": p.ObservationID}),
		PromptContextSurface:  "service.revenue.record",
	})
	if err != nil {
		return nil, rpcErrorFromServiceVentureErr(err, "service.revenue.record")
	}
	h.publishServiceVentureEvent(event, record.RunID, record.ObservationID)
	return map[string]any{"revenue_observation": record}, nil
}

func (h *Handler) serviceOutcomeRecord(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p serviceOutcomeRecordParams
	principal, rpcErr := decodeServiceWorkspaceActor(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := h.requireServiceRunMutationPrincipal(ctx, p.WorkspaceID, p.RunID, p.ActorID, principal, "service.outcome.record"); rpcErr != nil {
		return nil, rpcErr
	}
	record, event, err := h.store.RecordServiceOutcomeWithEvent(ctx, sqlite.ServiceOutcomeInput{
		OutcomeID:            p.OutcomeID,
		IdempotencyKey:       p.IdempotencyKey,
		WorkspaceID:          p.WorkspaceID,
		RunID:                p.RunID,
		PublicURL:            p.PublicURL,
		DeployHealthStatus:   p.DeployHealthStatus,
		DeployEvidenceRef:    p.DeployEvidenceRef,
		AnalyticsJSON:        p.AnalyticsJSON,
		AnalyticsEvidenceRef: p.AnalyticsEvidenceRef,
		SpendMicros:          p.SpendMicros,
		SpendEvidenceRef:     p.SpendEvidenceRef,
		RevenueMicros:        p.RevenueMicros,
		RevenueEvidenceRef:   p.RevenueEvidenceRef,
		QualityScore:         p.QualityScore,
		Decision:             p.Decision,
		DecisionReason:       p.DecisionReason,
		EvidenceRefsJSON:     p.EvidenceRefsJSON,
		ActorID:              p.ActorID,
		ActorType:            principal.PrincipalType,
		PromptContextEnvelope: h.serviceVenturePromptContextEnvelope(ctx, p.WorkspaceID, "service.outcome.record", map[string]string{
			"actor_id":   p.ActorID,
			"run_id":     p.RunID,
			"outcome_id": p.OutcomeID,
			"decision":   strings.ToUpper(strings.TrimSpace(p.Decision)),
		}),
		PromptContextSurface: "service.outcome.record",
	})
	if err != nil {
		return nil, rpcErrorFromServiceVentureErr(err, "service.outcome.record")
	}
	h.publishServiceVentureEvent(event, record.RunID, record.OutcomeID, record.Decision)
	return map[string]any{"outcome": record}, nil
}

func (h *Handler) serviceCoordinationGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p serviceCoordinationGetParams
	workspaceID, rpcErr := decodeServiceWorkspaceRead(ctx, raw, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	coordination, err := h.store.GetServiceCoordination(ctx, workspaceID, strings.TrimSpace(p.RunID))
	if err != nil {
		return nil, rpcErrorFromServiceVentureErr(err, "service.coordination.get")
	}
	return map[string]any{"coordination": coordination}, nil
}

func decodeServiceWorkspaceRead(ctx context.Context, raw json.RawMessage, p any) (string, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return "", &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := serviceParamString(p, "WorkspaceID")
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return "", rpcErr
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, workspaceID); rpcErr != nil {
		return "", rpcErr
	}
	return workspaceID, nil
}

func decodeServiceWorkspaceActor(ctx context.Context, raw json.RawMessage, p any) (AuthPrincipal, *RPCError) {
	if err := json.Unmarshal(raw, p); err != nil {
		return AuthPrincipal{}, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	workspaceID := serviceParamString(p, "WorkspaceID")
	actorID := serviceParamString(p, "ActorID")
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return AuthPrincipal{}, rpcErr
	}
	if rpcErr := requireTrimmedParam(actorID, "actor_id"); rpcErr != nil {
		return AuthPrincipal{}, rpcErr
	}
	return requireWorkspaceActorPrincipal(ctx, workspaceID, actorID, "actor_id")
}

func serviceParamString(p any, fieldName string) string {
	v := reflect.ValueOf(p)
	if !v.IsValid() {
		return ""
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return ""
	}
	field := v.FieldByName(fieldName)
	if !field.IsValid() || field.Kind() != reflect.String {
		return ""
	}
	return strings.TrimSpace(field.String())
}

func (h *Handler) serviceVenturePromptContextEnvelope(ctx context.Context, workspaceID, surface string, fields map[string]string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	envelope := sqlite.BuildServiceVenturePromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
	for key, value := range fields {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value := strings.TrimSpace(value); value != "" {
			envelope[key] = value
		}
	}
	return envelope
}

func (h *Handler) publishServiceVentureEvent(event sqlite.RuntimeEventRecord, fallbackEntityIDs ...string) {
	if strings.TrimSpace(event.EventID) == "" {
		return
	}
	h.publishRuntimeEventRecord(event, fallbackEntityIDs...)
}

func (h *Handler) requireServiceRunMutationPrincipal(ctx context.Context, workspaceID, runID, actorID string, principal AuthPrincipal, surface string) *RPCError {
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		return nil
	}
	workspaceID = strings.TrimSpace(workspaceID)
	runID = strings.TrimSpace(runID)
	actorID = strings.TrimSpace(actorID)
	run, err := h.store.GetServiceRun(ctx, workspaceID, runID)
	if err != nil {
		return rpcErrorFromServiceVentureErr(err, surface)
	}
	ok, rpcErr := h.agentHasActiveProjectRole(ctx, workspaceID, run.ProjectID, actorID)
	if rpcErr != nil {
		return rpcErr
	}
	if ok {
		return nil
	}
	return &RPCError{Code: errCodePermissionDenied, Message: "agent principal may mutate service run evidence only while holding an active project role"}
}

func (h *Handler) requireServiceRunStartPrincipal(ctx context.Context, workspaceID, projectID, actorID string, principal AuthPrincipal) *RPCError {
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		return nil
	}
	ok, rpcErr := h.agentHasActiveProjectRole(ctx, workspaceID, projectID, actorID)
	if rpcErr != nil {
		return rpcErr
	}
	if ok {
		return nil
	}
	return &RPCError{Code: errCodePermissionDenied, Message: "agent principal may start a service run only while holding an active project role"}
}

func (h *Handler) requireServiceRunBudgetAccountPrincipal(ctx context.Context, workspaceID, projectID, runID, accountID, actorID string, principal AuthPrincipal) *RPCError {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return nil
	}
	account, err := h.store.GetBudgetAccount(ctx, accountID)
	if err != nil {
		return rpcErrorFromBudgetLedgerErr(err)
	}
	if strings.TrimSpace(account.WorkspaceID) != strings.TrimSpace(workspaceID) {
		return &RPCError{Code: errCodePermissionDenied, Message: "service run budget account workspace mismatch"}
	}
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(account.PrincipalType)) {
	case "agent":
		if strings.TrimSpace(account.PrincipalID) == strings.TrimSpace(actorID) {
			return nil
		}
	case "service_run":
		if strings.TrimSpace(account.PrincipalID) == strings.TrimSpace(runID) {
			return nil
		}
	case "project":
		if strings.TrimSpace(account.PrincipalID) == strings.TrimSpace(projectID) {
			ok, rpcErr := h.agentHasActiveProjectRole(ctx, workspaceID, projectID, actorID)
			if rpcErr != nil {
				return rpcErr
			}
			if ok {
				return nil
			}
		}
	}
	return &RPCError{Code: errCodePermissionDenied, Message: "agent principal is not authorized to bind this budget account to the service run"}
}

func (h *Handler) agentHasActiveProjectRole(ctx context.Context, workspaceID, projectID, agentID string) (bool, *RPCError) {
	roles, err := h.store.ListProjectRoles(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), false)
	if err != nil {
		return false, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	for _, role := range roles {
		if strings.TrimSpace(role.AgentID) == strings.TrimSpace(agentID) && strings.EqualFold(strings.TrimSpace(role.Status), sqlite.ProjectRoleStatusActive) {
			return true, nil
		}
	}
	return false, nil
}

func rpcErrorFromServiceVentureErr(err error, surface string) *RPCError {
	if rpcErr := authorityRejectRPCError(err, surface); rpcErr != nil {
		return rpcErr
	}
	switch {
	case errors.Is(err, sqlite.ErrServiceVentureInvalid),
		errors.Is(err, sqlite.ErrBudgetExceeded),
		errors.Is(err, sql.ErrNoRows):
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	default:
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
}
