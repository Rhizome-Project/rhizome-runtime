package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

const agentManagedBudgetAccountMaxMicros int64 = 1_000_000_000

type budgetAccountEnsureParams struct {
	AccountID     string `json:"account_id"`
	PrincipalType string `json:"principal_type"`
	PrincipalID   string `json:"principal_id"`
	WorkspaceID   string `json:"workspace_id"`
	Currency      string `json:"currency"`
	LimitMicros   int64  `json:"limit_micros"`
	Status        string `json:"status"`
}

type budgetAccountGetParams struct {
	AccountID   string `json:"account_id"`
	WorkspaceID string `json:"workspace_id"`
}

type budgetReserveParams struct {
	ReservationID  string `json:"reservation_id"`
	IdempotencyKey string `json:"idempotency_key"`
	AccountID      string `json:"account_id"`
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	TaskID         string `json:"task_id"`
	RunID          string `json:"run_id"`
	ProviderID     string `json:"provider_id"`
	Model          string `json:"model"`
	AmountMicros   int64  `json:"amount_micros"`
	Reason         string `json:"reason"`
}

type budgetSpendParams struct {
	EntryID        string `json:"entry_id"`
	IdempotencyKey string `json:"idempotency_key"`
	AccountID      string `json:"account_id"`
	ReservationID  string `json:"reservation_id"`
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	TaskID         string `json:"task_id"`
	RunID          string `json:"run_id"`
	ProviderID     string `json:"provider_id"`
	Model          string `json:"model"`
	AmountMicros   int64  `json:"amount_micros"`
	Reason         string `json:"reason"`
}

type budgetReleaseParams struct {
	EntryID        string `json:"entry_id"`
	IdempotencyKey string `json:"idempotency_key"`
	AccountID      string `json:"account_id"`
	ReservationID  string `json:"reservation_id"`
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	TaskID         string `json:"task_id"`
	RunID          string `json:"run_id"`
	ProviderID     string `json:"provider_id"`
	Model          string `json:"model"`
	AmountMicros   int64  `json:"amount_micros"`
	Reason         string `json:"reason"`
}

type budgetRefundParams struct {
	EntryID        string `json:"entry_id"`
	IdempotencyKey string `json:"idempotency_key"`
	AccountID      string `json:"account_id"`
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	TaskID         string `json:"task_id"`
	RunID          string `json:"run_id"`
	ProviderID     string `json:"provider_id"`
	Model          string `json:"model"`
	SourceEntryID  string `json:"source_entry_id"`
	AmountMicros   int64  `json:"amount_micros"`
	Reason         string `json:"reason"`
}

type budgetLedgerListParams struct {
	AccountID     string `json:"account_id"`
	ReservationID string `json:"reservation_id"`
	WorkspaceID   string `json:"workspace_id"`
	TaskID        string `json:"task_id"`
	RunID         string `json:"run_id"`
	Limit         int    `json:"limit"`
}

type budgetReservationListParams struct {
	AccountID   string `json:"account_id"`
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	TaskID      string `json:"task_id"`
	RunID       string `json:"run_id"`
	Status      string `json:"status"`
	Limit       int    `json:"limit"`
}

type budgetHealthParams struct {
	StaleAfterSec int `json:"stale_after_sec"`
}

func (h *Handler) budgetAccountEnsure(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p budgetAccountEnsureParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	principal, rpcErr := requireWorkspacePrincipal(ctx, strings.TrimSpace(p.WorkspaceID))
	if rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := h.requireBudgetAccountEnsurePrincipal(ctx, principal, p); rpcErr != nil {
		return nil, rpcErr
	}
	snapshot, err := h.store.EnsureBudgetAccount(ctx, sqlite.BudgetAccountInput{
		AccountID:     p.AccountID,
		PrincipalType: p.PrincipalType,
		PrincipalID:   p.PrincipalID,
		WorkspaceID:   p.WorkspaceID,
		Currency:      p.Currency,
		LimitMicros:   p.LimitMicros,
		Status:        p.Status,
	})
	if err != nil {
		return nil, rpcErrorFromBudgetLedgerErr(err)
	}
	return map[string]any{"account": snapshot}, nil
}

func (h *Handler) budgetAccountGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p budgetAccountGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, strings.TrimSpace(p.WorkspaceID)); rpcErr != nil {
		return nil, rpcErr
	}
	snapshot, err := h.store.GetBudgetAccount(ctx, p.AccountID)
	if err != nil {
		return nil, rpcErrorFromBudgetLedgerErr(err)
	}
	if strings.TrimSpace(snapshot.WorkspaceID) != strings.TrimSpace(p.WorkspaceID) {
		return nil, &RPCError{Code: errCodePermissionDenied, Message: "workspace isolation violation"}
	}
	return map[string]any{"account": snapshot}, nil
}

func (h *Handler) budgetReserve(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p budgetReserveParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := h.requireBudgetAccountUsePrincipal(ctx, p.WorkspaceID, p.AgentID, p.AccountID, p.RunID, "budget.reserve"); rpcErr != nil {
		return nil, rpcErr
	}
	snapshot, err := h.store.ReserveBudget(ctx, sqlite.BudgetReservationInput{
		ReservationID:  p.ReservationID,
		IdempotencyKey: p.IdempotencyKey,
		AccountID:      p.AccountID,
		WorkspaceID:    p.WorkspaceID,
		AgentID:        p.AgentID,
		TaskID:         p.TaskID,
		RunID:          p.RunID,
		ProviderID:     p.ProviderID,
		Model:          p.Model,
		AmountMicros:   p.AmountMicros,
		Reason:         p.Reason,
	})
	if err != nil {
		return nil, rpcErrorFromBudgetLedgerErr(err)
	}
	return map[string]any{"account": snapshot, "reservation_id": p.ReservationID}, nil
}

func (h *Handler) budgetSpend(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p budgetSpendParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := h.requireBudgetAccountUsePrincipal(ctx, p.WorkspaceID, p.AgentID, p.AccountID, p.RunID, "budget.spend"); rpcErr != nil {
		return nil, rpcErr
	}
	snapshot, err := h.store.CaptureBudgetSpend(ctx, sqlite.BudgetSpendCaptureInput{
		EntryID:        p.EntryID,
		IdempotencyKey: p.IdempotencyKey,
		AccountID:      p.AccountID,
		ReservationID:  p.ReservationID,
		WorkspaceID:    p.WorkspaceID,
		AgentID:        p.AgentID,
		TaskID:         p.TaskID,
		RunID:          p.RunID,
		ProviderID:     p.ProviderID,
		Model:          p.Model,
		AmountMicros:   p.AmountMicros,
		Reason:         p.Reason,
	})
	if err != nil {
		return nil, rpcErrorFromBudgetLedgerErr(err)
	}
	return map[string]any{"account": snapshot, "entry_id": p.EntryID}, nil
}

func (h *Handler) budgetRelease(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p budgetReleaseParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := h.requireBudgetAccountUsePrincipal(ctx, p.WorkspaceID, p.AgentID, p.AccountID, p.RunID, "budget.release"); rpcErr != nil {
		return nil, rpcErr
	}
	snapshot, err := h.store.ReleaseBudgetReservation(ctx, sqlite.BudgetReservationReleaseInput{
		EntryID:        p.EntryID,
		IdempotencyKey: p.IdempotencyKey,
		AccountID:      p.AccountID,
		ReservationID:  p.ReservationID,
		WorkspaceID:    p.WorkspaceID,
		AgentID:        p.AgentID,
		TaskID:         p.TaskID,
		RunID:          p.RunID,
		ProviderID:     p.ProviderID,
		Model:          p.Model,
		AmountMicros:   p.AmountMicros,
		Reason:         p.Reason,
	})
	if err != nil {
		return nil, rpcErrorFromBudgetLedgerErr(err)
	}
	return map[string]any{"account": snapshot, "entry_id": p.EntryID}, nil
}

func (h *Handler) budgetRefund(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p budgetRefundParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if rpcErr := h.requireBudgetAccountUsePrincipal(ctx, p.WorkspaceID, p.AgentID, p.AccountID, p.RunID, "budget.refund"); rpcErr != nil {
		return nil, rpcErr
	}
	snapshot, err := h.store.RefundBudgetSpend(ctx, sqlite.BudgetRefundInput{
		EntryID:        p.EntryID,
		IdempotencyKey: p.IdempotencyKey,
		AccountID:      p.AccountID,
		WorkspaceID:    p.WorkspaceID,
		AgentID:        p.AgentID,
		TaskID:         p.TaskID,
		RunID:          p.RunID,
		ProviderID:     p.ProviderID,
		Model:          p.Model,
		SourceEntryID:  p.SourceEntryID,
		AmountMicros:   p.AmountMicros,
		Reason:         p.Reason,
	})
	if err != nil {
		return nil, rpcErrorFromBudgetLedgerErr(err)
	}
	return map[string]any{"account": snapshot, "entry_id": p.EntryID}, nil
}

func (h *Handler) budgetLedgerList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p budgetLedgerListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, strings.TrimSpace(p.WorkspaceID)); rpcErr != nil {
		return nil, rpcErr
	}
	entries, err := h.store.ListBudgetLedgerEntries(ctx, sqlite.BudgetLedgerEntryFilter{
		AccountID:     p.AccountID,
		ReservationID: p.ReservationID,
		WorkspaceID:   p.WorkspaceID,
		TaskID:        p.TaskID,
		RunID:         p.RunID,
		Limit:         p.Limit,
	})
	if err != nil {
		return nil, rpcErrorFromBudgetLedgerErr(err)
	}
	return map[string]any{"entries": entries, "count": len(entries)}, nil
}

func (h *Handler) budgetReservationList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p budgetReservationListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	if _, rpcErr := requireWorkspacePrincipal(ctx, strings.TrimSpace(p.WorkspaceID)); rpcErr != nil {
		return nil, rpcErr
	}
	reservations, err := h.store.ListBudgetReservations(ctx, sqlite.BudgetReservationFilter{
		AccountID:   p.AccountID,
		WorkspaceID: p.WorkspaceID,
		AgentID:     p.AgentID,
		TaskID:      p.TaskID,
		RunID:       p.RunID,
		Status:      p.Status,
		Limit:       p.Limit,
	})
	if err != nil {
		return nil, rpcErrorFromBudgetLedgerErr(err)
	}
	return map[string]any{"reservations": reservations, "count": len(reservations)}, nil
}

func (h *Handler) budgetHealth(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p budgetHealthParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	snapshot, err := h.store.GetBudgetLedgerHealthSnapshot(ctx, time.Duration(p.StaleAfterSec)*time.Second)
	if err != nil {
		return nil, rpcErrorFromBudgetLedgerErr(err)
	}
	return map[string]any{"budget_ledger": snapshot}, nil
}

func (h *Handler) requireBudgetAccountEnsurePrincipal(ctx context.Context, principal AuthPrincipal, p budgetAccountEnsureParams) *RPCError {
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		return nil
	}
	if !strings.EqualFold(strings.TrimSpace(p.PrincipalType), "agent") || strings.TrimSpace(p.PrincipalID) != strings.TrimSpace(principal.PrincipalID) {
		return &RPCError{Code: errCodePermissionDenied, Message: "agent principals may only ensure their own pre-provisioned budget account"}
	}
	existing, err := h.store.GetBudgetAccount(ctx, strings.TrimSpace(p.AccountID))
	if err != nil {
		if !errors.Is(err, sqlite.ErrBudgetAccountNotFound) {
			return rpcErrorFromBudgetLedgerErr(err)
		}
		if !agentBudgetAccountIDAllowed(strings.TrimSpace(p.WorkspaceID), strings.TrimSpace(principal.PrincipalID), strings.TrimSpace(p.AccountID)) {
			return &RPCError{Code: errCodePermissionDenied, Message: "agent principals may only create their managed runtime budget account"}
		}
		if p.LimitMicros < 0 || p.LimitMicros > agentManagedBudgetAccountMaxMicros {
			return &RPCError{Code: errCodePermissionDenied, Message: "agent runtime budget limit exceeds managed launch cap"}
		}
		return nil
	}
	if strings.TrimSpace(existing.WorkspaceID) != strings.TrimSpace(p.WorkspaceID) ||
		!strings.EqualFold(strings.TrimSpace(existing.PrincipalType), "agent") ||
		strings.TrimSpace(existing.PrincipalID) != strings.TrimSpace(principal.PrincipalID) {
		return &RPCError{Code: errCodePermissionDenied, Message: "agent principals may only ensure their own pre-provisioned budget account"}
	}
	if p.LimitMicros > existing.LimitMicros {
		return &RPCError{Code: errCodePermissionDenied, Message: "agent principals may not create or raise hard budget limits"}
	}
	return nil
}

func agentBudgetAccountIDAllowed(workspaceID, agentID, accountID string) bool {
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	accountID = strings.TrimSpace(accountID)
	if workspaceID == "" || agentID == "" || accountID == "" {
		return false
	}
	return accountID == serverManagedAgentBudgetAccountID(agentID) ||
		accountID == serverDefaultRuntimeBudgetAccountID(workspaceID, agentID)
}

func serverManagedAgentBudgetAccountID(agentID string) string {
	agentID = strings.ToLower(strings.TrimSpace(agentID))
	var builder strings.Builder
	for _, r := range agentID {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		case r == '-' || r == '_':
			builder.WriteRune(r)
		default:
			builder.WriteByte('-')
		}
	}
	return "pilot-agent-" + strings.Trim(builder.String(), "-_")
}

func serverDefaultRuntimeBudgetAccountID(workspaceID, agentID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(workspaceID) + "|" + strings.TrimSpace(agentID) + "|llm"))
	return "llm-budget-" + fmt.Sprintf("%x", sum[:8])
}

func (h *Handler) requireBudgetAccountUsePrincipal(ctx context.Context, workspaceID, agentID, accountID, runID, surface string) *RPCError {
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	principal, rpcErr := requireWorkspacePrincipal(ctx, workspaceID)
	if rpcErr != nil {
		return rpcErr
	}
	if strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") && agentID != strings.TrimSpace(principal.PrincipalID) {
		return &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match agent_id"}
	}
	account, err := h.store.GetBudgetAccount(ctx, strings.TrimSpace(accountID))
	if err != nil {
		return rpcErrorFromBudgetLedgerErr(err)
	}
	if strings.TrimSpace(account.WorkspaceID) != workspaceID {
		return &RPCError{Code: errCodePermissionDenied, Message: "workspace isolation violation"}
	}
	if !strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(account.PrincipalType)) {
	case "agent":
		if strings.TrimSpace(account.PrincipalID) == agentID {
			return nil
		}
	case "service_run":
		if strings.TrimSpace(account.PrincipalID) == strings.TrimSpace(runID) {
			return h.requireServiceRunMutationPrincipal(ctx, workspaceID, strings.TrimSpace(runID), agentID, principal, surface)
		}
	case "project":
		run, err := h.store.GetServiceRun(ctx, workspaceID, strings.TrimSpace(runID))
		if err != nil {
			return rpcErrorFromServiceVentureErr(err, surface)
		}
		if strings.TrimSpace(run.ProjectID) == strings.TrimSpace(account.PrincipalID) {
			ok, roleErr := h.agentHasActiveProjectRole(ctx, workspaceID, run.ProjectID, agentID)
			if roleErr != nil {
				return roleErr
			}
			if ok {
				return nil
			}
		}
	}
	return &RPCError{Code: errCodePermissionDenied, Message: "agent principal is not authorized for this budget account"}
}

func requireBudgetWorkspaceActor(ctx context.Context, workspaceID, agentID string) *RPCError {
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	principal, rpcErr := requireWorkspacePrincipal(ctx, workspaceID)
	if rpcErr != nil {
		return rpcErr
	}
	if strings.EqualFold(strings.TrimSpace(principal.PrincipalType), "agent") && agentID != strings.TrimSpace(principal.PrincipalID) {
		return &RPCError{Code: errCodePermissionDenied, Message: "actor mismatch: token identity does not match agent_id"}
	}
	return nil
}

func rpcErrorFromBudgetLedgerErr(err error) *RPCError {
	if err == nil {
		return nil
	}
	lower := strings.ToLower(err.Error())
	if errors.Is(err, sqlite.ErrBudgetExceeded) ||
		errors.Is(err, sqlite.ErrBudgetAccountNotFound) ||
		errors.Is(err, sqlite.ErrBudgetReservationNotFound) ||
		strings.Contains(lower, "required") ||
		strings.Contains(lower, "does not match") ||
		strings.Contains(lower, "not found") ||
		strings.Contains(lower, "idempotency key replay") {
		return &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	return &RPCError{Code: errCodeInternal, Message: err.Error()}
}
