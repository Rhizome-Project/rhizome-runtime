package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	BudgetCurrencyUSD          = "USD"
	BudgetAccountStatusActive  = "ACTIVE"
	BudgetLedgerHealthContract = "budget_ledger.health.v1"

	BudgetLedgerEntryReservation = "RESERVATION"
	BudgetLedgerEntrySpend       = "SPEND"
	BudgetLedgerEntryRelease     = "RELEASE"
	BudgetLedgerEntryRefund      = "REFUND"

	budgetReservationStatusOpen   = "OPEN"
	budgetReservationStatusClosed = "CLOSED"
)

var (
	ErrBudgetAccountNotFound     = errors.New("budget account not found")
	ErrBudgetReservationNotFound = errors.New("budget reservation not found")
)

type BudgetAccountInput struct {
	AccountID     string
	PrincipalType string
	PrincipalID   string
	WorkspaceID   string
	Currency      string
	LimitMicros   int64
	Status        string
}

type BudgetAccountSnapshot struct {
	AccountID       string `json:"account_id"`
	PrincipalType   string `json:"principal_type"`
	PrincipalID     string `json:"principal_id"`
	WorkspaceID     string `json:"workspace_id"`
	Currency        string `json:"currency"`
	LimitMicros     int64  `json:"limit_micros"`
	ReservedMicros  int64  `json:"reserved_micros"`
	SpentMicros     int64  `json:"spent_micros"`
	RefundedMicros  int64  `json:"refunded_micros"`
	AvailableMicros int64  `json:"available_micros"`
	Status          string `json:"status"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

type BudgetReservationInput struct {
	ReservationID  string
	IdempotencyKey string
	AccountID      string
	WorkspaceID    string
	AgentID        string
	TaskID         string
	RunID          string
	ProviderID     string
	Model          string
	AmountMicros   int64
	Reason         string
}

type BudgetSpendCaptureInput struct {
	EntryID        string
	IdempotencyKey string
	AccountID      string
	ReservationID  string
	WorkspaceID    string
	AgentID        string
	TaskID         string
	RunID          string
	ProviderID     string
	Model          string
	AmountMicros   int64
	Reason         string
}

type BudgetReservationReleaseInput struct {
	EntryID        string
	IdempotencyKey string
	AccountID      string
	ReservationID  string
	WorkspaceID    string
	AgentID        string
	TaskID         string
	RunID          string
	ProviderID     string
	Model          string
	AmountMicros   int64
	Reason         string
}

type BudgetRefundInput struct {
	EntryID        string
	IdempotencyKey string
	AccountID      string
	WorkspaceID    string
	AgentID        string
	TaskID         string
	RunID          string
	ProviderID     string
	Model          string
	SourceEntryID  string
	AmountMicros   int64
	Reason         string
}

type BudgetLedgerEntryFilter struct {
	AccountID     string
	ReservationID string
	WorkspaceID   string
	TaskID        string
	RunID         string
	Limit         int
}

type BudgetReservationFilter struct {
	AccountID   string
	WorkspaceID string
	AgentID     string
	TaskID      string
	RunID       string
	Status      string
	Limit       int
}

type BudgetLedgerEntryRecord struct {
	EntryID        string `json:"entry_id"`
	IdempotencyKey string `json:"idempotency_key"`
	AccountID      string `json:"account_id"`
	ReservationID  string `json:"reservation_id,omitempty"`
	EntryType      string `json:"entry_type"`
	AmountMicros   int64  `json:"amount_micros"`
	Currency       string `json:"currency"`
	WorkspaceID    string `json:"workspace_id"`
	AgentID        string `json:"agent_id"`
	TaskID         string `json:"task_id"`
	RunID          string `json:"run_id"`
	ProviderID     string `json:"provider_id"`
	Model          string `json:"model"`
	SourceEntryID  string `json:"source_entry_id,omitempty"`
	Reason         string `json:"reason,omitempty"`
	CreatedAt      string `json:"created_at"`
}

type BudgetReservationRecord struct {
	ReservationID   string `json:"reservation_id"`
	IdempotencyKey  string `json:"idempotency_key"`
	AccountID       string `json:"account_id"`
	AmountMicros    int64  `json:"amount_micros"`
	SpentMicros     int64  `json:"spent_micros"`
	ReleasedMicros  int64  `json:"released_micros"`
	RemainingMicros int64  `json:"remaining_micros"`
	Status          string `json:"status"`
	WorkspaceID     string `json:"workspace_id"`
	AgentID         string `json:"agent_id"`
	TaskID          string `json:"task_id"`
	RunID           string `json:"run_id"`
	ProviderID      string `json:"provider_id"`
	Model           string `json:"model"`
	Reason          string `json:"reason,omitempty"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

type BudgetLedgerHealthSnapshot struct {
	Contract                  string                             `json:"contract"`
	AccountCount              int                                `json:"account_count"`
	ExhaustedAccountCount     int                                `json:"exhausted_account_count"`
	OpenReservationCount      int                                `json:"open_reservation_count"`
	StaleOpenReservationCount int                                `json:"stale_open_reservation_count"`
	OverspentAccountCount     int                                `json:"overspent_account_count"`
	LedgerEntryCount          int                                `json:"ledger_entry_count"`
	LastLedgerEntryAt         string                             `json:"last_ledger_entry_at,omitempty"`
	ReferenceAt               string                             `json:"reference_at"`
	Status                    string                             `json:"status"`
	Message                   string                             `json:"message,omitempty"`
	Error                     string                             `json:"error,omitempty"`
	Reasons                   []string                           `json:"reasons,omitempty"`
	ExhaustedAccountExamples  []BudgetLedgerAccountHealthExample `json:"exhausted_account_examples,omitempty"`
}

type BudgetLedgerAccountHealthExample struct {
	AccountID       string `json:"account_id"`
	PrincipalType   string `json:"principal_type"`
	PrincipalID     string `json:"principal_id"`
	WorkspaceID     string `json:"workspace_id"`
	LimitMicros     int64  `json:"limit_micros"`
	ReservedMicros  int64  `json:"reserved_micros"`
	SpentMicros     int64  `json:"spent_micros"`
	RefundedMicros  int64  `json:"refunded_micros"`
	AvailableMicros int64  `json:"available_micros"`
	Status          string `json:"status"`
}

type budgetReservationRow struct {
	ReservationID  string
	IdempotencyKey string
	AccountID      string
	AmountMicros   int64
	SpentMicros    int64
	ReleasedMicros int64
	Status         string
	WorkspaceID    string
	AgentID        string
	TaskID         string
	RunID          string
	ProviderID     string
	Model          string
}

func (s *Store) EnsureBudgetAccount(ctx context.Context, input BudgetAccountInput) (BudgetAccountSnapshot, error) {
	accountID := strings.TrimSpace(input.AccountID)
	if accountID == "" {
		return BudgetAccountSnapshot{}, errors.New("account_id is required")
	}
	principalType := strings.TrimSpace(input.PrincipalType)
	if principalType == "" {
		return BudgetAccountSnapshot{}, errors.New("principal_type is required")
	}
	principalID := strings.TrimSpace(input.PrincipalID)
	if principalID == "" {
		return BudgetAccountSnapshot{}, errors.New("principal_id is required")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return BudgetAccountSnapshot{}, errors.New("workspace_id is required")
	}
	currency := normalizeBudgetCurrency(input.Currency)
	if input.LimitMicros < 0 {
		return BudgetAccountSnapshot{}, errors.New("limit_micros cannot be negative")
	}
	status := strings.TrimSpace(input.Status)
	if status == "" {
		status = BudgetAccountStatusActive
	}
	if status != BudgetAccountStatusActive {
		return BudgetAccountSnapshot{}, errors.New("budget account status must be ACTIVE")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return BudgetAccountSnapshot{}, fmt.Errorf("begin budget account tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	existing, err := s.getBudgetAccountSnapshotTx(ctx, tx, accountID)
	if err == nil {
		if existing.PrincipalType != principalType ||
			existing.PrincipalID != principalID ||
			existing.WorkspaceID != workspaceID ||
			existing.Currency != currency {
			return BudgetAccountSnapshot{}, errors.New("budget account identity fields cannot change")
		}
		if input.LimitMicros < existing.SpentMicros-existing.RefundedMicros+existing.ReservedMicros {
			return BudgetAccountSnapshot{}, fmt.Errorf("%w: budget limit below committed and reserved balance", ErrBudgetExceeded)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE budget_accounts
			    SET limit_micros = ?, status = ?, updated_at = ?
			  WHERE account_id = ?`,
			input.LimitMicros, status, now, accountID,
		); err != nil {
			return BudgetAccountSnapshot{}, fmt.Errorf("update budget account: %w", err)
		}
		snapshot, err := s.getBudgetAccountSnapshotTx(ctx, tx, accountID)
		if err != nil {
			return BudgetAccountSnapshot{}, err
		}
		if err := tx.Commit(); err != nil {
			return BudgetAccountSnapshot{}, fmt.Errorf("commit budget account update: %w", err)
		}
		return snapshot, nil
	}
	if !errors.Is(err, ErrBudgetAccountNotFound) {
		return BudgetAccountSnapshot{}, err
	}

	existing, err = s.getBudgetAccountSnapshotByPrincipalTx(ctx, tx, principalType, principalID, workspaceID, currency)
	if err == nil {
		if input.LimitMicros < existing.SpentMicros-existing.RefundedMicros+existing.ReservedMicros {
			return BudgetAccountSnapshot{}, fmt.Errorf("%w: budget limit below committed and reserved balance", ErrBudgetExceeded)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE budget_accounts
			    SET limit_micros = ?, status = ?, updated_at = ?
			  WHERE account_id = ?`,
			input.LimitMicros, status, now, existing.AccountID,
		); err != nil {
			return BudgetAccountSnapshot{}, fmt.Errorf("update budget account by principal identity: %w", err)
		}
		snapshot, err := s.getBudgetAccountSnapshotTx(ctx, tx, existing.AccountID)
		if err != nil {
			return BudgetAccountSnapshot{}, err
		}
		if err := tx.Commit(); err != nil {
			return BudgetAccountSnapshot{}, fmt.Errorf("commit budget account identity reuse: %w", err)
		}
		return snapshot, nil
	}
	if !errors.Is(err, ErrBudgetAccountNotFound) {
		return BudgetAccountSnapshot{}, err
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO budget_accounts(
			account_id, principal_type, principal_id, workspace_id, currency,
			limit_micros, reserved_micros, spent_micros, refunded_micros,
			status, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?)`,
		accountID, principalType, principalID, workspaceID, currency, input.LimitMicros, status, now, now,
	); err != nil {
		return BudgetAccountSnapshot{}, fmt.Errorf("insert budget account: %w", err)
	}

	snapshot, err := s.getBudgetAccountSnapshotTx(ctx, tx, accountID)
	if err != nil {
		return BudgetAccountSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return BudgetAccountSnapshot{}, fmt.Errorf("commit budget account create: %w", err)
	}
	return snapshot, nil
}

func (s *Store) getBudgetAccountSnapshotByPrincipalTx(ctx context.Context, tx *sql.Tx, principalType, principalID, workspaceID, currency string) (BudgetAccountSnapshot, error) {
	var out BudgetAccountSnapshot
	if err := tx.QueryRowContext(ctx,
		`SELECT account_id, principal_type, principal_id, workspace_id, currency,
		        limit_micros, reserved_micros, spent_micros, refunded_micros,
		        status, created_at, updated_at
		   FROM budget_accounts
		  WHERE principal_type = ?
		    AND principal_id = ?
		    AND workspace_id = ?
		    AND currency = ?`,
		principalType,
		principalID,
		workspaceID,
		currency,
	).Scan(
		&out.AccountID,
		&out.PrincipalType,
		&out.PrincipalID,
		&out.WorkspaceID,
		&out.Currency,
		&out.LimitMicros,
		&out.ReservedMicros,
		&out.SpentMicros,
		&out.RefundedMicros,
		&out.Status,
		&out.CreatedAt,
		&out.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return BudgetAccountSnapshot{}, ErrBudgetAccountNotFound
		}
		return BudgetAccountSnapshot{}, fmt.Errorf("query budget account by principal identity: %w", err)
	}
	out.AvailableMicros = budgetAvailableMicros(out)
	return out, nil
}

func (s *Store) GetBudgetAccount(ctx context.Context, accountID string) (BudgetAccountSnapshot, error) {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return BudgetAccountSnapshot{}, errors.New("account_id is required")
	}
	return s.getBudgetAccountSnapshot(ctx, accountID)
}

func (s *Store) ReserveBudget(ctx context.Context, input BudgetReservationInput) (BudgetAccountSnapshot, error) {
	reservationID := strings.TrimSpace(input.ReservationID)
	if reservationID == "" {
		return BudgetAccountSnapshot{}, errors.New("reservation_id is required")
	}
	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey == "" {
		return BudgetAccountSnapshot{}, errors.New("idempotency_key is required")
	}
	accountID := strings.TrimSpace(input.AccountID)
	if accountID == "" {
		return BudgetAccountSnapshot{}, errors.New("account_id is required")
	}
	if input.AmountMicros <= 0 {
		return BudgetAccountSnapshot{}, errors.New("amount_micros must be positive")
	}
	if err := requireBudgetScope(input.WorkspaceID, input.AgentID, input.TaskID, input.RunID, input.ProviderID, input.Model); err != nil {
		return BudgetAccountSnapshot{}, err
	}
	scope := normalizedBudgetScope(input.WorkspaceID, input.AgentID, input.TaskID, input.RunID, input.ProviderID, input.Model)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return BudgetAccountSnapshot{}, fmt.Errorf("begin budget reserve tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	expectedEntry := BudgetLedgerEntryRecord{
		EntryID:        reservationID,
		IdempotencyKey: idempotencyKey,
		AccountID:      accountID,
		ReservationID:  reservationID,
		EntryType:      BudgetLedgerEntryReservation,
		AmountMicros:   input.AmountMicros,
		WorkspaceID:    scope.workspaceID,
		AgentID:        scope.agentID,
		TaskID:         scope.taskID,
		RunID:          scope.runID,
		ProviderID:     scope.providerID,
		Model:          scope.model,
		Reason:         strings.TrimSpace(input.Reason),
	}
	if snapshot, handled, err := s.handleExistingBudgetEntryTx(ctx, tx, expectedEntry); handled || err != nil {
		return snapshot, err
	}

	account, err := s.getBudgetAccountSnapshotTx(ctx, tx, accountID)
	if err != nil {
		return BudgetAccountSnapshot{}, err
	}
	if account.Status != BudgetAccountStatusActive {
		return BudgetAccountSnapshot{}, errors.New("budget account is not active")
	}
	if account.WorkspaceID != scope.workspaceID {
		return BudgetAccountSnapshot{}, errors.New("budget reservation workspace does not match account")
	}
	if input.AmountMicros > account.AvailableMicros {
		return BudgetAccountSnapshot{}, fmt.Errorf("%w: reserve amount exceeds available budget", ErrBudgetExceeded)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO budget_reservations(
			reservation_id, idempotency_key, account_id, amount_micros, spent_micros, released_micros,
			status, workspace_id, agent_id, task_id, run_id, provider_id, model, reason, created_at, updated_at
		) VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		reservationID, idempotencyKey, accountID, input.AmountMicros, budgetReservationStatusOpen,
		scope.workspaceID, scope.agentID, scope.taskID, scope.runID, scope.providerID, scope.model,
		strings.TrimSpace(input.Reason), now, now,
	); err != nil {
		return BudgetAccountSnapshot{}, fmt.Errorf("insert budget reservation: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE budget_accounts
		    SET reserved_micros = reserved_micros + ?, updated_at = ?
		  WHERE account_id = ?`,
		input.AmountMicros, now, accountID,
	); err != nil {
		return BudgetAccountSnapshot{}, fmt.Errorf("update reserved budget: %w", err)
	}
	if err := s.insertBudgetLedgerEntryTx(ctx, tx, BudgetLedgerEntryRecord{
		EntryID:        expectedEntry.EntryID,
		IdempotencyKey: expectedEntry.IdempotencyKey,
		AccountID:      expectedEntry.AccountID,
		ReservationID:  expectedEntry.ReservationID,
		EntryType:      expectedEntry.EntryType,
		AmountMicros:   expectedEntry.AmountMicros,
		Currency:       account.Currency,
		WorkspaceID:    expectedEntry.WorkspaceID,
		AgentID:        expectedEntry.AgentID,
		TaskID:         expectedEntry.TaskID,
		RunID:          expectedEntry.RunID,
		ProviderID:     expectedEntry.ProviderID,
		Model:          expectedEntry.Model,
		Reason:         strings.TrimSpace(input.Reason),
		CreatedAt:      now,
	}); err != nil {
		return BudgetAccountSnapshot{}, err
	}

	snapshot, err := s.getBudgetAccountSnapshotTx(ctx, tx, accountID)
	if err != nil {
		return BudgetAccountSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return BudgetAccountSnapshot{}, fmt.Errorf("commit budget reserve: %w", err)
	}
	return snapshot, nil
}

func (s *Store) CaptureBudgetSpend(ctx context.Context, input BudgetSpendCaptureInput) (BudgetAccountSnapshot, error) {
	return s.applyBudgetReservationDelta(ctx, budgetReservationDeltaInput{
		entryID:        input.EntryID,
		idempotencyKey: input.IdempotencyKey,
		accountID:      input.AccountID,
		reservationID:  input.ReservationID,
		workspaceID:    input.WorkspaceID,
		agentID:        input.AgentID,
		taskID:         input.TaskID,
		runID:          input.RunID,
		providerID:     input.ProviderID,
		model:          input.Model,
		amountMicros:   input.AmountMicros,
		reason:         input.Reason,
		entryType:      BudgetLedgerEntrySpend,
	})
}

func (s *Store) ReleaseBudgetReservation(ctx context.Context, input BudgetReservationReleaseInput) (BudgetAccountSnapshot, error) {
	return s.applyBudgetReservationDelta(ctx, budgetReservationDeltaInput{
		entryID:        input.EntryID,
		idempotencyKey: input.IdempotencyKey,
		accountID:      input.AccountID,
		reservationID:  input.ReservationID,
		workspaceID:    input.WorkspaceID,
		agentID:        input.AgentID,
		taskID:         input.TaskID,
		runID:          input.RunID,
		providerID:     input.ProviderID,
		model:          input.Model,
		amountMicros:   input.AmountMicros,
		reason:         input.Reason,
		entryType:      BudgetLedgerEntryRelease,
	})
}

func (s *Store) RefundBudgetSpend(ctx context.Context, input BudgetRefundInput) (BudgetAccountSnapshot, error) {
	entryID := strings.TrimSpace(input.EntryID)
	if entryID == "" {
		return BudgetAccountSnapshot{}, errors.New("entry_id is required")
	}
	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey == "" {
		return BudgetAccountSnapshot{}, errors.New("idempotency_key is required")
	}
	accountID := strings.TrimSpace(input.AccountID)
	if accountID == "" {
		return BudgetAccountSnapshot{}, errors.New("account_id is required")
	}
	if input.AmountMicros <= 0 {
		return BudgetAccountSnapshot{}, errors.New("amount_micros must be positive")
	}
	sourceEntryID := strings.TrimSpace(input.SourceEntryID)
	if sourceEntryID == "" {
		return BudgetAccountSnapshot{}, errors.New("source_entry_id is required")
	}
	if err := requireBudgetScope(input.WorkspaceID, input.AgentID, input.TaskID, input.RunID, input.ProviderID, input.Model); err != nil {
		return BudgetAccountSnapshot{}, err
	}
	scope := normalizedBudgetScope(input.WorkspaceID, input.AgentID, input.TaskID, input.RunID, input.ProviderID, input.Model)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return BudgetAccountSnapshot{}, fmt.Errorf("begin budget refund tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	expectedEntry := BudgetLedgerEntryRecord{
		EntryID:        entryID,
		IdempotencyKey: idempotencyKey,
		AccountID:      accountID,
		EntryType:      BudgetLedgerEntryRefund,
		AmountMicros:   input.AmountMicros,
		WorkspaceID:    scope.workspaceID,
		AgentID:        scope.agentID,
		TaskID:         scope.taskID,
		RunID:          scope.runID,
		ProviderID:     scope.providerID,
		Model:          scope.model,
		SourceEntryID:  sourceEntryID,
		Reason:         strings.TrimSpace(input.Reason),
	}
	if snapshot, handled, err := s.handleExistingBudgetEntryTx(ctx, tx, expectedEntry); handled || err != nil {
		return snapshot, err
	}

	account, err := s.getBudgetAccountSnapshotTx(ctx, tx, accountID)
	if err != nil {
		return BudgetAccountSnapshot{}, err
	}
	if account.WorkspaceID != scope.workspaceID {
		return BudgetAccountSnapshot{}, errors.New("budget refund workspace does not match account")
	}
	sourceSpend, err := s.getBudgetLedgerEntryTx(ctx, tx, sourceEntryID)
	if err != nil {
		return BudgetAccountSnapshot{}, err
	}
	if sourceSpend.EntryType != BudgetLedgerEntrySpend {
		return BudgetAccountSnapshot{}, errors.New("source_entry_id must reference a budget spend entry")
	}
	if sourceSpend.AccountID != accountID ||
		sourceSpend.WorkspaceID != scope.workspaceID ||
		sourceSpend.AgentID != scope.agentID ||
		sourceSpend.TaskID != scope.taskID ||
		sourceSpend.RunID != scope.runID ||
		sourceSpend.ProviderID != scope.providerID ||
		sourceSpend.Model != scope.model {
		return BudgetAccountSnapshot{}, errors.New("budget refund source spend scope does not match input scope")
	}
	var alreadyRefunded sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(amount_micros), 0)
		   FROM budget_ledger_entries
		  WHERE entry_type = ?
		    AND source_entry_id = ?`,
		BudgetLedgerEntryRefund, sourceEntryID,
	).Scan(&alreadyRefunded); err != nil {
		return BudgetAccountSnapshot{}, fmt.Errorf("query source spend refunds: %w", err)
	}
	refundable := sourceSpend.AmountMicros - alreadyRefunded.Int64
	if input.AmountMicros > refundable {
		return BudgetAccountSnapshot{}, fmt.Errorf("%w: refund amount exceeds source spend balance", ErrBudgetExceeded)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE budget_accounts
		    SET refunded_micros = refunded_micros + ?, updated_at = ?
		  WHERE account_id = ?`,
		input.AmountMicros, now, accountID,
	); err != nil {
		return BudgetAccountSnapshot{}, fmt.Errorf("update refunded budget: %w", err)
	}
	if err := s.insertBudgetLedgerEntryTx(ctx, tx, BudgetLedgerEntryRecord{
		EntryID:        expectedEntry.EntryID,
		IdempotencyKey: expectedEntry.IdempotencyKey,
		AccountID:      expectedEntry.AccountID,
		EntryType:      expectedEntry.EntryType,
		AmountMicros:   expectedEntry.AmountMicros,
		Currency:       account.Currency,
		WorkspaceID:    expectedEntry.WorkspaceID,
		AgentID:        expectedEntry.AgentID,
		TaskID:         expectedEntry.TaskID,
		RunID:          expectedEntry.RunID,
		ProviderID:     expectedEntry.ProviderID,
		Model:          expectedEntry.Model,
		SourceEntryID:  expectedEntry.SourceEntryID,
		Reason:         strings.TrimSpace(input.Reason),
		CreatedAt:      now,
	}); err != nil {
		return BudgetAccountSnapshot{}, err
	}
	snapshot, err := s.getBudgetAccountSnapshotTx(ctx, tx, accountID)
	if err != nil {
		return BudgetAccountSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return BudgetAccountSnapshot{}, fmt.Errorf("commit budget refund: %w", err)
	}
	return snapshot, nil
}

func (s *Store) ListBudgetLedgerEntries(ctx context.Context, filter BudgetLedgerEntryFilter) ([]BudgetLedgerEntryRecord, error) {
	clauses := []string{"1=1"}
	args := []any{}
	if accountID := strings.TrimSpace(filter.AccountID); accountID != "" {
		clauses = append(clauses, "account_id = ?")
		args = append(args, accountID)
	}
	if reservationID := strings.TrimSpace(filter.ReservationID); reservationID != "" {
		clauses = append(clauses, "reservation_id = ?")
		args = append(args, reservationID)
	}
	if workspaceID := strings.TrimSpace(filter.WorkspaceID); workspaceID != "" {
		clauses = append(clauses, "workspace_id = ?")
		args = append(args, workspaceID)
	}
	if taskID := strings.TrimSpace(filter.TaskID); taskID != "" {
		clauses = append(clauses, "task_id = ?")
		args = append(args, taskID)
	}
	if runID := strings.TrimSpace(filter.RunID); runID != "" {
		clauses = append(clauses, "run_id = ?")
		args = append(args, runID)
	}
	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 500
	}
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx,
		`SELECT entry_id, idempotency_key, account_id, reservation_id, entry_type,
		        amount_micros, currency, workspace_id, agent_id, task_id, run_id,
		        provider_id, model, source_entry_id, reason, created_at
		   FROM budget_ledger_entries
		  WHERE `+strings.Join(clauses, " AND ")+`
		  ORDER BY created_at, entry_id
		  LIMIT ?`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("query budget ledger entries: %w", err)
	}
	defer rows.Close()

	var entries []BudgetLedgerEntryRecord
	for rows.Next() {
		var entry BudgetLedgerEntryRecord
		if err := rows.Scan(
			&entry.EntryID,
			&entry.IdempotencyKey,
			&entry.AccountID,
			&entry.ReservationID,
			&entry.EntryType,
			&entry.AmountMicros,
			&entry.Currency,
			&entry.WorkspaceID,
			&entry.AgentID,
			&entry.TaskID,
			&entry.RunID,
			&entry.ProviderID,
			&entry.Model,
			&entry.SourceEntryID,
			&entry.Reason,
			&entry.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan budget ledger entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate budget ledger entries: %w", err)
	}
	return entries, nil
}

func (s *Store) ListBudgetReservations(ctx context.Context, filter BudgetReservationFilter) ([]BudgetReservationRecord, error) {
	clauses := []string{"1=1"}
	args := []any{}
	if accountID := strings.TrimSpace(filter.AccountID); accountID != "" {
		clauses = append(clauses, "account_id = ?")
		args = append(args, accountID)
	}
	if workspaceID := strings.TrimSpace(filter.WorkspaceID); workspaceID != "" {
		clauses = append(clauses, "workspace_id = ?")
		args = append(args, workspaceID)
	}
	if agentID := strings.TrimSpace(filter.AgentID); agentID != "" {
		clauses = append(clauses, "agent_id = ?")
		args = append(args, agentID)
	}
	if taskID := strings.TrimSpace(filter.TaskID); taskID != "" {
		clauses = append(clauses, "task_id = ?")
		args = append(args, taskID)
	}
	if runID := strings.TrimSpace(filter.RunID); runID != "" {
		clauses = append(clauses, "run_id = ?")
		args = append(args, runID)
	}
	if status := strings.TrimSpace(filter.Status); status != "" {
		clauses = append(clauses, "status = ?")
		args = append(args, strings.ToUpper(status))
	}
	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 500
	}
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx,
		`SELECT reservation_id, idempotency_key, account_id, amount_micros,
		        spent_micros, released_micros, amount_micros - spent_micros - released_micros,
		        status, workspace_id, agent_id, task_id, run_id, provider_id, model, reason,
		        created_at, updated_at
		   FROM budget_reservations
		  WHERE `+strings.Join(clauses, " AND ")+`
		  ORDER BY created_at, reservation_id
		  LIMIT ?`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("query budget reservations: %w", err)
	}
	defer rows.Close()

	var reservations []BudgetReservationRecord
	for rows.Next() {
		var reservation BudgetReservationRecord
		if err := rows.Scan(
			&reservation.ReservationID,
			&reservation.IdempotencyKey,
			&reservation.AccountID,
			&reservation.AmountMicros,
			&reservation.SpentMicros,
			&reservation.ReleasedMicros,
			&reservation.RemainingMicros,
			&reservation.Status,
			&reservation.WorkspaceID,
			&reservation.AgentID,
			&reservation.TaskID,
			&reservation.RunID,
			&reservation.ProviderID,
			&reservation.Model,
			&reservation.Reason,
			&reservation.CreatedAt,
			&reservation.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan budget reservation: %w", err)
		}
		reservations = append(reservations, reservation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate budget reservations: %w", err)
	}
	return reservations, nil
}

func (s *Store) GetBudgetLedgerHealthSnapshot(ctx context.Context, staleAfter time.Duration) (BudgetLedgerHealthSnapshot, error) {
	referenceAt := time.Now().UTC()
	if staleAfter <= 0 {
		staleAfter = time.Hour
	}
	staleBefore := referenceAt.Add(-staleAfter).Format(time.RFC3339Nano)
	snapshot := BudgetLedgerHealthSnapshot{
		Contract:    BudgetLedgerHealthContract,
		ReferenceAt: referenceAt.Format(time.RFC3339Nano),
		Status:      "ok",
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM budget_accounts`).Scan(&snapshot.AccountCount); err != nil {
		return BudgetLedgerHealthSnapshot{}, fmt.Errorf("count budget accounts: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM budget_reservations WHERE status = ?`, budgetReservationStatusOpen).Scan(&snapshot.OpenReservationCount); err != nil {
		return BudgetLedgerHealthSnapshot{}, fmt.Errorf("count open budget reservations: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM budget_reservations WHERE status = ? AND created_at < ?`, budgetReservationStatusOpen, staleBefore).Scan(&snapshot.StaleOpenReservationCount); err != nil {
		return BudgetLedgerHealthSnapshot{}, fmt.Errorf("count stale budget reservations: %w", err)
	}
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(1)
		   FROM budget_accounts
		  WHERE spent_micros - refunded_micros + reserved_micros > limit_micros`,
	).Scan(&snapshot.OverspentAccountCount); err != nil {
		return BudgetLedgerHealthSnapshot{}, fmt.Errorf("count overspent budget accounts: %w", err)
	}
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(1)
		   FROM budget_accounts
		  WHERE status = ?
		    AND limit_micros > 0
		    AND limit_micros - spent_micros + refunded_micros - reserved_micros <= 0`,
		BudgetAccountStatusActive,
	).Scan(&snapshot.ExhaustedAccountCount); err != nil {
		return BudgetLedgerHealthSnapshot{}, fmt.Errorf("count exhausted budget accounts: %w", err)
	}
	exhaustedRows, err := s.db.QueryContext(ctx,
		`SELECT account_id, principal_type, principal_id, workspace_id,
		        limit_micros, reserved_micros, spent_micros, refunded_micros,
		        limit_micros - spent_micros + refunded_micros - reserved_micros AS available_micros,
		        status
		   FROM budget_accounts
		  WHERE status = ?
		    AND limit_micros > 0
		    AND limit_micros - spent_micros + refunded_micros - reserved_micros <= 0
		  ORDER BY available_micros ASC, updated_at ASC, account_id ASC
		  LIMIT 5`,
		BudgetAccountStatusActive,
	)
	if err != nil {
		return BudgetLedgerHealthSnapshot{}, fmt.Errorf("query exhausted budget account examples: %w", err)
	}
	for exhaustedRows.Next() {
		var example BudgetLedgerAccountHealthExample
		if err := exhaustedRows.Scan(
			&example.AccountID,
			&example.PrincipalType,
			&example.PrincipalID,
			&example.WorkspaceID,
			&example.LimitMicros,
			&example.ReservedMicros,
			&example.SpentMicros,
			&example.RefundedMicros,
			&example.AvailableMicros,
			&example.Status,
		); err != nil {
			_ = exhaustedRows.Close()
			return BudgetLedgerHealthSnapshot{}, fmt.Errorf("scan exhausted budget account example: %w", err)
		}
		snapshot.ExhaustedAccountExamples = append(snapshot.ExhaustedAccountExamples, example)
	}
	if err := exhaustedRows.Close(); err != nil {
		return BudgetLedgerHealthSnapshot{}, fmt.Errorf("close exhausted budget account examples: %w", err)
	}
	if err := exhaustedRows.Err(); err != nil {
		return BudgetLedgerHealthSnapshot{}, fmt.Errorf("iterate exhausted budget account examples: %w", err)
	}
	var lastLedgerEntryAt sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1), MAX(created_at) FROM budget_ledger_entries`).Scan(&snapshot.LedgerEntryCount, &lastLedgerEntryAt); err != nil {
		return BudgetLedgerHealthSnapshot{}, fmt.Errorf("summarize budget ledger entries: %w", err)
	}
	if lastLedgerEntryAt.Valid {
		snapshot.LastLedgerEntryAt = strings.TrimSpace(lastLedgerEntryAt.String)
	}
	if snapshot.StaleOpenReservationCount > 0 {
		snapshot.Reasons = append(snapshot.Reasons, "stale_open_reservations")
	}
	if snapshot.OverspentAccountCount > 0 {
		snapshot.Reasons = append(snapshot.Reasons, "overspent_accounts")
	}
	if snapshot.ExhaustedAccountCount > 0 {
		snapshot.Reasons = append(snapshot.Reasons, "exhausted_accounts")
	}
	if snapshot.StaleOpenReservationCount > 0 || snapshot.OverspentAccountCount > 0 {
		snapshot.Status = "degraded"
		snapshot.Message = fmt.Sprintf(
			"budget ledger degraded: stale_open_reservations=%d overspent_accounts=%d exhausted_accounts=%d",
			snapshot.StaleOpenReservationCount,
			snapshot.OverspentAccountCount,
			snapshot.ExhaustedAccountCount,
		)
	} else if snapshot.ExhaustedAccountCount > 0 {
		snapshot.Status = "exhausted"
		snapshot.Message = fmt.Sprintf("budget exhausted for %d account(s)", snapshot.ExhaustedAccountCount)
	} else if snapshot.AccountCount == 0 {
		snapshot.Message = "budget ledger initialized; no budget accounts configured"
	} else {
		snapshot.Message = "budget ledger healthy"
	}
	return snapshot, nil
}

type budgetReservationDeltaInput struct {
	entryID        string
	idempotencyKey string
	accountID      string
	reservationID  string
	workspaceID    string
	agentID        string
	taskID         string
	runID          string
	providerID     string
	model          string
	amountMicros   int64
	reason         string
	entryType      string
}

func (s *Store) applyBudgetReservationDelta(ctx context.Context, input budgetReservationDeltaInput) (BudgetAccountSnapshot, error) {
	entryID := strings.TrimSpace(input.entryID)
	if entryID == "" {
		return BudgetAccountSnapshot{}, errors.New("entry_id is required")
	}
	idempotencyKey := strings.TrimSpace(input.idempotencyKey)
	if idempotencyKey == "" {
		return BudgetAccountSnapshot{}, errors.New("idempotency_key is required")
	}
	accountID := strings.TrimSpace(input.accountID)
	if accountID == "" {
		return BudgetAccountSnapshot{}, errors.New("account_id is required")
	}
	reservationID := strings.TrimSpace(input.reservationID)
	if reservationID == "" {
		return BudgetAccountSnapshot{}, errors.New("reservation_id is required")
	}
	if input.amountMicros <= 0 {
		return BudgetAccountSnapshot{}, errors.New("amount_micros must be positive")
	}
	if err := requireBudgetScope(input.workspaceID, input.agentID, input.taskID, input.runID, input.providerID, input.model); err != nil {
		return BudgetAccountSnapshot{}, err
	}
	scope := normalizedBudgetScope(input.workspaceID, input.agentID, input.taskID, input.runID, input.providerID, input.model)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return BudgetAccountSnapshot{}, fmt.Errorf("begin budget %s tx: %w", strings.ToLower(input.entryType), err)
	}
	defer func() { _ = tx.Rollback() }()

	expectedEntry := BudgetLedgerEntryRecord{
		EntryID:        entryID,
		IdempotencyKey: idempotencyKey,
		AccountID:      accountID,
		ReservationID:  reservationID,
		EntryType:      input.entryType,
		AmountMicros:   input.amountMicros,
		WorkspaceID:    scope.workspaceID,
		AgentID:        scope.agentID,
		TaskID:         scope.taskID,
		RunID:          scope.runID,
		ProviderID:     scope.providerID,
		Model:          scope.model,
		Reason:         strings.TrimSpace(input.reason),
	}
	if snapshot, handled, err := s.handleExistingBudgetEntryTx(ctx, tx, expectedEntry); handled || err != nil {
		return snapshot, err
	}

	account, err := s.getBudgetAccountSnapshotTx(ctx, tx, accountID)
	if err != nil {
		return BudgetAccountSnapshot{}, err
	}
	if account.WorkspaceID != scope.workspaceID {
		return BudgetAccountSnapshot{}, errors.New("budget reservation delta workspace does not match account")
	}
	reservation, err := s.getBudgetReservationTx(ctx, tx, reservationID)
	if err != nil {
		return BudgetAccountSnapshot{}, err
	}
	if reservation.AccountID != accountID {
		return BudgetAccountSnapshot{}, errors.New("budget reservation account does not match input account")
	}
	if err := requireBudgetReservationScope(reservation, scope); err != nil {
		return BudgetAccountSnapshot{}, err
	}
	unspent := reservation.AmountMicros - reservation.SpentMicros - reservation.ReleasedMicros
	if input.amountMicros > unspent {
		return BudgetAccountSnapshot{}, fmt.Errorf("%w: reservation balance exceeded", ErrBudgetExceeded)
	}

	switch input.entryType {
	case BudgetLedgerEntrySpend:
		if _, err := tx.ExecContext(ctx,
			`UPDATE budget_reservations
			    SET spent_micros = spent_micros + ?,
			        status = CASE WHEN spent_micros + released_micros + ? >= amount_micros THEN ? ELSE status END,
			        updated_at = ?
			  WHERE reservation_id = ?`,
			input.amountMicros, input.amountMicros, budgetReservationStatusClosed, now, reservationID,
		); err != nil {
			return BudgetAccountSnapshot{}, fmt.Errorf("capture budget reservation spend: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE budget_accounts
			    SET reserved_micros = reserved_micros - ?,
			        spent_micros = spent_micros + ?,
			        updated_at = ?
			  WHERE account_id = ?`,
			input.amountMicros, input.amountMicros, now, accountID,
		); err != nil {
			return BudgetAccountSnapshot{}, fmt.Errorf("update captured budget spend: %w", err)
		}
	case BudgetLedgerEntryRelease:
		if _, err := tx.ExecContext(ctx,
			`UPDATE budget_reservations
			    SET released_micros = released_micros + ?,
			        status = CASE WHEN spent_micros + released_micros + ? >= amount_micros THEN ? ELSE status END,
			        updated_at = ?
			  WHERE reservation_id = ?`,
			input.amountMicros, input.amountMicros, budgetReservationStatusClosed, now, reservationID,
		); err != nil {
			return BudgetAccountSnapshot{}, fmt.Errorf("release budget reservation: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE budget_accounts
			    SET reserved_micros = reserved_micros - ?,
			        updated_at = ?
			  WHERE account_id = ?`,
			input.amountMicros, now, accountID,
		); err != nil {
			return BudgetAccountSnapshot{}, fmt.Errorf("update released budget reservation: %w", err)
		}
	default:
		return BudgetAccountSnapshot{}, errors.New("unsupported budget reservation delta type")
	}

	if err := s.insertBudgetLedgerEntryTx(ctx, tx, BudgetLedgerEntryRecord{
		EntryID:        expectedEntry.EntryID,
		IdempotencyKey: expectedEntry.IdempotencyKey,
		AccountID:      expectedEntry.AccountID,
		ReservationID:  expectedEntry.ReservationID,
		EntryType:      expectedEntry.EntryType,
		AmountMicros:   expectedEntry.AmountMicros,
		Currency:       account.Currency,
		WorkspaceID:    expectedEntry.WorkspaceID,
		AgentID:        expectedEntry.AgentID,
		TaskID:         expectedEntry.TaskID,
		RunID:          expectedEntry.RunID,
		ProviderID:     expectedEntry.ProviderID,
		Model:          expectedEntry.Model,
		Reason:         strings.TrimSpace(input.reason),
		CreatedAt:      now,
	}); err != nil {
		return BudgetAccountSnapshot{}, err
	}

	snapshot, err := s.getBudgetAccountSnapshotTx(ctx, tx, accountID)
	if err != nil {
		return BudgetAccountSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return BudgetAccountSnapshot{}, fmt.Errorf("commit budget %s: %w", strings.ToLower(input.entryType), err)
	}
	return snapshot, nil
}

type budgetScope struct {
	workspaceID string
	agentID     string
	taskID      string
	runID       string
	providerID  string
	model       string
}

func normalizedBudgetScope(workspaceID, agentID, taskID, runID, providerID, modelName string) budgetScope {
	return budgetScope{
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
		taskID:      strings.TrimSpace(taskID),
		runID:       strings.TrimSpace(runID),
		providerID:  strings.TrimSpace(providerID),
		model:       strings.TrimSpace(modelName),
	}
}

func requireBudgetScope(workspaceID, agentID, taskID, runID, providerID, modelName string) error {
	scope := normalizedBudgetScope(workspaceID, agentID, taskID, runID, providerID, modelName)
	if scope.workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	if scope.agentID == "" {
		return errors.New("agent_id is required")
	}
	if scope.taskID == "" {
		return errors.New("task_id is required")
	}
	if scope.runID == "" {
		return errors.New("run_id is required")
	}
	if scope.providerID == "" {
		return errors.New("provider_id is required")
	}
	if scope.model == "" {
		return errors.New("model is required")
	}
	return nil
}

func requireBudgetReservationScope(reservation budgetReservationRow, scope budgetScope) error {
	if reservation.WorkspaceID != scope.workspaceID ||
		reservation.AgentID != scope.agentID ||
		reservation.TaskID != scope.taskID ||
		reservation.RunID != scope.runID ||
		reservation.ProviderID != scope.providerID ||
		reservation.Model != scope.model {
		return errors.New("budget reservation scope does not match input scope")
	}
	return nil
}

func normalizeBudgetCurrency(currency string) string {
	currency = strings.ToUpper(strings.TrimSpace(currency))
	if currency == "" {
		return BudgetCurrencyUSD
	}
	return currency
}

func (s *Store) getBudgetAccountSnapshot(ctx context.Context, accountID string) (BudgetAccountSnapshot, error) {
	var out BudgetAccountSnapshot
	if err := s.db.QueryRowContext(ctx,
		`SELECT account_id, principal_type, principal_id, workspace_id, currency,
		        limit_micros, reserved_micros, spent_micros, refunded_micros,
		        status, created_at, updated_at
		   FROM budget_accounts
		  WHERE account_id = ?`,
		accountID,
	).Scan(
		&out.AccountID,
		&out.PrincipalType,
		&out.PrincipalID,
		&out.WorkspaceID,
		&out.Currency,
		&out.LimitMicros,
		&out.ReservedMicros,
		&out.SpentMicros,
		&out.RefundedMicros,
		&out.Status,
		&out.CreatedAt,
		&out.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return BudgetAccountSnapshot{}, ErrBudgetAccountNotFound
		}
		return BudgetAccountSnapshot{}, fmt.Errorf("query budget account: %w", err)
	}
	out.AvailableMicros = budgetAvailableMicros(out)
	return out, nil
}

func (s *Store) getBudgetAccountSnapshotTx(ctx context.Context, tx *sql.Tx, accountID string) (BudgetAccountSnapshot, error) {
	var out BudgetAccountSnapshot
	if err := tx.QueryRowContext(ctx,
		`SELECT account_id, principal_type, principal_id, workspace_id, currency,
		        limit_micros, reserved_micros, spent_micros, refunded_micros,
		        status, created_at, updated_at
		   FROM budget_accounts
		  WHERE account_id = ?`,
		accountID,
	).Scan(
		&out.AccountID,
		&out.PrincipalType,
		&out.PrincipalID,
		&out.WorkspaceID,
		&out.Currency,
		&out.LimitMicros,
		&out.ReservedMicros,
		&out.SpentMicros,
		&out.RefundedMicros,
		&out.Status,
		&out.CreatedAt,
		&out.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return BudgetAccountSnapshot{}, ErrBudgetAccountNotFound
		}
		return BudgetAccountSnapshot{}, fmt.Errorf("query budget account: %w", err)
	}
	out.AvailableMicros = budgetAvailableMicros(out)
	return out, nil
}

func budgetAvailableMicros(snapshot BudgetAccountSnapshot) int64 {
	return snapshot.LimitMicros - snapshot.SpentMicros + snapshot.RefundedMicros - snapshot.ReservedMicros
}

func (s *Store) getBudgetReservationTx(ctx context.Context, tx *sql.Tx, reservationID string) (budgetReservationRow, error) {
	var out budgetReservationRow
	if err := tx.QueryRowContext(ctx,
		`SELECT reservation_id, idempotency_key, account_id, amount_micros, spent_micros,
		        released_micros, status, workspace_id, agent_id, task_id, run_id, provider_id, model
		   FROM budget_reservations
		  WHERE reservation_id = ?`,
		reservationID,
	).Scan(
		&out.ReservationID,
		&out.IdempotencyKey,
		&out.AccountID,
		&out.AmountMicros,
		&out.SpentMicros,
		&out.ReleasedMicros,
		&out.Status,
		&out.WorkspaceID,
		&out.AgentID,
		&out.TaskID,
		&out.RunID,
		&out.ProviderID,
		&out.Model,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return budgetReservationRow{}, ErrBudgetReservationNotFound
		}
		return budgetReservationRow{}, fmt.Errorf("query budget reservation: %w", err)
	}
	return out, nil
}

func (s *Store) getBudgetLedgerEntryTx(ctx context.Context, tx *sql.Tx, entryID string) (BudgetLedgerEntryRecord, error) {
	var out BudgetLedgerEntryRecord
	if err := tx.QueryRowContext(ctx,
		`SELECT entry_id, idempotency_key, account_id, reservation_id, entry_type,
		        amount_micros, currency, workspace_id, agent_id, task_id, run_id,
		        provider_id, model, source_entry_id, reason, created_at
		   FROM budget_ledger_entries
		  WHERE entry_id = ?`,
		strings.TrimSpace(entryID),
	).Scan(
		&out.EntryID,
		&out.IdempotencyKey,
		&out.AccountID,
		&out.ReservationID,
		&out.EntryType,
		&out.AmountMicros,
		&out.Currency,
		&out.WorkspaceID,
		&out.AgentID,
		&out.TaskID,
		&out.RunID,
		&out.ProviderID,
		&out.Model,
		&out.SourceEntryID,
		&out.Reason,
		&out.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return BudgetLedgerEntryRecord{}, errors.New("budget ledger entry not found")
		}
		return BudgetLedgerEntryRecord{}, fmt.Errorf("query budget ledger entry: %w", err)
	}
	return out, nil
}

func (s *Store) handleExistingBudgetEntryTx(ctx context.Context, tx *sql.Tx, expected BudgetLedgerEntryRecord) (BudgetAccountSnapshot, bool, error) {
	var existing BudgetLedgerEntryRecord
	if err := tx.QueryRowContext(ctx,
		`SELECT entry_id, idempotency_key, account_id, reservation_id, entry_type,
		        amount_micros, currency, workspace_id, agent_id, task_id, run_id,
		        provider_id, model, source_entry_id, reason, created_at
		   FROM budget_ledger_entries
		  WHERE idempotency_key = ?`,
		strings.TrimSpace(expected.IdempotencyKey),
	).Scan(
		&existing.EntryID,
		&existing.IdempotencyKey,
		&existing.AccountID,
		&existing.ReservationID,
		&existing.EntryType,
		&existing.AmountMicros,
		&existing.Currency,
		&existing.WorkspaceID,
		&existing.AgentID,
		&existing.TaskID,
		&existing.RunID,
		&existing.ProviderID,
		&existing.Model,
		&existing.SourceEntryID,
		&existing.Reason,
		&existing.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return BudgetAccountSnapshot{}, false, nil
		}
		return BudgetAccountSnapshot{}, true, fmt.Errorf("query budget ledger idempotency key: %w", err)
	}
	if !sameBudgetLedgerMutation(existing, expected) {
		return BudgetAccountSnapshot{}, true, errors.New("budget idempotency key replay does not match original mutation")
	}
	snapshot, err := s.getBudgetAccountSnapshotTx(ctx, tx, existing.AccountID)
	return snapshot, true, err
}

func sameBudgetLedgerMutation(existing, expected BudgetLedgerEntryRecord) bool {
	return strings.TrimSpace(existing.EntryID) == strings.TrimSpace(expected.EntryID) &&
		strings.TrimSpace(existing.AccountID) == strings.TrimSpace(expected.AccountID) &&
		strings.TrimSpace(existing.ReservationID) == strings.TrimSpace(expected.ReservationID) &&
		strings.TrimSpace(existing.EntryType) == strings.TrimSpace(expected.EntryType) &&
		existing.AmountMicros == expected.AmountMicros &&
		strings.TrimSpace(existing.WorkspaceID) == strings.TrimSpace(expected.WorkspaceID) &&
		strings.TrimSpace(existing.AgentID) == strings.TrimSpace(expected.AgentID) &&
		strings.TrimSpace(existing.TaskID) == strings.TrimSpace(expected.TaskID) &&
		strings.TrimSpace(existing.RunID) == strings.TrimSpace(expected.RunID) &&
		strings.TrimSpace(existing.ProviderID) == strings.TrimSpace(expected.ProviderID) &&
		strings.TrimSpace(existing.Model) == strings.TrimSpace(expected.Model) &&
		strings.TrimSpace(existing.SourceEntryID) == strings.TrimSpace(expected.SourceEntryID) &&
		strings.TrimSpace(existing.Reason) == strings.TrimSpace(expected.Reason)
}

func (s *Store) insertBudgetLedgerEntryTx(ctx context.Context, tx *sql.Tx, entry BudgetLedgerEntryRecord) error {
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO budget_ledger_entries(
			entry_id, idempotency_key, account_id, reservation_id, entry_type, amount_micros,
			currency, workspace_id, agent_id, task_id, run_id, provider_id, model,
			source_entry_id, reason, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		strings.TrimSpace(entry.EntryID),
		strings.TrimSpace(entry.IdempotencyKey),
		strings.TrimSpace(entry.AccountID),
		strings.TrimSpace(entry.ReservationID),
		strings.TrimSpace(entry.EntryType),
		entry.AmountMicros,
		normalizeBudgetCurrency(entry.Currency),
		strings.TrimSpace(entry.WorkspaceID),
		strings.TrimSpace(entry.AgentID),
		strings.TrimSpace(entry.TaskID),
		strings.TrimSpace(entry.RunID),
		strings.TrimSpace(entry.ProviderID),
		strings.TrimSpace(entry.Model),
		strings.TrimSpace(entry.SourceEntryID),
		strings.TrimSpace(entry.Reason),
		strings.TrimSpace(entry.CreatedAt),
	); err != nil {
		return fmt.Errorf("insert budget ledger entry: %w", err)
	}
	return nil
}
