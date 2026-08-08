package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestBudgetLedgerReserveSpendRefundSurvivesReopen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "rhizome-budget.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	if _, err := store.EnsureBudgetAccount(ctx, sqlite.BudgetAccountInput{
		AccountID:     "budget-account-reopen",
		PrincipalType: "human",
		PrincipalID:   "developer",
		WorkspaceID:   "ws-budget",
		LimitMicros:   1000,
	}); err != nil {
		t.Fatalf("ensure budget account: %v", err)
	}
	snapshot, err := store.ReserveBudget(ctx, budgetReservationInput("reservation-reopen", "idem-reserve-reopen", 600))
	if err != nil {
		t.Fatalf("reserve budget: %v", err)
	}
	assertBudgetSnapshot(t, snapshot, 1000, 600, 0, 0, 400)

	snapshot, err = store.CaptureBudgetSpend(ctx, sqlite.BudgetSpendCaptureInput{
		EntryID:        "spend-reopen",
		IdempotencyKey: "idem-spend-reopen",
		AccountID:      "budget-account-reopen",
		ReservationID:  "reservation-reopen",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		AmountMicros:   400,
		Reason:         "captured model spend",
	})
	if err != nil {
		t.Fatalf("capture budget spend: %v", err)
	}
	assertBudgetSnapshot(t, snapshot, 1000, 200, 400, 0, 400)

	snapshot, err = store.RefundBudgetSpend(ctx, sqlite.BudgetRefundInput{
		EntryID:        "refund-reopen",
		IdempotencyKey: "idem-refund-reopen",
		AccountID:      "budget-account-reopen",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		SourceEntryID:  "spend-reopen",
		AmountMicros:   100,
		Reason:         "provider refund",
	})
	if err != nil {
		t.Fatalf("refund budget spend: %v", err)
	}
	assertBudgetSnapshot(t, snapshot, 1000, 200, 400, 100, 500)

	if err := store.Close(); err != nil {
		t.Fatalf("close store before reopen: %v", err)
	}

	reopened, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if err := reopened.ApplyMigrations(ctx); err != nil {
		t.Fatalf("reapply migrations after reopen: %v", err)
	}
	readback, err := reopened.GetBudgetAccount(ctx, "budget-account-reopen")
	if err != nil {
		t.Fatalf("read budget account after reopen: %v", err)
	}
	assertBudgetSnapshot(t, readback, 1000, 200, 400, 100, 500)

	entries, err := reopened.ListBudgetLedgerEntries(ctx, sqlite.BudgetLedgerEntryFilter{AccountID: "budget-account-reopen"})
	if err != nil {
		t.Fatalf("list budget ledger entries after reopen: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 durable ledger entries, got %d: %+v", len(entries), entries)
	}
}

func TestBudgetLedgerEnsureAccountReusesExistingPrincipalIdentity(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	first, err := store.EnsureBudgetAccount(ctx, sqlite.BudgetAccountInput{
		AccountID:     "pilot-agent-alpha",
		PrincipalType: "agent",
		PrincipalID:   "alpha",
		WorkspaceID:   "ws-budget-identity",
		LimitMicros:   1000,
	})
	if err != nil {
		t.Fatalf("first ensure budget account: %v", err)
	}
	if first.AccountID != "pilot-agent-alpha" {
		t.Fatalf("first ensure account id = %q, want pilot-agent-alpha", first.AccountID)
	}

	second, err := store.EnsureBudgetAccount(ctx, sqlite.BudgetAccountInput{
		AccountID:     "llm-budget-alpha",
		PrincipalType: "agent",
		PrincipalID:   "alpha",
		WorkspaceID:   "ws-budget-identity",
		LimitMicros:   2000,
	})
	if err != nil {
		t.Fatalf("second ensure budget account by same identity: %v", err)
	}
	if second.AccountID != "pilot-agent-alpha" {
		t.Fatalf("second ensure returned account id = %q, want canonical existing pilot-agent-alpha", second.AccountID)
	}
	assertBudgetSnapshot(t, second, 2000, 0, 0, 0, 2000)

	if _, err := store.GetBudgetAccount(ctx, "llm-budget-alpha"); !errors.Is(err, sqlite.ErrBudgetAccountNotFound) {
		t.Fatalf("alternate account id should not create a duplicate row, got err=%v", err)
	}
}

func TestBudgetLedgerRejectsOverReservationAtomically(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ensureBudgetAccountForTest(t, ctx, store, "budget-account-overreserve", 1000)

	if _, err := store.ReserveBudget(ctx, budgetReservationInputForAccount("budget-account-overreserve", "reservation-large", "idem-reserve-large", 800)); err != nil {
		t.Fatalf("first reserve budget: %v", err)
	}
	err := expectBudgetExceeded(store.ReserveBudget(ctx, budgetReservationInputForAccount("budget-account-overreserve", "reservation-too-large", "idem-reserve-too-large", 300)))
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.GetBudgetAccount(ctx, "budget-account-overreserve")
	if err != nil {
		t.Fatalf("get budget account: %v", err)
	}
	assertBudgetSnapshot(t, snapshot, 1000, 800, 0, 0, 200)

	entries, err := store.ListBudgetLedgerEntries(ctx, sqlite.BudgetLedgerEntryFilter{AccountID: "budget-account-overreserve"})
	if err != nil {
		t.Fatalf("list budget ledger entries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected failed over-reservation to leave one ledger entry, got %d", len(entries))
	}
}

func TestBudgetLedgerIdempotencyDoesNotDoubleApply(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ensureBudgetAccountForTest(t, ctx, store, "budget-account-idem", 1000)

	reserve := budgetReservationInputForAccount("budget-account-idem", "reservation-idem", "idem-reserve-idem", 600)
	first, err := store.ReserveBudget(ctx, reserve)
	if err != nil {
		t.Fatalf("first reserve budget: %v", err)
	}
	second, err := store.ReserveBudget(ctx, reserve)
	if err != nil {
		t.Fatalf("duplicate reserve budget: %v", err)
	}
	assertBudgetSnapshot(t, first, 1000, 600, 0, 0, 400)
	assertBudgetSnapshot(t, second, 1000, 600, 0, 0, 400)

	spend := sqlite.BudgetSpendCaptureInput{
		EntryID:        "spend-idem",
		IdempotencyKey: "idem-spend-idem",
		AccountID:      "budget-account-idem",
		ReservationID:  "reservation-idem",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		AmountMicros:   500,
	}
	if _, err := store.CaptureBudgetSpend(ctx, spend); err != nil {
		t.Fatalf("first capture budget spend: %v", err)
	}
	snapshot, err := store.CaptureBudgetSpend(ctx, spend)
	if err != nil {
		t.Fatalf("duplicate capture budget spend: %v", err)
	}
	assertBudgetSnapshot(t, snapshot, 1000, 100, 500, 0, 400)

	refund := sqlite.BudgetRefundInput{
		EntryID:        "refund-idem",
		IdempotencyKey: "idem-refund-idem",
		AccountID:      "budget-account-idem",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		SourceEntryID:  "spend-idem",
		AmountMicros:   200,
	}
	if _, err := store.RefundBudgetSpend(ctx, refund); err != nil {
		t.Fatalf("first refund budget spend: %v", err)
	}
	snapshot, err = store.RefundBudgetSpend(ctx, refund)
	if err != nil {
		t.Fatalf("duplicate refund budget spend: %v", err)
	}
	assertBudgetSnapshot(t, snapshot, 1000, 100, 500, 200, 600)

	entries, err := store.ListBudgetLedgerEntries(ctx, sqlite.BudgetLedgerEntryFilter{AccountID: "budget-account-idem"})
	if err != nil {
		t.Fatalf("list budget ledger entries: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected one reservation, one spend, and one refund entry after duplicate calls, got %d", len(entries))
	}
}

func TestBudgetLedgerIdempotencyReplayRejectsMismatchedBody(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ensureBudgetAccountForTest(t, ctx, store, "budget-account-idem-mismatch", 1000)

	reserve := budgetReservationInputForAccount("budget-account-idem-mismatch", "reservation-idem-mismatch", "idem-reserve-mismatch", 600)
	if _, err := store.ReserveBudget(ctx, reserve); err != nil {
		t.Fatalf("first reserve budget: %v", err)
	}
	reserve.AmountMicros = 700
	if _, err := store.ReserveBudget(ctx, reserve); err == nil {
		t.Fatal("expected mismatched idempotency replay to fail")
	}

	snapshot, err := store.GetBudgetAccount(ctx, "budget-account-idem-mismatch")
	if err != nil {
		t.Fatalf("get budget account: %v", err)
	}
	assertBudgetSnapshot(t, snapshot, 1000, 600, 0, 0, 400)
	entries, err := store.ListBudgetLedgerEntries(ctx, sqlite.BudgetLedgerEntryFilter{AccountID: "budget-account-idem-mismatch"})
	if err != nil {
		t.Fatalf("list budget ledger entries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected mismatched idempotency replay to leave one ledger entry, got %d", len(entries))
	}
}

func TestBudgetLedgerIdempotencyReplayRejectsReasonMismatchForPersistedEntries(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ensureBudgetAccountForTest(t, ctx, store, "budget-account-idem-reason", 2000)

	reserve := budgetReservationInputForAccount("budget-account-idem-reason", "reservation-reason-a", "idem-reserve-reason", 500)
	reserve.Reason = "reserve reason a"
	if _, err := store.ReserveBudget(ctx, reserve); err != nil {
		t.Fatalf("first reserve budget: %v", err)
	}
	reserve.Reason = "reserve reason b"
	if _, err := store.ReserveBudget(ctx, reserve); err == nil {
		t.Fatal("expected reserve reason mismatch replay to fail")
	}

	spend := sqlite.BudgetSpendCaptureInput{
		EntryID:        "spend-reason-a",
		IdempotencyKey: "idem-spend-reason",
		AccountID:      "budget-account-idem-reason",
		ReservationID:  "reservation-reason-a",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		AmountMicros:   200,
		Reason:         "spend reason a",
	}
	if _, err := store.CaptureBudgetSpend(ctx, spend); err != nil {
		t.Fatalf("first capture budget spend: %v", err)
	}
	spend.Reason = "spend reason b"
	if _, err := store.CaptureBudgetSpend(ctx, spend); err == nil {
		t.Fatal("expected spend reason mismatch replay to fail")
	}

	release := sqlite.BudgetReservationReleaseInput{
		EntryID:        "release-reason-a",
		IdempotencyKey: "idem-release-reason",
		AccountID:      "budget-account-idem-reason",
		ReservationID:  "reservation-reason-a",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		AmountMicros:   100,
		Reason:         "release reason a",
	}
	if _, err := store.ReleaseBudgetReservation(ctx, release); err != nil {
		t.Fatalf("first release budget reservation: %v", err)
	}
	release.Reason = "release reason b"
	if _, err := store.ReleaseBudgetReservation(ctx, release); err == nil {
		t.Fatal("expected release reason mismatch replay to fail")
	}

	refund := sqlite.BudgetRefundInput{
		EntryID:        "refund-reason-a",
		IdempotencyKey: "idem-refund-reason",
		AccountID:      "budget-account-idem-reason",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		SourceEntryID:  "spend-reason-a",
		AmountMicros:   50,
		Reason:         "refund reason a",
	}
	if _, err := store.RefundBudgetSpend(ctx, refund); err != nil {
		t.Fatalf("first refund budget spend: %v", err)
	}
	refund.Reason = "refund reason b"
	if _, err := store.RefundBudgetSpend(ctx, refund); err == nil {
		t.Fatal("expected refund reason mismatch replay to fail")
	}

	entries, err := store.ListBudgetLedgerEntries(ctx, sqlite.BudgetLedgerEntryFilter{AccountID: "budget-account-idem-reason"})
	if err != nil {
		t.Fatalf("list budget ledger entries: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("expected only first reserve/spend/release/refund entries, got %d", len(entries))
	}
}

func TestBudgetLedgerRefundIsBoundToSourceSpend(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ensureBudgetAccountForTest(t, ctx, store, "budget-account-refund-source", 1000)
	if _, err := store.ReserveBudget(ctx, budgetReservationInputForAccount("budget-account-refund-source", "reservation-refund-source", "idem-reserve-refund-source", 600)); err != nil {
		t.Fatalf("reserve budget: %v", err)
	}
	if _, err := store.CaptureBudgetSpend(ctx, sqlite.BudgetSpendCaptureInput{
		EntryID:        "spend-source-a",
		IdempotencyKey: "idem-spend-source-a",
		AccountID:      "budget-account-refund-source",
		ReservationID:  "reservation-refund-source",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		AmountMicros:   300,
	}); err != nil {
		t.Fatalf("capture source spend a: %v", err)
	}
	if _, err := store.CaptureBudgetSpend(ctx, sqlite.BudgetSpendCaptureInput{
		EntryID:        "spend-source-b",
		IdempotencyKey: "idem-spend-source-b",
		AccountID:      "budget-account-refund-source",
		ReservationID:  "reservation-refund-source",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		AmountMicros:   200,
	}); err != nil {
		t.Fatalf("capture source spend b: %v", err)
	}
	if _, err := store.RefundBudgetSpend(ctx, sqlite.BudgetRefundInput{
		EntryID:        "refund-source-a-1",
		IdempotencyKey: "idem-refund-source-a-1",
		AccountID:      "budget-account-refund-source",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		SourceEntryID:  "spend-source-a",
		AmountMicros:   250,
	}); err != nil {
		t.Fatalf("refund source spend a: %v", err)
	}
	if err := expectBudgetExceeded(store.RefundBudgetSpend(ctx, sqlite.BudgetRefundInput{
		EntryID:        "refund-source-a-too-much",
		IdempotencyKey: "idem-refund-source-a-too-much",
		AccountID:      "budget-account-refund-source",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		SourceEntryID:  "spend-source-a",
		AmountMicros:   100,
	})); err != nil {
		t.Fatal(err)
	}

	snapshot, err := store.GetBudgetAccount(ctx, "budget-account-refund-source")
	if err != nil {
		t.Fatalf("get budget account: %v", err)
	}
	assertBudgetSnapshot(t, snapshot, 1000, 100, 500, 250, 650)
}

func TestBudgetLedgerCaptureAndReleaseRequireOpenReservationBalance(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ensureBudgetAccountForTest(t, ctx, store, "budget-account-balance", 1000)
	if _, err := store.ReserveBudget(ctx, budgetReservationInputForAccount("budget-account-balance", "reservation-balance", "idem-reserve-balance", 300)); err != nil {
		t.Fatalf("reserve budget: %v", err)
	}

	overCapture := sqlite.BudgetSpendCaptureInput{
		EntryID:        "spend-over-balance",
		IdempotencyKey: "idem-spend-over-balance",
		AccountID:      "budget-account-balance",
		ReservationID:  "reservation-balance",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		AmountMicros:   400,
	}
	if err := expectBudgetExceeded(store.CaptureBudgetSpend(ctx, overCapture)); err != nil {
		t.Fatal(err)
	}

	if _, err := store.CaptureBudgetSpend(ctx, sqlite.BudgetSpendCaptureInput{
		EntryID:        "spend-balance",
		IdempotencyKey: "idem-spend-balance",
		AccountID:      "budget-account-balance",
		ReservationID:  "reservation-balance",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		AmountMicros:   250,
	}); err != nil {
		t.Fatalf("capture budget spend: %v", err)
	}
	if err := expectBudgetExceeded(store.ReleaseBudgetReservation(ctx, sqlite.BudgetReservationReleaseInput{
		EntryID:        "release-over-balance",
		IdempotencyKey: "idem-release-over-balance",
		AccountID:      "budget-account-balance",
		ReservationID:  "reservation-balance",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		AmountMicros:   60,
	})); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.ReleaseBudgetReservation(ctx, sqlite.BudgetReservationReleaseInput{
		EntryID:        "release-balance",
		IdempotencyKey: "idem-release-balance",
		AccountID:      "budget-account-balance",
		ReservationID:  "reservation-balance",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		AmountMicros:   50,
	})
	if err != nil {
		t.Fatalf("release remaining budget reservation: %v", err)
	}
	assertBudgetSnapshot(t, snapshot, 1000, 0, 250, 0, 750)
}

func TestBudgetLedgerRejectsReservationScopeMismatch(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ensureBudgetAccountForTest(t, ctx, store, "budget-account-scope", 1000)
	if _, err := store.ReserveBudget(ctx, budgetReservationInputForAccount("budget-account-scope", "reservation-scope", "idem-reserve-scope", 300)); err != nil {
		t.Fatalf("reserve budget: %v", err)
	}

	_, err := store.CaptureBudgetSpend(ctx, sqlite.BudgetSpendCaptureInput{
		EntryID:        "spend-scope-mismatch",
		IdempotencyKey: "idem-spend-scope-mismatch",
		AccountID:      "budget-account-scope",
		ReservationID:  "reservation-scope",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-b",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		AmountMicros:   100,
	})
	if err == nil {
		t.Fatal("expected scope mismatch to fail closed")
	}

	snapshot, err := store.GetBudgetAccount(ctx, "budget-account-scope")
	if err != nil {
		t.Fatalf("get budget account: %v", err)
	}
	assertBudgetSnapshot(t, snapshot, 1000, 300, 0, 0, 700)
	entries, err := store.ListBudgetLedgerEntries(ctx, sqlite.BudgetLedgerEntryFilter{AccountID: "budget-account-scope"})
	if err != nil {
		t.Fatalf("list budget ledger entries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected failed scope mismatch to leave only the reservation entry, got %d", len(entries))
	}
}

func TestBudgetLedgerHealthSnapshotReportsOpenAndStaleReservations(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ensureBudgetAccountForTest(t, ctx, store, "budget-account-health", 1000)
	if _, err := store.ReserveBudget(ctx, budgetReservationInputForAccount("budget-account-health", "reservation-health", "idem-reserve-health", 300)); err != nil {
		t.Fatalf("reserve budget: %v", err)
	}

	snapshot, err := store.GetBudgetLedgerHealthSnapshot(ctx, time.Hour)
	if err != nil {
		t.Fatalf("get budget ledger health snapshot: %v", err)
	}
	if snapshot.Status != "ok" || snapshot.AccountCount != 1 || snapshot.OpenReservationCount != 1 || snapshot.StaleOpenReservationCount != 0 || snapshot.LedgerEntryCount != 1 || snapshot.LastLedgerEntryAt == "" {
		t.Fatalf("unexpected fresh budget ledger health snapshot: %+v", snapshot)
	}

	time.Sleep(2 * time.Millisecond)
	snapshot, err = store.GetBudgetLedgerHealthSnapshot(ctx, time.Nanosecond)
	if err != nil {
		t.Fatalf("get stale budget ledger health snapshot: %v", err)
	}
	if snapshot.Status != "degraded" || snapshot.StaleOpenReservationCount != 1 {
		t.Fatalf("expected stale open reservation to degrade health snapshot, got %+v", snapshot)
	}
	if len(snapshot.Reasons) != 1 || snapshot.Reasons[0] != "stale_open_reservations" {
		t.Fatalf("expected stale reservation reason token, got %+v", snapshot.Reasons)
	}
}

func TestListBudgetReservationsFiltersOpenReservationsByAgent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ensureBudgetAccountForTest(t, ctx, store, "budget-account-list", 1000)
	if _, err := store.ReserveBudget(ctx, sqlite.BudgetReservationInput{
		ReservationID:  "reservation-open",
		IdempotencyKey: "idem-reserve-open",
		AccountID:      "budget-account-list",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-budget",
		TaskID:         "task-budget",
		RunID:          "run-budget",
		ProviderID:     "provider-budget",
		Model:          "model-budget",
		AmountMicros:   300,
	}); err != nil {
		t.Fatalf("reserve open budget: %v", err)
	}
	if _, err := store.ReserveBudget(ctx, sqlite.BudgetReservationInput{
		ReservationID:  "reservation-other-agent",
		IdempotencyKey: "idem-reserve-other-agent",
		AccountID:      "budget-account-list",
		WorkspaceID:    "ws-budget",
		AgentID:        "other-agent",
		TaskID:         "task-budget",
		RunID:          "run-budget",
		ProviderID:     "provider-budget",
		Model:          "model-budget",
		AmountMicros:   100,
	}); err != nil {
		t.Fatalf("reserve other agent budget: %v", err)
	}
	if _, err := store.ReleaseBudgetReservation(ctx, sqlite.BudgetReservationReleaseInput{
		EntryID:        "release-open",
		IdempotencyKey: "idem-release-open",
		AccountID:      "budget-account-list",
		ReservationID:  "reservation-open",
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-budget",
		TaskID:         "task-budget",
		RunID:          "run-budget",
		ProviderID:     "provider-budget",
		Model:          "model-budget",
		AmountMicros:   300,
	}); err != nil {
		t.Fatalf("release budget: %v", err)
	}

	open, err := store.ListBudgetReservations(ctx, sqlite.BudgetReservationFilter{
		AccountID:   "budget-account-list",
		WorkspaceID: "ws-budget",
		AgentID:     "agent-budget",
		Status:      "OPEN",
	})
	if err != nil {
		t.Fatalf("list open reservations: %v", err)
	}
	if len(open) != 0 {
		t.Fatalf("expected no open reservations for released agent budget, got %+v", open)
	}

	closed, err := store.ListBudgetReservations(ctx, sqlite.BudgetReservationFilter{
		AccountID:   "budget-account-list",
		WorkspaceID: "ws-budget",
		AgentID:     "agent-budget",
		Status:      "CLOSED",
	})
	if err != nil {
		t.Fatalf("list closed reservations: %v", err)
	}
	if len(closed) != 1 || closed[0].ReservationID != "reservation-open" || closed[0].RemainingMicros != 0 {
		t.Fatalf("unexpected closed reservation records: %+v", closed)
	}
}

func TestBudgetLedgerHealthSnapshotReportsExhaustedAccounts(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	ensureBudgetAccountForTest(t, ctx, store, "budget-account-exhausted", 500)
	if _, err := store.ReserveBudget(ctx, budgetReservationInputForAccount("budget-account-exhausted", "reservation-exhausted", "idem-reserve-exhausted", 500)); err != nil {
		t.Fatalf("reserve budget: %v", err)
	}

	snapshot, err := store.GetBudgetLedgerHealthSnapshot(ctx, time.Hour)
	if err != nil {
		t.Fatalf("get budget ledger health snapshot: %v", err)
	}
	if snapshot.Status != "exhausted" || snapshot.ExhaustedAccountCount != 1 {
		t.Fatalf("expected exhausted budget health snapshot, got %+v", snapshot)
	}
	if snapshot.Contract != sqlite.BudgetLedgerHealthContract {
		t.Fatalf("expected budget health contract %q, got %+v", sqlite.BudgetLedgerHealthContract, snapshot)
	}
	if len(snapshot.Reasons) != 1 || snapshot.Reasons[0] != "exhausted_accounts" {
		t.Fatalf("expected exhausted account reason token, got %+v", snapshot.Reasons)
	}
	if len(snapshot.ExhaustedAccountExamples) != 1 {
		t.Fatalf("expected one exhausted account example, got %+v", snapshot.ExhaustedAccountExamples)
	}
	example := snapshot.ExhaustedAccountExamples[0]
	if example.AccountID != "budget-account-exhausted" || example.AvailableMicros != 0 || example.ReservedMicros != 500 {
		t.Fatalf("unexpected exhausted account example: %+v", example)
	}
}

func ensureBudgetAccountForTest(t *testing.T, ctx context.Context, store *sqlite.Store, accountID string, limitMicros int64) {
	t.Helper()
	if _, err := store.EnsureBudgetAccount(ctx, sqlite.BudgetAccountInput{
		AccountID:     accountID,
		PrincipalType: "human",
		PrincipalID:   "developer",
		WorkspaceID:   "ws-budget",
		LimitMicros:   limitMicros,
	}); err != nil {
		t.Fatalf("ensure budget account: %v", err)
	}
}

func budgetReservationInput(reservationID, idempotencyKey string, amountMicros int64) sqlite.BudgetReservationInput {
	return budgetReservationInputForAccount("budget-account-reopen", reservationID, idempotencyKey, amountMicros)
}

func budgetReservationInputForAccount(accountID, reservationID, idempotencyKey string, amountMicros int64) sqlite.BudgetReservationInput {
	return sqlite.BudgetReservationInput{
		ReservationID:  reservationID,
		IdempotencyKey: idempotencyKey,
		AccountID:      accountID,
		WorkspaceID:    "ws-budget",
		AgentID:        "agent-a",
		TaskID:         "task-a",
		RunID:          "run-a",
		ProviderID:     "openai",
		Model:          "gpt-test",
		AmountMicros:   amountMicros,
		Reason:         "reserve provider spend",
	}
}

func assertBudgetSnapshot(t *testing.T, got sqlite.BudgetAccountSnapshot, limit, reserved, spent, refunded, available int64) {
	t.Helper()
	if got.LimitMicros != limit ||
		got.ReservedMicros != reserved ||
		got.SpentMicros != spent ||
		got.RefundedMicros != refunded ||
		got.AvailableMicros != available {
		t.Fatalf("unexpected budget snapshot: got=%+v want limit=%d reserved=%d spent=%d refunded=%d available=%d", got, limit, reserved, spent, refunded, available)
	}
}

func expectBudgetExceeded(_ sqlite.BudgetAccountSnapshot, err error) error {
	if !errors.Is(err, sqlite.ErrBudgetExceeded) {
		return err
	}
	return nil
}
