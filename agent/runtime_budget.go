package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

const (
	defaultRuntimeBudgetReserveMicros                   int64 = 1000
	defaultRuntimeBudgetMicrosPerToken                  int64 = 1
	runtimeBudgetCleanupTimeout                               = 15 * time.Second
	runtimeBudgetCleanupMaxAttempts                           = 3
	runtimeBudgetCleanupRetryBaseDelay                        = 50 * time.Millisecond
	runtimeBudgetCaptureFailedAfterProviderSuccessState       = "capture_failed_after_provider_success"
)

var errRuntimeBudgetExhausted = errors.New("runtime budget exhausted")

type runtimeBudgetedLLM struct {
	inner   ChatLLM
	runtime *Runtime
}

type runtimeLLMBudgetReservation struct {
	enabled       bool
	accountID     string
	reservationID string
	baseEntryID   string
	taskID        string
	runID         string
	providerID    string
	model         string
	amountMicros  int64
	tokenMicros   int64
}

func wrapRuntimeBudgetedLLM(llm ChatLLM, runtime *Runtime) ChatLLM {
	if llm == nil || runtime == nil {
		return llm
	}
	return &runtimeBudgetedLLM{inner: llm, runtime: runtime}
}

func (l *runtimeBudgetedLLM) Chat(ctx context.Context, messages []Message, tools []ToolDef) (*LLMResponse, error) {
	if l == nil || l.inner == nil {
		return nil, fmt.Errorf("llm backend unavailable")
	}
	if err := l.runtime.checkSoakAbort(ctx); err != nil {
		return nil, err
	}
	soakProviderCallSlot, err := l.runtime.acquireSoakProviderCallSlot(ctx)
	if err != nil {
		return nil, err
	}

	reservation, err := l.runtime.reserveLLMCallBudget(ctx)
	if err != nil {
		l.runtime.releaseSoakProviderCallSlot(soakProviderCallSlot)
		return nil, err
	}
	if err := l.runtime.checkSoakAbort(ctx); err != nil {
		l.runtime.releaseSoakProviderCallSlot(soakProviderCallSlot)
		cleanupCtx, cancel := runtimeBudgetCleanupContext()
		defer cancel()
		l.runtime.releaseLLMCallBudgetBestEffort(cleanupCtx, reservation, reservation.amountMicros, soakGuardBudgetReleaseReason(err))
		return nil, err
	}

	callCtx := ctx
	cancelProviderCall := func() {}
	if timeout := l.runtime.cfg.ProviderCallTimeout; timeout > 0 {
		callCtx, cancelProviderCall = context.WithTimeout(ctx, timeout)
	}
	response, callErr := l.inner.Chat(callCtx, messages, tools)
	cancelProviderCall()
	if callErr != nil {
		if timeout := l.runtime.cfg.ProviderCallTimeout; timeout > 0 && errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			callErr = fmt.Errorf("llm provider call timed out after %s: %w", timeout, callErr)
		}
		cleanupCtx, cancel := runtimeBudgetCleanupContext()
		defer cancel()
		if response != nil && reportedLLMTokenUsage(response.Usage) > 0 {
			l.runtime.recordLLMResponseTokenUsage(response)
			if spendErr := l.runtime.captureLLMCallSpend(cleanupCtx, reservation, response); spendErr != nil {
				return nil, fmt.Errorf("%w; partial usage budget capture failed: %v", callErr, spendErr)
			}
			return nil, callErr
		}
		l.runtime.releaseLLMCallBudgetBestEffort(cleanupCtx, reservation, reservation.amountMicros, "llm_provider_error")
		return nil, callErr
	}

	l.runtime.recordLLMResponseTokenUsage(response)

	cleanupCtx, cancel := runtimeBudgetCleanupContext()
	defer cancel()
	if err := l.runtime.captureLLMCallSpend(cleanupCtx, reservation, response); err != nil {
		return nil, err
	}
	return response, nil
}

func reportedLLMTokenUsage(usage TokenUsage) int {
	if usage.TotalTokens > 0 {
		return usage.TotalTokens
	}
	total := usage.PromptTokens + usage.CompletionTokens
	if total > 0 {
		return total
	}
	return 0
}

func (r *Runtime) recordLLMResponseTokenUsage(response *LLMResponse) {
	if r == nil || response == nil {
		return
	}
	if usedTokens := reportedLLMTokenUsage(response.Usage); usedTokens > 0 {
		r.RecordTokenUsage(0, usedTokens)
	}
}

func runtimeBudgetCleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), runtimeBudgetCleanupTimeout)
}

func runtimeBudgetLedgerConfigured(cfg RuntimeConfig) bool {
	return strings.TrimSpace(cfg.BudgetAccountID) != "" || cfg.BudgetHardLimitMicros > 0 || cfg.BudgetReserveMicros > 0
}

func defaultRuntimeBudgetAccountID(workspaceID, agentID string) string {
	basis := strings.TrimSpace(workspaceID) + "|" + strings.TrimSpace(agentID) + "|llm"
	if strings.TrimSpace(workspaceID) == "" || strings.TrimSpace(agentID) == "" {
		return ""
	}
	return "llm-budget-" + shortHash(basis)
}

func (r *Runtime) reserveLLMCallBudget(ctx context.Context) (runtimeLLMBudgetReservation, error) {
	if r == nil {
		return runtimeLLMBudgetReservation{}, nil
	}
	if err := r.preflightLocalTokenBudget(); err != nil {
		return runtimeLLMBudgetReservation{}, err
	}

	cfg := r.cfg
	cfg.ApplyDefaults()
	if !runtimeBudgetLedgerConfigured(cfg) {
		return runtimeLLMBudgetReservation{}, nil
	}
	if r.client == nil {
		return runtimeLLMBudgetReservation{}, fmt.Errorf("%w: rhizome client unavailable", errRuntimeBudgetExhausted)
	}
	if strings.TrimSpace(cfg.BudgetAccountID) == "" {
		return runtimeLLMBudgetReservation{}, fmt.Errorf("%w: budget account id unavailable", errRuntimeBudgetExhausted)
	}
	if cfg.BudgetReserveMicros <= 0 {
		return runtimeLLMBudgetReservation{}, fmt.Errorf("%w: budget reserve micros unavailable", errRuntimeBudgetExhausted)
	}

	accountID, err := r.ensureRuntimeBudgetAccount(ctx, cfg)
	if err != nil {
		return runtimeLLMBudgetReservation{}, err
	}
	cfg.BudgetAccountID = accountID

	taskID, _, runID := r.currentToolExecutionContext()
	callID := newRuntimeID("llm-budget")
	taskID = firstNonEmpty(strings.TrimSpace(taskID), "runtime-"+strings.TrimSpace(cfg.AgentID))
	runID = firstNonEmpty(strings.TrimSpace(runID), callID)
	providerID := firstNonEmpty(strings.TrimSpace(cfg.ProviderID), strings.TrimSpace(cfg.LLMBackend), "unknown-provider")
	model := firstNonEmpty(strings.TrimSpace(cfg.Model), defaultModel)
	reservationID := "res-" + callID

	_, err = r.client.ReserveBudget(ctx, BudgetReserveInput{
		ReservationID:  reservationID,
		IdempotencyKey: "llm.reserve:" + callID,
		AccountID:      accountID,
		WorkspaceID:    cfg.WorkspaceID,
		AgentID:        cfg.AgentID,
		TaskID:         taskID,
		RunID:          runID,
		ProviderID:     providerID,
		Model:          model,
		AmountMicros:   cfg.BudgetReserveMicros,
		Reason:         "runtime_llm_provider_call",
	})
	if err != nil {
		return runtimeLLMBudgetReservation{}, fmt.Errorf("%w: reserve llm provider budget: %v", errRuntimeBudgetExhausted, err)
	}

	return runtimeLLMBudgetReservation{
		enabled:       true,
		accountID:     accountID,
		reservationID: reservationID,
		baseEntryID:   callID,
		taskID:        taskID,
		runID:         runID,
		providerID:    providerID,
		model:         model,
		amountMicros:  cfg.BudgetReserveMicros,
		tokenMicros:   cfg.BudgetMicrosPerToken,
	}, nil
}

func (r *Runtime) preflightLocalTokenBudget() error {
	if r == nil {
		return nil
	}
	r.tokenMu.Lock()
	loaded := r.limitsLoaded
	groupID := strings.TrimSpace(r.limitsGroupID)
	daily := r.dailyRemaining
	weekly := r.weeklyRemaining
	r.tokenMu.Unlock()

	if !loaded || groupID == "" {
		return nil
	}
	if daily <= 0 {
		return fmt.Errorf("%w: daily token budget exhausted for limit group %s", errRuntimeBudgetExhausted, groupID)
	}
	if weekly <= 0 {
		return fmt.Errorf("%w: weekly token budget exhausted for limit group %s", errRuntimeBudgetExhausted, groupID)
	}
	return nil
}

func (r *Runtime) ensureRuntimeBudgetAccount(ctx context.Context, cfg RuntimeConfig) (string, error) {
	accountID := strings.TrimSpace(cfg.BudgetAccountID)
	if r == nil || r.client == nil || cfg.BudgetHardLimitMicros <= 0 {
		return accountID, nil
	}
	r.budgetMu.Lock()
	if r.budgetAccountEnsured {
		ensuredAccountID := strings.TrimSpace(r.cfg.BudgetAccountID)
		r.budgetMu.Unlock()
		return firstNonEmpty(ensuredAccountID, accountID), nil
	}
	r.budgetMu.Unlock()

	principalType, principalID := runtimeBudgetAccountPrincipal(cfg)
	log.Printf("[budget] ensuring account_id=%s principal=%s/%s agent=%s group=%s workspace=%s", accountID, principalType, principalID, cfg.AgentID, cfg.GroupID, cfg.WorkspaceID)
	account, err := r.client.EnsureBudgetAccount(ctx, BudgetAccountEnsureInput{
		AccountID:     accountID,
		PrincipalType: principalType,
		PrincipalID:   principalID,
		WorkspaceID:   cfg.WorkspaceID,
		Currency:      "USD",
		LimitMicros:   cfg.BudgetHardLimitMicros,
		Status:        "ACTIVE",
	})
	if err != nil {
		return "", fmt.Errorf("%w: ensure budget account: %v", errRuntimeBudgetExhausted, err)
	}
	canonicalAccountID := strings.TrimSpace(account.AccountID)
	if canonicalAccountID == "" {
		return "", fmt.Errorf("%w: ensure budget account returned empty account id", errRuntimeBudgetExhausted)
	}
	if canonicalAccountID != accountID {
		log.Printf("[budget] using canonical account_id=%s for requested account_id=%s principal=%s/%s workspace=%s", canonicalAccountID, accountID, principalType, principalID, cfg.WorkspaceID)
	}

	r.budgetMu.Lock()
	r.cfg.BudgetAccountID = canonicalAccountID
	r.budgetAccountEnsured = true
	r.budgetMu.Unlock()
	return canonicalAccountID, nil
}

func runtimeBudgetAccountPrincipal(cfg RuntimeConfig) (string, string) {
	agentID := strings.TrimSpace(cfg.AgentID)
	if agentID != "" {
		return "agent", agentID
	}
	groupID := strings.TrimSpace(cfg.GroupID)
	if groupID != "" && !strings.EqualFold(groupID, "fake") {
		return "agent_group", groupID
	}
	workspaceID := strings.TrimSpace(cfg.WorkspaceID)
	if workspaceID != "" {
		return "workspace", workspaceID
	}
	return "agent", strings.TrimSpace(cfg.AgentID)
}

func (r *Runtime) captureLLMCallSpend(ctx context.Context, reservation runtimeLLMBudgetReservation, response *LLMResponse) error {
	if r == nil || !reservation.enabled {
		return nil
	}
	spend := reservation.amountMicros
	if response != nil && reservation.tokenMicros > 0 {
		if usedTokens := reportedLLMTokenUsage(response.Usage); usedTokens > 0 {
			spend = int64(usedTokens) * reservation.tokenMicros
		}
	}
	if spend <= 0 {
		spend = reservation.amountMicros
	}
	overspent := spend > reservation.amountMicros
	estimatedSpend := spend
	if overspent {
		log.Printf("[budget] llm spend estimate %d exceeds reservation %d; failing closed after capturing reserved amount", spend, reservation.amountMicros)
		spend = reservation.amountMicros
	}

	spendInput := BudgetSpendInput{
		EntryID:        "spend-" + reservation.baseEntryID,
		IdempotencyKey: "llm.spend:" + reservation.baseEntryID,
		AccountID:      reservation.accountID,
		ReservationID:  reservation.reservationID,
		WorkspaceID:    r.cfg.WorkspaceID,
		AgentID:        r.cfg.AgentID,
		TaskID:         reservation.taskID,
		RunID:          reservation.runID,
		ProviderID:     reservation.providerID,
		Model:          reservation.model,
		AmountMicros:   spend,
		Reason:         "runtime_llm_provider_usage",
	}
	err := callRuntimeBudgetCleanupWithRetry(ctx, "budget.spend", func(attemptCtx context.Context) error {
		_, err := r.client.CaptureBudgetSpend(attemptCtx, spendInput)
		return err
	})
	if err != nil {
		state := runtimeBudgetCaptureFailedAfterProviderSuccessState
		entryID := "spend-" + reservation.baseEntryID
		log.Printf("[budget] state=%s account_id=%s reservation_id=%s entry_id=%s: capture llm provider spend failed after provider success: %v", state, reservation.accountID, reservation.reservationID, entryID, err)
		persistCtx, cancel := runtimeBudgetCleanupContext()
		defer cancel()
		if persistErr := r.persistBudgetCaptureFailureState(persistCtx, reservation, entryID, err); persistErr != nil {
			log.Printf("[budget] state=%s account_id=%s reservation_id=%s entry_id=%s: persist capture-failure state failed: %v", state, reservation.accountID, reservation.reservationID, entryID, persistErr)
			return fmt.Errorf("%w: state=%s: capture llm provider spend after provider success: %v; persist capture-failure state: %v", errRuntimeBudgetExhausted, state, err, persistErr)
		}
		return fmt.Errorf("%w: state=%s: capture llm provider spend after provider success: %v", errRuntimeBudgetExhausted, state, err)
	}
	if overspent {
		return fmt.Errorf("%w: llm provider usage exceeded reserved budget: estimated_spend_micros=%d reserved_micros=%d", errRuntimeBudgetExhausted, estimatedSpend, reservation.amountMicros)
	}
	if remaining := reservation.amountMicros - spend; remaining > 0 {
		r.releaseLLMCallBudgetBestEffort(ctx, reservation, remaining, "runtime_llm_provider_unused_reservation")
	}
	return nil
}

func (r *Runtime) persistBudgetCaptureFailureState(ctx context.Context, reservation runtimeLLMBudgetReservation, entryID string, cause error) error {
	if r == nil || r.client == nil {
		return nil
	}
	state := runtimeBudgetCaptureFailedAfterProviderSuccessState
	summary := fmt.Sprintf("%s: reservation_id=%s entry_id=%s", state, reservation.reservationID, entryID)
	if cause != nil {
		summary = summary + ": " + strings.TrimSpace(cause.Error())
	}
	return r.updateScratch(ctx, func(scratch *RuntimeScratchState) {
		scratch.LastWakeReason = state
		scratch.LastWakeSummary = summary
		scratch.LastWakeTaskID = reservation.taskID
		scratch.LastWakeAt = time.Now().UTC().Format(time.RFC3339Nano)
		scratch.LastSummary = summary
		advisory := "BUDGET_CAPTURE_FAILED_AFTER_PROVIDER_SUCCESS: keep reservation unresolved; do not issue new real LLM calls until budget ledger is repaired"
		scratch.AdvisorySignals = append(scratch.AdvisorySignals, advisory)
		if len(scratch.AdvisorySignals) > 5 {
			scratch.AdvisorySignals = scratch.AdvisorySignals[len(scratch.AdvisorySignals)-5:]
		}
	})
}

func (r *Runtime) releaseLLMCallBudget(ctx context.Context, reservation runtimeLLMBudgetReservation, amountMicros int64, reason string) error {
	if r == nil || !reservation.enabled || amountMicros <= 0 {
		return nil
	}
	releaseInput := BudgetReleaseInput{
		EntryID:        "release-" + reservation.baseEntryID,
		IdempotencyKey: "llm.release:" + reservation.baseEntryID,
		AccountID:      reservation.accountID,
		ReservationID:  reservation.reservationID,
		WorkspaceID:    r.cfg.WorkspaceID,
		AgentID:        r.cfg.AgentID,
		TaskID:         reservation.taskID,
		RunID:          reservation.runID,
		ProviderID:     reservation.providerID,
		Model:          reservation.model,
		AmountMicros:   amountMicros,
		Reason:         reason,
	}
	err := callRuntimeBudgetCleanupWithRetry(ctx, "budget.release", func(attemptCtx context.Context) error {
		_, err := r.client.ReleaseBudget(attemptCtx, releaseInput)
		return err
	})
	if err != nil {
		return fmt.Errorf("%w: release llm provider budget: %v", errRuntimeBudgetExhausted, err)
	}
	return nil
}

func (r *Runtime) releaseLLMCallBudgetBestEffort(ctx context.Context, reservation runtimeLLMBudgetReservation, amountMicros int64, reason string) bool {
	if err := r.releaseLLMCallBudget(ctx, reservation, amountMicros, reason); err != nil {
		log.Printf("[budget] release cleanup deferred account_id=%s reservation_id=%s amount_micros=%d reason=%s: %v", reservation.accountID, reservation.reservationID, amountMicros, reason, err)
		return false
	}
	return true
}

func callRuntimeBudgetCleanupWithRetry(ctx context.Context, op string, fn func(context.Context) error) error {
	if fn == nil {
		return errors.New("budget cleanup function is required")
	}
	var lastErr error
	for attempt := 1; attempt <= runtimeBudgetCleanupMaxAttempts; attempt++ {
		err := fn(ctx)
		if err == nil {
			if attempt > 1 {
				log.Printf("[budget] %s cleanup succeeded after transient retry attempt=%d", op, attempt)
			}
			return nil
		}
		lastErr = err
		if attempt >= runtimeBudgetCleanupMaxAttempts || !isTransientRuntimeBudgetCleanupError(err) {
			return err
		}
		delay := time.Duration(attempt) * runtimeBudgetCleanupRetryBaseDelay
		log.Printf("[budget] %s cleanup transient failure attempt=%d/%d; retrying after %s: %v", op, attempt, runtimeBudgetCleanupMaxAttempts, delay, err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf("%w; retry context finished: %v", err, ctx.Err())
		case <-timer.C:
		}
	}
	return lastErr
}

func isTransientRuntimeBudgetCleanupError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if message == "" {
		return false
	}
	if strings.Contains(message, "http 502") ||
		strings.Contains(message, "http 503") ||
		strings.Contains(message, "http 504") {
		return true
	}
	if strings.Contains(message, " transport:") ||
		strings.Contains(message, " read body:") {
		return true
	}
	return false
}
