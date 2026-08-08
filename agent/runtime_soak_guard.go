package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

var errRuntimeSoakGuard = errors.New("runtime real LLM soak guard tripped")

func (r *Runtime) soakGuardConfigured() bool {
	if r == nil {
		return false
	}
	return runtimeSoakGuardConfigured(r.cfg)
}

func (r *Runtime) checkSoakAbort(ctx context.Context) error {
	if r == nil || !r.soakGuardConfigured() {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: context canceled before provider call: %v", errRuntimeSoakGuard, err)
	}
	return r.checkSoakStopFile()
}

func (r *Runtime) checkSoakStopFile() error {
	if r == nil || !r.soakGuardConfigured() {
		return nil
	}
	stopFile := strings.TrimSpace(r.cfg.SoakStopFile)
	if stopFile == "" {
		return nil
	}
	if _, err := os.Stat(stopFile); err == nil {
		return fmt.Errorf("%w: stop-file exists: %s", errRuntimeSoakGuard, stopFile)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: stop-file check failed for %s: %v", errRuntimeSoakGuard, stopFile, err)
	}
	return nil
}

func (r *Runtime) acquireSoakProviderCallSlot(ctx context.Context) (bool, error) {
	if r == nil || !r.soakGuardConfigured() {
		return false, nil
	}
	if err := r.checkSoakAbort(ctx); err != nil {
		return false, err
	}
	maxCalls := r.cfg.SoakMaxProviderCalls
	if maxCalls <= 0 {
		return false, nil
	}
	r.soakMu.Lock()
	defer r.soakMu.Unlock()
	if r.soakProviderCalls >= maxCalls {
		return false, fmt.Errorf("%w: provider-call limit reached: %d/%d", errRuntimeSoakGuard, r.soakProviderCalls, maxCalls)
	}
	r.soakProviderCalls++
	return true, nil
}

func (r *Runtime) releaseSoakProviderCallSlot(acquired bool) {
	if r == nil || !acquired {
		return
	}
	r.soakMu.Lock()
	if r.soakProviderCalls > 0 {
		r.soakProviderCalls--
	}
	r.soakMu.Unlock()
}

func (r *Runtime) watchSoakStopFile(ctx context.Context, cancel context.CancelFunc) {
	defer r.runWG.Done()
	if r == nil || cancel == nil {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := r.checkSoakStopFile(); err != nil {
			logRuntimeSoakGuardStop(err)
			cancel()
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func soakGuardBudgetReleaseReason(err error) string {
	if err == nil {
		return "real_llm_soak_guard"
	}
	message := err.Error()
	switch {
	case strings.Contains(message, "stop-file"):
		return "real_llm_soak_guard_stop_file"
	case strings.Contains(message, "context canceled"):
		return "real_llm_soak_guard_context_canceled"
	case strings.Contains(message, "provider-call limit"):
		return "real_llm_soak_guard_provider_call_limit"
	default:
		return "real_llm_soak_guard"
	}
}

func logRuntimeSoakGuardStop(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "[soak_guard] %v\n", err)
}
