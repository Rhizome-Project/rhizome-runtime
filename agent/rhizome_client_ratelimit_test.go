package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Locks the generic HTTP 429 retry on the RPC client. A 429 means the per-token /rpc bucket was momentarily
// empty and the request was NOT processed, so call() must retry with backoff rather than surfacing the
// first 429 as a fatal error. This is the round-27 fix: the lead's agent.bootstrap exited on a single 429
// and aborted the whole fail-closed roster start.
func TestRhizomeClientCallRetriesTransientRateLimit(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&attempts, 1) <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"error":"rate limit exceeded"}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"result":null}`)
	}))
	defer srv.Close()

	prevDelay := rhizomeWorkNextRetryDelay
	rhizomeWorkNextRetryDelay = func(int) time.Duration { return time.Millisecond }
	defer func() { rhizomeWorkNextRetryDelay = prevDelay }()

	c := NewRhizomeClient(srv.URL, "test-token")
	if err := c.call(context.Background(), "agent.bootstrap", map[string]any{}, nil); err != nil {
		t.Fatalf("expected bootstrap call to succeed after retrying transient 429s, got %v", err)
	}
	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Fatalf("expected 3 attempts (two 429s then ok), got %d", got)
	}
}

// A persistently rate-limited endpoint must still surface the 429 after the bounded attempts (no infinite
// loop) so a genuinely throttled caller is not hidden forever.
func TestRhizomeClientCallSurfacesPersistentRateLimit(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":"rate limit exceeded"}`)
	}))
	defer srv.Close()

	prevDelay := rhizomeWorkNextRetryDelay
	rhizomeWorkNextRetryDelay = func(int) time.Duration { return time.Millisecond }
	defer func() { rhizomeWorkNextRetryDelay = prevDelay }()

	c := NewRhizomeClient(srv.URL, "test-token")
	err := c.call(context.Background(), "agent.bootstrap", map[string]any{}, nil)
	if err == nil {
		t.Fatalf("expected persistent 429 to surface an error after bounded attempts")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "429") {
		t.Fatalf("expected a 429 error, got %v", err)
	}
	if got := atomic.LoadInt32(&attempts); got != 4 {
		t.Fatalf("expected 4 bounded attempts for a persistent 429, got %d", got)
	}
}

// A non-429 error (e.g. a 400) must NOT be retried - it is surfaced on the first attempt.
func TestRhizomeClientCallDoesNotRetryNon429(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":"bad request"}`)
	}))
	defer srv.Close()

	prevDelay := rhizomeWorkNextRetryDelay
	rhizomeWorkNextRetryDelay = func(int) time.Duration { return time.Millisecond }
	defer func() { rhizomeWorkNextRetryDelay = prevDelay }()

	c := NewRhizomeClient(srv.URL, "test-token")
	if err := c.call(context.Background(), "agent.bootstrap", map[string]any{}, nil); err == nil {
		t.Fatalf("expected a 400 to surface an error")
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("expected exactly 1 attempt for a non-retryable 400, got %d", got)
	}
}
