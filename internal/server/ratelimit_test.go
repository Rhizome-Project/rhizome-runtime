package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRateLimiterRefillsContinuously(t *testing.T) {
	rl := NewRateLimiter(2, 100*time.Millisecond)
	key := "dashboard-token"

	for i := 0; i < 4; i++ {
		if !rl.Allow(key) {
			t.Fatalf("expected initial burst token %d to be allowed", i+1)
		}
	}
	if rl.Allow(key) {
		t.Fatal("expected bucket to be exhausted before refill")
	}

	time.Sleep(60 * time.Millisecond)

	if !rl.Allow(key) {
		t.Fatal("expected partial refill to allow a request before a full window elapsed")
	}
}

func TestRateLimiterInstancesDoNotShareBuckets(t *testing.T) {
	left := NewRateLimiter(1, time.Second)
	right := NewRateLimiter(1, time.Second)
	key := "shared-token"

	if !left.Allow(key) {
		t.Fatal("expected first limiter to allow initial request")
	}
	if !left.Allow(key) {
		t.Fatal("expected first limiter burst to allow a second request")
	}
	if left.Allow(key) {
		t.Fatal("expected first limiter to exhaust its own bucket")
	}
	if !right.Allow(key) {
		t.Fatal("expected a separate limiter instance to keep an independent bucket")
	}
}

func TestPeerIPRateLimitMiddlewareIgnoresAttackerControlledHeaders(t *testing.T) {
	rl := NewRateLimiter(1, time.Hour)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	handler := PeerIPRateLimitMiddleware(rl, next)

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/auth/human/login?token=query-token-%d", i), nil)
		req.RemoteAddr = fmt.Sprintf("203.0.113.7:%d", 49152+i)
		req.Header.Set("Authorization", fmt.Sprintf("Bearer attacker-token-%d", i))
		req.Header.Set("X-Forwarded-For", fmt.Sprintf("198.51.100.%d", i+1))
		req.Header.Set("X-Real-IP", fmt.Sprintf("192.0.2.%d", i+1))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		want := http.StatusNoContent
		if i == 2 {
			want = http.StatusTooManyRequests
		}
		if rec.Code != want {
			t.Fatalf("request %d returned %d, want %d", i+1, rec.Code, want)
		}
	}
}

func TestRateLimitMiddlewareStillKeysAuthenticatedRequestsByAuthorization(t *testing.T) {
	rl := NewRateLimiter(1, time.Hour)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	handler := RateLimitMiddleware(rl, next)

	request := func(token string) int {
		req := httptest.NewRequest(http.MethodPost, "/api/workspace/security/update", nil)
		req.RemoteAddr = "203.0.113.7:49152"
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}

	if got := request("token-a"); got != http.StatusNoContent {
		t.Fatalf("first token-a request returned %d", got)
	}
	if got := request("token-a"); got != http.StatusNoContent {
		t.Fatalf("burst token-a request returned %d", got)
	}
	if got := request("token-a"); got != http.StatusTooManyRequests {
		t.Fatalf("exhausted token-a request returned %d", got)
	}
	if got := request("token-b"); got != http.StatusNoContent {
		t.Fatalf("independent token-b request returned %d", got)
	}
}
