package server

import (
	"net/http"
	"sync"
	"time"
)

// RateLimiter is a simple per-token rate limiter using token bucket algorithm.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    int           // tokens per interval
	burst   int           // max tokens
	window  time.Duration // refill interval
}

type bucket struct {
	tokens    float64
	lastCheck time.Time
}

// NewRateLimiter creates a rate limiter that allows `rate` requests per `window`.
func NewRateLimiter(rate int, window time.Duration) *RateLimiter {
	rl := &RateLimiter{
		buckets: make(map[string]*bucket),
		rate:    rate,
		burst:   rate * 2,
		window:  window,
	}

	go rl.cleanupLoop()
	return rl
}

func (rl *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(rl.window * 10)
	defer ticker.Stop()
	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		for key, b := range rl.buckets {
			if now.Sub(b.lastCheck) > rl.window*10 {
				delete(rl.buckets, key)
			}
		}
		rl.mu.Unlock()
	}
}

// Allow checks if a request from the given key is allowed.
func (rl *RateLimiter) Allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	b, ok := rl.buckets[key]
	if !ok {
		rl.buckets[key] = &bucket{tokens: float64(rl.burst - 1), lastCheck: now}
		return true
	}

	if rl.rate <= 0 || rl.window <= 0 {
		return true
	}

	// Refill tokens continuously so short-lived bursts recover smoothly.
	elapsed := now.Sub(b.lastCheck).Seconds()
	if elapsed > 0 {
		refill := elapsed * float64(rl.rate) / rl.window.Seconds()
		if refill > 0 {
			b.tokens += refill
			if b.tokens > float64(rl.burst) {
				b.tokens = float64(rl.burst)
			}
		}
		b.lastCheck = now
	}

	if b.tokens < 1 {
		return false
	}

	b.tokens -= 1
	return true
}

// RateLimitMiddleware returns a middleware that limits authenticated requests
// per Bearer token, falling back to the request's reported client address.
func RateLimitMiddleware(rl *RateLimiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Use auth token as key when present, fallback to client IP.
		key := r.Header.Get("Authorization")
		if key == "" {
			key = r.URL.Query().Get("token")
		}
		if key == "" {
			key = requestClientIP(r)
		}
		if key == "" {
			key = r.RemoteAddr
		}

		if !rl.Allow(key) {
			http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// PeerIPRateLimitMiddleware returns a middleware for unauthenticated endpoints.
// It deliberately ignores credentials, query parameters, and forwarding
// headers so callers cannot obtain a fresh bucket with attacker-chosen input.
func PeerIPRateLimitMiddleware(rl *RateLimiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := "peer:" + requestTransportPeerIP(r)
		if !rl.Allow(key) {
			http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}
