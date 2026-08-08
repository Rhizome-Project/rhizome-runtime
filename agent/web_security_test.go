package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestManagerWebLoopbackRequestGateRejectsHostileHost(t *testing.T) {
	handler := managerWebTestSecurityGate(t)
	req := managerWebSecurityRequest(http.MethodGet, "http://attacker.example:8420/api/overview", "")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestManagerWebLoopbackRequestGateRejectsNonLoopbackPeer(t *testing.T) {
	handler := managerWebTestSecurityGate(t)
	req := managerWebSecurityRequest(http.MethodGet, "http://127.0.0.1:8420/api/overview", "")
	req.RemoteAddr = "192.0.2.10:41000"

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestManagerWebLoopbackRequestGateRejectsCrossOriginMutation(t *testing.T) {
	handler := managerWebTestSecurityGate(t)
	req := managerWebSecurityRequest(http.MethodPost, "http://127.0.0.1:8420/api/defaults", `{}`)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://attacker.example")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestManagerWebLoopbackRequestGateRejectsTextPlainMutation(t *testing.T) {
	handler := managerWebTestSecurityGate(t)
	req := managerWebSecurityRequest(http.MethodPost, "http://127.0.0.1:8420/api/defaults", `{}`)
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Origin", "http://127.0.0.1:8420")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnsupportedMediaType)
	}
}

func TestManagerWebLoopbackRequestGateAcceptsSameOriginJSONMutation(t *testing.T) {
	handler := managerWebTestSecurityGate(t)
	req := managerWebSecurityRequest(http.MethodPost, "http://127.0.0.1:8420/api/defaults", `{}`)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Origin", "http://127.0.0.1:8420")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestManagerWebLoopbackRequestGateAcceptsOriginAbsentJSONClient(t *testing.T) {
	handler := managerWebTestSecurityGate(t)
	req := managerWebSecurityRequest(http.MethodPost, "http://localhost:8420/api/defaults", `{}`)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestManagerWebLoopbackRequestGateRejectsCrossSiteFetchWithoutOrigin(t *testing.T) {
	handler := managerWebTestSecurityGate(t)
	req := managerWebSecurityRequest(http.MethodPost, "http://127.0.0.1:8420/api/defaults", `{}`)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Sec-Fetch-Site", "cross-site")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestManagerWebLoopbackRequestGateAddsSecurityHeadersToRejections(t *testing.T) {
	handler := managerWebTestSecurityGate(t)
	req := managerWebSecurityRequest(http.MethodGet, "http://attacker.example:8420/api/overview", "")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	for name, want := range map[string]string{
		"Cache-Control":                "no-store",
		"Cross-Origin-Resource-Policy": "same-origin",
		"X-Content-Type-Options":       "nosniff",
		"X-Frame-Options":              "DENY",
	} {
		if got := rec.Header().Get(name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
}

func managerWebTestSecurityGate(t *testing.T) http.Handler {
	t.Helper()
	listenerAddr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8420}
	return managerWebLoopbackRequestGate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), listenerAddr)
}

func managerWebSecurityRequest(method, target, body string) *http.Request {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.RemoteAddr = "127.0.0.1:41000"
	return req
}
