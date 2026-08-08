package server_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/server"
)

func TestDashboardRuntimeHealthUsesAuthenticatedDiagnostics(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	dashboardHTML := string(body)

	for _, needle := range []string{
		`const response = await fetch(window.location.origin + "/api/diagnostics", {`,
		`if (TOKEN) headers.Authorization = 'Bearer ' + TOKEN;`,
		`throw new Error("runtime diagnostics endpoint returned " + String(response.status));`,
	} {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard runtime health wiring is missing %q", needle)
		}
	}

	if strings.Contains(dashboardHTML, `const response = await fetch(window.location.origin + "/health", {`) {
		t.Fatal("dashboard runtime health still uses public /health liveness instead of authenticated diagnostics")
	}
}

func TestDashboardRuntimeHealthRendersStructuredDiagnosticsTruth(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	dashboardHTML := string(body)

	for _, needle := range []string{
		`const semantics = payload.semantics || {};`,
		`const extended = payload.extended_readiness || {};`,
		`const reviewerScarcityHealth = extended.reviewer_scarcity_health || {};`,
		`["Reviewer Scarcity", extended.reviewer_scarcity],`,
		`["Operator Queue Lag", extended.operator_queue_lag],`,
		`["Stuck Agents", extended.stuck_agents],`,
		`runtimeHealthReviewerScarcityBreakdown(reviewerScarcityHealth)`,
		`<summary>Diagnostics · '+diagnosticSignals.length+'</summary>`,
		`<details class="raw-section"><summary>'+esc(s[0])+'</summary><pre>'+esc(JSON.stringify(s[1], null, 2))+'</pre></details>`,
		`['Semantics', semantics],`,
		`['Extended Readiness', extended],`,
		`['Reviewer Scarcity Health', reviewerScarcityHealth],`,
		`['Authority Node', authorityNode],`,
		`['Authority Lease', authorityLease],`,
		`['Loop Readiness', loopReadiness],`,
		`<strong>Readiness</strong><br>`,
		`<strong>Degraded Signal</strong><br>`,
	} {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard runtime health diagnostics rendering is missing %q", needle)
		}
	}
}

func TestDashboardRuntimeHealthUsesDedicatedRefreshPath(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	dashboardHTML := string(body)

	for _, needle := range []string{
		`loadRuntimeHealth().catch(err => console.error('initial runtime health', err));`,
		`runtimeHealthRefreshTimer = setInterval(() => {`,
		`if (TOKEN) loadRuntimeHealth().catch(err => console.error('runtime health refresh', err));`,
	} {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard runtime health dedicated refresh wiring is missing %q", needle)
		}
	}

	if strings.Contains(dashboardHTML, `await Promise.all([loadRuntimeHealth(),`) {
		t.Fatal("dashboard global refresh still fans runtime diagnostics into the general refresh storm")
	}
}
