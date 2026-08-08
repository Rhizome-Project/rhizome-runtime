package server_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/server"
)

func TestDashboardSmoke_ModalOverlayControls(t *testing.T) {
	handler := server.ServeDashboard()

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	res := rec.Result()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected status OK, got %v", res.Status)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	bodyStr := string(body)

	for _, needle := range []string{
		`id="delete-confirm" onclick="if(event.target===this)cancelDelete()"`,
		`id="resolve-overlay" onclick="if(event.target===this)cancelResolve()"`,
		`function dashboardCloseTopOverlay()`,
		`.resolve-box .btn-row button`,
	} {
		if !strings.Contains(bodyStr, needle) {
			t.Fatalf("dashboard HTML missing %q", needle)
		}
	}
}
