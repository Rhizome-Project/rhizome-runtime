package server_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/server"
)

func TestAssetsRoute_ForceGraph(t *testing.T) {
	// Our mux handler as implemented in serve_cli.go:
	// mux.Handle("/assets/", http.FileServer(http.FS(server.AssetsFS)))
	mux := http.NewServeMux()
	mux.Handle("/assets/", http.FileServer(http.FS(server.AssetsFS)))

	req := httptest.NewRequest(http.MethodGet, "/assets/force-graph.min.js", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	res := rec.Result()
	if res.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", res.Status)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	if len(body) == 0 {
		t.Errorf("Expected non-empty response body for force-graph library")
	}

	if !strings.Contains(string(body), "function") {
		t.Errorf("Response body doesn't look like JavaScript")
	}
}
