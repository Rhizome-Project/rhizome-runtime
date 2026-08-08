package server

import (
	"strings"
	"testing"
)

func TestCorridorFitRPCContractsPendingSurface(t *testing.T) {
	methods := []string{
		"workspace.instrumentation.corridor.fit.report",
		"workspace.instrumentation.corridor.fit.cluster",
		"workspace.instrumentation.corridor.fit.snapshot",
	}
	missing := make([]string, 0, len(methods))
	for _, method := range methods {
		if _, ok := rpcMethodSchemas[method]; !ok {
			missing = append(missing, method)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("expected corridor fit RPC surface to be landed, missing %v", missing)
	}

	report := rpcMethodSchemas[methods[0]]
	cluster := rpcMethodSchemas[methods[1]]
	snapshot := rpcMethodSchemas[methods[2]]
	if !strings.Contains(strings.ToLower(report.Description), "read-only") {
		t.Fatalf("expected corridor fit report schema to stay read-only, got %q", report.Description)
	}
	if !strings.Contains(strings.ToLower(cluster.Description), "proto-cluster") {
		t.Fatalf("expected corridor fit cluster schema to describe proto-cluster inspection, got %q", cluster.Description)
	}
	if !strings.Contains(strings.ToLower(snapshot.Description), "persisted event") {
		t.Fatalf("expected corridor fit snapshot schema to describe persisted event parity, got %q", snapshot.Description)
	}
}

func TestDashboardCorridorFitContractsPendingSurface(t *testing.T) {
	required := []string{
		"workspace.instrumentation.corridor.fit.report",
		"workspace.instrumentation.corridor.fit.cluster",
		"workspace.instrumentation.corridor.fit.snapshot",
		"cluster.corridor_fit_snapshot",
	}
	present := 0
	for _, needle := range required {
		if strings.Contains(dashboardHTML, needle) {
			present++
		}
	}
	if present == 0 {
		t.Fatal("expected corridor fit dashboard surface to be landed")
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard corridor fit surface is missing %s", needle)
		}
	}
	if strings.Contains(strings.ToLower(dashboardHTML), "apply corridor fit") {
		t.Fatal("dashboard corridor fit surface should stay read-only")
	}
}
