package server

import (
	"strings"
	"testing"
)

func TestDashboardGraphInspectorOpensActionInModal(t *testing.T) {
	required := []string{
		`dashboardAction(function(dashboardEvent){showActionDetail((refID))})`,
		`>Open Action</button>`,
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard graph action modal hook is missing %s", needle)
		}
	}

	legacy := `switchTab('actions');setTimeout(()=>showActionDetail((refID))`
	if strings.Contains(dashboardHTML, legacy) {
		t.Fatalf("dashboard graph inspector still routes actions through the actions tab")
	}
}
