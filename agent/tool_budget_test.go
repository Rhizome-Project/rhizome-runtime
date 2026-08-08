package main

import "testing"

func TestBudgetToolsRegisteredAndAmbientSpendBlocked(t *testing.T) {
	agent := &Agent{
		Client:      NewRhizomeClient("http://127.0.0.1/rpc", "token"),
		WorkspaceID: "ws-budget",
		AgentID:     "agent-budget",
	}
	names := agent.baseToolNames()
	for _, name := range []string{"budget_account_ensure", "budget_account_get", "budget_reserve", "budget_spend", "budget_release", "budget_refund", "budget_ledger_list", "budget_reservations_list"} {
		if _, ok := names[name]; !ok {
			t.Fatalf("expected registered budget tool %s", name)
		}
	}
	for _, name := range []string{"budget_account_ensure", "budget_reserve", "budget_spend", "budget_release", "budget_refund"} {
		if ambientAutonomyToolAllowed(name) {
			t.Fatalf("expected ambient autonomy to block spend mutation tool %s", name)
		}
	}
	for _, name := range []string{"budget_account_get", "budget_ledger_list", "budget_reservations_list"} {
		if !ambientAutonomyToolAllowed(name) {
			t.Fatalf("expected ambient autonomy to allow budget context tool %s", name)
		}
	}
}
