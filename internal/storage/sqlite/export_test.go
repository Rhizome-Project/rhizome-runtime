package sqlite

import "time"

// SetAgentWorkPreSelectionSweepBudgetForTest overrides the per-sweep bounded sub-context budget so a
// test can shorten/extend the deadline window (P1.5). Returns a restore func.
func SetAgentWorkPreSelectionSweepBudgetForTest(d time.Duration) (restore func()) {
	prev := agentWorkPreSelectionSweepBudget
	agentWorkPreSelectionSweepBudget = d
	return func() { agentWorkPreSelectionSweepBudget = prev }
}

// SetAgentWorkRequiredReceiptSweepFaultForTest injects a deterministic receipt-sweep failure so a
// test can exercise the A1 swallow-and-continue path (P5) without relying on an unreliable in-memory
// ctx deadline. Returns a restore func.
func SetAgentWorkRequiredReceiptSweepFaultForTest(fault func() error) (restore func()) {
	prev := agentWorkRequiredReceiptSweepFault
	agentWorkRequiredReceiptSweepFault = fault
	return func() { agentWorkRequiredReceiptSweepFault = prev }
}
