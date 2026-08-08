package main

import (
	"context"
	"strings"
)

// DECISIVE-PATH PRIMITIVE - agent-side enactment.
//
// executeRuntimeCarrierDecisivePath is the SINGLE agent-side enactment of decisivePathRoute for a runtime
// carrier task. Every carrier-kind publisher routes through it, so the fail-closed default applies
// uniformly: an unrecognized carrier kind still terminalizes (a typed terminal blocker) instead of
// silently looping (this is what closes the R23->R24 "two publishers, one bug" trap - the recognition is
// no longer where a new kind can fall through to no-route).
//
// It enacts only the route a DELEGATE PUBLISHER owns: typed_terminal_blocker (via
// publishTypedTerminalBlockerTaskOutcome, which does block-then-cancel-on-missing-claim and is robust to
// claim drift between decision and execution). The non-terminal / owner routes
// (yield / complete-on-receipt / integrate) are the OWNING agent's own task cycle or tool call, not a
// delegate publisher action, so they are returned for the caller to observe (and, when a future
// migration needs runtime enactment of yield/complete, wired here) rather than enacted on someone else's
// behalf. Returns the chosen route so callers can record/observe it.
func (r *Runtime) executeRuntimeCarrierDecisivePath(ctx context.Context, taskID string, in DecisivePathInput, reason, carrierLabel string) (DecisivePathRoute, error) {
	route := decisivePathRoute(in)
	taskID = strings.TrimSpace(taskID)
	if r == nil || r.client == nil || taskID == "" {
		return route, nil
	}
	if route.Route == decisivePathRouteTypedBlocker {
		if err := r.publishTypedTerminalBlockerTaskOutcome(ctx, taskID, reason, carrierLabel); err != nil {
			return route, err
		}
	}
	return route, nil
}

// runtimeCarrierDecisivePathInput normalizes a hydrated carrier task, at the publisher's decision point,
// into the shared DecisivePathInput. The publisher is reached only after admission found NO fresh
// claimable path and the carrier is NOT in active ownership, so HasFreshClaimablePath is false and the
// route resolves to a typed terminal blocker (block if claimed-but-blocked, CANCELLED-close if unclaimed).
func runtimeCarrierDecisivePathInput(task WorkspaceTaskRecord, carrierKind string) DecisivePathInput {
	return DecisivePathInput{
		CarrierKind:            carrierKind,
		AlreadyTerminal:        taskSubmitTaskIsTerminal(task),
		ClaimedActiveOwnership: taskClaimStatusIsActiveOwnership(taskClaimStatus(task)),
		HasFreshClaimablePath:  false,
	}
}
