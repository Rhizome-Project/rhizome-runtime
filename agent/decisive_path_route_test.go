package main

import "testing"

// Locks the fail-closed universality + the per-kind upgrades of the shared decisive-path route function.
func TestDecisivePathRouteFailsClosed(t *testing.T) {
	cases := []struct {
		name       string
		in         DecisivePathInput
		wantRoute  string
		wantVerb   string
		wantReason string
	}{
		{
			name:       "unknown kind, unclaimed, no path -> typed terminal blocker (CANCELLED), fail-closed reason",
			in:         DecisivePathInput{CarrierKind: "some_future_carrier_kind_r26"},
			wantRoute:  decisivePathRouteTypedBlocker,
			wantVerb:   decisivePathVerbCancelUnclaimed,
			wantReason: "unclassified_decisive_path_carrier",
		},
		{
			name:       "empty kind, claimed, no path -> typed terminal blocker (BLOCK), fail-closed reason",
			in:         DecisivePathInput{CarrierKind: "", ClaimedActiveOwnership: true},
			wantRoute:  decisivePathRouteTypedBlocker,
			wantVerb:   decisivePathVerbBlock,
			wantReason: "unclassified_decisive_path_carrier",
		},
		{
			name:       "authority carrier, unclaimed, no path -> CANCELLED close with kind reason",
			in:         DecisivePathInput{CarrierKind: decisivePathKindAuthorityTransition},
			wantRoute:  decisivePathRouteTypedBlocker,
			wantVerb:   decisivePathVerbCancelUnclaimed,
			wantReason: "no_fresh_claimable_authority_transition_path",
		},
		{
			name:       "side-effect successor, claimed, no path -> BLOCK with kind reason",
			in:         DecisivePathInput{CarrierKind: decisivePathKindSideEffectSuccessor, ClaimedActiveOwnership: true},
			wantRoute:  decisivePathRouteTypedBlocker,
			wantVerb:   decisivePathVerbBlock,
			wantReason: "no_fresh_claimable_side_effect_successor_path",
		},
		{
			name:       "fresh claimable path -> yield (re-entrant, never strands)",
			in:         DecisivePathInput{CarrierKind: decisivePathKindAuthorityTransition, HasFreshClaimablePath: true},
			wantRoute:  decisivePathRouteYield,
			wantVerb:   decisivePathVerbYieldRelease,
			wantReason: "fresh_claimable_path_yield:" + decisivePathKindAuthorityTransition,
		},
		{
			name:       "integrate-eligible + evidence -> integrate",
			in:         DecisivePathInput{CarrierKind: decisivePathKindPatchQueueValidation, IntegrateEligible: true, EvidenceReady: true},
			wantRoute:  decisivePathRouteIntegrate,
			wantVerb:   decisivePathVerbIntegrate,
			wantReason: "evidence_ready_integrate",
		},
		{
			name:       "durable receipt satisfied -> complete",
			in:         DecisivePathInput{CarrierKind: decisivePathKindReviewTask, DurableReceiptSatisfied: true},
			wantRoute:  decisivePathRouteCompleteOnReceipt,
			wantVerb:   decisivePathVerbComplete,
			wantReason: "durable_receipt_satisfied",
		},
		{
			name:       "already terminal -> noop",
			in:         DecisivePathInput{CarrierKind: decisivePathKindReviewTask, AlreadyTerminal: true},
			wantRoute:  decisivePathRouteCompleteOnReceipt,
			wantVerb:   decisivePathVerbNoop,
			wantReason: "already_terminal",
		},
		{
			name:       "owner NEVER satisfiable (non-agent/ineligible principal) -> typed terminal blocker",
			in:         DecisivePathInput{CarrierKind: decisivePathKindProjectClaimRepair, OwnerBound: true, OwnerSatisfiability: ownerSatisfiabilityNever},
			wantRoute:  decisivePathRouteTypedBlocker,
			wantVerb:   decisivePathVerbCancelUnclaimed,
			wantReason: "owner_cannot_satisfy_required_role:" + decisivePathKindProjectClaimRepair,
		},
		{
			name:       "owner AWAITING role (integration continuation, role acquirable, no holder yet) -> DEFERRED, not terminal (no silent integration loss)",
			in:         DecisivePathInput{CarrierKind: decisivePathKindPatchQueueIntegrationCont, OwnerBound: true, OwnerSatisfiability: ownerSatisfiabilityAwaitingRole},
			wantRoute:  decisivePathRouteDeferred,
			wantVerb:   decisivePathVerbDefer,
			wantReason: "owner_role_not_yet_available:" + decisivePathKindPatchQueueIntegrationCont,
		},
		{
			name:       "owner SATISFIABLE now (eligible holder) -> claimable (yield), never stranded",
			in:         DecisivePathInput{CarrierKind: decisivePathKindPatchQueueIntegrationCont, OwnerBound: true, OwnerSatisfiability: ownerSatisfiabilityNow},
			wantRoute:  decisivePathRouteYield,
			wantVerb:   decisivePathVerbYieldRelease,
			wantReason: "owner_satisfiable_claimable:" + decisivePathKindPatchQueueIntegrationCont,
		},
		{
			name:       "owner-bound but modality UNDETERMINED -> fail-closed typed terminal blocker",
			in:         DecisivePathInput{CarrierKind: decisivePathKindPatchQueueIntegrationCont, OwnerBound: true},
			wantRoute:  decisivePathRouteTypedBlocker,
			wantVerb:   decisivePathVerbCancelUnclaimed,
			wantReason: "owner_satisfiability_undetermined:" + decisivePathKindPatchQueueIntegrationCont,
		},
		{
			name:       "integrate beats yield (decision+evidence wins over a fresh path)",
			in:         DecisivePathInput{CarrierKind: decisivePathKindPatchQueueValidation, IntegrateEligible: true, EvidenceReady: true, HasFreshClaimablePath: true},
			wantRoute:  decisivePathRouteIntegrate,
			wantVerb:   decisivePathVerbIntegrate,
			wantReason: "evidence_ready_integrate",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := decisivePathRoute(tc.in)
			if got.Route != tc.wantRoute || got.Verb != tc.wantVerb || got.Reason != tc.wantReason {
				t.Fatalf("decisivePathRoute = {Route:%q Verb:%q Reason:%q}, want {Route:%q Verb:%q Reason:%q}",
					got.Route, got.Verb, got.Reason, tc.wantRoute, tc.wantVerb, tc.wantReason)
			}
		})
	}
}

// The structural fence for the typed-terminal-blocker CANCELLED-close (design 1c). The route function is
// the DECISION half of the fence: it must NEVER select the unconditional-cancel verb for a carrier in
// active ownership (an owned carrier routes to BLOCK, never a blind CANCELLED close), and EVERY terminal
// BLOCK/CANCELLED route must be owner-fenced. The EXECUTION half lives in executeRuntimeCarrierDecisivePath,
// whose only terminal enactment is publishTypedTerminalBlockerTaskOutcome's block-then-cancel-on-missing-
// claim path - the CANCELLED close fires ONLY after the server authoritatively reports no claim exists, so
// an owned carrier gets BlockTask success and never reaches cancel. Centralizing execution in the one
// executor is what stops any future carrier kind from re-introducing an unfenced blind cancel (the concern
// the v1 review raised against a centralized resolver). Together these prove: an owned carrier can never be
// blind-CANCELLED, and no carrier kind can opt out of the fence.
func TestDecisivePathCancelIsOwnerFenced(t *testing.T) {
	kinds := []string{
		"", "some_future_carrier_kind", decisivePathKindAuthorityTransition,
		decisivePathKindSideEffectSuccessor, decisivePathKindPatchQueueValidation,
		decisivePathKindPatchQueueIntegrationCont, decisivePathKindReviewTask,
	}
	for _, k := range kinds {
		// Active ownership must route to BLOCK, never the unconditional CANCELLED close of an owned carrier.
		owned := decisivePathRoute(DecisivePathInput{CarrierKind: k, ClaimedActiveOwnership: true})
		if owned.Verb == decisivePathVerbCancelUnclaimed {
			t.Fatalf("kind %q owned carrier selected cancel_unclaimed (blind CANCELLED of an owned carrier); want block", k)
		}
		if owned.Route == decisivePathRouteTypedBlocker && !owned.OwnerFence {
			t.Fatalf("kind %q owned typed-terminal-blocker route is not owner-fenced", k)
		}
		// Unclaimed carriers route to cancel_unclaimed; that CANCELLED-close route must also be owner-fenced.
		unclaimed := decisivePathRoute(DecisivePathInput{CarrierKind: k})
		if unclaimed.Verb == decisivePathVerbCancelUnclaimed && !unclaimed.OwnerFence {
			t.Fatalf("kind %q unclaimed cancel_unclaimed route is not owner-fenced", k)
		}
	}
	// A NEVER-satisfiable owner-bound carrier CANCELLED-closes an unclaimed gated carrier; it is fenced.
	never := decisivePathRoute(DecisivePathInput{CarrierKind: decisivePathKindPatchQueueIntegrationCont, OwnerBound: true, OwnerSatisfiability: ownerSatisfiabilityNever})
	if never.Route != decisivePathRouteTypedBlocker || !never.OwnerFence {
		t.Fatalf("never-satisfiable owner carrier must be an owner-fenced typed terminal blocker, got %+v", never)
	}
	// The DEFERRED (awaiting-role) route is non-terminal but still owner-fenced (only the role holder may
	// later claim it) and is a SEPARATE route class - never a silent PENDING - so observe can tell a healthy
	// defer from a stuck carrier.
	deferred := decisivePathRoute(DecisivePathInput{CarrierKind: decisivePathKindPatchQueueIntegrationCont, OwnerBound: true, OwnerSatisfiability: ownerSatisfiabilityAwaitingRole})
	if deferred.Route != decisivePathRouteDeferred || deferred.Verb != decisivePathVerbDefer || !deferred.OwnerFence {
		t.Fatalf("awaiting-role carrier must be an owner-fenced deferred route, got %+v", deferred)
	}
}

// Every recognized kind must yield a kind-specific (non-default) blocker reason; an unknown kind must hit
// the fail-closed default. This is the structural "no carrier without a route" assertion.
func TestDecisivePathBlockerReasonCoversEveryKind(t *testing.T) {
	kinds := []string{
		decisivePathKindAuthorityTransition, decisivePathKindSideEffectSuccessor,
		decisivePathKindPatchQueueRevision, decisivePathKindPatchQueueValidation,
		decisivePathKindPatchQueueDecisionCont, decisivePathKindPatchQueueIntegrationCont,
		decisivePathKindProjectClaimRepair, decisivePathKindBranchCheckoutOwnership,
		decisivePathKindReviewTask,
	}
	for _, k := range kinds {
		if r := decisivePathBlockerReason(k); r == "" || r == "unclassified_decisive_path_carrier" {
			t.Fatalf("recognized kind %q must have a specific blocker reason, got %q", k, r)
		}
	}
	if r := decisivePathBlockerReason("totally-unknown"); r != "unclassified_decisive_path_carrier" {
		t.Fatalf("unknown kind must fail closed to unclassified_decisive_path_carrier, got %q", r)
	}
}
