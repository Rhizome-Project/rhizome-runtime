package main

// DECISIVE-PATH PRIMITIVE - canonical forward routing.
//
// decisivePathRoute is THE single source of truth for the forward route of any entity that can be born
// active/pending (carrier / lane / claim / side-effect successor / authority-transition / branch-checkout
// ownership / patch-queue decision-continuation / validation / review). It is a PURE function (no I/O):
// each module (agent runtime via RPC, internal/storage/sqlite via sql.Tx) NORMALIZES its native
// entity state into DecisivePathInput, calls this function, and EXECUTES the returned route with its own
// publisher. No route-decision logic is written twice.
//
// FAIL-CLOSED is the universality: for ANY input this returns a route. The default is
// routeTypedTerminalBlocker with reason `unclassified_decisive_path_carrier`. Per-kind branches only
// UPGRADE the default (to yield / integrate / complete-on-receipt). There is NO "(_, false) -> no route"
// path, so an 8th/Nth boundary that no branch recognizes still terminalizes deterministically instead of
// looping (this is the inversion that closes the R23->R24 trap: "born active/pending" is the closed set;
// "has a route" is a property every member proves by construction, not an enumerated allowlist).
//
// KEEP BYTE-IDENTICAL with internal/storage/sqlite/decisive_path_route.go (regex drift guard:
// internal/storage/sqlite/decisive_path_route_drift_test.go). Pure-primitive only: no imports, no module
// helpers. Classification (native entity -> CarrierKind) and execution (RPC/Tx) stay per-module; only the
// route DECISION is shared.

// Route kinds - exactly one is chosen for every entity.
const (
	decisivePathRouteCompleteOnReceipt = "complete_on_receipt"
	decisivePathRouteYield             = "yield"
	decisivePathRouteTypedBlocker      = "typed_terminal_blocker"
	decisivePathRouteIntegrate         = "integrate"
	decisivePathRouteDeferred          = "deferred_awaiting_owner"
)

// Execution verbs - what the per-module publisher must do to enact the route.
const (
	decisivePathVerbNoop            = "noop"                 // already terminal; nothing to do
	decisivePathVerbComplete        = "complete"             // durable receipt satisfies the contract -> resolve
	decisivePathVerbYieldRelease    = "yield_release"        // release/reclaim for a fresh selection (re-entrant)
	decisivePathVerbBlock           = "block"                // typed terminal blocker on a CLAIMED carrier
	decisivePathVerbCancelUnclaimed = "cancel_unclaimed"     // typed terminal blocker on an UNCLAIMED carrier (CANCELLED close, FENCED)
	decisivePathVerbIntegrate       = "integrate"            // product-ready + ACCEPTED -> integration cascade
	decisivePathVerbDefer           = "defer_awaiting_owner" // owner-absent-but-acquirable -> leave PENDING + register a GUARANTEED re-attempt; NON-terminal, observable
)

// Owner-satisfiability MODALITY (tri-state) for an owner-bound carrier at birth. The caller resolves which
// modality applies - typically by reusing the SAME role predicate claim admission uses (read==write parity,
// the 3ec326e1 recipe) - and the route fn maps each modality to a route. With OwnerBound set, an empty or
// unrecognized modality is FAIL-CLOSED (an owner-bound carrier whose satisfiability the caller did not
// resolve must never be minted unclaimable).
const (
	ownerSatisfiabilityNow          = "satisfiable_now" // an eligible owner holds the required role -> claimable
	ownerSatisfiabilityNever        = "never"           // owner can never satisfy (non-agent / ineligible principal) -> typed terminal
	ownerSatisfiabilityAwaitingRole = "awaiting_role"   // role acquirable but no active holder NOW -> deferred + guaranteed re-attempt
)

// Recognized carrier kinds. An empty / unrecognized kind is NOT an error - it falls to the fail-closed
// default. Each module stamps the kind onto its entity at birth (the creation chokepoint).
const (
	decisivePathKindAuthorityTransition       = "authority_transition"
	decisivePathKindSideEffectSuccessor       = "side_effect_resolution_successor"
	decisivePathKindPatchQueueRevision        = "patch_queue_revision_followup"
	decisivePathKindPatchQueueValidation      = "patch_queue_validation_followup"
	decisivePathKindPatchQueueDecisionCont    = "patch_queue_decision_continuation"
	decisivePathKindPatchQueueIntegrationCont = "patch_queue_integration_continuation"
	decisivePathKindProjectClaimRepair        = "project_claim_repair"
	decisivePathKindBranchCheckoutOwnership   = "branch_checkout_ownership"
	decisivePathKindReviewTask                = "review_task"
)

// DecisivePathInput is the normalized, module-agnostic state of one decisive-path entity. The caller fills
// it from its native representation (a hydrated task record, or a sql row). Booleans are pre-resolved
// facts so the decision is pure.
type DecisivePathInput struct {
	CarrierKind string // one of decisivePathKind*, or "" / unknown -> fail-closed default

	AlreadyTerminal         bool   // the entity is already in a terminal status (RESOLVED/FAILED/CANCELLED/DONE)
	DurableReceiptSatisfied bool   // a durable receipt already satisfies the entity's completion contract
	IntegrateEligible       bool   // this kind+state can route to integration (e.g. ACCEPTED queue decision)
	EvidenceReady           bool   // product-ready evidence is present (build/test/coverage) for integrate/complete
	HasFreshClaimablePath   bool   // a fresh claimable forward path materialized (admission would admit a successor)
	ClaimedActiveOwnership  bool   // the entity carries a live CLAIMED/RUNNING/ACTIVE claim (vs unclaimed/BLOCKED/RELEASED)
	OwnerBound              bool   // the entity has a declared owner binding; when set, consult OwnerSatisfiability
	OwnerSatisfiability     string // tri-state ownerSatisfiability* modality (now/never/awaiting_role); only consulted when OwnerBound. "" or unknown with OwnerBound => fail-closed.
}

// DecisivePathRoute is the verdict: exactly one route, the verb to enact it, a typed reason, and whether
// execution must be owner-fenced (the executing actor must own the entity's lane or be workspace
// authority - this fences the CANCELLED-close leg that the storage CANCELLED path does NOT itself gate).
type DecisivePathRoute struct {
	Route      string
	Verb       string
	Reason     string
	OwnerFence bool
}

// decisivePathRoute computes the guaranteed forward route. ORDER (first match wins): already-terminal ->
// durable-receipt-complete -> integrate -> deferral (owner cannot satisfy role) -> fresh-claimable-yield
// -> typed-terminal-blocker (fail-closed default).
func decisivePathRoute(in DecisivePathInput) DecisivePathRoute {
	kind := decisivePathNormalizeKind(in.CarrierKind)

	// (0) Already terminal: nothing to enact; report a no-op complete so callers are idempotent.
	if in.AlreadyTerminal {
		return DecisivePathRoute{Route: decisivePathRouteCompleteOnReceipt, Verb: decisivePathVerbNoop, Reason: "already_terminal"}
	}

	// (1) A durable receipt already satisfies the completion contract -> complete (owner-fenced).
	if in.DurableReceiptSatisfied {
		return DecisivePathRoute{Route: decisivePathRouteCompleteOnReceipt, Verb: decisivePathVerbComplete, Reason: "durable_receipt_satisfied", OwnerFence: true}
	}

	// (2) Integration-eligible + product-ready evidence -> integrate. Authority is NOT weakened: the
	// integration WRITE still hard-rejects unless the item is ACCEPTED + canonical-head proof; this route
	// only routes a decided+evidenced entity toward that gate, never around it.
	if in.IntegrateEligible && in.EvidenceReady {
		return DecisivePathRoute{Route: decisivePathRouteIntegrate, Verb: decisivePathVerbIntegrate, Reason: "evidence_ready_integrate", OwnerFence: true}
	}

	// (3) Owner-binding MODALITY (the universal owner-satisfiability classifier, tri-state). A carrier born
	// owner-bound is route-classified by WHETHER its declared owner can satisfy the required role, so
	// #4(a)/#4(b)/#6 fall out of ONE predicate instead of ad-hoc special cases:
	//   - NEVER satisfiable (owner is a non-agent / fundamentally ineligible principal, e.g. the R29
	//     claim-repair carrier owned by a workspace user) -> typed terminal blocker. An owner that can never
	//     hold the role is never claimable, so terminalizing is correct + observable.
	//   - AWAITING role (the role is acquirable but has no active holder NOW, e.g. an integration
	//     continuation when no INTEGRATOR is active) -> DEFERRED: a SEPARATE, route-classified, OBSERVABLE
	//     state with a GUARANTEED re-attempt, never a silent PENDING. Born-terminalizing this case would
	//     silently LOSE completed product work the moment a role-holder appears late; a loop is observable +
	//     recoverable, a lost integration is silent data loss - so defer, do not kill.
	//   - SATISFIABLE now (an eligible owner holds the role) -> the carrier has a fresh claimable path; it
	//     is minted/kept claimable (yield route, never stranded).
	//   - owner-bound but modality UNDETERMINED ("" / unknown) -> FAIL-CLOSED typed terminal blocker.
	if in.OwnerBound {
		switch decisivePathNormalizeKind(in.OwnerSatisfiability) {
		case ownerSatisfiabilityNow:
			return DecisivePathRoute{Route: decisivePathRouteYield, Verb: decisivePathVerbYieldRelease, Reason: "owner_satisfiable_claimable:" + kind}
		case ownerSatisfiabilityAwaitingRole:
			return DecisivePathRoute{Route: decisivePathRouteDeferred, Verb: decisivePathVerbDefer, Reason: "owner_role_not_yet_available:" + kind, OwnerFence: true}
		case ownerSatisfiabilityNever:
			verb := decisivePathVerbBlock
			if !in.ClaimedActiveOwnership {
				verb = decisivePathVerbCancelUnclaimed
			}
			return DecisivePathRoute{Route: decisivePathRouteTypedBlocker, Verb: verb, Reason: "owner_cannot_satisfy_required_role:" + kind, OwnerFence: true}
		default:
			verb := decisivePathVerbBlock
			if !in.ClaimedActiveOwnership {
				verb = decisivePathVerbCancelUnclaimed
			}
			return DecisivePathRoute{Route: decisivePathRouteTypedBlocker, Verb: verb, Reason: "owner_satisfiability_undetermined:" + kind, OwnerFence: true}
		}
	}

	// (4) A fresh claimable forward path materialized -> yield (release for a fresh selection; re-entrant,
	// non-terminal). This is the only non-terminal route and never silently strands the entity.
	if in.HasFreshClaimablePath {
		return DecisivePathRoute{Route: decisivePathRouteYield, Verb: decisivePathVerbYieldRelease, Reason: "fresh_claimable_path_yield:" + kind}
	}

	// (5) FAIL-CLOSED default: no fresh path and no receipt -> typed terminal blocker. Verb is BLOCK for a
	// claimed carrier, CANCELLED-close for an unclaimed one (the R23/R24 missing-claim fallback, now owned
	// here ONCE so every kind inherits it). Owner-fenced. The reason is kind-specific when recognized, or
	// `unclassified_decisive_path_carrier` for an unrecognized kind (still a deterministic route, never a loop).
	verb := decisivePathVerbBlock
	if !in.ClaimedActiveOwnership {
		verb = decisivePathVerbCancelUnclaimed
	}
	return DecisivePathRoute{Route: decisivePathRouteTypedBlocker, Verb: verb, Reason: decisivePathBlockerReason(kind), OwnerFence: true}
}

// decisivePathBlockerReason returns the typed terminal-blocker reason for a kind, or the fail-closed
// default for an unrecognized kind. KEEP BYTE-IDENTICAL across modules.
func decisivePathBlockerReason(kind string) string {
	switch decisivePathNormalizeKind(kind) {
	case decisivePathKindAuthorityTransition:
		return "no_fresh_claimable_authority_transition_path"
	case decisivePathKindSideEffectSuccessor:
		return "no_fresh_claimable_side_effect_successor_path"
	case decisivePathKindPatchQueueRevision:
		return "no_fresh_claimable_patch_queue_revision_path"
	case decisivePathKindPatchQueueValidation:
		return "no_fresh_claimable_patch_queue_validation_path"
	case decisivePathKindPatchQueueDecisionCont:
		return "no_fresh_claimable_decision_continuation_path"
	case decisivePathKindPatchQueueIntegrationCont:
		return "no_fresh_claimable_integration_continuation_path"
	case decisivePathKindProjectClaimRepair:
		return "no_fresh_claimable_project_claim_repair_path"
	case decisivePathKindBranchCheckoutOwnership:
		return "no_fresh_claimable_branch_checkout_ownership_path"
	case decisivePathKindReviewTask:
		return "no_fresh_review_drain_after_decision_recorded"
	default:
		return "unclassified_decisive_path_carrier"
	}
}

// decisivePathNormalizeKind lowercases + trims a carrier kind for comparison. KEEP BYTE-IDENTICAL.
func decisivePathNormalizeKind(kind string) string {
	out := make([]byte, 0, len(kind))
	start := 0
	end := len(kind)
	for start < end && (kind[start] == ' ' || kind[start] == '\t' || kind[start] == '\n' || kind[start] == '\r') {
		start++
	}
	for end > start && (kind[end-1] == ' ' || kind[end-1] == '\t' || kind[end-1] == '\n' || kind[end-1] == '\r') {
		end--
	}
	for i := start; i < end; i++ {
		c := kind[i]
		if c >= 'A' && c <= 'Z' {
			c = c + ('a' - 'A')
		}
		out = append(out, c)
	}
	return string(out)
}
