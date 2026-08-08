package sqlite

import "testing"

func requireDissentSplitSupport(t *testing.T) {
	t.Helper()
	if normalizeKnowledgeClaimType("dissent_marker") != "DISSENT_MARKER" ||
		normalizeKnowledgeClaimType("dissent_content") != "DISSENT_CONTENT" {
		t.Fatalf("expected bounded RMP-02 dissent split support to be landed")
	}
}

func TestRMP02KnowledgeClaimNormalizationSupportsDissentSplit(t *testing.T) {
	t.Parallel()

	requireDissentSplitSupport(t)

	if got := normalizeKnowledgeClaimType("dissent_marker"); got != "DISSENT_MARKER" {
		t.Fatalf("normalizeKnowledgeClaimType(dissent_marker) = %q, want DISSENT_MARKER", got)
	}
	if got := normalizeKnowledgeClaimType("dissent_content"); got != "DISSENT_CONTENT" {
		t.Fatalf("normalizeKnowledgeClaimType(dissent_content) = %q, want DISSENT_CONTENT", got)
	}
	if got := normalizeKnowledgeClaimType("dissent"); got != "DISSENT" {
		t.Fatalf("normalizeKnowledgeClaimType(dissent) = %q, want legacy compatibility DISSENT", got)
	}
}

func TestRMP02MemoryGraphCanonicalTypingMapsDissentSplitToMarkerAndContentSemantics(t *testing.T) {
	t.Parallel()

	requireDissentSplitSupport(t)

	cases := []struct {
		name      string
		claimType string
		wantType  string
		wantPred  string
		wantMod   string
	}{
		{
			name:      "marker stays first-class",
			claimType: "DISSENT_MARKER",
			wantType:  "DISSENT_MARKER",
			wantPred:  "signals_dissent",
			wantMod:   "observed",
		},
		{
			name:      "content stays first-class",
			claimType: "DISSENT_CONTENT",
			wantType:  "DISSENT_CONTENT",
			wantPred:  "critiques",
			wantMod:   "proposed",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			record := KnowledgeClaimRecord{ClaimType: tc.claimType}
			gotType := canonicalMemoryTypeFromKnowledgeClaim(record)
			if gotType != tc.wantType {
				t.Fatalf("canonicalMemoryTypeFromKnowledgeClaim(%q) = %q, want %q", tc.claimType, gotType, tc.wantType)
			}
			if gotPred := memoryGraphPredicateForType(gotType); gotPred != tc.wantPred {
				t.Fatalf("memoryGraphPredicateForType(%q) = %q, want %q", gotType, gotPred, tc.wantPred)
			}
			if gotMod := memoryGraphClaimModality(record, gotType); gotMod != tc.wantMod {
				t.Fatalf("memoryGraphClaimModality(%q -> %q) = %q, want %q", tc.claimType, gotType, gotMod, tc.wantMod)
			}
		})
	}
}

func TestRMP02DifferentialClaimSelectionSupportsDissentSplitAndRecoverableArchiveQuota(t *testing.T) {
	t.Parallel()

	requireDissentSplitSupport(t)

	claims := []KnowledgeClaimRecord{
		{ClaimID: "marker-active", ClaimType: "DISSENT_MARKER", Status: "ACTIVE"},
		{ClaimID: "content-active", ClaimType: "DISSENT_CONTENT", Status: "ACTIVE"},
		{ClaimID: "marker-archived", ClaimType: "DISSENT_MARKER", Status: "ARCHIVED", LifecycleReason: rmpArchivedReasonExpired, ArchivedAt: stringPtr("2026-03-28T00:00:00Z")},
		{ClaimID: "content-archived", ClaimType: "DISSENT_CONTENT", Status: "ARCHIVED", LifecycleReason: rmpArchivedReasonExpired, ArchivedAt: stringPtr("2026-03-28T00:00:00Z")},
		{ClaimID: "lesson-archived", ClaimType: "LESSON", Status: "ARCHIVED", LifecycleReason: rmpArchivedReasonExpired, ArchivedAt: stringPtr("2026-03-28T00:00:00Z")},
		{ClaimID: "procedure-active", ClaimType: "PROCEDURE", Status: "ACTIVE"},
	}

	selected := collectMemoryPacketDifferentialClaimsWithBudget(claims, 6, 2)
	if !hasKnowledgeClaim(selected, "marker-active") ||
		!hasKnowledgeClaim(selected, "content-active") {
		t.Fatalf("expected split dissent marker/content claims in differential selection, got %+v", selected)
	}
	if hasKnowledgeClaim(selected, "procedure-active") {
		t.Fatalf("did not expect procedure to remain in differential selection after procedural lane split, got %+v", selected)
	}
	if !hasKnowledgeClaim(selected, "marker-archived") && !hasKnowledgeClaim(selected, "content-archived") {
		t.Fatalf("expected at least one recoverable archived dissent claim in bounded differential selection, got %+v", selected)
	}
	if hasKnowledgeClaim(selected, "lesson-archived") {
		t.Fatalf("did not expect archived lesson in differential selection, got %+v", selected)
	}
	if isMemoryPacketDifferentialClaim(KnowledgeClaimRecord{ClaimType: "PROCEDURE", Status: "ACTIVE"}) {
		t.Fatalf("expected procedure to leave differential lane after procedural split")
	}

	if !isMemoryPacketRecoverableArchivedContrastiveClaim(KnowledgeClaimRecord{
		ClaimType:       "DISSENT_MARKER",
		ArchivedAt:      stringPtr("2026-03-28T00:00:00Z"),
		LifecycleReason: rmpArchivedReasonExpired,
	}) {
		t.Fatalf("expected archived dissent marker to count as recoverable contrastive claim")
	}
	if !isMemoryPacketRecoverableArchivedContrastiveClaim(KnowledgeClaimRecord{
		ClaimType:       "DISSENT_CONTENT",
		ArchivedAt:      stringPtr("2026-03-28T00:00:00Z"),
		LifecycleReason: rmpArchivedReasonExpired,
	}) {
		t.Fatalf("expected archived dissent content to count as recoverable contrastive claim")
	}
}
