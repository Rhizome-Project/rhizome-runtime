package sqlite

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	KnowledgeClaimFreshnessContract     = "knowledge_claim.freshness.v1"
	knowledgeClaimFreshnessExampleLimit = 8
	knowledgeClaimFreshnessMaxItems     = 500
)

type KnowledgeClaimFreshnessSummary struct {
	Contract                      string                           `json:"contract"`
	WorkspaceID                   string                           `json:"workspace_id"`
	ReferenceAt                   string                           `json:"reference_at"`
	TotalClaimCount               int                              `json:"total_claim_count"`
	FreshClaimCount               int                              `json:"fresh_claim_count"`
	NonAuthoritativeClaimCount    int                              `json:"non_authoritative_claim_count"`
	WeakProvenanceClaimCount      int                              `json:"weak_provenance_claim_count"`
	ReviewClaimCount              int                              `json:"review_claim_count"`
	OverdueReviewClaimCount       int                              `json:"overdue_review_claim_count"`
	StaleClaimCount               int                              `json:"stale_claim_count"`
	DisputedClaimCount            int                              `json:"disputed_claim_count"`
	SupersededClaimCount          int                              `json:"superseded_claim_count"`
	ArchivedClaimCount            int                              `json:"archived_claim_count"`
	RecoverableArchivedClaimCount int                              `json:"recoverable_archived_claim_count"`
	NeedsAttention                bool                             `json:"needs_attention"`
	AttentionReasons              []string                         `json:"attention_reasons,omitempty"`
	Examples                      []KnowledgeClaimFreshnessExample `json:"examples,omitempty"`
	Summary                       string                           `json:"summary"`
}

type KnowledgeClaimFreshnessExample struct {
	ClaimID             string   `json:"claim_id"`
	ClaimType           string   `json:"claim_type"`
	Status              string   `json:"status"`
	Freshness           string   `json:"freshness"`
	ProvenanceStrength  string   `json:"provenance_strength"`
	DowngradeReason     string   `json:"downgrade_reason,omitempty"`
	FreshnessReasons    []string `json:"freshness_reasons,omitempty"`
	ReviewDueAt         string   `json:"review_due_at,omitempty"`
	SupersededByClaimID string   `json:"superseded_by_claim_id,omitempty"`
	ConflictsClaimID    string   `json:"conflicts_claim_id,omitempty"`
	SourceKind          string   `json:"source_kind,omitempty"`
	SourceID            string   `json:"source_id,omitempty"`
}

type knowledgeClaimFreshnessClassification struct {
	Freshness          string
	ProvenanceStrength string
	DowngradeReason    string
	Reasons            []string
	Authoritative      bool
	NeedsAttention     bool
}

func (s *Store) BuildKnowledgeClaimFreshnessSummary(ctx context.Context, workspaceID, referenceAt string, limit int) (KnowledgeClaimFreshnessSummary, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return KnowledgeClaimFreshnessSummary{}, fmt.Errorf("workspace_id is required")
	}
	referenceAt = strings.TrimSpace(referenceAt)
	if referenceAt == "" {
		var err error
		referenceAt, err = s.workspaceReferenceTimestamp(ctx, workspaceID, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return KnowledgeClaimFreshnessSummary{}, err
		}
	}
	exampleLimit := limit
	if exampleLimit <= 0 || exampleLimit > knowledgeClaimFreshnessExampleLimit {
		exampleLimit = knowledgeClaimFreshnessExampleLimit
	}
	claims, err := s.listKnowledgeClaimsForFreshness(ctx, workspaceID)
	if err != nil {
		return KnowledgeClaimFreshnessSummary{}, err
	}
	return buildKnowledgeClaimFreshnessSummary(workspaceID, referenceAt, claims, exampleLimit), nil
}

func (s *Store) listKnowledgeClaimsForFreshness(ctx context.Context, workspaceID string) ([]KnowledgeClaimRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT `+knowledgeClaimSelectColumns("")+`
		   FROM knowledge_claims
		  WHERE workspace_id = ?
		  ORDER BY claim_id ASC`,
		workspaceID,
	)
	if err != nil {
		return nil, fmt.Errorf("list knowledge claims for freshness: %w", err)
	}
	defer rows.Close()
	return collectKnowledgeClaimRows(rows)
}

func buildKnowledgeClaimFreshnessSummary(workspaceID, referenceAt string, claims []KnowledgeClaimRecord, exampleLimit int) KnowledgeClaimFreshnessSummary {
	if exampleLimit <= 0 {
		exampleLimit = knowledgeClaimFreshnessExampleLimit
	}
	summary := KnowledgeClaimFreshnessSummary{
		Contract:    KnowledgeClaimFreshnessContract,
		WorkspaceID: strings.TrimSpace(workspaceID),
		ReferenceAt: strings.TrimSpace(referenceAt),
		Examples:    make([]KnowledgeClaimFreshnessExample, 0, min(exampleLimit, knowledgeClaimFreshnessExampleLimit)),
	}
	for _, claim := range claims {
		classification := classifyKnowledgeClaimFreshness(claim, referenceAt)
		summary.TotalClaimCount++
		if classification.Authoritative {
			summary.FreshClaimCount++
		} else {
			summary.NonAuthoritativeClaimCount++
		}
		if classification.ProvenanceStrength == "weak" {
			summary.WeakProvenanceClaimCount++
			summary.AttentionReasons = appendUniqueString(summary.AttentionReasons, "WEAK_PROVENANCE_CLAIMS")
		}
		switch classification.Freshness {
		case "review":
			summary.ReviewClaimCount++
		case "review_overdue":
			summary.ReviewClaimCount++
			summary.OverdueReviewClaimCount++
			summary.AttentionReasons = appendUniqueString(summary.AttentionReasons, "OVERDUE_REVIEW_CLAIMS")
		case "stale":
			summary.StaleClaimCount++
			summary.AttentionReasons = appendUniqueString(summary.AttentionReasons, "STALE_CLAIMS")
		case "disputed", "conflicted":
			summary.DisputedClaimCount++
			summary.AttentionReasons = appendUniqueString(summary.AttentionReasons, "DISPUTED_CLAIMS")
		case "superseded":
			summary.SupersededClaimCount++
			summary.AttentionReasons = appendUniqueString(summary.AttentionReasons, "SUPERSEDED_CLAIMS")
		case "archived":
			summary.ArchivedClaimCount++
			if classification.DowngradeReason == "recoverable_archived_contrastive" {
				summary.RecoverableArchivedClaimCount++
			}
		}
		if classification.NeedsAttention || classification.Freshness != "fresh" || classification.ProvenanceStrength == "weak" {
			if len(summary.Examples) < cap(summary.Examples) {
				summary.Examples = append(summary.Examples, knowledgeClaimFreshnessExample(claim, classification))
			}
		}
	}
	summary.NeedsAttention = len(summary.AttentionReasons) > 0
	if summary.TotalClaimCount == 0 {
		summary.Summary = fmt.Sprintf("knowledge claim freshness for %s: no claims observed", summary.WorkspaceID)
	} else {
		summary.Summary = fmt.Sprintf(
			"knowledge claim freshness for %s: %d fresh, %d non-authoritative, %d weak provenance",
			summary.WorkspaceID,
			summary.FreshClaimCount,
			summary.NonAuthoritativeClaimCount,
			summary.WeakProvenanceClaimCount,
		)
	}
	return summary
}

func decorateKnowledgeClaimForMemoryPacket(claim KnowledgeClaimRecord, referenceAt, role string) KnowledgeClaimRecord {
	classification := classifyKnowledgeClaimFreshness(claim, referenceAt)
	if strings.TrimSpace(role) == "recoverable_archived_contrastive" && classification.Freshness == "archived" {
		classification.DowngradeReason = "recoverable_archived_contrastive"
		classification.Reasons = appendUniqueString(classification.Reasons, "recoverable_archived_contrastive")
	}
	claim.Freshness = classification.Freshness
	claim.ProvenanceStrength = classification.ProvenanceStrength
	claim.DowngradeReason = classification.DowngradeReason
	claim.FreshnessReasons = append([]string(nil), classification.Reasons...)
	return claim
}

func isKnowledgeClaimAuthoritativeForMemoryPacket(claim KnowledgeClaimRecord, referenceAt string) bool {
	return classifyKnowledgeClaimFreshness(claim, referenceAt).Authoritative
}

func classifyKnowledgeClaimFreshness(claim KnowledgeClaimRecord, referenceAt string) knowledgeClaimFreshnessClassification {
	status := normalizeKnowledgeClaimStatus(claim.Status)
	classification := knowledgeClaimFreshnessClassification{
		Freshness:          "fresh",
		ProvenanceStrength: knowledgeClaimProvenanceStrength(claim),
		Reasons:            make([]string, 0, 4),
	}
	if classification.ProvenanceStrength == "weak" {
		classification.Reasons = appendUniqueString(classification.Reasons, "weak_provenance")
	}

	archivedAt := strings.TrimSpace(derefString(claim.ArchivedAt))
	switch {
	case archivedAt != "":
		classification.Freshness = "archived"
		classification.DowngradeReason = "archived"
		classification.Reasons = appendUniqueString(classification.Reasons, "archived")
	case strings.TrimSpace(claim.SupersededByClaimID) != "" || status == "SUPERSEDED":
		classification.Freshness = "superseded"
		classification.DowngradeReason = "superseded"
		classification.Reasons = appendUniqueString(classification.Reasons, "superseded")
		classification.NeedsAttention = true
	case status == "STALE":
		classification.Freshness = "stale"
		classification.DowngradeReason = "stale"
		classification.Reasons = appendUniqueString(classification.Reasons, "stale")
		classification.NeedsAttention = true
	case status == "DISPUTED":
		classification.Freshness = "disputed"
		classification.DowngradeReason = "disputed"
		classification.Reasons = appendUniqueString(classification.Reasons, "disputed")
		classification.NeedsAttention = true
	case strings.TrimSpace(claim.ConflictsClaimID) != "":
		classification.Freshness = "conflicted"
		classification.DowngradeReason = "conflicting_claim"
		classification.Reasons = appendUniqueString(classification.Reasons, "conflicting_claim")
		classification.NeedsAttention = true
	case status == "REVIEW":
		classification.Freshness = "review"
		classification.DowngradeReason = "review_pending"
		classification.Reasons = appendUniqueString(classification.Reasons, "review_pending")
		if knowledgeClaimReviewDueExpired(claim.ReviewDueAt, referenceAt) {
			classification.Freshness = "review_overdue"
			classification.DowngradeReason = "review_overdue"
			classification.Reasons = appendUniqueString(classification.Reasons, "review_overdue")
			classification.NeedsAttention = true
		}
	case knowledgeClaimReviewDueExpired(claim.ReviewDueAt, referenceAt):
		classification.Freshness = "review_overdue"
		classification.DowngradeReason = "review_overdue"
		classification.Reasons = appendUniqueString(classification.Reasons, "review_overdue")
		classification.NeedsAttention = true
	case status != "ACTIVE" && status != "CONFIRMED":
		classification.Freshness = strings.ToLower(status)
		classification.DowngradeReason = strings.ToLower(status)
		classification.Reasons = appendUniqueString(classification.Reasons, strings.ToLower(status))
		classification.NeedsAttention = true
	}

	if classification.ProvenanceStrength == "weak" && classification.DowngradeReason == "" {
		classification.DowngradeReason = "weak_provenance"
		classification.NeedsAttention = true
	}
	classification.Authoritative = classification.Freshness == "fresh" &&
		classification.ProvenanceStrength == "strong" &&
		(status == "ACTIVE" || status == "CONFIRMED") &&
		archivedAt == ""
	return classification
}

func knowledgeClaimProvenanceStrength(claim KnowledgeClaimRecord) string {
	if strings.TrimSpace(claim.SourceKind) == "" || strings.TrimSpace(claim.SourceID) == "" {
		return "weak"
	}
	return "strong"
}

func knowledgeClaimReviewDueExpired(reviewDueAt *string, referenceAt string) bool {
	due := strings.TrimSpace(derefString(reviewDueAt))
	referenceAt = strings.TrimSpace(referenceAt)
	if due == "" || referenceAt == "" {
		return false
	}
	dueTime, dueErr := time.Parse(time.RFC3339Nano, due)
	refTime, refErr := time.Parse(time.RFC3339Nano, referenceAt)
	if dueErr == nil && refErr == nil {
		return !dueTime.After(refTime)
	}
	return due <= referenceAt
}

func knowledgeClaimFreshnessExample(claim KnowledgeClaimRecord, classification knowledgeClaimFreshnessClassification) KnowledgeClaimFreshnessExample {
	return KnowledgeClaimFreshnessExample{
		ClaimID:             strings.TrimSpace(claim.ClaimID),
		ClaimType:           strings.TrimSpace(claim.ClaimType),
		Status:              normalizeKnowledgeClaimStatus(claim.Status),
		Freshness:           classification.Freshness,
		ProvenanceStrength:  classification.ProvenanceStrength,
		DowngradeReason:     classification.DowngradeReason,
		FreshnessReasons:    append([]string(nil), classification.Reasons...),
		ReviewDueAt:         strings.TrimSpace(derefString(claim.ReviewDueAt)),
		SupersededByClaimID: strings.TrimSpace(claim.SupersededByClaimID),
		ConflictsClaimID:    strings.TrimSpace(claim.ConflictsClaimID),
		SourceKind:          strings.TrimSpace(claim.SourceKind),
		SourceID:            strings.TrimSpace(claim.SourceID),
	}
}
