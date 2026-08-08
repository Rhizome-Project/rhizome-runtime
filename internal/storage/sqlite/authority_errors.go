package sqlite

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
)

type AuthorityRejectCode string

const (
	AuthorityRejectMissing        AuthorityRejectCode = "authority_missing"
	AuthorityRejectStale          AuthorityRejectCode = "authority_stale"
	AuthorityRejectTermMismatch   AuthorityRejectCode = "authority_term_mismatch"
	AuthorityRejectLeaseExpired   AuthorityRejectCode = "authority_lease_expired"
	AuthorityRejectRejoinRejected AuthorityRejectCode = "authority_rejoin_rejected"
)

type AuthorityRejectError struct {
	RejectCode               AuthorityRejectCode      `json:"reject_code"`
	RejectMessage            string                   `json:"reject_message"`
	WorkspaceID              string                   `json:"workspace_id,omitempty"`
	Scope                    string                   `json:"scope,omitempty"`
	HolderAuthorityNodeID    string                   `json:"holder_authority_node_id,omitempty"`
	ExpectedAuthorityNodeID  string                   `json:"expected_authority_node_id,omitempty"`
	Term                     int64                    `json:"term,omitempty"`
	ExpectedTerm             int64                    `json:"expected_term,omitempty"`
	LeaseTokenFingerprint    string                   `json:"lease_token_fingerprint,omitempty"`
	LeaseExpiresAt           string                   `json:"lease_expires_at,omitempty"`
	ReferenceAt              string                   `json:"reference_at,omitempty"`
	CommitWatermark          int64                    `json:"commit_watermark,omitempty"`
	AppliedWatermark         int64                    `json:"applied_watermark,omitempty"`
	AuthorityStatus          WorkspaceAuthorityStatus `json:"authority_status,omitempty"`
	AuthorityRecordUpdatedAt string                   `json:"authority_record_updated_at,omitempty"`
	Retryable                bool                     `json:"retryable"`
	Surface                  string                   `json:"surface,omitempty"`
}

func (e *AuthorityRejectError) Error() string {
	if e == nil {
		return ""
	}
	if msg := strings.TrimSpace(e.RejectMessage); msg != "" {
		return msg
	}
	if code := strings.TrimSpace(string(e.RejectCode)); code != "" {
		return "authority rejected: " + code
	}
	return "authority rejected"
}

type authorityRejectContext struct {
	WorkspaceID              string
	Scope                    string
	HolderAuthorityNodeID    string
	ExpectedAuthorityNodeID  string
	Term                     int64
	ExpectedTerm             int64
	LeaseToken               string
	LeaseExpiresAt           string
	ReferenceAt              string
	CommitWatermark          int64
	AppliedWatermark         int64
	AuthorityStatus          WorkspaceAuthorityStatus
	AuthorityRecordUpdatedAt string
	Retryable                bool
	Surface                  string
}

func authorityRejectError(code AuthorityRejectCode, message string, record WorkspaceAuthorityRecord, ctx authorityRejectContext) error {
	workspaceID := strings.TrimSpace(ctx.WorkspaceID)
	if workspaceID == "" {
		workspaceID = strings.TrimSpace(record.WorkspaceID)
	}
	scope := normalizeWorkspaceAuthorityScope(firstNonEmpty(strings.TrimSpace(ctx.Scope), strings.TrimSpace(record.Scope)))
	holder := strings.TrimSpace(firstNonEmpty(ctx.HolderAuthorityNodeID, record.HolderAuthorityNodeID))
	term := ctx.Term
	if term == 0 {
		term = record.Term
	}
	leaseExpiresAt := strings.TrimSpace(firstNonEmpty(ctx.LeaseExpiresAt, record.LeaseExpiresAt))
	commitWatermark := ctx.CommitWatermark
	if commitWatermark == 0 {
		commitWatermark = record.CommitWatermark
	}
	appliedWatermark := ctx.AppliedWatermark
	if appliedWatermark == 0 {
		appliedWatermark = record.AppliedWatermark
	}
	status := ctx.AuthorityStatus
	if status == "" {
		status = record.Status
	}
	updatedAt := strings.TrimSpace(firstNonEmpty(ctx.AuthorityRecordUpdatedAt, record.UpdatedAt))
	token := strings.TrimSpace(ctx.LeaseToken)
	if token == "" {
		token = strings.TrimSpace(record.LeaseToken)
	}
	return &AuthorityRejectError{
		RejectCode:               code,
		RejectMessage:            strings.TrimSpace(message),
		WorkspaceID:              workspaceID,
		Scope:                    scope,
		HolderAuthorityNodeID:    holder,
		ExpectedAuthorityNodeID:  strings.TrimSpace(ctx.ExpectedAuthorityNodeID),
		Term:                     term,
		ExpectedTerm:             ctx.ExpectedTerm,
		LeaseTokenFingerprint:    authorityLeaseTokenFingerprint(token),
		LeaseExpiresAt:           leaseExpiresAt,
		ReferenceAt:              strings.TrimSpace(ctx.ReferenceAt),
		CommitWatermark:          commitWatermark,
		AppliedWatermark:         appliedWatermark,
		AuthorityStatus:          status,
		AuthorityRecordUpdatedAt: updatedAt,
		Retryable:                ctx.Retryable,
		Surface:                  strings.TrimSpace(ctx.Surface),
	}
}

func authorityLeaseTokenFingerprint(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(token))
	encoded := hex.EncodeToString(sum[:])
	if len(encoded) > 16 {
		encoded = encoded[:16]
	}
	return "sha256:" + encoded
}

func IsAuthorityReject(err error) bool {
	var target *AuthorityRejectError
	return errors.As(err, &target)
}

func AsAuthorityReject(err error) (*AuthorityRejectError, bool) {
	var target *AuthorityRejectError
	if !errors.As(err, &target) || target == nil {
		return nil, false
	}
	return target, true
}
