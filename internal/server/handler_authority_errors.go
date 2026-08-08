package server

import (
	"context"
	"errors"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func authorityRejectRPCError(err error, surface string) *RPCError {
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject == nil {
		return nil
	}
	surface = strings.TrimSpace(firstNonEmpty(surface, reject.Surface))
	return &RPCError{
		Code:    errCodePermissionDenied,
		Message: "authority rejected",
		Details: map[string]any{
			"error_kind":                  "authority_reject",
			"reject_code":                 string(reject.RejectCode),
			"reject_message":              strings.TrimSpace(reject.RejectMessage),
			"workspace_id":                strings.TrimSpace(reject.WorkspaceID),
			"scope":                       strings.TrimSpace(reject.Scope),
			"holder_authority_node_id":    strings.TrimSpace(reject.HolderAuthorityNodeID),
			"expected_authority_node_id":  strings.TrimSpace(reject.ExpectedAuthorityNodeID),
			"term":                        reject.Term,
			"expected_term":               reject.ExpectedTerm,
			"lease_token_fingerprint":     strings.TrimSpace(reject.LeaseTokenFingerprint),
			"lease_expires_at":            strings.TrimSpace(reject.LeaseExpiresAt),
			"reference_at":                strings.TrimSpace(reject.ReferenceAt),
			"commit_watermark":            reject.CommitWatermark,
			"applied_watermark":           reject.AppliedWatermark,
			"authority_status":            strings.TrimSpace(string(reject.AuthorityStatus)),
			"authority_record_updated_at": strings.TrimSpace(reject.AuthorityRecordUpdatedAt),
			"retryable":                   reject.Retryable,
			"surface":                     surface,
		},
	}
}

func rpcErrorFromStoreAuthority(err error, surface string) *RPCError {
	if rpcErr := authorityRejectRPCError(err, surface); rpcErr != nil {
		return rpcErr
	}
	return &RPCError{Code: errCodeInternal, Message: err.Error()}
}

func rpcErrorFromProjectPatchQueueStore(err error, surface string) *RPCError {
	if rpcErr := authorityRejectRPCError(err, surface); rpcErr != nil {
		return rpcErr
	}
	if errors.Is(err, sqlite.ErrProjectPatchQueueInvalid) {
		return &RPCError{
			Code:    errCodeInvalidParams,
			Message: err.Error(),
			Details: map[string]any{
				"error_kind": "project_patch_queue_invalid",
				"surface":    strings.TrimSpace(surface),
			},
		}
	}
	return &RPCError{Code: errCodeInternal, Message: err.Error()}
}

func (h *Handler) loadWorkspaceTimeAuthority(ctx context.Context, workspaceID, surface string) (sqlite.WorkspaceTimeAuthority, *RPCError) {
	authority, err := h.store.GetWorkspaceTimeAuthority(ctx, strings.TrimSpace(workspaceID))
	if err != nil {
		return sqlite.WorkspaceTimeAuthority{}, rpcErrorFromStoreAuthority(err, surface)
	}
	return authority, nil
}
