package server

import (
	"errors"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestAuthorityRejectRPCErrorUsesPermissionDeniedAndStructuredDetails(t *testing.T) {
	rpcErr := authorityRejectRPCError(&sqlite.AuthorityRejectError{
		RejectCode:               sqlite.AuthorityRejectLeaseExpired,
		RejectMessage:            "workspace authority lease is expired",
		WorkspaceID:              "ws-auth",
		Scope:                    "workspace:control",
		HolderAuthorityNodeID:    "authnode-live",
		ExpectedAuthorityNodeID:  "authnode-live",
		Term:                     7,
		ExpectedTerm:             7,
		LeaseTokenFingerprint:    "sha256:deadbeefcafebabe",
		LeaseExpiresAt:           "2026-04-10T10:00:00Z",
		ReferenceAt:              "2026-04-10T10:05:00Z",
		CommitWatermark:          128,
		AppliedWatermark:         126,
		AuthorityStatus:          sqlite.WorkspaceAuthorityStatusExpired,
		AuthorityRecordUpdatedAt: "2026-04-10T09:59:00Z",
		Retryable:                false,
	}, "workspace.control.command.request")
	if rpcErr == nil {
		t.Fatal("expected authority reject RPC error")
	}
	if rpcErr.Code != errCodePermissionDenied {
		t.Fatalf("expected permission denied code, got %+v", rpcErr)
	}
	if rpcErr.Message != "authority rejected" {
		t.Fatalf("unexpected authority reject message: %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority details, got %+v", rpcErr.Details)
	}
	if details["error_kind"] != "authority_reject" {
		t.Fatalf("expected authority error kind, got %+v", details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectLeaseExpired) {
		t.Fatalf("expected authority reject code in details, got %+v", details)
	}
	if details["surface"] != "workspace.control.command.request" {
		t.Fatalf("expected surface in details, got %+v", details)
	}
	if details["holder_authority_node_id"] != "authnode-live" || details["lease_token_fingerprint"] != "sha256:deadbeefcafebabe" {
		t.Fatalf("expected authority holder metadata in details, got %+v", details)
	}
}

func TestRPCErrorFromStoreAuthorityLeavesGenericErrorsInternal(t *testing.T) {
	rpcErr := rpcErrorFromStoreAuthority(errors.New("plain store failure"), "workspace.policy.put")
	if rpcErr == nil {
		t.Fatal("expected generic RPC error")
	}
	if rpcErr.Code != errCodeInternal || rpcErr.Message != "plain store failure" {
		t.Fatalf("unexpected generic RPC error %+v", rpcErr)
	}
}
