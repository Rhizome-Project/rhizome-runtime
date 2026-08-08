package sqlite

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestTransferWorkspaceAuthorityAllowsNewHolderFencedAppendAfterTransferBoundary(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: "ws-d2a-transfer-boundary",
		Title:       "D2A Transfer Boundary",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	currentNode, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	referenceAt := time.Now().UTC().Round(0)
	peerNodeID := "authnode-999-450"
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		peerNodeID,
		"sqlite_peer_store",
		"peer-host",
		"boot-"+peerNodeID,
		referenceAt.Format(time.RFC3339Nano),
		referenceAt.Format(time.RFC3339Nano),
		string(RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("insert peer runtime node: %v", err)
	}
	if _, _, err := store.ClaimWorkspaceAuthority(ctx, WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-d2a-transfer-boundary",
		HolderAuthorityNodeID: currentNode.AuthorityNodeID,
		LeaseToken:            "lease-d2a-transfer-1",
		Term:                  1,
		LeaseExpiresAt:        referenceAt.Add(time.Hour).Format(time.RFC3339Nano),
		CommitWatermark:       2,
		AppliedWatermark:      1,
		ReferenceAt:           referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}
	if _, _, err := store.PutCapabilityPolicyWithEvent(ctx, CapabilityPolicyInput{
		WorkspaceID: "ws-d2a-transfer-boundary",
		SubjectType: "agent",
		SubjectID:   "agent-d2a",
		Capability:  "tool.call",
		ToolID:      "before-transfer",
		Effect:      "DENY",
		Reason:      "advance journal head",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("seed pre-transfer event: %v", err)
	}

	var journalHead int64
	if err := store.DB().QueryRowContext(ctx, `
SELECT COALESCE(MAX(ingest_seq), 0)
  FROM runtime_events
 WHERE workspace_id = ?`,
		"ws-d2a-transfer-boundary",
	).Scan(&journalHead); err != nil {
		t.Fatalf("query runtime journal head: %v", err)
	}

	record, transferEvent, err := store.TransferWorkspaceAuthority(ctx, WorkspaceAuthorityTransferInput{
		WorkspaceID:                  "ws-d2a-transfer-boundary",
		Scope:                        authorityScopeWorkspace,
		CurrentHolderAuthorityNodeID: currentNode.AuthorityNodeID,
		CurrentLeaseToken:            "lease-d2a-transfer-1",
		CurrentTerm:                  1,
		NewHolderAuthorityNodeID:     peerNodeID,
		NewLeaseToken:                "lease-d2a-transfer-2",
		NewTerm:                      2,
		LeaseExpiresAt:               referenceAt.Add(2 * time.Hour).Format(time.RFC3339Nano),
		CommitWatermark:              journalHead,
		AppliedWatermark:             1,
		ActorType:                    authorityActorTypeSystem,
		ActorID:                      "tests",
		ReferenceAt:                  referenceAt.Add(30 * time.Minute).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("transfer workspace authority: %v", err)
	}
	if transferEvent.IngestSeq != journalHead+1 {
		t.Fatalf("expected authority.transferred boundary at %d, got %+v", journalHead+1, transferEvent)
	}

	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin fenced append tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var appended RuntimeEventRecord
	fenceInput := WorkspaceAuthorityFenceInput{
		WorkspaceID:                   "ws-d2a-transfer-boundary",
		Scope:                         authorityScopeWorkspace,
		ExpectedHolderAuthorityNodeID: peerNodeID,
		ExpectedLeaseToken:            "lease-d2a-transfer-2",
		ExpectedTerm:                  2,
		ReferenceAt:                   referenceAt.Add(31 * time.Minute).Format(time.RFC3339Nano),
	}
	if _, err := store.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var appendErr error
		appended, appendErr = store.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: "ws-d2a-transfer-boundary",
			EventType:   "d2a.transfer.append",
			EntityType:  "transfer_probe",
			EntityID:    "probe-1",
			ActorType:   "system",
			ActorID:     "tests",
			PayloadJSON: `{"probe":"new-holder-append"}`,
			CreatedAt:   referenceAt.Add(31 * time.Minute).Format(time.RFC3339Nano),
		})
		return appendErr
	}); err != nil {
		t.Fatalf("append first new-holder fenced runtime event: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit first new-holder fenced append: %v", err)
	}

	if appended.IngestSeq != transferEvent.IngestSeq+1 {
		t.Fatalf("expected first new-holder fenced append immediately after transfer boundary, transfer=%+v appended=%+v", transferEvent, appended)
	}
	assertTransferAuthorityMetadata(t, appended, record)
}

func assertTransferAuthorityMetadata(t *testing.T, event RuntimeEventRecord, authority WorkspaceAuthorityRecord) {
	t.Helper()

	if event.AuthorityHolderNodeID != authority.HolderAuthorityNodeID {
		t.Fatalf("expected authority holder %q, got %q", authority.HolderAuthorityNodeID, event.AuthorityHolderNodeID)
	}
	if event.AuthorityTerm != authority.Term {
		t.Fatalf("expected authority term %d, got %d", authority.Term, event.AuthorityTerm)
	}
	expectedFingerprint := authorityLeaseTokenFingerprint(authority.LeaseToken)
	if event.AuthorityLeaseTokenFingerprint != expectedFingerprint {
		t.Fatalf("expected authority lease fingerprint %q, got %q", expectedFingerprint, event.AuthorityLeaseTokenFingerprint)
	}
}
