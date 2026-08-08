package living_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func claimLivingTestWorkspaceAuthority(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) sqlite.WorkspaceAuthorityRecord {
	t.Helper()

	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	now := time.Now().UTC()
	referenceAt := now.Format(time.RFC3339Nano)
	registeredAt := strings.TrimSpace(node.RegisteredAt)
	if registeredAt == "" {
		registeredAt = referenceAt
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(authority_node_id) DO UPDATE SET
	node_kind = excluded.node_kind,
	host_label = excluded.host_label,
	boot_instance_id = excluded.boot_instance_id,
	last_seen_at = excluded.last_seen_at,
	status = excluded.status
`, node.AuthorityNodeID, node.NodeKind, node.HostLabel, node.BootInstanceID, registeredAt, referenceAt, string(sqlite.RuntimeNodeStatusOnline)); err != nil {
		t.Fatalf("seed runtime authority node for %s: %v", workspaceID, err)
	}
	if existing, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace"); err == nil {
		if existing.Status == sqlite.WorkspaceAuthorityStatusActive && existing.HolderAuthorityNodeID == node.AuthorityNodeID {
			return existing
		}
		term := existing.Term
		if existing.HolderAuthorityNodeID != node.AuthorityNodeID || existing.Status != sqlite.WorkspaceAuthorityStatusActive {
			term++
		}
		if term <= 0 {
			term = 1
		}
		commitWatermark := existing.CommitWatermark
		if commitWatermark <= 0 {
			commitWatermark = 1
		}
		appliedWatermark := existing.AppliedWatermark
		if appliedWatermark <= 0 {
			appliedWatermark = 1
		}
		leaseToken := strings.TrimSpace(existing.LeaseToken)
		if leaseToken == "" || existing.HolderAuthorityNodeID != node.AuthorityNodeID || existing.Status != sqlite.WorkspaceAuthorityStatusActive {
			leaseToken = "lease-living-tests-" + workspaceID
		}
		if _, err := store.DB().ExecContext(ctx, `
INSERT INTO workspace_authority(
	workspace_id, scope, holder_authority_node_id, lease_token, term, lease_expires_at,
	commit_watermark, applied_watermark, status, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(workspace_id, scope) DO UPDATE SET
	holder_authority_node_id = excluded.holder_authority_node_id,
	lease_token = excluded.lease_token,
	term = excluded.term,
	lease_expires_at = excluded.lease_expires_at,
	commit_watermark = excluded.commit_watermark,
	applied_watermark = excluded.applied_watermark,
	status = excluded.status,
	updated_at = excluded.updated_at
`, workspaceID, "workspace", node.AuthorityNodeID, leaseToken, term, now.Add(time.Hour).Format(time.RFC3339Nano), commitWatermark, appliedWatermark, string(sqlite.WorkspaceAuthorityStatusActive), referenceAt); err != nil {
			t.Fatalf("seed workspace authority for %s: %v", workspaceID, err)
		}
		record, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
		if err != nil {
			t.Fatalf("reload seeded workspace authority for %s: %v", workspaceID, err)
		}
		return record
	}

	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO workspace_authority(
	workspace_id, scope, holder_authority_node_id, lease_token, term, lease_expires_at,
	commit_watermark, applied_watermark, status, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(workspace_id, scope) DO UPDATE SET
	holder_authority_node_id = excluded.holder_authority_node_id,
	lease_token = excluded.lease_token,
	term = excluded.term,
	lease_expires_at = excluded.lease_expires_at,
	commit_watermark = excluded.commit_watermark,
	applied_watermark = excluded.applied_watermark,
	status = excluded.status,
	updated_at = excluded.updated_at
`, workspaceID, "workspace", node.AuthorityNodeID, "lease-living-tests-"+workspaceID, int64(1), now.Add(time.Hour).Format(time.RFC3339Nano), int64(1), int64(1), string(sqlite.WorkspaceAuthorityStatusActive), referenceAt); err != nil {
		t.Fatalf("seed workspace authority for %s: %v", workspaceID, err)
	}
	record, err := store.GetWorkspaceAuthority(ctx, workspaceID, "workspace")
	if err != nil {
		t.Fatalf("reload seeded workspace authority for %s: %v", workspaceID, err)
	}
	return record
}
