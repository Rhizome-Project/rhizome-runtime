package sqlite

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	testDBCacheBytes []byte
	testDBCacheOnce  sync.Once
)

func NewTestStore(t *testing.T) *Store {
	t.Helper()

	testDBCacheOnce.Do(func() {
		cacheDir, err := os.MkdirTemp("", "rhizome-sqlite-master-cache-*")
		if err != nil {
			panic("MkdirTemp failed for master cache: " + err.Error())
		}
		defer func() { _ = os.RemoveAll(cacheDir) }()

		dbPath := filepath.Join(cacheDir, "rhizome-sqlite-master-cache.db")
		masterStore, err := NewStore(dbPath)
		if err != nil {
			panic("NewStore failed for master cache: " + err.Error())
		}
		if err := masterStore.ApplyMigrations(context.Background()); err != nil {
			_ = masterStore.Close()
			panic("ApplyMigrations failed for master cache: " + err.Error())
		}
		_ = masterStore.Close()

		bytes, err := os.ReadFile(dbPath)
		if err != nil {
			panic("ReadFile failed for master cache: " + err.Error())
		}
		testDBCacheBytes = bytes
	})

	dbPath := filepath.Join(t.TempDir(), "rhizome.db")
	if err := os.WriteFile(dbPath, testDBCacheBytes, 0644); err != nil {
		t.Fatalf("WriteFile failed to copy cache to temp dir: %v", err)
	}

	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	store.AllowLegacyPatchOnlySubmitsForTesting()
	node, err := store.EnsureLocalAuthorityNode(context.Background())
	if err != nil {
		_ = store.Close()
		t.Fatalf("EnsureLocalAuthorityNode failed: %v", err)
	}
	authorityNodeIDLiteral := strings.ReplaceAll(node.AuthorityNodeID, `'`, `''`)
	triggerSQL := fmt.Sprintf(`
CREATE TRIGGER IF NOT EXISTS test_seed_workspace_authority_after_insert
AFTER INSERT ON workspaces
WHEN instr(lower(NEW.workspace_id), 'memory') > 0
BEGIN
	INSERT INTO workspace_authority(
		workspace_id,
		scope,
		holder_authority_node_id,
		lease_token,
		term,
		lease_expires_at,
		commit_watermark,
		applied_watermark,
		status,
		updated_at
	) VALUES (
		NEW.workspace_id,
		'workspace',
		'%s',
		'lease-test-auto-' || NEW.workspace_id,
		1,
		strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ','now','+1 hour'),
		1,
		1,
		'ACTIVE',
		strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ','now')
	)
	ON CONFLICT(workspace_id, scope) DO NOTHING;
END
`, authorityNodeIDLiteral)
	if _, err := store.db.ExecContext(context.Background(), triggerSQL); err != nil {
		_ = store.Close()
		t.Fatalf("install workspace authority seed trigger failed: %v", err)
	}

	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func claimTestWorkspaceAuthority(t testing.TB, ctx context.Context, store *Store, workspaceID string) WorkspaceAuthorityRecord {
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
	if _, err := store.db.ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(authority_node_id) DO UPDATE SET
	node_kind = excluded.node_kind,
	host_label = excluded.host_label,
	boot_instance_id = excluded.boot_instance_id,
	last_seen_at = excluded.last_seen_at,
	status = excluded.status
`, node.AuthorityNodeID, node.NodeKind, node.HostLabel, node.BootInstanceID, registeredAt, referenceAt, string(RuntimeNodeStatusOnline)); err != nil {
		t.Fatalf("seed runtime authority node for %s: %v", workspaceID, err)
	}
	if existing, err := store.GetWorkspaceAuthority(ctx, workspaceID, authorityScopeWorkspace); err == nil {
		if existing.Status == WorkspaceAuthorityStatusActive && existing.HolderAuthorityNodeID == node.AuthorityNodeID {
			return existing
		}
		term := existing.Term
		if existing.HolderAuthorityNodeID != node.AuthorityNodeID || existing.Status != WorkspaceAuthorityStatusActive {
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
		if leaseToken == "" || existing.HolderAuthorityNodeID != node.AuthorityNodeID || existing.Status != WorkspaceAuthorityStatusActive {
			leaseToken = nextID("lease")
		}
		if _, err := store.db.ExecContext(ctx, `
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
`, workspaceID, authorityScopeWorkspace, node.AuthorityNodeID, leaseToken, term, now.Add(time.Hour).Format(time.RFC3339Nano), commitWatermark, appliedWatermark, string(WorkspaceAuthorityStatusActive), referenceAt); err != nil {
			t.Fatalf("seed workspace authority for %s: %v", workspaceID, err)
		}
		record, err := store.GetWorkspaceAuthority(ctx, workspaceID, authorityScopeWorkspace)
		if err != nil {
			t.Fatalf("reload seeded workspace authority for %s: %v", workspaceID, err)
		}
		return record
	}

	if _, err := store.db.ExecContext(ctx, `
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
`, workspaceID, authorityScopeWorkspace, node.AuthorityNodeID, nextID("lease"), int64(1), now.Add(time.Hour).Format(time.RFC3339Nano), int64(1), int64(1), string(WorkspaceAuthorityStatusActive), referenceAt); err != nil {
		t.Fatalf("seed workspace authority for %s: %v", workspaceID, err)
	}
	record, err := store.GetWorkspaceAuthority(ctx, workspaceID, authorityScopeWorkspace)
	if err != nil {
		t.Fatalf("reload seeded workspace authority for %s: %v", workspaceID, err)
	}
	return record
}

func requireSameWorkspaceTimeAuthorityFields(t *testing.T, got, want WorkspaceTimeAuthority) {
	t.Helper()

	if got.WorkspaceID != want.WorkspaceID || got.CurrentEpoch != want.CurrentEpoch || got.PolicyMode != want.PolicyMode || got.EpochAnchorAt != want.EpochAnchorAt || got.RuntimeEventAnchorAt != want.RuntimeEventAnchorAt || got.ReferenceAt == "" {
		t.Fatalf("unexpected workspace time authority fields: got=%+v want=%+v", got, want)
	}
	requireWorkspaceTimeAuthorityTemporalContract(t, got)
	requireWorkspaceTimeAuthorityTemporalContract(t, want)
}

func requireWorkspaceTimeAuthorityTemporalContract(t *testing.T, authority WorkspaceTimeAuthority) {
	t.Helper()
	if authority.TemporalContract == nil {
		t.Fatalf("expected workspace time authority temporal contract, got %+v", authority)
	}
	contract := authority.TemporalContract
	if contract.SchemaVersion != temporalContractSchemaVersion ||
		contract.Domain != "control_epoch" ||
		contract.HorizonKind != "current_epoch" ||
		contract.Basis != temporalBasisControlEpoch ||
		contract.Mapping != temporalMappingExplicitPhi ||
		contract.WallClockComparable ||
		contract.State != temporalStateLive {
		t.Fatalf("unexpected workspace time authority temporal contract %+v", contract)
	}
	if contract.CurrentEpoch != authority.CurrentEpoch || contract.TargetEpoch != authority.CurrentEpoch {
		t.Fatalf("expected workspace time authority temporal contract to pin current epoch, got contract=%+v authority=%+v", contract, authority)
	}
	if contract.ReferenceAt != authority.ReferenceAt {
		t.Fatalf("expected workspace time authority temporal contract to reuse reference_at, got contract=%+v authority=%+v", contract, authority)
	}
}

func mustReconcileMemoryProjectionWorkspace(t *testing.T, ctx context.Context, store *Store, workspaceID string) {
	t.Helper()

	if _, err := store.ReconcileMemoryProjectionWorkspace(ctx, workspaceID, 64); err != nil {
		t.Fatalf("reconcile memory projection workspace %s: %v", workspaceID, err)
	}
}
