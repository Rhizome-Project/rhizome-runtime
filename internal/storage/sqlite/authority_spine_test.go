package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestEnsureLocalAuthorityNodePersistsStableAuthorityNodeID(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "authority-stable.db")

	firstStore, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new first store: %v", err)
	}
	ctx := context.Background()
	if err := firstStore.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations first store: %v", err)
	}
	firstRecord, err := firstStore.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node first store: %v", err)
	}
	firstBoot := firstRecord.BootInstanceID
	if err := firstStore.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	secondStore, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new second store: %v", err)
	}
	defer func() { _ = secondStore.Close() }()
	if err := secondStore.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations second store: %v", err)
	}
	secondRecord, err := secondStore.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node second store: %v", err)
	}

	if secondRecord.AuthorityNodeID != firstRecord.AuthorityNodeID {
		t.Fatalf("expected authority_node_id to persist across reopen, first=%+v second=%+v", firstRecord, secondRecord)
	}
	if secondRecord.BootInstanceID == firstBoot {
		t.Fatalf("expected boot_instance_id to refresh per store boot, first=%q second=%q", firstBoot, secondRecord.BootInstanceID)
	}
}

func TestEnsureLocalAuthorityNodePopulatesDiagnosticsSnapshot(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	record, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}

	diag := store.LocalAuthorityNodeDiagnostics()
	if diag.State != "ok" {
		t.Fatalf("expected authority diagnostics ok, got %+v", diag)
	}
	if diag.AuthorityNodeID != record.AuthorityNodeID {
		t.Fatalf("expected diagnostics authority id %q, got %+v", record.AuthorityNodeID, diag)
	}
	if diag.BootInstanceID != record.BootInstanceID || diag.HostLabel != record.HostLabel {
		t.Fatalf("expected diagnostics to mirror runtime node record, record=%+v diag=%+v", record, diag)
	}
}

func TestEnsureLocalAuthorityNodeRejectsMalformedIdentityFile(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "authority-malformed.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	identityPath := filepath.Join(filepath.Dir(dbPath), ".rhizome-authority-node-id")
	if err := os.WriteFile(identityPath, []byte("not-an-authority-id\n"), 0o600); err != nil {
		t.Fatalf("write malformed identity file: %v", err)
	}

	if _, err := store.EnsureLocalAuthorityNode(ctx); err == nil {
		t.Fatal("expected malformed authority identity file to fail")
	}
}

func TestCurrentAuthorityNodeDiagnosticsDetectsIdentityDrift(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "authority-drift.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	record, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}

	identityPath := filepath.Join(filepath.Dir(dbPath), ".rhizome-authority-node-id")
	if err := os.WriteFile(identityPath, []byte("authnode-999-1\n"), 0o600); err != nil {
		t.Fatalf("overwrite authority identity file: %v", err)
	}

	diag := store.CurrentAuthorityNodeDiagnostics(ctx)
	if diag.State != "error" {
		t.Fatalf("expected authority diagnostics error after identity drift, got %+v", diag)
	}
	if diag.AuthorityNodeID == record.AuthorityNodeID {
		t.Fatalf("expected drift diagnostics to stop trusting cached authority id, diag=%+v record=%+v", diag, record)
	}
}

func TestCurrentAuthorityNodeDiagnosticsMarksOfflineRuntimeNodeDegraded(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	record, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `UPDATE runtime_nodes SET status = ? WHERE authority_node_id = ?`,
		string(sqlite.RuntimeNodeStatusOffline),
		record.AuthorityNodeID,
	); err != nil {
		t.Fatalf("mark runtime node offline: %v", err)
	}

	diag := store.CurrentAuthorityNodeDiagnostics(ctx)
	if diag.State != "degraded" {
		t.Fatalf("expected degraded authority diagnostics for offline runtime node, got %+v", diag)
	}
	if diag.Status != string(sqlite.RuntimeNodeStatusOffline) {
		t.Fatalf("expected offline runtime status in diagnostics, got %+v", diag)
	}
	if !strings.Contains(strings.ToLower(diag.Message), "offline") {
		t.Fatalf("expected offline authority diagnostics message, got %+v", diag)
	}
}

func TestCurrentLocalAuthorityLeaseDiagnosticsMarksGraceLeaseDegraded(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-authority-lease-diag-grace"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Authority Lease Diagnostics Grace",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	result, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("ensure local workspace authority: %v", err)
	}
	if result.Status.Authority == nil {
		t.Fatalf("expected authority row after ensure, got %+v", result)
	}

	graceExpiry := time.Now().UTC().Add(-30 * time.Second).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
UPDATE workspace_authority
   SET lease_expires_at = ?, status = ?, updated_at = ?
 WHERE workspace_id = ? AND scope = ?`,
		graceExpiry,
		string(sqlite.WorkspaceAuthorityStatusActive),
		time.Now().UTC().Format(time.RFC3339Nano),
		workspaceID,
		"workspace",
	); err != nil {
		t.Fatalf("mark authority lease grace-window stale: %v", err)
	}

	diag := store.CurrentLocalAuthorityLeaseDiagnostics(ctx, "workspace")
	if diag.State != "degraded" {
		t.Fatalf("expected degraded lease diagnostics for grace-window stale lease, got %+v", diag)
	}
	if diag.TotalHeld != 1 || diag.Grace != 1 {
		t.Fatalf("expected one held grace lease, got %+v", diag)
	}
	if len(diag.Items) != 1 {
		t.Fatalf("expected one authority lease diagnostics item, got %+v", diag)
	}
	if diag.Items[0].WorkspaceID != workspaceID || diag.Items[0].LeaseState != "grace" {
		t.Fatalf("expected grace diagnostics item for workspace %q, got %+v", workspaceID, diag.Items[0])
	}
	if diag.Items[0].HolderAuthorityID == "" || diag.Items[0].Term <= 0 {
		t.Fatalf("expected diagnostics item to include holder and term, got %+v", diag.Items[0])
	}
	if diag.Items[0].LeaseExpiresAt == "" {
		t.Fatalf("expected diagnostics item to include lease expiry, got %+v", diag.Items[0])
	}
}

// TestEnsureLocalWorkspaceAuthorityAdoptsOrphanedHolderLease is the CA-25
// regression: deleting the holder's runtime_nodes row (FK ON DELETE SET NULL)
// leaves an ACTIVE lease with a NULL holder that no node holds. The local node
// must self-heal by adopting it on the next ensure, instead of being permanently
// wedged behind a force-break that ForceBreak refuses to perform.
func TestEnsureLocalWorkspaceAuthorityAdoptsOrphanedHolderLease(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-authority-orphaned-holder"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Authority Orphaned Holder",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	first, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("ensure local workspace authority: %v", err)
	}
	if first.Status.Authority == nil || strings.TrimSpace(first.Status.Authority.HolderAuthorityNodeID) == "" {
		t.Fatalf("expected a held authority lease after first ensure, got %+v", first)
	}
	priorTerm := first.Status.Authority.Term

	// Simulate the wedge: delete the holder's runtime_nodes row. The FK
	// ON DELETE SET NULL nulls holder_authority_node_id while the lease stays ACTIVE
	// with a future expiry.
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM runtime_nodes WHERE authority_node_id = ?`, first.Status.Authority.HolderAuthorityNodeID); err != nil {
		t.Fatalf("delete holder runtime node: %v", err)
	}
	var holder sql.NullString
	var status string
	if err := store.DB().QueryRowContext(ctx, `SELECT holder_authority_node_id, status FROM workspace_authority WHERE workspace_id = ? AND scope = ?`, workspaceID, "workspace").Scan(&holder, &status); err != nil {
		t.Fatalf("read wedged authority row: %v", err)
	}
	if holder.Valid && strings.TrimSpace(holder.String) != "" {
		t.Fatalf("expected NULL holder after node deletion, got %q", holder.String)
	}
	if status != string(sqlite.WorkspaceAuthorityStatusActive) {
		t.Fatalf("expected lease to remain ACTIVE (the wedge), got status %q", status)
	}

	// Confirm the wedge: ForceBreak refuses the incomplete (NULL-holder) row, so the
	// pre-fix recovery route is a dead end.
	if _, fbErr := store.ForceBreakWorkspaceAuthority(ctx, sqlite.ForceBreakWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		Scope:       "workspace",
		ActorType:   "operator",
		ActorID:     "tests",
	}); fbErr == nil {
		t.Fatalf("expected force-break to reject the NULL-holder row (documents the wedge)")
	}

	// The fix: the local node adopts the orphaned lease on the next ensure.
	adopted, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("ensure after orphan must adopt, not error: %v", err)
	}
	if adopted.Action != "ADOPTED" {
		t.Fatalf("expected ADOPTED action for orphaned-holder recovery, got %q", adopted.Action)
	}
	if adopted.Status.Authority == nil || strings.TrimSpace(adopted.Status.Authority.HolderAuthorityNodeID) == "" {
		t.Fatalf("expected the local node to hold the lease after adoption, got %+v", adopted.Status)
	}
	if adopted.Status.Authority.Term <= priorTerm {
		t.Fatalf("expected adoption to advance the term beyond %d, got %d", priorTerm, adopted.Status.Authority.Term)
	}
	if adopted.Status.LeaseState != "healthy" {
		t.Fatalf("expected a healthy lease after adoption, got %q", adopted.Status.LeaseState)
	}

	// And a subsequent ensure is a no-op (fully recovered).
	steady, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		Scope:       "workspace",
		ActorType:   "system",
		ActorID:     "tests",
	})
	if err != nil {
		t.Fatalf("steady-state ensure after adoption: %v", err)
	}
	if steady.Action != "UNCHANGED" {
		t.Fatalf("expected UNCHANGED after recovery, got %q", steady.Action)
	}
}

func TestCurrentAuthorityLeaseDiagnosticsMarksForeignLiveLeaseDegraded(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-authority-lease-diag-foreign-live"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Authority Lease Diagnostics Foreign",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	localNode, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}

	now := time.Now().UTC()
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(authority_node_id) DO UPDATE SET
	node_kind = excluded.node_kind,
	host_label = excluded.host_label,
	boot_instance_id = excluded.boot_instance_id,
	last_seen_at = excluded.last_seen_at,
	status = excluded.status
`,
		"authnode-999-401",
		"sqlite_local_store",
		"peer-host",
		"authboot-999-401",
		now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
		string(sqlite.RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("seed foreign runtime node: %v", err)
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
`,
		workspaceID,
		"workspace",
		"authnode-999-401",
		"lease-foreign-live-1",
		int64(4),
		now.Add(10*time.Minute).Format(time.RFC3339Nano),
		int64(7),
		int64(7),
		string(sqlite.WorkspaceAuthorityStatusActive),
		now.Format(time.RFC3339Nano),
	); err != nil {
		t.Fatalf("seed foreign workspace authority: %v", err)
	}

	diag := store.CurrentAuthorityLeaseDiagnostics(ctx, "workspace")
	if diag.State != "degraded" {
		t.Fatalf("expected degraded authority lease diagnostics for foreign live lease, got %+v", diag)
	}
	if diag.LocalAuthorityNodeID != localNode.AuthorityNodeID {
		t.Fatalf("expected local authority node id %q, got %+v", localNode.AuthorityNodeID, diag)
	}
	if diag.TotalHeld != 0 || diag.ForeignLive != 1 {
		t.Fatalf("expected zero local-held and one foreign live lease, got %+v", diag)
	}
	if len(diag.Items) != 1 {
		t.Fatalf("expected one authority lease diagnostics item, got %+v", diag)
	}
	item := diag.Items[0]
	if item.WorkspaceID != workspaceID || item.LeaseState != "foreign_live" {
		t.Fatalf("expected foreign_live item for workspace %q, got %+v", workspaceID, item)
	}
	if item.LocalHolder {
		t.Fatalf("expected foreign live item to report non-local holder, got %+v", item)
	}
	if item.HolderAuthorityID != "authnode-999-401" || !item.LeaseLive {
		t.Fatalf("expected foreign holder and live lease in item, got %+v", item)
	}
}

func TestExpiredLocalWorkspaceAuthorityDoesNotDegradeLeaseDiagnostics(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-authority-lease-diag-expired"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Authority Lease Diagnostics Expired",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	localNode, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}

	now := time.Now().UTC()
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
`,
		workspaceID,
		"workspace",
		localNode.AuthorityNodeID,
		"lease-expired-local-1",
		int64(1),
		now.Add(-10*time.Minute).Format(time.RFC3339Nano),
		int64(0),
		int64(0),
		string(sqlite.WorkspaceAuthorityStatusExpired),
		now.Add(-9*time.Minute).Format(time.RFC3339Nano),
	); err != nil {
		t.Fatalf("seed expired workspace authority: %v", err)
	}

	diag := store.CurrentLocalAuthorityLeaseDiagnostics(ctx, "workspace")
	if diag.State != "ok" {
		t.Fatalf("expected expired local authority row not to degrade diagnostics, got %+v", diag)
	}
	if diag.TotalHeld != 0 || diag.Stale != 0 || diag.Problems != 0 || diag.Grace != 0 {
		t.Fatalf("expected expired local authority row not to count as held/stale/problem, got %+v", diag)
	}
	if len(diag.Items) != 1 {
		t.Fatalf("expected one expired authority diagnostics item, got %+v", diag)
	}
	item := diag.Items[0]
	if item.WorkspaceID != workspaceID || item.LeaseState != "expired" || item.AuthorityStatus != string(sqlite.WorkspaceAuthorityStatusExpired) {
		t.Fatalf("expected expired diagnostics item for workspace %q, got %+v", workspaceID, item)
	}
	if !item.LocalHolder || item.LeaseLive {
		t.Fatalf("expected expired item to preserve holder identity without reporting a live lease, got %+v", item)
	}

	result, err := store.RunLocalWorkspaceAuthorityLeaseMaintenance(ctx, sqlite.LocalWorkspaceAuthorityLeaseMaintenanceInput{
		Scope:     "workspace",
		ActorType: "system",
		ActorID:   "tests",
	})
	if err != nil {
		t.Fatalf("run local workspace authority lease maintenance: %v", err)
	}
	if result.Problems != 0 || result.Expired != 0 {
		t.Fatalf("expected expired local authority row to be skipped without new expiration, got %+v", result)
	}
	if len(result.Items) != 1 || result.Items[0].LeaseState != "expired" || result.Items[0].Action != "ALREADY_EXPIRED" {
		t.Fatalf("expected already-expired maintenance item, got %+v", result.Items)
	}
}

func TestMigration0078WorkspaceAuthoritySchemaAllowsNullHolder(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-schema",
		Title:       "Authority Schema",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO workspace_authority (
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
) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)`,
		"ws-authority-schema",
		"workspace",
		"",
		0,
		"",
		0,
		0,
		string(sqlite.WorkspaceAuthorityStatusExpired),
		"2026-04-10T00:00:00Z",
	); err != nil {
		t.Fatalf("insert workspace_authority row: %v", err)
	}
}

func TestRecordAuthorityEventGrantedWritesRuntimeAndAuditTrail(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-event",
		Title:       "Authority Event",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}

	event, err := store.RecordAuthorityEvent(ctx, sqlite.AuthorityEventInput{
		WorkspaceID:           "ws-authority-event",
		EventType:             sqlite.AuthorityEventGranted,
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-1",
		Term:                  1,
		LeaseExpiresAt:        "2026-04-10T12:00:00Z",
		CommitWatermark:       7,
		AppliedWatermark:      5,
		ReferenceAt:           "2026-04-10T11:59:00Z",
		ActorID:               "authority_spine",
	})
	if err != nil {
		t.Fatalf("record authority event: %v", err)
	}

	if event.EventType != sqlite.AuthorityEventGranted || event.EntityType != "workspace_authority" || event.EntityID != "ws-authority-event/workspace" {
		t.Fatalf("unexpected authority runtime event %+v", event)
	}
	if event.AuthorityHolderNodeID != node.AuthorityNodeID || event.AuthorityTerm != 1 {
		t.Fatalf("expected authority event metadata to carry holder/term, got %+v", event)
	}
	if got, want := event.AuthorityLeaseTokenFingerprint, testAuthorityLeaseTokenFingerprint("lease-1"); got != want {
		t.Fatalf("expected authority event lease fingerprint %q, got %q", want, got)
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode authority event payload: %v", err)
	}
	if payload["typed_event_type"] != "WORKSPACE_AUTHORITY_EVENT" || payload["holder_authority_node_id"] != node.AuthorityNodeID {
		t.Fatalf("unexpected authority event payload %+v", payload)
	}

	auditEvents, err := store.ListAuditEvents(ctx, sqlite.AuditEventFilter{
		EventType:  sqlite.AuthorityEventGranted,
		EntityType: "workspace_authority",
		EntityID:   "ws-authority-event/workspace",
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("list authority audit events: %v", err)
	}
	if len(auditEvents) != 1 {
		t.Fatalf("expected one authority audit event, got %+v", auditEvents)
	}
}

func TestRecordAuthorityEventRejectedCarriesRejectCode(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-reject",
		Title:       "Authority Reject",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	event, err := store.RecordAuthorityEvent(ctx, sqlite.AuthorityEventInput{
		WorkspaceID:             "ws-authority-reject",
		EventType:               sqlite.AuthorityEventRejected,
		RejectCode:              "authority_stale",
		RejectMessage:           "holder term is stale",
		ExpectedAuthorityNodeID: "authnode-100-1",
		ExpectedTerm:            2,
		ActorType:               "system",
		ActorID:                 "authority_spine",
		ReferenceAt:             "2026-04-10T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("record rejected authority event: %v", err)
	}
	if event.AuthorityHolderNodeID != "" || event.AuthorityTerm != 0 || event.AuthorityLeaseTokenFingerprint != "" {
		t.Fatalf("expected rejected authority event not to masquerade as holder-backed metadata, got %+v", event)
	}

	payload := map[string]any{}
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode authority reject payload: %v", err)
	}
	if payload["reject_code"] != "authority_stale" || payload["expected_authority_node_id"] != "authnode-100-1" {
		t.Fatalf("unexpected reject payload %+v", payload)
	}
}

func TestRecordAuthorityEventTransferStartedCarriesAuthorityMetadata(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-transfer-started",
		Title:       "Authority Transfer Started",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	event, err := store.RecordAuthorityEvent(ctx, sqlite.AuthorityEventInput{
		WorkspaceID:                   "ws-authority-transfer-started",
		EventType:                     sqlite.AuthorityEventTransferStarted,
		HolderAuthorityNodeID:         "authnode-100-2",
		PreviousHolderAuthorityNodeID: "authnode-100-1",
		LeaseToken:                    "lease-transfer-started-2",
		Term:                          2,
		LeaseExpiresAt:                "2026-04-10T13:00:00Z",
		CommitWatermark:               11,
		AppliedWatermark:              9,
		ActorType:                     "system",
		ActorID:                       "authority_spine",
		ReferenceAt:                   "2026-04-10T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("record transfer-started authority event: %v", err)
	}

	assertRuntimeEventAuthorityMetadata(t, event, sqlite.WorkspaceAuthorityRecord{
		WorkspaceID:           "ws-authority-transfer-started",
		Scope:                 "workspace",
		HolderAuthorityNodeID: "authnode-100-2",
		LeaseToken:            "lease-transfer-started-2",
		Term:                  2,
		Status:                sqlite.WorkspaceAuthorityStatusTransferring,
	})
}

func TestRecordAuthorityEventRejectsUnknownEventType(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-invalid",
		Title:       "Authority Invalid",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.RecordAuthorityEvent(ctx, sqlite.AuthorityEventInput{
		WorkspaceID: "ws-authority-invalid",
		EventType:   "authority.unknown",
		ActorID:     "authority_spine",
	}); err == nil {
		t.Fatal("expected unsupported authority event type to fail")
	}
}

func TestRecordAuthorityEventRejectsContradictoryStatus(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-status-mismatch",
		Title:       "Authority Status Mismatch",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}

	if _, err := store.RecordAuthorityEvent(ctx, sqlite.AuthorityEventInput{
		WorkspaceID:           "ws-authority-status-mismatch",
		EventType:             sqlite.AuthorityEventGranted,
		HolderAuthorityNodeID: node.AuthorityNodeID,
		Term:                  1,
		Status:                sqlite.WorkspaceAuthorityStatusRejected,
		ActorID:               "authority_spine",
	}); err == nil {
		t.Fatal("expected contradictory authority status to fail")
	}
}

func TestRecordAuthorityEventRejectRequiresCanonicalEvidence(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-reject-evidence",
		Title:       "Authority Reject Evidence",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	base := sqlite.AuthorityEventInput{
		WorkspaceID:             "ws-authority-reject-evidence",
		EventType:               sqlite.AuthorityEventRejected,
		RejectCode:              "authority_stale",
		ExpectedAuthorityNodeID: "authnode-100-1",
		ExpectedTerm:            2,
		ReferenceAt:             "2026-04-10T12:01:00Z",
		ActorID:                 "authority_spine",
	}
	tests := []struct {
		name  string
		input sqlite.AuthorityEventInput
	}{
		{
			name: "missing expected authority node",
			input: func() sqlite.AuthorityEventInput {
				in := base
				in.ExpectedAuthorityNodeID = ""
				return in
			}(),
		},
		{
			name: "missing expected term",
			input: func() sqlite.AuthorityEventInput {
				in := base
				in.ExpectedTerm = 0
				return in
			}(),
		},
		{
			name: "missing reference time",
			input: func() sqlite.AuthorityEventInput {
				in := base
				in.ReferenceAt = ""
				return in
			}(),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := store.RecordAuthorityEvent(ctx, tc.input); err == nil {
				t.Fatalf("expected reject event without canonical evidence to fail for %s", tc.name)
			}
		})
	}
}

func TestClaimWorkspaceAuthorityCreatesActiveRecordAndGrantedEvent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-claim",
		Title:       "Authority Claim",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	referenceAt := time.Now().UTC().Round(0)

	record, event, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-claim",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-claim-1",
		Term:                  1,
		LeaseExpiresAt:        referenceAt.Add(time.Hour).Format(time.RFC3339Nano),
		CommitWatermark:       11,
		AppliedWatermark:      9,
		ReferenceAt:           referenceAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}
	if record.Status != sqlite.WorkspaceAuthorityStatusActive || record.HolderAuthorityNodeID != node.AuthorityNodeID {
		t.Fatalf("unexpected claimed authority record %+v", record)
	}
	if event.EventType != sqlite.AuthorityEventGranted {
		t.Fatalf("expected authority.granted event, got %+v", event)
	}
	assertRuntimeEventAuthorityMetadata(t, event, record)

	got, err := store.GetWorkspaceAuthority(ctx, "ws-authority-claim", "workspace")
	if err != nil {
		t.Fatalf("get workspace authority: %v", err)
	}
	if got.HolderAuthorityNodeID != node.AuthorityNodeID || got.LeaseToken != "lease-claim-1" || got.Term != 1 {
		t.Fatalf("unexpected persisted workspace authority %+v", got)
	}
}

func TestWorkspaceAuthorityTransitionEventsRecordServerReferenceAt(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-authority-transition-server-reference"

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Authority Transition Server Reference",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	currentNode, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	registeredAt := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"authnode-999-203",
		"sqlite_local_store",
		"peer-host",
		"authboot-server-reference-peer",
		registeredAt,
		registeredAt,
		string(sqlite.RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("insert peer runtime node: %v", err)
	}

	callerReferenceAt := time.Now().UTC().Add(24 * time.Hour).Round(0)
	beforeClaim := time.Now().UTC().Add(-time.Second)
	record, event, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           workspaceID,
		HolderAuthorityNodeID: currentNode.AuthorityNodeID,
		LeaseToken:            "lease-server-reference-1",
		Term:                  1,
		LeaseExpiresAt:        callerReferenceAt.Add(time.Hour).Format(time.RFC3339Nano),
		CommitWatermark:       3,
		AppliedWatermark:      2,
		ReferenceAt:           callerReferenceAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}
	assertAuthorityEventReferenceAtInWindow(t, event, beforeClaim, time.Now().UTC().Add(time.Second), callerReferenceAt.Format(time.RFC3339Nano))

	renewCallerReferenceAt := callerReferenceAt.Add(30 * time.Minute)
	beforeRenew := time.Now().UTC().Add(-time.Second)
	record, event, err = store.RenewWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityRenewInput{
		WorkspaceID:           workspaceID,
		HolderAuthorityNodeID: currentNode.AuthorityNodeID,
		LeaseToken:            record.LeaseToken,
		Term:                  record.Term,
		LeaseExpiresAt:        callerReferenceAt.Add(2 * time.Hour).Format(time.RFC3339Nano),
		CommitWatermark:       4,
		AppliedWatermark:      3,
		ReferenceAt:           renewCallerReferenceAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("renew workspace authority: %v", err)
	}
	assertAuthorityEventReferenceAtInWindow(t, event, beforeRenew, time.Now().UTC().Add(time.Second), renewCallerReferenceAt.Format(time.RFC3339Nano))

	transferCallerReferenceAt := callerReferenceAt.Add(time.Hour)
	beforeTransfer := time.Now().UTC().Add(-time.Second)
	_, event, err = store.TransferWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityTransferInput{
		WorkspaceID:                  workspaceID,
		CurrentHolderAuthorityNodeID: currentNode.AuthorityNodeID,
		CurrentLeaseToken:            record.LeaseToken,
		CurrentTerm:                  record.Term,
		NewHolderAuthorityNodeID:     "authnode-999-203",
		NewLeaseToken:                "lease-server-reference-2",
		NewTerm:                      record.Term + 1,
		LeaseExpiresAt:               callerReferenceAt.Add(3 * time.Hour).Format(time.RFC3339Nano),
		CommitWatermark:              5,
		AppliedWatermark:             4,
		ReferenceAt:                  transferCallerReferenceAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("transfer workspace authority: %v", err)
	}
	assertAuthorityEventReferenceAtInWindow(t, event, beforeTransfer, time.Now().UTC().Add(time.Second), transferCallerReferenceAt.Format(time.RFC3339Nano))
}

func TestClaimWorkspaceAuthorityRejectsLiveHolder(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-live-holder",
		Title:       "Authority Live Holder",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	referenceAt := time.Now().UTC().Round(0)
	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-live-holder",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-live-1",
		Term:                  1,
		LeaseExpiresAt:        referenceAt.Add(time.Hour).Format(time.RFC3339Nano),
		ReferenceAt:           referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("initial claim workspace authority: %v", err)
	}

	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-live-holder",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-live-2",
		Term:                  2,
		LeaseExpiresAt:        referenceAt.Add(2 * time.Hour).Format(time.RFC3339Nano),
		ReferenceAt:           referenceAt.Add(30 * time.Minute).Format(time.RFC3339Nano),
	}); err == nil {
		t.Fatal("expected competing live authority claim to fail")
	}
}

func TestClaimWorkspaceAuthorityUsesServerTimeForLiveHolder(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-live-holder-server-time",
		Title:       "Authority Live Holder Server Time",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	now := time.Now().UTC().Round(0)
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"authnode-999-201",
		"sqlite_local_store",
		"peer-host",
		"authboot-server-time-peer",
		now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
		string(sqlite.RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("insert peer runtime node: %v", err)
	}
	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-live-holder-server-time",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-live-server-time-1",
		Term:                  1,
		LeaseExpiresAt:        now.Add(2 * time.Minute).Format(time.RFC3339Nano),
		ReferenceAt:           now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("initial claim workspace authority: %v", err)
	}

	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-live-holder-server-time",
		HolderAuthorityNodeID: "authnode-999-201",
		LeaseToken:            "lease-live-server-time-2",
		Term:                  2,
		LeaseExpiresAt:        now.Add(4 * time.Minute).Format(time.RFC3339Nano),
		ReferenceAt:           now.Add(3 * time.Minute).Format(time.RFC3339Nano),
	}); err == nil {
		t.Fatal("expected future reference_at takeover of server-live authority to fail")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %v", err)
	}
	got, err := store.GetWorkspaceAuthority(ctx, "ws-authority-live-holder-server-time", "workspace")
	if err != nil {
		t.Fatalf("get workspace authority: %v", err)
	}
	if got.HolderAuthorityNodeID != node.AuthorityNodeID || got.LeaseToken != "lease-live-server-time-1" {
		t.Fatalf("expected original live holder to remain, got %+v", got)
	}
}

func TestWorkspaceAuthorityRejectsNewLeaseExpiredAtServerTime(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-new-lease-expired",
		Title:       "Authority New Lease Expired",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	now := time.Now().UTC().Round(0)
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"authnode-999-202",
		"sqlite_local_store",
		"peer-host",
		"authboot-new-lease-peer",
		now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
		string(sqlite.RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("insert peer runtime node: %v", err)
	}
	expiredLease := now.Add(-time.Second).Format(time.RFC3339Nano)
	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-new-lease-expired",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-new-expired-claim",
		Term:                  1,
		LeaseExpiresAt:        expiredLease,
		ReferenceAt:           now.Add(-time.Minute).Format(time.RFC3339Nano),
	}); err == nil {
		t.Fatal("expected expired claim lease to fail")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectLeaseExpired {
		t.Fatalf("expected lease-expired claim reject, got %v", err)
	}
	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-new-lease-expired",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-new-expired-live",
		Term:                  1,
		LeaseExpiresAt:        now.Add(time.Hour).Format(time.RFC3339Nano),
		ReferenceAt:           now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("claim live workspace authority: %v", err)
	}
	if _, _, err := store.RenewWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityRenewInput{
		WorkspaceID:           "ws-authority-new-lease-expired",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-new-expired-live",
		Term:                  1,
		LeaseExpiresAt:        expiredLease,
		CommitWatermark:       1,
		AppliedWatermark:      1,
		ReferenceAt:           now.Format(time.RFC3339Nano),
	}); err == nil {
		t.Fatal("expected expired renew lease to fail")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectLeaseExpired {
		t.Fatalf("expected lease-expired renew reject, got %v", err)
	}
	if _, _, err := store.TransferWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityTransferInput{
		WorkspaceID:                  "ws-authority-new-lease-expired",
		CurrentHolderAuthorityNodeID: node.AuthorityNodeID,
		CurrentLeaseToken:            "lease-new-expired-live",
		CurrentTerm:                  1,
		NewHolderAuthorityNodeID:     "authnode-999-202",
		NewLeaseToken:                "lease-new-expired-transfer",
		NewTerm:                      2,
		LeaseExpiresAt:               expiredLease,
		CommitWatermark:              1,
		AppliedWatermark:             1,
		ReferenceAt:                  now.Format(time.RFC3339Nano),
	}); err == nil {
		t.Fatal("expected expired transfer lease to fail")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectLeaseExpired {
		t.Fatalf("expected lease-expired transfer reject, got %v", err)
	}
}

func TestClaimWorkspaceAuthorityRejectsTransferringOrWatermarkRegression(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("rejects transferring row", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: "ws-authority-claim-transfering",
			Title:       "Authority Claim Transferring",
			CreatedBy:   "developer",
		}); err != nil {
			t.Fatalf("create workspace: %v", err)
		}
		node, err := store.EnsureLocalAuthorityNode(ctx)
		if err != nil {
			t.Fatalf("ensure local authority node: %v", err)
		}
		referenceAt := time.Now().UTC().Round(0)
		if _, err := store.DB().ExecContext(ctx, `
INSERT INTO workspace_authority(workspace_id, scope, holder_authority_node_id, lease_token, term, lease_expires_at, commit_watermark, applied_watermark, status, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			"ws-authority-claim-transfering",
			"workspace",
			node.AuthorityNodeID,
			"lease-transferring-1",
			1,
			referenceAt.Add(time.Hour).Format(time.RFC3339Nano),
			10,
			9,
			string(sqlite.WorkspaceAuthorityStatusTransferring),
			referenceAt.Format(time.RFC3339Nano),
		); err != nil {
			t.Fatalf("insert transferring authority row: %v", err)
		}

		if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
			WorkspaceID:           "ws-authority-claim-transfering",
			HolderAuthorityNodeID: node.AuthorityNodeID,
			LeaseToken:            "lease-transferring-2",
			Term:                  2,
			LeaseExpiresAt:        referenceAt.Add(2 * time.Hour).Format(time.RFC3339Nano),
			CommitWatermark:       10,
			AppliedWatermark:      9,
			ReferenceAt:           referenceAt.Add(30 * time.Minute).Format(time.RFC3339Nano),
		}); err == nil {
			t.Fatal("expected claim over TRANSFERRING authority row to fail")
		}
	})

	t.Run("rejects watermark regression", func(t *testing.T) {
		t.Parallel()

		store := sqlite.NewTestStore(t)
		if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
			WorkspaceID: "ws-authority-claim-watermark",
			Title:       "Authority Claim Watermark",
			CreatedBy:   "developer",
		}); err != nil {
			t.Fatalf("create workspace: %v", err)
		}
		node, err := store.EnsureLocalAuthorityNode(ctx)
		if err != nil {
			t.Fatalf("ensure local authority node: %v", err)
		}
		referenceAt := time.Now().UTC().Round(0)
		if _, err := store.DB().ExecContext(ctx, `
INSERT INTO workspace_authority(workspace_id, scope, holder_authority_node_id, lease_token, term, lease_expires_at, commit_watermark, applied_watermark, status, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			"ws-authority-claim-watermark",
			"workspace",
			node.AuthorityNodeID,
			"lease-old-1",
			1,
			referenceAt.Add(-time.Minute).Format(time.RFC3339Nano),
			20,
			19,
			string(sqlite.WorkspaceAuthorityStatusExpired),
			referenceAt.Format(time.RFC3339Nano),
		); err != nil {
			t.Fatalf("insert expired authority row: %v", err)
		}

		if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
			WorkspaceID:           "ws-authority-claim-watermark",
			HolderAuthorityNodeID: node.AuthorityNodeID,
			LeaseToken:            "lease-new-2",
			Term:                  2,
			LeaseExpiresAt:        referenceAt.Add(2 * time.Hour).Format(time.RFC3339Nano),
			CommitWatermark:       18,
			AppliedWatermark:      17,
			ReferenceAt:           referenceAt.Add(30 * time.Minute).Format(time.RFC3339Nano),
		}); err == nil {
			t.Fatal("expected claim with regressing watermarks to fail")
		}
	})
}

func TestCheckWorkspaceAuthorityFenceRejectsExpiredLease(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-fence-expired",
		Title:       "Authority Fence Expired",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	referenceAt := time.Now().UTC().Round(0)
	leaseExpiresAt := referenceAt.Add(time.Minute)
	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-fence-expired",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-expired-1",
		Term:                  1,
		LeaseExpiresAt:        leaseExpiresAt.Format(time.RFC3339Nano),
		ReferenceAt:           referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}

	if _, err := store.CheckWorkspaceAuthorityFence(ctx, sqlite.WorkspaceAuthorityFenceInput{
		WorkspaceID:                   "ws-authority-fence-expired",
		ExpectedHolderAuthorityNodeID: node.AuthorityNodeID,
		ExpectedLeaseToken:            "lease-expired-1",
		ExpectedTerm:                  1,
		ReferenceAt:                   leaseExpiresAt.Add(time.Second).Format(time.RFC3339Nano),
	}); err == nil {
		t.Fatal("expected expired authority fence to fail closed")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectLeaseExpired {
		t.Fatalf("expected lease-expired authority reject, got %+v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-authority-fence-expired",
		EventType:   sqlite.AuthorityEventRejected,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list authority reject events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 authority reject event, got %d", len(events))
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode authority reject event payload: %v", err)
	}
	if payload["reject_code"] != string(sqlite.AuthorityRejectLeaseExpired) {
		t.Fatalf("expected authority_lease_expired payload, got %+v", payload)
	}
}

func TestCheckWorkspaceAuthorityFenceRejectsBackdatedReferenceAfterServerExpiry(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-authority-fence-backdated-expiry"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Authority Fence Backdated Expiry",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	record := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
	expiredAt := time.Now().UTC().Add(-time.Minute)
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE workspace_authority SET lease_expires_at = ?, status = ?, updated_at = ? WHERE workspace_id = ? AND scope = ?`,
		expiredAt.Format(time.RFC3339Nano),
		string(sqlite.WorkspaceAuthorityStatusActive),
		expiredAt.Format(time.RFC3339Nano),
		workspaceID,
		"workspace",
	); err != nil {
		t.Fatalf("backdate authority lease expiry: %v", err)
	}

	if _, err := store.CheckWorkspaceAuthorityFence(ctx, sqlite.WorkspaceAuthorityFenceInput{
		WorkspaceID:                   workspaceID,
		ExpectedHolderAuthorityNodeID: record.HolderAuthorityNodeID,
		ExpectedLeaseToken:            record.LeaseToken,
		ExpectedTerm:                  record.Term,
		ReferenceAt:                   expiredAt.Add(-time.Minute).Format(time.RFC3339Nano),
	}); err == nil {
		t.Fatal("expected backdated reference to fail against server-time expiry")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectLeaseExpired {
		t.Fatalf("expected lease-expired authority reject, got %+v", err)
	}
}

func TestClaimWorkspaceAuthorityRejectsUnknownRuntimeNode(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-claim-missing-node",
		Title:       "Authority Claim Missing Node",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	referenceAt := time.Now().UTC().Round(0)

	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-claim-missing-node",
		HolderAuthorityNodeID: "authnode-888-1",
		LeaseToken:            "lease-missing-node-1",
		Term:                  1,
		LeaseExpiresAt:        referenceAt.Add(time.Hour).Format(time.RFC3339Nano),
		ReferenceAt:           referenceAt.Format(time.RFC3339Nano),
	}); err == nil {
		t.Fatal("expected claim helper to reject unknown runtime node holder")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected missing authority reject, got %+v", err)
	}
}

func TestRenewWorkspaceAuthorityExtendsLeaseAndRecordsEvent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-renew",
		Title:       "Authority Renew",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	base := time.Now().UTC().Add(time.Hour).Round(0)
	claimReferenceAt := base.Format(time.RFC3339Nano)
	claimExpiresAt := base.Add(time.Hour).Format(time.RFC3339Nano)
	renewReferenceAt := base.Add(30 * time.Minute).Format(time.RFC3339Nano)
	renewExpiresAt := base.Add(2*time.Hour + 30*time.Minute).Format(time.RFC3339Nano)
	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-renew",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-renew-1",
		Term:                  1,
		LeaseExpiresAt:        claimExpiresAt,
		CommitWatermark:       5,
		AppliedWatermark:      4,
		ReferenceAt:           claimReferenceAt,
	}); err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}

	record, event, err := store.RenewWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityRenewInput{
		WorkspaceID:           "ws-authority-renew",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-renew-1",
		Term:                  1,
		LeaseExpiresAt:        renewExpiresAt,
		CommitWatermark:       6,
		AppliedWatermark:      5,
		ReferenceAt:           renewReferenceAt,
	})
	if err != nil {
		t.Fatalf("renew workspace authority: %v", err)
	}
	if event.EventType != sqlite.AuthorityEventRenewed {
		t.Fatalf("expected authority.renewed event, got %+v", event)
	}
	assertRuntimeEventAuthorityMetadata(t, event, record)
	if record.LeaseExpiresAt != renewExpiresAt || record.CommitWatermark != 6 || record.AppliedWatermark != 5 {
		t.Fatalf("unexpected renewed workspace authority %+v", record)
	}
}

func TestExpireWorkspaceAuthorityMarksExpiredAndRecordsEvent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-expire",
		Title:       "Authority Expire",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	now := time.Now().UTC()
	claimReferenceAt := now.Add(time.Hour).Format(time.RFC3339Nano)
	leaseExpiresAt := now.Add(2 * time.Hour).Format(time.RFC3339Nano)
	expireReferenceAt := now.Add(3 * time.Hour).Format(time.RFC3339Nano)
	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-expire",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-expire-1",
		Term:                  1,
		LeaseExpiresAt:        leaseExpiresAt,
		CommitWatermark:       8,
		AppliedWatermark:      7,
		ReferenceAt:           claimReferenceAt,
	}); err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}

	record, event, err := store.ExpireWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityExpireInput{
		WorkspaceID:           "ws-authority-expire",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-expire-1",
		Term:                  1,
		CommitWatermark:       8,
		AppliedWatermark:      7,
		ReferenceAt:           expireReferenceAt,
	})
	if err != nil {
		t.Fatalf("expire workspace authority: %v", err)
	}
	if record.Status != sqlite.WorkspaceAuthorityStatusExpired || event.EventType != sqlite.AuthorityEventExpired {
		t.Fatalf("unexpected expired authority result record=%+v event=%+v", record, event)
	}
	assertRuntimeEventAuthorityMetadata(t, event, record)
}

func TestExpireWorkspaceAuthorityRejectsStaleReference(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-expire-stale-ref",
		Title:       "Authority Expire Stale Reference",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}

	baseRef := time.Now().UTC()
	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-expire-stale-ref",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-expire-stale-1",
		Term:                  1,
		LeaseExpiresAt:        baseRef.Add(2 * time.Hour).Format(time.RFC3339Nano),
		CommitWatermark:       3,
		AppliedWatermark:      2,
		ReferenceAt:           baseRef.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}

	if _, _, err := store.RenewWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityRenewInput{
		WorkspaceID:           "ws-authority-expire-stale-ref",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-expire-stale-1",
		Term:                  1,
		LeaseExpiresAt:        baseRef.Add(3 * time.Hour).Format(time.RFC3339Nano),
		CommitWatermark:       4,
		AppliedWatermark:      3,
		ReferenceAt:           baseRef.Add(30 * time.Second).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("renew workspace authority: %v", err)
	}

	if _, _, err := store.ExpireWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityExpireInput{
		WorkspaceID:           "ws-authority-expire-stale-ref",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-expire-stale-1",
		Term:                  1,
		CommitWatermark:       4,
		AppliedWatermark:      3,
		ReferenceAt:           baseRef.Format(time.RFC3339Nano),
	}); err == nil {
		t.Fatal("expected stale reference_at expire attempt to fail")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %+v", err)
	}
}

func TestTransferWorkspaceAuthorityMovesHolderAndRecordsEvent(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-transfer",
		Title:       "Authority Transfer",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	currentNode, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	referenceAt := time.Now().UTC().Round(0)
	now := referenceAt.Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"authnode-999-2",
		"sqlite_local_store",
		"peer-host",
		"authboot-999-1",
		now,
		now,
		string(sqlite.RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("insert peer runtime node: %v", err)
	}
	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-transfer",
		HolderAuthorityNodeID: currentNode.AuthorityNodeID,
		LeaseToken:            "lease-transfer-1",
		Term:                  1,
		LeaseExpiresAt:        referenceAt.Add(time.Hour).Format(time.RFC3339Nano),
		CommitWatermark:       10,
		AppliedWatermark:      9,
		ReferenceAt:           now,
	}); err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}

	record, event, err := store.TransferWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityTransferInput{
		WorkspaceID:                  "ws-authority-transfer",
		CurrentHolderAuthorityNodeID: currentNode.AuthorityNodeID,
		CurrentLeaseToken:            "lease-transfer-1",
		CurrentTerm:                  1,
		NewHolderAuthorityNodeID:     "authnode-999-2",
		NewLeaseToken:                "lease-transfer-2",
		NewTerm:                      2,
		LeaseExpiresAt:               referenceAt.Add(2 * time.Hour).Format(time.RFC3339Nano),
		CommitWatermark:              11,
		AppliedWatermark:             10,
		ReferenceAt:                  referenceAt.Add(30 * time.Minute).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("transfer workspace authority: %v", err)
	}
	if record.HolderAuthorityNodeID != "authnode-999-2" || record.Term != 2 {
		t.Fatalf("unexpected transferred authority record %+v", record)
	}
	if event.EventType != sqlite.AuthorityEventTransferred {
		t.Fatalf("expected authority.transferred event, got %+v", event)
	}
	assertRuntimeEventAuthorityMetadata(t, event, record)
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode transfer event payload: %v", err)
	}
	if payload["previous_holder_authority_node_id"] != currentNode.AuthorityNodeID {
		t.Fatalf("expected previous holder in payload, got %+v", payload)
	}
}

func TestTransferWorkspaceAuthorityRejectsCommitWatermarkBehindJournalHead(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-transfer-stale-head",
		Title:       "Authority Transfer Stale Head",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	currentNode, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	referenceAt := time.Now().UTC().Round(0)
	now := referenceAt.Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"authnode-999-300",
		"sqlite_local_store",
		"peer-host",
		"authboot-999-300",
		now,
		now,
		string(sqlite.RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("insert peer runtime node: %v", err)
	}
	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-transfer-stale-head",
		HolderAuthorityNodeID: currentNode.AuthorityNodeID,
		LeaseToken:            "lease-transfer-stale-1",
		Term:                  1,
		LeaseExpiresAt:        referenceAt.Add(time.Hour).Format(time.RFC3339Nano),
		CommitWatermark:       1,
		AppliedWatermark:      1,
		ReferenceAt:           referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}
	if _, _, err := store.PutCapabilityPolicyWithEvent(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: "ws-authority-transfer-stale-head",
		SubjectType: "agent",
		SubjectID:   "agent-stale",
		Capability:  "tool.call",
		ToolID:      "dangerous-tool",
		Effect:      "DENY",
		Reason:      "advance runtime journal head",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("advance runtime journal head: %v", err)
	}

	var journalHead int64
	if err := store.DB().QueryRowContext(ctx, `
SELECT COALESCE(MAX(ingest_seq), 0)
  FROM runtime_events
 WHERE workspace_id = ?`,
		"ws-authority-transfer-stale-head",
	).Scan(&journalHead); err != nil {
		t.Fatalf("query runtime journal head: %v", err)
	}
	if journalHead < 2 {
		t.Fatalf("expected runtime journal head to advance, got %d", journalHead)
	}

	if _, _, err := store.TransferWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityTransferInput{
		WorkspaceID:                  "ws-authority-transfer-stale-head",
		CurrentHolderAuthorityNodeID: currentNode.AuthorityNodeID,
		CurrentLeaseToken:            "lease-transfer-stale-1",
		CurrentTerm:                  1,
		NewHolderAuthorityNodeID:     "authnode-999-300",
		NewLeaseToken:                "lease-transfer-stale-2",
		NewTerm:                      2,
		LeaseExpiresAt:               referenceAt.Add(2 * time.Hour).Format(time.RFC3339Nano),
		CommitWatermark:              journalHead - 1,
		AppliedWatermark:             1,
		ReferenceAt:                  referenceAt.Add(30 * time.Minute).Format(time.RFC3339Nano),
	}); err == nil {
		t.Fatal("expected transfer to reject stale commit watermark")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject, got %+v", err)
	}

	record, err := store.GetWorkspaceAuthority(ctx, "ws-authority-transfer-stale-head", "workspace")
	if err != nil {
		t.Fatalf("reload workspace authority: %v", err)
	}
	if record.HolderAuthorityNodeID != currentNode.AuthorityNodeID || record.Term != 1 {
		t.Fatalf("expected authority holder to remain unchanged after reject, got %+v", record)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-authority-transfer-stale-head",
		EventType:   sqlite.AuthorityEventTransferred,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list transfer runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected rejected transfer not to persist authority.transferred, got %+v", events)
	}
}

func TestTransferWorkspaceAuthorityCreatesJournalBoundaryAndRejectsOldHolderWrites(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-transfer-boundary",
		Title:       "Authority Transfer Boundary",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	currentNode, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	referenceAt := time.Now().UTC().Round(0)
	now := referenceAt.Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"authnode-999-301",
		"sqlite_local_store",
		"peer-host",
		"authboot-999-301",
		now,
		now,
		string(sqlite.RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("insert peer runtime node: %v", err)
	}
	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-transfer-boundary",
		HolderAuthorityNodeID: currentNode.AuthorityNodeID,
		LeaseToken:            "lease-transfer-boundary-1",
		Term:                  1,
		LeaseExpiresAt:        referenceAt.Add(time.Hour).Format(time.RFC3339Nano),
		CommitWatermark:       2,
		AppliedWatermark:      1,
		ReferenceAt:           referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}
	preTransferPolicy, preTransferEvent, err := store.PutCapabilityPolicyWithEvent(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: "ws-authority-transfer-boundary",
		SubjectType: "agent",
		SubjectID:   "agent-boundary",
		Capability:  "tool.call",
		ToolID:      "before-transfer",
		Effect:      "DENY",
		Reason:      "pre-transfer journal head",
		CreatedBy:   "developer",
	})
	if err != nil {
		t.Fatalf("seed pre-transfer runtime event: %v", err)
	}

	var journalHead int64
	if err := store.DB().QueryRowContext(ctx, `
SELECT COALESCE(MAX(ingest_seq), 0)
  FROM runtime_events
 WHERE workspace_id = ?`,
		"ws-authority-transfer-boundary",
	).Scan(&journalHead); err != nil {
		t.Fatalf("query pre-transfer runtime journal head: %v", err)
	}
	if preTransferEvent.IngestSeq != journalHead {
		t.Fatalf("expected seeded event to match journal head, event=%+v head=%d", preTransferEvent, journalHead)
	}

	record, event, err := store.TransferWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityTransferInput{
		WorkspaceID:                  "ws-authority-transfer-boundary",
		CurrentHolderAuthorityNodeID: currentNode.AuthorityNodeID,
		CurrentLeaseToken:            "lease-transfer-boundary-1",
		CurrentTerm:                  1,
		NewHolderAuthorityNodeID:     "authnode-999-301",
		NewLeaseToken:                "lease-transfer-boundary-2",
		NewTerm:                      2,
		LeaseExpiresAt:               referenceAt.Add(2 * time.Hour).Format(time.RFC3339Nano),
		CommitWatermark:              journalHead,
		AppliedWatermark:             1,
		ReferenceAt:                  referenceAt.Add(30 * time.Minute).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("transfer workspace authority: %v", err)
	}
	if event.IngestSeq != journalHead+1 {
		t.Fatalf("expected authority.transferred to create next journal boundary, event=%+v head=%d", event, journalHead)
	}
	assertRuntimeEventAuthorityMetadata(t, event, record)
	if preTransferPolicy.PolicyID == "" {
		t.Fatalf("expected pre-transfer policy to persist, got %+v", preTransferPolicy)
	}

	if _, _, err := store.PutCapabilityPolicyWithEvent(ctx, sqlite.CapabilityPolicyInput{
		WorkspaceID: "ws-authority-transfer-boundary",
		SubjectType: "agent",
		SubjectID:   "agent-boundary",
		Capability:  "tool.call",
		ToolID:      "after-transfer-stale",
		Effect:      "DENY",
		Reason:      "old holder must reject",
		CreatedBy:   "developer",
	}); err == nil {
		t.Fatal("expected stale old-holder write to fail after transfer")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected stale authority reject after transfer, got %+v", err)
	}

	policies, err := store.ListCapabilityPolicies(ctx, sqlite.CapabilityPolicyFilter{
		WorkspaceID: "ws-authority-transfer-boundary",
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("list capability policies after stale old-holder write: %v", err)
	}
	if len(policies) != 1 || policies[0].PolicyID != preTransferPolicy.PolicyID {
		t.Fatalf("expected old-holder reject not to persist a new capability policy, got %+v", policies)
	}
}

func TestWithFencedWorkspaceAuthorityRunsMutationOnFreshAuthority(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-fenced-helper",
		Title:       "Authority Fenced Helper",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	ref := time.Now().UTC()
	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-fenced-helper",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-fenced-1",
		Term:                  1,
		LeaseExpiresAt:        ref.Add(2 * time.Hour).Format(time.RFC3339Nano),
		ReferenceAt:           ref.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}

	called := false
	record, err := store.WithFencedWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityFenceInput{
		WorkspaceID:                   "ws-authority-fenced-helper",
		ExpectedHolderAuthorityNodeID: node.AuthorityNodeID,
		ExpectedLeaseToken:            "lease-fenced-1",
		ExpectedTerm:                  1,
		ReferenceAt:                   ref.Add(30 * time.Second).Format(time.RFC3339Nano),
	}, func(tx *sql.Tx, authority sqlite.WorkspaceAuthorityRecord) error {
		called = true
		_, err := tx.ExecContext(ctx, `UPDATE workspaces SET title = ? WHERE workspace_id = ?`, "Authority Fenced Helper Updated", authority.WorkspaceID)
		return err
	})
	if err != nil {
		t.Fatalf("with fenced workspace authority: %v", err)
	}
	if !called {
		t.Fatal("expected fenced mutation callback to run")
	}
	if record.HolderAuthorityNodeID != node.AuthorityNodeID {
		t.Fatalf("unexpected fenced authority record %+v", record)
	}

	var title string
	if err := store.DB().QueryRowContext(ctx, `SELECT title FROM workspaces WHERE workspace_id = ?`, "ws-authority-fenced-helper").Scan(&title); err != nil {
		t.Fatalf("query workspace title: %v", err)
	}
	if title != "Authority Fenced Helper Updated" {
		t.Fatalf("expected fenced mutation to commit workspace title, got %q", title)
	}
}

func TestWithFencedWorkspaceAuthorityRejectsExpiredAuthorityBeforeMutation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-fenced-expired",
		Title:       "Authority Fenced Expired",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	ref := time.Now().UTC()
	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-fenced-expired",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-fenced-expired-1",
		Term:                  1,
		LeaseExpiresAt:        ref.Add(30 * time.Second).Format(time.RFC3339Nano),
		ReferenceAt:           ref.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}

	called := false
	if _, err := store.WithFencedWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityFenceInput{
		WorkspaceID:                   "ws-authority-fenced-expired",
		ExpectedHolderAuthorityNodeID: node.AuthorityNodeID,
		ExpectedLeaseToken:            "lease-fenced-expired-1",
		ExpectedTerm:                  1,
		ReferenceAt:                   ref.Add(31 * time.Second).Format(time.RFC3339Nano),
	}, func(tx *sql.Tx, authority sqlite.WorkspaceAuthorityRecord) error {
		called = true
		return nil
	}); err == nil {
		t.Fatal("expected expired fenced authority to fail")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectLeaseExpired {
		t.Fatalf("expected lease-expired authority reject, got %+v", err)
	}
	if called {
		t.Fatal("expected fenced mutation callback not to run on fence failure")
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-authority-fenced-expired",
		EventType:   sqlite.AuthorityEventRejected,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list fenced authority reject events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 fenced authority reject event, got %d", len(events))
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode fenced authority reject payload: %v", err)
	}
	if payload["reject_code"] != string(sqlite.AuthorityRejectLeaseExpired) {
		t.Fatalf("expected fenced authority_lease_expired payload, got %+v", payload)
	}
}

func TestCheckWorkspaceAuthorityFenceRejectsOfflineHolder(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-fenced-offline-check",
		Title:       "Authority Fenced Offline Check",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	ref := time.Now().UTC()
	record, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-fenced-offline-check",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-fenced-offline-1",
		Term:                  1,
		LeaseExpiresAt:        ref.Add(2 * time.Hour).Format(time.RFC3339Nano),
		ReferenceAt:           ref.Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE runtime_nodes SET status = ? WHERE authority_node_id = ?`,
		string(sqlite.RuntimeNodeStatusOffline),
		node.AuthorityNodeID,
	); err != nil {
		t.Fatalf("mark runtime node offline: %v", err)
	}

	if _, err := store.CheckWorkspaceAuthorityFence(ctx, sqlite.WorkspaceAuthorityFenceInput{
		WorkspaceID:                   "ws-authority-fenced-offline-check",
		ExpectedHolderAuthorityNodeID: node.AuthorityNodeID,
		ExpectedLeaseToken:            record.LeaseToken,
		ExpectedTerm:                  record.Term,
		ReferenceAt:                   ref.Add(30 * time.Second).Format(time.RFC3339Nano),
	}); err == nil {
		t.Fatal("expected offline authority holder fence check to fail")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected authority stale reject for offline holder, got %+v", err)
	}
}

func TestWithFencedWorkspaceAuthorityRejectsOfflineAuthorityBeforeMutation(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-fenced-offline-mutate",
		Title:       "Authority Fenced Offline Mutate",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	ref := time.Now().UTC()
	record, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-fenced-offline-mutate",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-fenced-offline-2",
		Term:                  1,
		LeaseExpiresAt:        ref.Add(2 * time.Hour).Format(time.RFC3339Nano),
		ReferenceAt:           ref.Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE runtime_nodes SET status = ? WHERE authority_node_id = ?`,
		string(sqlite.RuntimeNodeStatusOffline),
		node.AuthorityNodeID,
	); err != nil {
		t.Fatalf("mark runtime node offline: %v", err)
	}

	called := false
	if _, err := store.WithFencedWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityFenceInput{
		WorkspaceID:                   "ws-authority-fenced-offline-mutate",
		ExpectedHolderAuthorityNodeID: node.AuthorityNodeID,
		ExpectedLeaseToken:            record.LeaseToken,
		ExpectedTerm:                  record.Term,
		ReferenceAt:                   ref.Add(30 * time.Second).Format(time.RFC3339Nano),
	}, func(tx *sql.Tx, authority sqlite.WorkspaceAuthorityRecord) error {
		called = true
		return nil
	}); err == nil {
		t.Fatal("expected offline authority holder mutation to fail")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected authority stale reject for offline holder, got %+v", err)
	}
	if called {
		t.Fatal("expected fenced mutation callback not to run when holder is offline")
	}
}

func TestRenewWorkspaceAuthorityRejectsLeaseTokenMismatchAndJournalsRejoinReject(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: "ws-authority-renew-rejoin",
		Title:       "Authority Renew Rejoin Reject",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	node, err := store.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		t.Fatalf("ensure local authority node: %v", err)
	}
	ref := time.Now().UTC()
	if _, _, err := store.ClaimWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityClaimInput{
		WorkspaceID:           "ws-authority-renew-rejoin",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-renew-rejoin-1",
		Term:                  1,
		LeaseExpiresAt:        ref.Add(2 * time.Hour).Format(time.RFC3339Nano),
		ReferenceAt:           ref.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("claim workspace authority: %v", err)
	}

	if _, _, err := store.RenewWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityRenewInput{
		WorkspaceID:           "ws-authority-renew-rejoin",
		HolderAuthorityNodeID: node.AuthorityNodeID,
		LeaseToken:            "lease-renew-rejoin-wrong",
		Term:                  1,
		LeaseExpiresAt:        ref.Add(3 * time.Hour).Format(time.RFC3339Nano),
		ReferenceAt:           ref.Add(30 * time.Second).Format(time.RFC3339Nano),
	}); err == nil {
		t.Fatal("expected renew with mismatched lease token to fail")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectRejoinRejected {
		t.Fatalf("expected rejoin-rejected authority error, got %+v", err)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: "ws-authority-renew-rejoin",
		EventType:   sqlite.AuthorityEventRejoinRejected,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list authority rejoin reject events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 authority rejoin reject event, got %d", len(events))
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("decode authority rejoin reject payload: %v", err)
	}
	if payload["reject_code"] != string(sqlite.AuthorityRejectRejoinRejected) {
		t.Fatalf("expected authority_rejoin_rejected payload, got %+v", payload)
	}
}

func assertAuthorityEventReferenceAtInWindow(t *testing.T, event sqlite.RuntimeEventRecord, earliest, latest time.Time, forbidden string) {
	t.Helper()
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("decode authority event payload: %v", err)
	}
	raw, ok := payload["reference_at"].(string)
	if !ok || strings.TrimSpace(raw) == "" {
		t.Fatalf("authority event missing reference_at payload: %+v", payload)
	}
	if raw == forbidden {
		t.Fatalf("authority event reference_at reused caller-supplied timestamp %s", forbidden)
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatalf("parse authority event reference_at %q: %v", raw, err)
	}
	if parsed.Before(earliest) || parsed.After(latest) {
		t.Fatalf("authority event reference_at %s outside server-time window [%s, %s]", raw, earliest.Format(time.RFC3339Nano), latest.Format(time.RFC3339Nano))
	}
}
