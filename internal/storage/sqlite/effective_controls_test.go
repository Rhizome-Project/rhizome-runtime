package sqlite_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestPersistEffectiveControlsRoundTripAndExpiry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := sqlite.NewTestStore(t)
	createEffectiveControlsWorkspace(t, ctx, store, "ws-effective-roundtrip")

	input := sqlite.EffectiveControlsInput{
		WorkspaceID:    "ws-effective-roundtrip",
		ProtoClusterID: "cluster-alpha",
		Epoch:          7,
		TTLSeconds:     60,
		ControlMode:    "steady",
		CandidateMode:  "throughput",
		CandidateControls: sqlite.ControlSuggestedControls{
			FanoutCap:     4,
			ReviewDepth:   1,
			ContextCap:    8,
			PriorityFocus: "throughput",
		},
		AdvisoryControls: sqlite.ControlSuggestedControls{
			FanoutCap:     3,
			ReviewDepth:   1,
			ContextCap:    6,
			PriorityFocus: "review",
		},
		EffectiveControls: sqlite.ControlSuggestedControls{
			FanoutCap:     2,
			ReviewDepth:   2,
			ContextCap:    5,
			PriorityFocus: "safety",
		},
		ResolvedFrom: "proto_cluster",
		MatchScore:   92,
		BasisSummary: "pilot arbitration snapshot",
		GeneratedAt:  "2026-04-08T10:00:00Z",
		ActorID:      "test.actor",
	}

	record, err := store.PersistEffectiveControls(ctx, input)
	if err != nil {
		t.Fatalf("persist effective controls: %v", err)
	}
	if record.ExpiresAt != "2026-04-08T10:01:00Z" {
		t.Fatalf("expected expiry to track ttl, got %q", record.ExpiresAt)
	}
	if record.ControlMode != "steady" || record.CandidateMode != "throughput" {
		t.Fatalf("expected modes to round-trip, got %+v", record)
	}

	liveRecord, live, err := store.LoadEffectiveControls(ctx, input.WorkspaceID, input.ProtoClusterID, "2026-04-08T10:00:30Z")
	if err != nil {
		t.Fatalf("load live effective controls: %v", err)
	}
	if !live || liveRecord.Expired {
		t.Fatalf("expected controls to be live, got live=%v record=%+v", live, liveRecord)
	}
	if liveRecord.Pending {
		t.Fatalf("expected current controls not to be pending, got %+v", liveRecord)
	}
	if liveRecord.EffectiveControls.PriorityFocus != "safety" || liveRecord.CandidateControls.FanoutCap != 4 {
		t.Fatalf("expected controls payload to round-trip, got %+v", liveRecord)
	}
	assertEffectiveControlsTemporalContract(t, liveRecord.TemporalContract, "LIVE", "")

	expiredRecord, live, err := store.LoadEffectiveControls(ctx, input.WorkspaceID, input.ProtoClusterID, "2026-04-08T10:01:00Z")
	if err != nil {
		t.Fatalf("load expired effective controls: %v", err)
	}
	if live || !expiredRecord.Expired {
		t.Fatalf("expected expired controls at boundary, got live=%v record=%+v", live, expiredRecord)
	}
	assertEffectiveControlsTemporalContract(t, expiredRecord.TemporalContract, "EXPIRED", "")
}

func TestLoadEffectiveControlsKeepsFutureGeneratedRecordsPending(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := sqlite.NewTestStore(t)
	createEffectiveControlsWorkspace(t, ctx, store, "ws-effective-pending")

	if _, err := store.PersistEffectiveControls(ctx, sqlite.EffectiveControlsInput{
		WorkspaceID:       "ws-effective-pending",
		ProtoClusterID:    "cluster-alpha",
		Epoch:             8,
		TTLSeconds:        60,
		CandidateControls: sampleSuggestedControls(4, "throughput"),
		AdvisoryControls:  sampleSuggestedControls(3, "review"),
		EffectiveControls: sampleSuggestedControls(2, "memory"),
		GeneratedAt:       "2026-04-08T11:00:00Z",
	}); err != nil {
		t.Fatalf("persist future-generated effective controls: %v", err)
	}

	record, live, err := store.LoadEffectiveControls(ctx, "ws-effective-pending", "cluster-alpha", "2026-04-08T10:59:59Z")
	if err != nil {
		t.Fatalf("load pending effective controls: %v", err)
	}
	if live || record.Expired || !record.Pending {
		t.Fatalf("expected future-generated effective controls to stay pending, got live=%v record=%+v", live, record)
	}
	assertEffectiveControlsTemporalContract(t, record.TemporalContract, "PENDING", "")

	record, live, err = store.LoadEffectiveControls(ctx, "ws-effective-pending", "cluster-alpha", "2026-04-08T11:00:00Z")
	if err != nil {
		t.Fatalf("load active effective controls: %v", err)
	}
	if !live || record.Pending || record.Expired {
		t.Fatalf("expected controls to become live at generated_at boundary, got live=%v record=%+v", live, record)
	}
	assertEffectiveControlsTemporalContract(t, record.TemporalContract, "LIVE", "")
}

func TestPersistEffectiveControlsRejectsStaleEpoch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := sqlite.NewTestStore(t)
	createEffectiveControlsWorkspace(t, ctx, store, "ws-effective-stale-epoch")

	_, err := store.PersistEffectiveControls(ctx, sqlite.EffectiveControlsInput{
		WorkspaceID:       "ws-effective-stale-epoch",
		ProtoClusterID:    "cluster-alpha",
		Epoch:             5,
		TTLSeconds:        120,
		CandidateControls: sampleSuggestedControls(4, "throughput"),
		AdvisoryControls:  sampleSuggestedControls(3, "review"),
		EffectiveControls: sampleSuggestedControls(2, "safety"),
		GeneratedAt:       "2026-04-08T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("seed effective controls: %v", err)
	}

	_, err = store.PersistEffectiveControls(ctx, sqlite.EffectiveControlsInput{
		WorkspaceID:       "ws-effective-stale-epoch",
		ProtoClusterID:    "cluster-alpha",
		Epoch:             4,
		TTLSeconds:        120,
		CandidateControls: sampleSuggestedControls(6, "throughput"),
		AdvisoryControls:  sampleSuggestedControls(5, "review"),
		EffectiveControls: sampleSuggestedControls(4, "safety"),
		GeneratedAt:       "2026-04-08T12:05:00Z",
	})
	if !errors.Is(err, sqlite.ErrStaleEffectiveControlsEpoch) {
		t.Fatalf("expected stale epoch error, got %v", err)
	}

	record, live, err := store.LoadEffectiveControls(ctx, "ws-effective-stale-epoch", "cluster-alpha", "2026-04-08T12:00:30Z")
	if err != nil {
		t.Fatalf("load post-stale-reject controls: %v", err)
	}
	if !live || record.Epoch != 5 || record.CandidateControls.FanoutCap != 4 {
		t.Fatalf("expected original controls to remain, got live=%v record=%+v", live, record)
	}
}

func TestPersistEffectiveControlsRejectsOlderGeneratedAtWithinEpoch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := sqlite.NewTestStore(t)
	createEffectiveControlsWorkspace(t, ctx, store, "ws-effective-stale-ts")

	_, err := store.PersistEffectiveControls(ctx, sqlite.EffectiveControlsInput{
		WorkspaceID:       "ws-effective-stale-ts",
		ProtoClusterID:    "cluster-alpha",
		Epoch:             9,
		TTLSeconds:        120,
		CandidateControls: sampleSuggestedControls(4, "throughput"),
		AdvisoryControls:  sampleSuggestedControls(3, "review"),
		EffectiveControls: sampleSuggestedControls(2, "safety"),
		GeneratedAt:       "2026-04-08T14:05:00Z",
	})
	if err != nil {
		t.Fatalf("seed effective controls: %v", err)
	}

	_, err = store.PersistEffectiveControls(ctx, sqlite.EffectiveControlsInput{
		WorkspaceID:       "ws-effective-stale-ts",
		ProtoClusterID:    "cluster-alpha",
		Epoch:             9,
		TTLSeconds:        120,
		CandidateControls: sampleSuggestedControls(9, "throughput"),
		AdvisoryControls:  sampleSuggestedControls(8, "review"),
		EffectiveControls: sampleSuggestedControls(7, "safety"),
		GeneratedAt:       "2026-04-08T14:04:59Z",
	})
	if !errors.Is(err, sqlite.ErrStaleEffectiveControlsEpoch) {
		t.Fatalf("expected stale generated_at error, got %v", err)
	}
}

func TestPersistEffectiveControlsKeepsScopesIndependent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := sqlite.NewTestStore(t)
	createEffectiveControlsWorkspace(t, ctx, store, "ws-effective-scope")

	_, err := store.PersistEffectiveControls(ctx, sqlite.EffectiveControlsInput{
		WorkspaceID:       "ws-effective-scope",
		ProtoClusterID:    "",
		Epoch:             1,
		TTLSeconds:        int((2 * time.Minute).Seconds()),
		CandidateControls: sampleSuggestedControls(2, "workspace"),
		AdvisoryControls:  sampleSuggestedControls(2, "workspace"),
		EffectiveControls: sampleSuggestedControls(1, "workspace"),
		GeneratedAt:       "2026-04-08T16:00:00Z",
	})
	if err != nil {
		t.Fatalf("persist workspace-scope controls: %v", err)
	}
	_, err = store.PersistEffectiveControls(ctx, sqlite.EffectiveControlsInput{
		WorkspaceID:       "ws-effective-scope",
		ProtoClusterID:    "cluster-alpha",
		Epoch:             3,
		TTLSeconds:        int((2 * time.Minute).Seconds()),
		CandidateControls: sampleSuggestedControls(5, "cluster"),
		AdvisoryControls:  sampleSuggestedControls(4, "cluster"),
		EffectiveControls: sampleSuggestedControls(3, "cluster"),
		GeneratedAt:       "2026-04-08T16:00:00Z",
	})
	if err != nil {
		t.Fatalf("persist cluster-scope controls: %v", err)
	}

	workspaceRecord, live, err := store.LoadEffectiveControls(ctx, "ws-effective-scope", "", "2026-04-08T16:00:30Z")
	if err != nil {
		t.Fatalf("load workspace-scope controls: %v", err)
	}
	if !live || workspaceRecord.EffectiveControls.PriorityFocus != "workspace" {
		t.Fatalf("expected workspace-scope controls to stay isolated, got live=%v record=%+v", live, workspaceRecord)
	}

	clusterRecord, live, err := store.LoadEffectiveControls(ctx, "ws-effective-scope", "cluster-alpha", "2026-04-08T16:00:30Z")
	if err != nil {
		t.Fatalf("load cluster-scope controls: %v", err)
	}
	if !live || clusterRecord.EffectiveControls.PriorityFocus != "cluster" {
		t.Fatalf("expected cluster-scope controls to stay isolated, got live=%v record=%+v", live, clusterRecord)
	}
}

func TestResolveEffectiveControlsScopeMarksWorkspaceFallbackTemporalContract(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := sqlite.NewTestStore(t)
	createEffectiveControlsWorkspace(t, ctx, store, "ws-effective-fallback")

	if _, err := store.PersistEffectiveControls(ctx, sqlite.EffectiveControlsInput{
		WorkspaceID:       "ws-effective-fallback",
		Epoch:             1,
		TTLSeconds:        120,
		CandidateControls: sampleSuggestedControls(2, "workspace"),
		AdvisoryControls:  sampleSuggestedControls(2, "workspace"),
		EffectiveControls: sampleSuggestedControls(1, "workspace"),
		GeneratedAt:       "2026-04-08T17:00:00Z",
	}); err != nil {
		t.Fatalf("persist workspace fallback controls: %v", err)
	}

	resolution, err := store.ResolveEffectiveControlsScope(ctx, "ws-effective-fallback", "cluster-missing", "2026-04-08T17:00:30Z")
	if err != nil {
		t.Fatalf("resolve effective controls scope: %v", err)
	}
	if !resolution.Found || !resolution.Live || resolution.ScopeSource != "workspace_fallback" {
		t.Fatalf("expected workspace fallback resolution, got %+v", resolution)
	}
	assertEffectiveControlsTemporalContract(t, resolution.Record.TemporalContract, "LIVE", "workspace_fallback")
}

func TestPersistEffectiveControlsRejectsMissingWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := sqlite.NewTestStore(t)
	workspaceID := "ws-effective-missing-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "codex",
		Status:      "active",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := store.PersistEffectiveControls(ctx, sqlite.EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    "cluster-alpha",
		Epoch:             1,
		TTLSeconds:        60,
		CandidateControls: sampleSuggestedControls(3, "throughput"),
		AdvisoryControls:  sampleSuggestedControls(2, "review"),
		EffectiveControls: sampleSuggestedControls(1, "safety"),
		GeneratedAt:       "2026-04-10T12:00:00Z",
		ActorID:           "tester",
	}); err == nil {
		t.Fatal("expected missing workspace authority to fail")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected authority missing reject, got %+v", err)
	}

	assertNoEffectiveControlsSideEffects(t, ctx, store, workspaceID, "cluster-alpha")
}

func TestPersistEffectiveControlsRejectsStaleWorkspaceAuthorityWithoutSideEffects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := sqlite.NewTestStore(t)
	workspaceID := "ws-effective-stale-authority"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "codex",
		Status:      "active",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)

	referenceAt := time.Now().UTC().Round(0)
	peerNodeID := "authnode-999-3"
	var journalHead int64
	if err := store.DB().QueryRowContext(ctx, `
SELECT COALESCE(MAX(ingest_seq), 0)
  FROM runtime_events
 WHERE workspace_id = ?`,
		workspaceID,
	).Scan(&journalHead); err != nil {
		t.Fatalf("query runtime journal head before transfer: %v", err)
	}
	commitWatermark := current.CommitWatermark + 1
	if journalHead > commitWatermark {
		commitWatermark = journalHead
	}
	appliedWatermark := current.AppliedWatermark + 1
	if appliedWatermark > commitWatermark {
		appliedWatermark = commitWatermark
	}
	if _, err := store.DB().ExecContext(ctx, `
INSERT INTO runtime_nodes(authority_node_id, node_kind, host_label, boot_instance_id, registered_at, last_seen_at, status)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		peerNodeID,
		"sqlite_peer_store",
		"peer-host",
		"boot-peer-2",
		referenceAt.Format(time.RFC3339Nano),
		referenceAt.Format(time.RFC3339Nano),
		string(sqlite.RuntimeNodeStatusOnline),
	); err != nil {
		t.Fatalf("insert peer runtime node: %v", err)
	}
	if _, _, err := store.TransferWorkspaceAuthority(ctx, sqlite.WorkspaceAuthorityTransferInput{
		WorkspaceID:                  workspaceID,
		Scope:                        "workspace",
		CurrentHolderAuthorityNodeID: current.HolderAuthorityNodeID,
		CurrentLeaseToken:            current.LeaseToken,
		CurrentTerm:                  current.Term,
		NewHolderAuthorityNodeID:     peerNodeID,
		NewLeaseToken:                "lease-peer-effective-1",
		NewTerm:                      current.Term + 1,
		LeaseExpiresAt:               referenceAt.Add(10 * time.Minute).Format(time.RFC3339Nano),
		CommitWatermark:              commitWatermark,
		AppliedWatermark:             appliedWatermark,
		ActorType:                    "system",
		ActorID:                      "tester",
		ReferenceAt:                  referenceAt.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("transfer workspace authority: %v", err)
	}

	if _, err := store.PersistEffectiveControls(ctx, sqlite.EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    "cluster-alpha",
		Epoch:             1,
		TTLSeconds:        60,
		CandidateControls: sampleSuggestedControls(3, "throughput"),
		AdvisoryControls:  sampleSuggestedControls(2, "review"),
		EffectiveControls: sampleSuggestedControls(1, "safety"),
		GeneratedAt:       "2026-04-10T12:05:00Z",
		ActorID:           "tester",
	}); err == nil {
		t.Fatal("expected stale workspace authority to fail")
	} else if reject, ok := sqlite.AsAuthorityReject(err); !ok || reject.RejectCode != sqlite.AuthorityRejectStale {
		t.Fatalf("expected authority stale reject, got %+v", err)
	}

	assertNoEffectiveControlsSideEffects(t, ctx, store, workspaceID, "cluster-alpha")
}

func createEffectiveControlsWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "codex",
		Status:      "active",
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
	claimEffectiveControlsWorkspaceAuthority(t, ctx, store, workspaceID)
}

func assertNoEffectiveControlsSideEffects(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, protoClusterID string) {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `
SELECT COUNT(*)
  FROM workspace_effective_controls
 WHERE workspace_id = ? AND proto_cluster_id = ?`,
		workspaceID,
		protoClusterID,
	).Scan(&count); err != nil {
		t.Fatalf("count workspace_effective_controls rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected no workspace_effective_controls row, got count=%d", count)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "effective_controls.persisted",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list effective_controls.persisted events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no effective_controls.persisted events, got %+v", events)
	}
}

func sampleSuggestedControls(fanout int, focus string) sqlite.ControlSuggestedControls {
	return sqlite.ControlSuggestedControls{
		FanoutCap:      fanout,
		ReviewDepth:    1,
		ContextCap:     fanout + 2,
		BridgeQuota:    fanout,
		MergeThreshold: float64(fanout + 1),
		PriorityFocus:  focus,
	}
}

func assertEffectiveControlsTemporalContract(t *testing.T, contract *sqlite.TemporalHorizonContract, state, scopeSource string) {
	t.Helper()
	if contract == nil {
		t.Fatalf("expected effective-controls temporal contract, got nil")
	}
	if contract.SchemaVersion != "1.0" ||
		contract.Domain != "effective_controls" ||
		contract.HorizonKind != "ttl_window" ||
		contract.Basis != "wall_clock" ||
		contract.Mapping != "exact_wall_clock" ||
		!contract.WallClockComparable {
		t.Fatalf("expected wall-clock effective-controls contract, got %+v", contract)
	}
	if contract.State != state {
		t.Fatalf("expected effective-controls temporal state %s, got %+v", state, contract)
	}
	if scopeSource != "" && contract.ScopeSource != scopeSource {
		t.Fatalf("expected effective-controls scope source %s, got %+v", scopeSource, contract)
	}
}
