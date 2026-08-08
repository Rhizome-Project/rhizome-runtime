package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"
)

func createP5CReplayStateWorkspace(t *testing.T, ctx context.Context, store *Store, workspaceID, title string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       title,
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
}

func TestRuntimeReplayReconstructsEffectiveControlsSnapshotsAndInvalidations(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	const workspaceID = "ws-p5c-replay-state"
	createP5CReplayStateWorkspace(t, ctx, store, workspaceID, "P5C Replay State")

	now := time.Now().UTC()
	workspaceLive, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		Epoch:             1,
		TTLSeconds:        600,
		ControlMode:       "steady",
		CandidateMode:     "steady",
		CandidateControls: p5cSuggestedControls(5, "workspace-candidate"),
		AdvisoryControls:  p5cSuggestedControls(4, "workspace-advisory"),
		EffectiveControls: p5cSuggestedControls(3, "workspace-effective"),
		ResolvedFrom:      "workspace",
		MatchScore:        90,
		BasisSummary:      "workspace live controls",
		GeneratedAt:       now.Add(-1 * time.Minute).Format(time.RFC3339Nano),
		ActorID:           "operator.workspace",
	})
	if err != nil {
		t.Fatalf("persist workspace effective controls: %v", err)
	}
	clusterLive, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    "cluster-alpha",
		Epoch:             2,
		TTLSeconds:        600,
		ControlMode:       "coherence",
		CandidateMode:     "synergy",
		CandidateControls: p5cSuggestedControls(6, "cluster-live-candidate"),
		AdvisoryControls:  p5cSuggestedControls(5, "cluster-live-advisory"),
		EffectiveControls: p5cSuggestedControls(4, "cluster-live-effective"),
		ResolvedFrom:      "proto_cluster",
		MatchScore:        95,
		BasisSummary:      "cluster live controls",
		GeneratedAt:       now.Add(-30 * time.Second).Format(time.RFC3339Nano),
		ActorID:           "operator.cluster.live",
	})
	if err != nil {
		t.Fatalf("persist live cluster effective controls: %v", err)
	}
	clusterPending, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    "cluster-beta",
		Epoch:             3,
		TTLSeconds:        600,
		ControlMode:       "coherence",
		CandidateMode:     "throughput",
		CandidateControls: p5cSuggestedControls(7, "cluster-pending-candidate"),
		AdvisoryControls:  p5cSuggestedControls(6, "cluster-pending-advisory"),
		EffectiveControls: p5cSuggestedControls(5, "cluster-pending-effective"),
		ResolvedFrom:      "proto_cluster",
		MatchScore:        88,
		BasisSummary:      "cluster pending controls",
		GeneratedAt:       now.Add(5 * time.Minute).Format(time.RFC3339Nano),
		ActorID:           "operator.cluster.pending",
	})
	if err != nil {
		t.Fatalf("persist pending cluster effective controls: %v", err)
	}
	clusterExpired, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    "cluster-gamma",
		Epoch:             4,
		TTLSeconds:        60,
		ControlMode:       "steady",
		CandidateMode:     "recovery",
		CandidateControls: p5cSuggestedControls(3, "cluster-expired-candidate"),
		AdvisoryControls:  p5cSuggestedControls(2, "cluster-expired-advisory"),
		EffectiveControls: p5cSuggestedControls(1, "cluster-expired-effective"),
		ResolvedFrom:      "proto_cluster",
		MatchScore:        72,
		BasisSummary:      "cluster expired controls",
		GeneratedAt:       now.Add(-30 * time.Minute).Format(time.RFC3339Nano),
		ActorID:           "operator.cluster.expired",
	})
	if err != nil {
		t.Fatalf("persist expired cluster effective controls: %v", err)
	}

	filter := UnifiedControlReportFilter{
		WorkspaceID:    workspaceID,
		ProtoClusterID: clusterLive.ProtoClusterID,
	}
	snapshotGeneratedAt := now.Format(time.RFC3339Nano)
	timeAuthority := WorkspaceTimeAuthority{
		WorkspaceID:  workspaceID,
		ReferenceAt:  snapshotGeneratedAt,
		CurrentEpoch: 9,
		PolicyMode:   "steady",
	}
	advisoryReport := UnifiedControlReport{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterLive.ProtoClusterID,
		Resolved:          true,
		ResolvedFrom:      "replay-test",
		MatchScore:        96,
		AdvisoryOnly:      true,
		ControlOrder:      []string{"runtime_event_ingest"},
		ControlMode:       clusterLive.ControlMode,
		CandidateMode:     clusterLive.CandidateMode,
		AttentionBand:     "WATCH",
		CandidateControls: clusterLive.CandidateControls,
		AdvisoryControls:  clusterLive.AdvisoryControls,
		EffectiveControls: clusterLive.CandidateControls,
		EffectiveControlsAudit: &UnifiedControlEffectiveControlsAudit{
			Found:        false,
			Live:         false,
			Pending:      false,
			Expired:      false,
			ScopeSource:  "candidate_only",
			Epoch:        clusterLive.Epoch,
			TTLSeconds:   clusterLive.TTLSeconds,
			ExpiresAt:    clusterLive.ExpiresAt,
			GeneratedAt:  clusterLive.GeneratedAt,
			ActorID:      clusterLive.ActorID,
			ResolvedFrom: clusterLive.ResolvedFrom,
			MatchScore:   clusterLive.MatchScore,
			BasisSummary: clusterLive.BasisSummary,
		},
		AppliedActions: []string{"tighten_context_cap"},
		SuppressedHints: []string{
			"hint-legacy",
		},
		AppliedActionAudit: []UnifiedControlAppliedActionAudit{
			{Action: "tighten_context_cap", SourceKinds: []string{"governed_hint"}, Summary: "clamp context"},
		},
		SuppressedHintAudit: []UnifiedControlSuppressedHintAudit{
			{HintID: "hint-legacy", SourceKind: "legacy", Reason: "governed_hint_preferred"},
		},
		AuditSummary: &UnifiedControlAuditSummary{
			AppliedEntryCount:     1,
			HintBackedActionCount: 1,
			SuppressedEntryCount:  1,
		},
		AuditCoverage: &UnifiedControlAuditCoverage{
			AppliedEntriesWithSourceKinds: 1,
			SuppressedEntriesWithReason:   1,
		},
		GovernedHintOutcomes: []UnifiedControlGovernedHintOutcome{
			{HintID: "hint-refresh", ArbitrationOutcome: "applied", AppliedActions: []string{"tighten_context_cap"}},
		},
		Contradictions: []string{"memory attention remains advisory"},
		CooldownBasis: &UnifiedControlCooldownBasis{
			CurrentMode:     clusterLive.ControlMode,
			CandidateMode:   clusterLive.CandidateMode,
			CandidateStreak: 2,
			RequiredStreak:  3,
			RemainingStreak: 1,
			CooldownActive:  true,
			Reason:          "awaiting stabilization",
		},
		TimeAuthority: timeAuthority,
		GeneratedAt:   snapshotGeneratedAt,
		Summary:       "advisory replay snapshot",
	}
	if _, err := store.RecordUnifiedControlSnapshot(ctx, advisoryReport, filter, UnifiedControlSnapshotInput{ActorID: "auditor"}); err != nil {
		t.Fatalf("record advisory unified control snapshot: %v", err)
	}
	effectiveReport := advisoryReport
	effectiveReport.AdvisoryOnly = false
	effectiveReport.EffectiveControls = clusterLive.EffectiveControls
	effectiveReport.EffectiveControlsAudit = &UnifiedControlEffectiveControlsAudit{
		Found:        true,
		Live:         true,
		Pending:      false,
		Expired:      false,
		ScopeSource:  "proto_cluster",
		Epoch:        clusterLive.Epoch,
		TTLSeconds:   clusterLive.TTLSeconds,
		ExpiresAt:    clusterLive.ExpiresAt,
		GeneratedAt:  clusterLive.GeneratedAt,
		ActorID:      clusterLive.ActorID,
		ResolvedFrom: clusterLive.ResolvedFrom,
		MatchScore:   clusterLive.MatchScore,
		BasisSummary: clusterLive.BasisSummary,
	}
	effectiveReport.Summary = "effective replay snapshot"
	if _, err := store.RecordUnifiedControlSnapshot(ctx, effectiveReport, filter, UnifiedControlSnapshotInput{ActorID: "auditor"}); err != nil {
		t.Fatalf("record effective unified control snapshot: %v", err)
	}

	invalidation := MemoryInvalidationRecord{
		InvalidationID:       "meminv-p5c-replay",
		WorkspaceID:          workspaceID,
		AgentID:              "agent-a",
		SessionID:            "session-a",
		ReportScope:          "SESSION",
		ReportID:             "report-a",
		ResidencyTier:        "L2",
		ReplicaKind:          "summary",
		CoherenceClass:       "STALE_OK",
		CanonicalMemoryID:    "mem-1",
		CacheKey:             "cache-1",
		RefKind:              "memory",
		RefID:                "mem-1",
		PreviousVersionToken: "v1",
		CurrentVersionToken:  "v2",
		Reason:               "version_mismatch",
		TriggerCause:         "memory.write",
		State:                "OPEN",
	}
	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:     "rtev-p5c-mem-enqueued",
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		EntityID:    invalidation.InvalidationID,
		ActorType:   "system",
		ActorID:     "memory_coherence",
		PayloadJSON: mustJSON(memoryInvalidationRuntimeEventPayload(invalidation, "MEMORY_INVALIDATION")),
		CreatedAt:   now.Add(-20 * time.Second).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record memory invalidation enqueue event: %v", err)
	}
	invalidation.DeliveryAttemptCount = 1
	invalidation.LastDeliveryAttemptAt = now.Add(-10 * time.Second).Format(time.RFC3339Nano)
	invalidation.LeaseExpiresAt = now.Add(20 * time.Second).Format(time.RFC3339Nano)
	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:     "rtev-p5c-mem-delivered",
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_delivered",
		EntityType:  "memory_invalidation",
		EntityID:    invalidation.InvalidationID,
		ActorType:   "agent",
		ActorID:     invalidation.AgentID,
		PayloadJSON: mustJSON(memoryInvalidationRuntimeEventPayload(invalidation, "MEMORY_INVALIDATION_DELIVERED")),
		CreatedAt:   now.Add(-10 * time.Second).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record memory invalidation delivered event: %v", err)
	}
	invalidation.State = "ACKED"
	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:     "rtev-p5c-mem-acked",
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_acked",
		EntityType:  "memory_invalidation",
		EntityID:    invalidation.InvalidationID,
		ActorType:   "agent",
		ActorID:     invalidation.AgentID,
		PayloadJSON: mustJSON(memoryInvalidationRuntimeEventPayload(invalidation, "MEMORY_INVALIDATION_ACK")),
		CreatedAt:   now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record memory invalidation ack event: %v", err)
	}
	deadLettered := MemoryInvalidationRecord{
		InvalidationID:       "meminv-p5c-dead-letter",
		WorkspaceID:          workspaceID,
		AgentID:              "agent-b",
		SessionID:            "session-b",
		ReportScope:          "SESSION",
		ReportID:             "report-b",
		ResidencyTier:        "L2",
		ReplicaKind:          "summary",
		CoherenceClass:       "STRICT",
		CanonicalMemoryID:    "mem-2",
		CacheKey:             "cache-2",
		RefKind:              "memory",
		RefID:                "mem-2",
		PreviousVersionToken: "v9",
		CurrentVersionToken:  "v10",
		Reason:               "apply_failed",
		TriggerCause:         "projection.refresh",
		State:                "DEAD_LETTER",
		FailureCount:         3,
		LastFailureAt:        now.Add(-5 * time.Second).Format(time.RFC3339Nano),
		LastFailureReason:    "projection worker failed repeatedly",
		DeadLetteredAt:       now.Add(-5 * time.Second).Format(time.RFC3339Nano),
	}
	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:     "rtev-p5c-mem-dead-letter",
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_dead_lettered",
		EntityType:  "memory_invalidation",
		EntityID:    deadLettered.InvalidationID,
		ActorType:   "agent",
		ActorID:     deadLettered.AgentID,
		PayloadJSON: mustJSON(memoryInvalidationRuntimeEventPayload(deadLettered, "MEMORY_INVALIDATION_DEAD_LETTER")),
		CreatedAt:   now.Add(-5 * time.Second).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("record memory invalidation dead-letter event: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       100,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}

	if len(report.EffectiveControls) != 4 {
		t.Fatalf("expected four replayed effective-control scopes, got %+v", report.EffectiveControls)
	}
	controlsByScope := map[string]EffectiveControlsRecord{}
	for _, record := range report.EffectiveControls {
		key := record.ProtoClusterID
		if key == "" {
			key = "workspace"
		}
		controlsByScope[key] = record
	}
	if got := controlsByScope["workspace"]; got.EffectiveControls.PriorityFocus != workspaceLive.EffectiveControls.PriorityFocus || got.Pending || got.Expired {
		t.Fatalf("expected workspace scope to replay as live, got %+v", got)
	}
	if got := controlsByScope["cluster-alpha"]; got.EffectiveControls.PriorityFocus != clusterLive.EffectiveControls.PriorityFocus || got.Pending || got.Expired {
		t.Fatalf("expected live cluster scope to stay live, got %+v", got)
	}
	if got := controlsByScope["cluster-beta"]; !got.Pending || got.Expired || got.EffectiveControls.PriorityFocus != clusterPending.EffectiveControls.PriorityFocus {
		t.Fatalf("expected pending cluster scope to stay pending, got %+v", got)
	}
	if got := controlsByScope["cluster-gamma"]; !got.Expired || got.Pending || got.EffectiveControls.PriorityFocus != clusterExpired.EffectiveControls.PriorityFocus {
		t.Fatalf("expected expired cluster scope to stay expired, got %+v", got)
	}

	if len(report.UnifiedSnapshots) != 2 {
		t.Fatalf("expected advisory and effective unified snapshots, got %+v", report.UnifiedSnapshots)
	}
	snapshotsByType := map[string]RuntimeReplayUnifiedControlSnapshot{}
	for _, snapshot := range report.UnifiedSnapshots {
		snapshotsByType[snapshot.EventType] = snapshot
	}
	advisorySnapshot, ok := snapshotsByType["cluster.unified_control_advisory_snapshot"]
	if !ok || !advisorySnapshot.AdvisoryOnly || advisorySnapshot.TypedEventType != "UNIFIED_CONTROL_ADVISORY_SNAPSHOT" {
		t.Fatalf("expected advisory replay snapshot, got %+v", report.UnifiedSnapshots)
	}
	if advisorySnapshot.EffectiveControlsAudit == nil || advisorySnapshot.EffectiveControlsAudit.Live || advisorySnapshot.EffectiveControlsAudit.Found {
		t.Fatalf("expected advisory snapshot to retain candidate/effective distinction, got %+v", advisorySnapshot)
	}
	if advisorySnapshot.EffectiveControls != (ControlSuggestedControls{}) {
		t.Fatalf("expected advisory snapshot typed replay not to claim effective controls without a found record, got %+v", advisorySnapshot)
	}
	effectiveSnapshot, ok := snapshotsByType["cluster.unified_control_effective_snapshot"]
	if !ok || effectiveSnapshot.AdvisoryOnly || effectiveSnapshot.TypedEventType != "UNIFIED_CONTROL_EFFECTIVE_SNAPSHOT" {
		t.Fatalf("expected effective replay snapshot, got %+v", report.UnifiedSnapshots)
	}
	if effectiveSnapshot.EffectiveControlsAudit == nil || !effectiveSnapshot.EffectiveControlsAudit.Live || effectiveSnapshot.EffectiveControls.PriorityFocus != clusterLive.EffectiveControls.PriorityFocus {
		t.Fatalf("expected effective snapshot to preserve live effective controls, got %+v", effectiveSnapshot)
	}

	if len(report.MemoryInvalidations) != 2 {
		t.Fatalf("expected one replayed memory invalidation, got %+v", report.MemoryInvalidations)
	}
	invalidationsByID := map[string]RuntimeReplayMemoryInvalidation{}
	for _, item := range report.MemoryInvalidations {
		invalidationsByID[item.InvalidationID] = item
	}
	replayedInvalidation := invalidationsByID[invalidation.InvalidationID]
	if replayedInvalidation.State != "ACKED" || replayedInvalidation.LastEventType != "memory.invalidation_acked" || replayedInvalidation.TypedEventType != "MEMORY_INVALIDATION_ACK" {
		t.Fatalf("expected acked invalidation replay state, got %+v", replayedInvalidation)
	}
	if replayedInvalidation.DeliveredAt == "" || replayedInvalidation.AcknowledgedAt == "" || replayedInvalidation.RefID != invalidation.RefID {
		t.Fatalf("expected invalidation transition timestamps and refs to replay, got %+v", replayedInvalidation)
	}
	deadLetteredReplay := invalidationsByID[deadLettered.InvalidationID]
	if deadLetteredReplay.State != "DEAD_LETTER" || deadLetteredReplay.LastEventType != "memory.invalidation_dead_lettered" || deadLetteredReplay.DeadLetteredAt == "" {
		t.Fatalf("expected dead-letter invalidation replay state, got %+v", deadLetteredReplay)
	}
	if deadLetteredReplay.FailureCount != 3 || deadLetteredReplay.LastFailureReason != deadLettered.LastFailureReason {
		t.Fatalf("expected dead-letter failure details to replay, got %+v", deadLetteredReplay)
	}
}

func TestRuntimeReplayRejectsMalformedEffectiveControlsSnapshotsAndInvalidations(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	const workspaceID = "ws-p5c-replay-state-malformed"
	createP5CReplayStateWorkspace(t, ctx, store, workspaceID, "P5C Replay State Malformed")

	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:     "rtev-p5c-bad-effective",
		WorkspaceID: workspaceID,
		EventType:   effectiveControlsEventType,
		EntityType:  effectiveControlsEntityType,
		EntityID:    "workspace:" + workspaceID,
		ActorType:   "operator",
		ActorID:     "operator-bad",
		PayloadJSON: `{"workspace_id":"` + workspaceID + `","proto_cluster_id":"cluster-bad","epoch":1,"ttl_seconds":60,"expires_at":"2026-04-08T03:01:00Z","generated_at":"2026-04-08T03:00:00Z"}`,
		CreatedAt:   "2026-04-08T03:00:00Z",
	}); err != nil {
		t.Fatalf("record malformed effective-controls event: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:     "rtev-p5c-bad-snapshot",
		WorkspaceID: workspaceID,
		EventType:   "cluster.unified_control_advisory_snapshot",
		EntityType:  "instrumentation_unified_control",
		EntityID:    "cluster-bad",
		ActorType:   "operator",
		ActorID:     "auditor",
		PayloadJSON: `{"workspace_id":"` + workspaceID + `","summary":"bad snapshot","typed_event_type":"UNIFIED_CONTROL_EFFECTIVE_SNAPSHOT","event_kind":"cluster.unified_control_effective_snapshot","advisory_only":false,"report":{"workspace_id":"` + workspaceID + `","proto_cluster_id":"cluster-bad","resolved":true,"advisory_only":false,"generated_at":"2026-04-08T03:01:00Z","summary":"bad snapshot"}}`,
		CreatedAt:   "2026-04-08T03:01:00Z",
	}); err != nil {
		t.Fatalf("record malformed unified snapshot event: %v", err)
	}
	if _, err := store.RecordRuntimeEvent(ctx, RuntimeEventInput{
		EventID:     "rtev-p5c-bad-invalidation",
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		EntityID:    "meminv-bad",
		ActorType:   "system",
		ActorID:     "memory_coherence",
		PayloadJSON: `{"invalidation_id":"meminv-bad","agent_id":"agent-a","ref_kind":"memory","ref_id":"mem-1","reason":"version_mismatch","state":"ACKED","typed_event_type":"MEMORY_INVALIDATION"}`,
		CreatedAt:   "2026-04-08T03:02:00Z",
	}); err != nil {
		t.Fatalf("record malformed memory invalidation event: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       20,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}

	if len(report.EffectiveControls) != 0 {
		t.Fatalf("did not expect malformed effective-controls payload to fabricate state, got %+v", report.EffectiveControls)
	}
	if len(report.UnifiedSnapshots) != 0 {
		t.Fatalf("did not expect malformed unified snapshot payload to fabricate state, got %+v", report.UnifiedSnapshots)
	}
	if len(report.MemoryInvalidations) != 0 {
		t.Fatalf("did not expect malformed memory invalidation payload to fabricate state, got %+v", report.MemoryInvalidations)
	}

	found := map[string]bool{}
	for _, finding := range report.Evaluation.Findings {
		if finding.Code == "malformed_event_payload" {
			found[finding.SourceEventID] = true
		}
	}
	for _, eventID := range []string{"rtev-p5c-bad-effective", "rtev-p5c-bad-snapshot", "rtev-p5c-bad-invalidation"} {
		if !found[eventID] {
			t.Fatalf("expected malformed_event_payload finding for %s, got %+v", eventID, report.Evaluation.Findings)
		}
	}
}

func TestRuntimeReplaySurfacesUnifiedControlRollbackTrace(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	const (
		workspaceID = "ws-p5c-replay-rollback-trace"
		clusterID   = "cluster-rollback-trace"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P5C Replay Rollback Trace",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	now := time.Now().UTC()
	filter := UnifiedControlReportFilter{
		WorkspaceID:    workspaceID,
		ProtoClusterID: clusterID,
	}
	live, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Epoch:             1,
		TTLSeconds:        600,
		ControlMode:       "coherence",
		CandidateMode:     "throughput",
		CandidateControls: p5cSuggestedControls(6, "candidate-live"),
		AdvisoryControls:  p5cSuggestedControls(5, "advisory-live"),
		EffectiveControls: p5cSuggestedControls(4, "effective-live"),
		ResolvedFrom:      "proto_cluster",
		MatchScore:        92,
		BasisSummary:      "live effective controls",
		GeneratedAt:       now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
		ActorID:           "operator.live",
	})
	if err != nil {
		t.Fatalf("persist live effective controls: %v", err)
	}

	effectiveSnapshotReport := UnifiedControlReport{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Resolved:          true,
		ResolvedFrom:      "proto_cluster",
		AdvisoryOnly:      false,
		ControlMode:       live.ControlMode,
		CandidateMode:     live.CandidateMode,
		CandidateControls: live.CandidateControls,
		AdvisoryControls:  live.AdvisoryControls,
		EffectiveControls: live.EffectiveControls,
		EffectiveControlsAudit: &UnifiedControlEffectiveControlsAudit{
			Found:        true,
			Live:         true,
			Expired:      false,
			Pending:      false,
			ScopeSource:  "proto_cluster",
			Epoch:        live.Epoch,
			TTLSeconds:   live.TTLSeconds,
			ExpiresAt:    live.ExpiresAt,
			GeneratedAt:  live.GeneratedAt,
			ActorID:      live.ActorID,
			ResolvedFrom: live.ResolvedFrom,
			MatchScore:   live.MatchScore,
			BasisSummary: live.BasisSummary,
		},
		GeneratedAt: now.Add(-90 * time.Second).Format(time.RFC3339Nano),
		Summary:     "live effective snapshot",
	}
	effectiveEvent, err := store.RecordUnifiedControlSnapshot(ctx, effectiveSnapshotReport, filter, UnifiedControlSnapshotInput{ActorID: "auditor"})
	if err != nil {
		t.Fatalf("record effective snapshot: %v", err)
	}

	expired, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Epoch:             2,
		TTLSeconds:        1,
		ControlMode:       live.ControlMode,
		CandidateMode:     live.CandidateMode,
		CandidateControls: p5cSuggestedControls(7, "candidate-expired"),
		AdvisoryControls:  p5cSuggestedControls(6, "advisory-expired"),
		EffectiveControls: p5cSuggestedControls(3, "effective-expired"),
		ResolvedFrom:      "proto_cluster",
		MatchScore:        79,
		BasisSummary:      "expired effective controls",
		GeneratedAt:       now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		ActorID:           "operator.expired",
	})
	if err != nil {
		t.Fatalf("persist expired effective controls: %v", err)
	}

	advisorySnapshotReport := UnifiedControlReport{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Resolved:          true,
		ResolvedFrom:      "proto_cluster",
		AdvisoryOnly:      true,
		ControlMode:       expired.ControlMode,
		CandidateMode:     expired.CandidateMode,
		CandidateControls: expired.CandidateControls,
		AdvisoryControls:  expired.AdvisoryControls,
		EffectiveControls: expired.CandidateControls,
		EffectiveControlsAudit: &UnifiedControlEffectiveControlsAudit{
			Found:        true,
			Live:         false,
			Expired:      true,
			Pending:      false,
			ScopeSource:  "proto_cluster",
			Epoch:        expired.Epoch,
			TTLSeconds:   expired.TTLSeconds,
			ExpiresAt:    expired.ExpiresAt,
			GeneratedAt:  expired.GeneratedAt,
			ActorID:      expired.ActorID,
			ResolvedFrom: expired.ResolvedFrom,
			MatchScore:   expired.MatchScore,
			BasisSummary: expired.BasisSummary,
		},
		GeneratedAt: now.Format(time.RFC3339Nano),
		Summary:     "expired advisory snapshot",
	}
	advisoryEvent, err := store.RecordUnifiedControlSnapshot(ctx, advisorySnapshotReport, filter, UnifiedControlSnapshotInput{ActorID: "auditor"})
	if err != nil {
		t.Fatalf("record advisory rollback snapshot: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}

	finding := p5cRequireReplayFinding(t, report, "unified_control_effective_snapshot_rolled_back")
	if finding.EntityType != "instrumentation_unified_control" || finding.EntityID != clusterID || finding.SourceEventID != advisoryEvent.EventID {
		t.Fatalf("expected rollback trace finding to stay source-addressable to advisory snapshot, got %+v", finding)
	}
	if !strings.Contains(finding.Message, "expired") {
		t.Fatalf("expected rollback trace finding to explain expiration, got %+v", finding)
	}
	if p5cHasReplayFindingCode(report, "unified_control_effective_snapshot_missing_rollback_trace") {
		t.Fatalf("did not expect missing rollback trace warning when advisory demotion exists, got %+v", report.Evaluation.Findings)
	}
	if effectiveEvent.EventID == advisoryEvent.EventID {
		t.Fatalf("expected distinct effective and advisory snapshot events, got %+v", report.UnifiedSnapshots)
	}
	if report.Evaluation.Verdict != "pass" {
		t.Fatalf("expected rollback trace to stay informational only, got %+v", report.Evaluation)
	}
}

func TestRuntimeReplayWarnsWhenEffectiveSnapshotLosesLiveBasisWithoutRollbackTrace(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	const (
		workspaceID = "ws-p5c-replay-missing-rollback-trace"
		clusterID   = "cluster-missing-rollback-trace"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P5C Replay Missing Rollback Trace",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	now := time.Now().UTC()
	filter := UnifiedControlReportFilter{
		WorkspaceID:    workspaceID,
		ProtoClusterID: clusterID,
	}
	live, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Epoch:             1,
		TTLSeconds:        600,
		ControlMode:       "coherence",
		CandidateMode:     "throughput",
		CandidateControls: p5cSuggestedControls(6, "candidate-live"),
		AdvisoryControls:  p5cSuggestedControls(5, "advisory-live"),
		EffectiveControls: p5cSuggestedControls(4, "effective-live"),
		ResolvedFrom:      "proto_cluster",
		MatchScore:        93,
		BasisSummary:      "live effective controls",
		GeneratedAt:       now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
		ActorID:           "operator.live",
	})
	if err != nil {
		t.Fatalf("persist live effective controls: %v", err)
	}

	effectiveSnapshotReport := UnifiedControlReport{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Resolved:          true,
		ResolvedFrom:      "proto_cluster",
		AdvisoryOnly:      false,
		ControlMode:       live.ControlMode,
		CandidateMode:     live.CandidateMode,
		CandidateControls: live.CandidateControls,
		AdvisoryControls:  live.AdvisoryControls,
		EffectiveControls: live.EffectiveControls,
		EffectiveControlsAudit: &UnifiedControlEffectiveControlsAudit{
			Found:        true,
			Live:         true,
			Expired:      false,
			Pending:      false,
			ScopeSource:  "proto_cluster",
			Epoch:        live.Epoch,
			TTLSeconds:   live.TTLSeconds,
			ExpiresAt:    live.ExpiresAt,
			GeneratedAt:  live.GeneratedAt,
			ActorID:      live.ActorID,
			ResolvedFrom: live.ResolvedFrom,
			MatchScore:   live.MatchScore,
			BasisSummary: live.BasisSummary,
		},
		GeneratedAt: now.Add(-90 * time.Second).Format(time.RFC3339Nano),
		Summary:     "live effective snapshot",
	}
	effectiveEvent, err := store.RecordUnifiedControlSnapshot(ctx, effectiveSnapshotReport, filter, UnifiedControlSnapshotInput{ActorID: "auditor"})
	if err != nil {
		t.Fatalf("record effective snapshot: %v", err)
	}

	if _, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Epoch:             2,
		TTLSeconds:        1,
		ControlMode:       live.ControlMode,
		CandidateMode:     live.CandidateMode,
		CandidateControls: p5cSuggestedControls(7, "candidate-expired"),
		AdvisoryControls:  p5cSuggestedControls(6, "advisory-expired"),
		EffectiveControls: p5cSuggestedControls(3, "effective-expired"),
		ResolvedFrom:      "proto_cluster",
		MatchScore:        80,
		BasisSummary:      "expired effective controls",
		GeneratedAt:       now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		ActorID:           "operator.expired",
	}); err != nil {
		t.Fatalf("persist expired effective controls: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}

	finding := p5cRequireReplayFinding(t, report, "unified_control_effective_snapshot_missing_rollback_trace")
	if finding.EntityType != "instrumentation_unified_control" || finding.EntityID != clusterID || finding.SourceEventID != effectiveEvent.EventID {
		t.Fatalf("expected missing rollback trace warning to point at effective snapshot, got %+v", finding)
	}
	if !strings.Contains(finding.Message, "expired") {
		t.Fatalf("expected missing rollback trace warning to explain expired basis, got %+v", finding)
	}
	if p5cHasReplayFindingCode(report, "unified_control_effective_snapshot_rolled_back") {
		t.Fatalf("did not expect rollback-traced info finding without advisory demotion snapshot, got %+v", report.Evaluation.Findings)
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected missing rollback trace to warn, got %+v", report.Evaluation)
	}
}

func TestRuntimeReplayRejectsLaterAdvisorySnapshotAsRollbackProofWhenBasisStaysLive(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	const (
		workspaceID = "ws-p5c-replay-live-basis-advisory"
		clusterID   = "cluster-live-basis-advisory"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P5C Replay Live Basis Advisory",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	now := time.Now().UTC()
	filter := UnifiedControlReportFilter{
		WorkspaceID:    workspaceID,
		ProtoClusterID: clusterID,
	}
	live, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Epoch:             1,
		TTLSeconds:        600,
		ControlMode:       "coherence",
		CandidateMode:     "throughput",
		CandidateControls: p5cSuggestedControls(6, "candidate-live"),
		AdvisoryControls:  p5cSuggestedControls(5, "advisory-live"),
		EffectiveControls: p5cSuggestedControls(4, "effective-live"),
		ResolvedFrom:      "proto_cluster",
		MatchScore:        94,
		BasisSummary:      "live effective controls",
		GeneratedAt:       now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
		ActorID:           "operator.live",
	})
	if err != nil {
		t.Fatalf("persist live effective controls: %v", err)
	}

	effectiveSnapshotReport := UnifiedControlReport{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Resolved:          true,
		ResolvedFrom:      "proto_cluster",
		AdvisoryOnly:      false,
		ControlMode:       live.ControlMode,
		CandidateMode:     live.CandidateMode,
		CandidateControls: live.CandidateControls,
		AdvisoryControls:  live.AdvisoryControls,
		EffectiveControls: live.EffectiveControls,
		EffectiveControlsAudit: &UnifiedControlEffectiveControlsAudit{
			Found:        true,
			Live:         true,
			Expired:      false,
			Pending:      false,
			ScopeSource:  "proto_cluster",
			Epoch:        live.Epoch,
			TTLSeconds:   live.TTLSeconds,
			ExpiresAt:    live.ExpiresAt,
			GeneratedAt:  live.GeneratedAt,
			ActorID:      live.ActorID,
			ResolvedFrom: live.ResolvedFrom,
			MatchScore:   live.MatchScore,
			BasisSummary: live.BasisSummary,
		},
		GeneratedAt: now.Add(-90 * time.Second).Format(time.RFC3339Nano),
		Summary:     "live effective snapshot",
	}
	if _, err := store.RecordUnifiedControlSnapshot(ctx, effectiveSnapshotReport, filter, UnifiedControlSnapshotInput{ActorID: "auditor"}); err != nil {
		t.Fatalf("record effective snapshot: %v", err)
	}

	// Keep the basis live, but emit a later advisory snapshot that would be a false rollback explanation
	// if replay trusted timestamps and snapshot wording alone.
	refreshedLive, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Epoch:             2,
		TTLSeconds:        600,
		ControlMode:       live.ControlMode,
		CandidateMode:     live.CandidateMode,
		CandidateControls: p5cSuggestedControls(7, "candidate-live-refresh"),
		AdvisoryControls:  p5cSuggestedControls(6, "advisory-live-refresh"),
		EffectiveControls: p5cSuggestedControls(5, "effective-live-refresh"),
		ResolvedFrom:      "proto_cluster",
		MatchScore:        95,
		BasisSummary:      "refreshed live effective controls",
		GeneratedAt:       now.Add(-30 * time.Second).Format(time.RFC3339Nano),
		ActorID:           "operator.live.refresh",
	})
	if err != nil {
		t.Fatalf("persist refreshed live effective controls: %v", err)
	}

	advisorySnapshotReport := UnifiedControlReport{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Resolved:          true,
		ResolvedFrom:      "candidate_only",
		AdvisoryOnly:      true,
		ControlMode:       refreshedLive.ControlMode,
		CandidateMode:     refreshedLive.CandidateMode,
		CandidateControls: refreshedLive.CandidateControls,
		AdvisoryControls:  refreshedLive.AdvisoryControls,
		EffectiveControls: refreshedLive.CandidateControls,
		EffectiveControlsAudit: &UnifiedControlEffectiveControlsAudit{
			Found:        false,
			Live:         false,
			Expired:      false,
			Pending:      false,
			ScopeSource:  "candidate_only",
			Epoch:        refreshedLive.Epoch,
			TTLSeconds:   refreshedLive.TTLSeconds,
			ExpiresAt:    refreshedLive.ExpiresAt,
			GeneratedAt:  refreshedLive.GeneratedAt,
			ActorID:      refreshedLive.ActorID,
			ResolvedFrom: refreshedLive.ResolvedFrom,
			MatchScore:   refreshedLive.MatchScore,
			BasisSummary: refreshedLive.BasisSummary,
		},
		GeneratedAt: now.Format(time.RFC3339Nano),
		Summary:     "candidate-only advisory snapshot",
	}
	advisoryEvent, err := store.RecordUnifiedControlSnapshot(ctx, advisorySnapshotReport, filter, UnifiedControlSnapshotInput{ActorID: "auditor"})
	if err != nil {
		t.Fatalf("record advisory snapshot: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}

	finding := p5cRequireReplayFinding(t, report, "unified_control_effective_snapshot_rollback_trace_unexplained")
	if finding.SourceEventID != advisoryEvent.EventID || finding.EntityID != clusterID {
		t.Fatalf("expected unexplained rollback warning to point at later advisory snapshot, got %+v", finding)
	}
	if !strings.Contains(finding.Message, "still live") {
		t.Fatalf("expected warning to explain live-basis conflict, got %+v", finding)
	}
	if p5cHasReplayFindingCode(report, "unified_control_effective_snapshot_rolled_back") {
		t.Fatalf("did not expect false rollback-traced info finding while basis stays live, got %+v", report.Evaluation.Findings)
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected false rollback explanation to warn, got %+v", report.Evaluation)
	}
}

func TestRuntimeReplayUsesNewestAdvisorySnapshotForRollbackReasoning(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	const (
		workspaceID = "ws-p5c-replay-newest-advisory"
		clusterID   = "cluster-newest-advisory"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P5C Replay Newest Advisory",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	now := time.Now().UTC()
	filter := UnifiedControlReportFilter{WorkspaceID: workspaceID, ProtoClusterID: clusterID}
	live, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Epoch:             1,
		TTLSeconds:        600,
		ControlMode:       "coherence",
		CandidateMode:     "throughput",
		CandidateControls: p5cSuggestedControls(6, "candidate-live"),
		AdvisoryControls:  p5cSuggestedControls(5, "advisory-live"),
		EffectiveControls: p5cSuggestedControls(4, "effective-live"),
		ResolvedFrom:      "proto_cluster",
		MatchScore:        90,
		BasisSummary:      "live effective controls",
		GeneratedAt:       now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
		ActorID:           "operator.live",
	})
	if err != nil {
		t.Fatalf("persist live effective controls: %v", err)
	}
	if _, err := store.RecordUnifiedControlSnapshot(ctx, UnifiedControlReport{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Resolved:          true,
		ResolvedFrom:      "proto_cluster",
		AdvisoryOnly:      false,
		ControlMode:       live.ControlMode,
		CandidateMode:     live.CandidateMode,
		CandidateControls: live.CandidateControls,
		AdvisoryControls:  live.AdvisoryControls,
		EffectiveControls: live.EffectiveControls,
		EffectiveControlsAudit: &UnifiedControlEffectiveControlsAudit{
			Found:        true,
			Live:         true,
			ScopeSource:  "proto_cluster",
			Epoch:        live.Epoch,
			TTLSeconds:   live.TTLSeconds,
			ExpiresAt:    live.ExpiresAt,
			GeneratedAt:  live.GeneratedAt,
			ActorID:      live.ActorID,
			ResolvedFrom: live.ResolvedFrom,
			MatchScore:   live.MatchScore,
			BasisSummary: live.BasisSummary,
		},
		GeneratedAt: now.Add(-90 * time.Second).Format(time.RFC3339Nano),
		Summary:     "live effective snapshot",
	}, filter, UnifiedControlSnapshotInput{ActorID: "auditor"}); err != nil {
		t.Fatalf("record effective snapshot: %v", err)
	}

	expired, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Epoch:             2,
		TTLSeconds:        1,
		ControlMode:       live.ControlMode,
		CandidateMode:     live.CandidateMode,
		CandidateControls: p5cSuggestedControls(7, "candidate-expired"),
		AdvisoryControls:  p5cSuggestedControls(6, "advisory-expired"),
		EffectiveControls: p5cSuggestedControls(3, "effective-expired"),
		ResolvedFrom:      "proto_cluster",
		MatchScore:        78,
		BasisSummary:      "expired effective controls",
		GeneratedAt:       now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		ActorID:           "operator.expired",
	})
	if err != nil {
		t.Fatalf("persist expired effective controls: %v", err)
	}

	advisoryReport := UnifiedControlReport{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Resolved:          true,
		ResolvedFrom:      "proto_cluster",
		AdvisoryOnly:      true,
		ControlMode:       expired.ControlMode,
		CandidateMode:     expired.CandidateMode,
		CandidateControls: expired.CandidateControls,
		AdvisoryControls:  expired.AdvisoryControls,
		EffectiveControls: expired.CandidateControls,
		EffectiveControlsAudit: &UnifiedControlEffectiveControlsAudit{
			Found:        true,
			Live:         false,
			Expired:      true,
			ScopeSource:  "proto_cluster",
			Epoch:        expired.Epoch,
			TTLSeconds:   expired.TTLSeconds,
			ExpiresAt:    expired.ExpiresAt,
			GeneratedAt:  expired.GeneratedAt,
			ActorID:      expired.ActorID,
			ResolvedFrom: expired.ResolvedFrom,
			MatchScore:   expired.MatchScore,
			BasisSummary: expired.BasisSummary,
		},
		GeneratedAt: now.Format(time.RFC3339Nano),
		Summary:     "expired advisory snapshot older",
	}
	if _, err := store.RecordUnifiedControlSnapshot(ctx, advisoryReport, filter, UnifiedControlSnapshotInput{ActorID: "auditor-1"}); err != nil {
		t.Fatalf("record first advisory snapshot: %v", err)
	}
	advisoryReport.Summary = "expired advisory snapshot newer"
	newestAdvisoryEvent, err := store.RecordUnifiedControlSnapshot(ctx, advisoryReport, filter, UnifiedControlSnapshotInput{ActorID: "auditor-2"})
	if err != nil {
		t.Fatalf("record second advisory snapshot: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, RuntimeReplayFilter{WorkspaceID: workspaceID, Limit: 50})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}

	finding := p5cRequireReplayFinding(t, report, "unified_control_effective_snapshot_rolled_back")
	if finding.SourceEventID != newestAdvisoryEvent.EventID {
		t.Fatalf("expected rollback reasoning to use newest advisory snapshot, got %+v", finding)
	}
}

func TestRuntimeReplayUsesWorkspaceFallbackBasisForEffectiveSnapshotTrace(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	const (
		workspaceID = "ws-p5c-replay-workspace-fallback"
		clusterID   = "cluster-workspace-fallback"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P5C Replay Workspace Fallback",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	now := time.Now().UTC()
	filter := UnifiedControlReportFilter{WorkspaceID: workspaceID, ProtoClusterID: clusterID}
	workspaceLive, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		Epoch:             1,
		TTLSeconds:        600,
		ControlMode:       "steady",
		CandidateMode:     "steady",
		CandidateControls: p5cSuggestedControls(5, "workspace-candidate"),
		AdvisoryControls:  p5cSuggestedControls(4, "workspace-advisory"),
		EffectiveControls: p5cSuggestedControls(3, "workspace-effective"),
		ResolvedFrom:      "workspace",
		MatchScore:        88,
		BasisSummary:      "workspace fallback live controls",
		GeneratedAt:       now.Add(-1 * time.Minute).Format(time.RFC3339Nano),
		ActorID:           "operator.workspace",
	})
	if err != nil {
		t.Fatalf("persist workspace effective controls: %v", err)
	}

	if _, err := store.RecordUnifiedControlSnapshot(ctx, UnifiedControlReport{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Resolved:          true,
		ResolvedFrom:      "workspace_fallback",
		AdvisoryOnly:      false,
		ControlMode:       workspaceLive.ControlMode,
		CandidateMode:     workspaceLive.CandidateMode,
		CandidateControls: workspaceLive.CandidateControls,
		AdvisoryControls:  workspaceLive.AdvisoryControls,
		EffectiveControls: workspaceLive.EffectiveControls,
		EffectiveControlsAudit: &UnifiedControlEffectiveControlsAudit{
			Found:        true,
			Live:         true,
			ScopeSource:  "workspace_fallback",
			Epoch:        workspaceLive.Epoch,
			TTLSeconds:   workspaceLive.TTLSeconds,
			ExpiresAt:    workspaceLive.ExpiresAt,
			GeneratedAt:  workspaceLive.GeneratedAt,
			ActorID:      workspaceLive.ActorID,
			ResolvedFrom: workspaceLive.ResolvedFrom,
			MatchScore:   workspaceLive.MatchScore,
			BasisSummary: workspaceLive.BasisSummary,
		},
		GeneratedAt: now.Format(time.RFC3339Nano),
		Summary:     "effective snapshot backed by workspace fallback",
	}, filter, UnifiedControlSnapshotInput{ActorID: "auditor"}); err != nil {
		t.Fatalf("record workspace-fallback effective snapshot: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, RuntimeReplayFilter{WorkspaceID: workspaceID, Limit: 20})
	if err != nil {
		t.Fatalf("replay runtime journal: %v", err)
	}

	if p5cHasReplayFindingCode(report, "unified_control_effective_snapshot_missing_rollback_trace") {
		t.Fatalf("did not expect workspace fallback basis to look missing, got %+v", report.Evaluation.Findings)
	}
	if report.Evaluation.Verdict != "pass" {
		t.Fatalf("expected workspace fallback basis to stay replay-consistent, got %+v", report.Evaluation)
	}
}

func TestRuntimeReplayDoesNotWarnMissingRollbackTraceUnderIncompleteWindow(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	const (
		workspaceID = "ws-p5c-replay-incomplete-window"
		clusterID   = "cluster-incomplete-window"
	)
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P5C Replay Incomplete Window",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	now := time.Now().UTC()
	filter := UnifiedControlReportFilter{WorkspaceID: workspaceID, ProtoClusterID: clusterID}
	live, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Epoch:             1,
		TTLSeconds:        600,
		ControlMode:       "coherence",
		CandidateMode:     "throughput",
		CandidateControls: p5cSuggestedControls(6, "candidate-live"),
		AdvisoryControls:  p5cSuggestedControls(5, "advisory-live"),
		EffectiveControls: p5cSuggestedControls(4, "effective-live"),
		ResolvedFrom:      "proto_cluster",
		MatchScore:        91,
		BasisSummary:      "live effective controls",
		GeneratedAt:       now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
		ActorID:           "operator.live",
	})
	if err != nil {
		t.Fatalf("persist live effective controls: %v", err)
	}
	if _, err := store.RecordUnifiedControlSnapshot(ctx, UnifiedControlReport{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Resolved:          true,
		ResolvedFrom:      "proto_cluster",
		AdvisoryOnly:      false,
		ControlMode:       live.ControlMode,
		CandidateMode:     live.CandidateMode,
		CandidateControls: live.CandidateControls,
		AdvisoryControls:  live.AdvisoryControls,
		EffectiveControls: live.EffectiveControls,
		EffectiveControlsAudit: &UnifiedControlEffectiveControlsAudit{
			Found:        true,
			Live:         true,
			ScopeSource:  "proto_cluster",
			Epoch:        live.Epoch,
			TTLSeconds:   live.TTLSeconds,
			ExpiresAt:    live.ExpiresAt,
			GeneratedAt:  live.GeneratedAt,
			ActorID:      live.ActorID,
			ResolvedFrom: live.ResolvedFrom,
			MatchScore:   live.MatchScore,
			BasisSummary: live.BasisSummary,
		},
		GeneratedAt: now.Add(-90 * time.Second).Format(time.RFC3339Nano),
		Summary:     "live effective snapshot",
	}, filter, UnifiedControlSnapshotInput{ActorID: "auditor"}); err != nil {
		t.Fatalf("record effective snapshot: %v", err)
	}
	if _, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
		WorkspaceID:       workspaceID,
		ProtoClusterID:    clusterID,
		Epoch:             2,
		TTLSeconds:        1,
		ControlMode:       live.ControlMode,
		CandidateMode:     live.CandidateMode,
		CandidateControls: p5cSuggestedControls(7, "candidate-expired"),
		AdvisoryControls:  p5cSuggestedControls(6, "advisory-expired"),
		EffectiveControls: p5cSuggestedControls(3, "effective-expired"),
		ResolvedFrom:      "proto_cluster",
		MatchScore:        77,
		BasisSummary:      "expired effective controls",
		GeneratedAt:       now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		ActorID:           "operator.expired",
	}); err != nil {
		t.Fatalf("persist expired effective controls: %v", err)
	}

	report, err := store.ReplayRuntimeJournal(ctx, RuntimeReplayFilter{WorkspaceID: workspaceID, Limit: 2})
	if err != nil {
		t.Fatalf("replay bounded runtime journal: %v", err)
	}

	if !report.WindowIncomplete {
		t.Fatalf("expected bounded replay window to be incomplete, got %+v", report)
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected incomplete window replay to stay warning-scoped, got %+v", report.Evaluation)
	}
	p5cRequireReplayFinding(t, report, "replay_scope_partial")
	if p5cHasReplayFindingCode(report, "unified_control_effective_snapshot_missing_rollback_trace") {
		t.Fatalf("did not expect missing rollback trace warning under incomplete window, got %+v", report.Evaluation.Findings)
	}
}

func TestRuntimeReplayMarksControlTruthPartialUnderBoundedWindow(t *testing.T) {
	ctx := context.Background()
	store := NewTestStore(t)
	const workspaceID = "ws-p5c-replay-bounded"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P5C Replay Bounded",
		CreatedBy:   "tester",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	now := time.Now().UTC()
	for idx := 0; idx < 3; idx++ {
		if _, err := store.PersistEffectiveControls(ctx, EffectiveControlsInput{
			WorkspaceID:       workspaceID,
			ProtoClusterID:    "cluster-bounded",
			Epoch:             idx + 1,
			TTLSeconds:        600,
			ControlMode:       "steady",
			CandidateMode:     "steady",
			CandidateControls: p5cSuggestedControls(5+idx, "candidate"),
			AdvisoryControls:  p5cSuggestedControls(4+idx, "advisory"),
			EffectiveControls: p5cSuggestedControls(3+idx, "effective"),
			ResolvedFrom:      "proto_cluster",
			GeneratedAt:       now.Add(time.Duration(-idx) * time.Minute).Format(time.RFC3339Nano),
			ActorID:           "operator",
		}); err != nil {
			t.Fatalf("persist effective controls %d: %v", idx+1, err)
		}
	}

	report, err := store.ReplayRuntimeJournal(ctx, RuntimeReplayFilter{
		WorkspaceID: workspaceID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("replay bounded runtime journal: %v", err)
	}
	if !report.Truncated || !report.WindowIncomplete {
		t.Fatalf("expected bounded replay window to remain explicitly incomplete, got %+v", report)
	}
	if report.Evaluation.Verdict != "warn" {
		t.Fatalf("expected bounded replay window to remain warning-scoped, got %+v", report.Evaluation)
	}
	p5cRequireReplayFinding(t, report, "replay_scope_partial")
}

func p5cSuggestedControls(fanout int, focus string) ControlSuggestedControls {
	return ControlSuggestedControls{
		FanoutCap:      fanout,
		ReviewDepth:    maxInt(fanout-1, 1),
		ContextCap:     fanout + 2,
		BridgeQuota:    maxInt(fanout-2, 0),
		MergeThreshold: float64(fanout + 1),
		PriorityFocus:  focus,
	}
}

func p5cRequireReplayFinding(t *testing.T, report RuntimeReplayReport, code string) RuntimeReplayFinding {
	t.Helper()
	for _, finding := range report.Evaluation.Findings {
		if finding.Code == code {
			return finding
		}
	}
	t.Fatalf("expected replay finding %s, got %+v", code, report.Evaluation.Findings)
	return RuntimeReplayFinding{}
}

func p5cHasReplayFindingCode(report RuntimeReplayReport, code string) bool {
	for _, finding := range report.Evaluation.Findings {
		if finding.Code == code {
			return true
		}
	}
	return false
}
