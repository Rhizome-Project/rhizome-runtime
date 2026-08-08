package sqlite

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestMemoryGraphDualWriteFromWorkspaceMemoryLifecycle(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-graph-memory"
		agentID     = "agent-memory-graph"
		sessionID   = "sess-memory-graph"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Graph",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Graph Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   sessionID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "update_digest",
		Title:       "Handoff pending for deploy",
		Body:        "Handoff pending to the next operator after deployment validation.",
		Summary:     "Handoff pending",
		AgentID:     agentID,
		SessionID:   sessionID,
		SourceKind:  "session_event",
		SourceID:    sessionID,
		Importance:  0.7,
		Confidence:  0.8,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get memory graph node: %v", err)
	}
	if detail.TimeAuthority.WorkspaceID != workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected memory graph detail time authority, got %+v", detail.TimeAuthority)
	}
	if detail.Node.MemoryType != "HANDOFF" {
		t.Fatalf("expected HANDOFF node type, got %+v", detail.Node)
	}
	if detail.Node.OriginKind != "workspace_memory" || detail.Node.OriginID != record.MemoryID {
		t.Fatalf("unexpected origin mapping: %+v", detail.Node)
	}
	if detail.Node.LifecycleState != "ACTIVE" {
		t.Fatalf("expected ACTIVE lifecycle, got %+v", detail.Node)
	}
	if len(detail.Refs) < 3 {
		t.Fatalf("expected refs for origin/agent/session, got %+v", detail.Refs)
	}

	if _, err := store.ArchiveWorkspaceMemory(ctx, WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "superseded",
	}); err != nil {
		t.Fatalf("archive workspace memory: %v", err)
	}
	detail, err = store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get archived memory graph node: %v", err)
	}
	if detail.Node.LifecycleState != "ARCHIVED" || detail.Node.ArchivedAt == nil {
		t.Fatalf("expected archived node after archive, got %+v", detail.Node)
	}

	if _, err := store.RestoreWorkspaceMemory(ctx, WorkspaceMemoryRestoreInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		RestoredBy:  "developer",
	}); err != nil {
		t.Fatalf("restore workspace memory: %v", err)
	}
	detail, err = store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get restored memory graph node: %v", err)
	}
	if detail.Node.LifecycleState != "ACTIVE" || detail.Node.ArchivedAt != nil {
		t.Fatalf("expected active node after restore, got %+v", detail.Node)
	}
}

func TestMemoryGraphAnchorStateSurfacesRevisionProtectAndAccess(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-graph-anchor-state"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Graph Anchor State",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "procedure",
		Title:       "Run doctor before release",
		Body:        "Procedural memory should surface protected anchor fields.",
		Summary:     "Run doctor before release.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.7,
		Confidence:  0.9,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get anchor-state node: %v", err)
	}
	if detail.Node.SemanticLineageID != "workspace_memory:"+record.MemoryID {
		t.Fatalf("expected semantic lineage id for workspace memory, got %+v", detail.Node)
	}
	if detail.Node.Revision != 1 {
		t.Fatalf("expected initial revision 1, got %+v", detail.Node)
	}
	if !detail.Node.Protect {
		t.Fatalf("expected procedural node to surface protect=true, got %+v", detail.Node)
	}
	if detail.Node.Unresolved {
		t.Fatalf("did not expect procedural node to start unresolved, got %+v", detail.Node)
	}
	if detail.Node.LastAnyAccess != nil || detail.Node.LastTrustedAccess != nil || detail.Node.TLife != 0 {
		t.Fatalf("did not expect salience-backed access fields before touch, got %+v", detail.Node)
	}

	if err := store.TouchMemoryNodeTrusted(ctx, workspaceID, nodeID, 0.2, DefaultRMPSalienceConfig()); err != nil {
		t.Fatalf("trusted touch: %v", err)
	}
	detail, err = store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get touched anchor-state node: %v", err)
	}
	if detail.Node.LastAnyAccess == nil || detail.Node.LastTrustedAccess == nil || detail.Node.TLife <= 0 {
		t.Fatalf("expected salience-backed access fields after trusted touch, got %+v", detail.Node)
	}
	if detail.Node.RetentionBand == "" || detail.Node.RetentionHotUntil == nil || detail.Node.RetentionWarmUntil == nil || detail.Node.RetentionExpiresAt == nil {
		t.Fatalf("expected trusted touch to surface retention thresholds, got %+v", detail.Node)
	}

	if _, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		MemoryID:    record.MemoryID,
		WorkspaceID: workspaceID,
		MemoryType:  "procedure",
		Title:       "Run doctor before release",
		Body:        "Procedural memory revision should bump when content changes.",
		Summary:     "Run doctor before release, updated.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.7,
		Confidence:  0.9,
	}); err != nil {
		t.Fatalf("update workspace memory: %v", err)
	}
	detail, err = store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get revised anchor-state node: %v", err)
	}
	if detail.Node.Revision < 2 {
		t.Fatalf("expected revision bump after content change, got %+v", detail.Node)
	}

	unresolvedRecord, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "update_digest",
		Title:       "Handoff pending",
		Body:        "Handoff-style memory should surface unresolved anchor state.",
		Summary:     "Handoff pending.",
		SourceKind:  "session_event",
		SourceID:    "sess-anchor-state",
		Importance:  0.6,
		Confidence:  0.7,
	})
	if err != nil {
		t.Fatalf("record unresolved workspace memory: %v", err)
	}
	unresolvedDetail, err := store.GetMemoryGraphNode(ctx, workspaceID, memoryGraphNodeID("workspace_memory", unresolvedRecord.MemoryID))
	if err != nil {
		t.Fatalf("get unresolved anchor-state node: %v", err)
	}
	if !unresolvedDetail.Node.Unresolved || unresolvedDetail.Node.EpistemicStatus != "ALLEGED" {
		t.Fatalf("expected handoff-style node to surface unresolved anchor state, got %+v", unresolvedDetail.Node)
	}
}

func TestMemoryGraphAnchorStateDoesNotRefreshAccessMarkersOnUntrustedTouch(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-graph-untrusted-anchor"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Graph Untrusted Anchor State",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Untrusted touch should not refresh anchor clocks",
		Body:        "Unsafe access should leave surfaced anchor-state access markers unchanged.",
		Summary:     "Unsafe access leaves anchor clocks unchanged.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.7,
		Confidence:  0.8,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	seedTrusted := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano)
	seedAny := time.Now().UTC().Add(-90 * time.Minute).Format(time.RFC3339Nano)
	seedLife := 14400.0
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, nodeID, workspaceID, 0.8, seedTrusted, seedAny, 3, 0.1, seedLife, seedAny, seedAny, seedAny, seedAny); err != nil {
		t.Fatalf("seed salience row: %v", err)
	}

	before, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get seeded memory graph node: %v", err)
	}
	if before.Node.LastAnyAccess == nil || *before.Node.LastAnyAccess != seedAny {
		t.Fatalf("expected seeded last_any_access, got %+v", before.Node)
	}
	if before.Node.LastTrustedAccess == nil || *before.Node.LastTrustedAccess != seedTrusted {
		t.Fatalf("expected seeded last_trusted_access, got %+v", before.Node)
	}
	if before.Node.TLife != seedLife {
		t.Fatalf("expected seeded t_life %v, got %+v", seedLife, before.Node)
	}

	if err := store.TouchMemoryNodeUntrusted(ctx, workspaceID, nodeID); err != nil {
		t.Fatalf("untrusted touch: %v", err)
	}
	after, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get untrusted-touched memory graph node: %v", err)
	}
	if after.Node.LastAnyAccess == nil || *after.Node.LastAnyAccess != seedAny {
		t.Fatalf("expected untrusted touch to leave last_any_access unchanged, got %+v", after.Node)
	}
	if after.Node.LastTrustedAccess == nil || *after.Node.LastTrustedAccess != seedTrusted {
		t.Fatalf("expected untrusted touch to leave last_trusted_access unchanged, got %+v", after.Node)
	}
	if after.Node.TLife != seedLife {
		t.Fatalf("expected untrusted touch to leave t_life unchanged, got %+v", after.Node)
	}
}

func TestMemoryGraphAnchorStateSurfacesRetentionBandAndExpiryFieldsWhenAvailable(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-graph-retention"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Graph Retention",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "procedure",
		Title:       "Retention anchor",
		Body:        "Retention band and expiry should surface from salience thresholds.",
		Summary:     "Retention anchor.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.7,
		Confidence:  0.9,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	now := time.Now().UTC()
	hotAt := now.Add(30 * time.Minute).Format(time.RFC3339Nano)
	warmAt := now.Add(90 * time.Minute).Format(time.RFC3339Nano)
	gcAt := now.Add(3 * time.Hour).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(memory_id) DO UPDATE SET
			a_i=excluded.a_i,
			t_i_star=excluded.t_i_star,
			t_i_acc=excluded.t_i_acc,
			n_i=excluded.n_i,
			q_i=excluded.q_i,
			h_i=excluded.h_i,
			t_hot=excluded.t_hot,
			t_warm=excluded.t_warm,
			t_gc=excluded.t_gc,
			updated_at=excluded.updated_at
	`, nodeID, workspaceID, 0.8, now.Add(-2*time.Hour).Format(time.RFC3339Nano), now.Add(-30*time.Minute).Format(time.RFC3339Nano), 3, 0.2, 7200.0, hotAt, warmAt, gcAt, now.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("seed salience row: %v", err)
	}

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get memory graph node: %v", err)
	}

	nodeJSON, err := json.Marshal(detail.Node)
	if err != nil {
		t.Fatalf("marshal memory graph node: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(nodeJSON, &payload); err != nil {
		t.Fatalf("decode memory graph node: %v", err)
	}

	if payload["retention_hot_until"] != hotAt || payload["retention_warm_until"] != warmAt || payload["retention_expires_at"] != gcAt {
		t.Fatalf("expected exact retention expiry surfaces, got %+v", payload)
	}
	if got, ok := payload["retention_band"].(string); !ok || strings.TrimSpace(got) == "" {
		t.Fatalf("expected non-empty retention_band, got %+v", payload)
	}
	if got, ok := payload["retention_prunable"].(bool); !ok || got {
		t.Fatalf("expected protected retention surface to stay non-prunable, got %+v", payload)
	}
	if payload["retention_guard_reason"] != "PROTECT" {
		t.Fatalf("expected protected retention guard reason, got %+v", payload)
	}
}

func TestMemoryGraphDualWriteFromKnowledgeClaimLifecycle(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-graph-claim"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Graph",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	memoryRecord, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Use deterministic replay",
		Body:        "Deterministic replay remains the canonical explanation path.",
		Summary:     "Use deterministic replay",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.8,
		Confidence:  0.9,
	})
	if err != nil {
		t.Fatalf("record backing memory: %v", err)
	}

	claim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-replay",
		ClaimType:   "decision",
		Status:      "confirmed",
		Subject:     "Replay policy",
		Body:        "Replay policy is canonical.",
		Summary:     "Replay is canonical.",
		Confidence:  0.92,
		SourceKind:  "manual",
		SourceID:    "developer",
		MemoryID:    memoryRecord.MemoryID,
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	nodeID := memoryGraphNodeID("knowledge_claim", claim.ClaimID)
	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get claim graph node: %v", err)
	}
	if detail.Node.MemoryType != "DECISION_RECORD" || detail.Node.CompatType != "DECISION" || detail.Node.EpistemicStatus != "VERIFIED" {
		t.Fatalf("unexpected claim node: %+v", detail.Node)
	}
	if len(detail.OutboundEdges) == 0 || detail.OutboundEdges[0].EdgeType != "DERIVED_FROM" {
		t.Fatalf("expected DERIVED_FROM edge, got %+v", detail.OutboundEdges)
	}
	if detail.OutboundEdges[0].ToMemoryID != memoryGraphNodeID("workspace_memory", memoryRecord.MemoryID) {
		t.Fatalf("unexpected derived edge target: %+v", detail.OutboundEdges[0])
	}

	if _, err := store.DisputeKnowledgeClaim(ctx, KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "developer",
		Reason:      "counter-evidence",
	}); err != nil {
		t.Fatalf("dispute claim: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)
	detail, err = store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get disputed claim node: %v", err)
	}
	if detail.Node.EpistemicStatus != "DISPUTED" {
		t.Fatalf("expected DISPUTED epistemic status, got %+v", detail.Node)
	}
}

func TestMemoryGraphKnowledgeClaimSupersededByRelationFollowsLifecycleLink(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-graph-supersede"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Supersede Graph",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)

	older, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-older",
		ClaimType:   "fact",
		Status:      "confirmed",
		Subject:     "Legacy runtime policy",
		Body:        "The older claim stays only for lineage.",
		Summary:     "Older claim.",
		Confidence:  0.6,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record older claim: %v", err)
	}
	newer, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-newer",
		ClaimType:   "fact",
		Status:      "confirmed",
		Subject:     "Current runtime policy",
		Body:        "The newer claim replaces the legacy claim.",
		Summary:     "Newer claim.",
		Confidence:  0.9,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record newer claim: %v", err)
	}

	if _, err := store.SupersedeKnowledgeClaim(ctx, KnowledgeClaimLifecycleInput{
		WorkspaceID:        workspaceID,
		ClaimID:            older.ClaimID,
		ActorID:            "developer",
		Reason:             "newer canonical wording",
		SupersedingClaimID: newer.ClaimID,
	}); err != nil {
		t.Fatalf("supersede older claim: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, memoryGraphNodeID("knowledge_claim", older.ClaimID))
	if err != nil {
		t.Fatalf("get superseded claim node: %v", err)
	}
	if detail.Node.LifecycleState != "SUPERSEDED" {
		t.Fatalf("expected SUPERSEDED lifecycle, got %+v", detail.Node)
	}

	found := false
	for _, edge := range detail.OutboundEdges {
		if edge.EdgeType == "SUPERSEDED_BY" && edge.ToMemoryID == memoryGraphNodeID("knowledge_claim", newer.ClaimID) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected SUPERSEDED_BY edge to newer claim, got %+v", detail.OutboundEdges)
	}
}

func TestMemoryGraphKnowledgeClaimPreservesDissentType(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-graph-dissent"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Dissent Graph",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	claim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-dissent",
		ClaimType:   "dissent",
		Status:      "active",
		Subject:     "Keep the contrarian path visible",
		Body:        "Dissent should stay first-class in the semantic claim graph.",
		Summary:     "Keep dissent visible.",
		Confidence:  0.61,
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record dissent claim: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, memoryGraphNodeID("knowledge_claim", claim.ClaimID))
	if err != nil {
		t.Fatalf("get dissent claim node: %v", err)
	}
	if detail.Node.MemoryType != "DISSENT" || detail.Node.MemoryLayer != "SEMANTIC" {
		t.Fatalf("expected DISSENT semantic node, got %+v", detail.Node)
	}
}

func TestMemoryGraphKnowledgeClaimDifferentiatesDissentMarkerAndContent(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-graph-dissent-split"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Claim Dissent Split Graph",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	cases := []struct {
		claimID        string
		claimType      string
		wantPredicate  string
		wantModality   string
		wantMemoryType string
	}{
		{claimID: "claim-dissent-marker", claimType: "dissent_marker", wantPredicate: "signals_dissent", wantModality: "observed", wantMemoryType: "DISSENT_MARKER"},
		{claimID: "claim-dissent-content", claimType: "dissent_content", wantPredicate: "critiques", wantModality: "proposed", wantMemoryType: "DISSENT_CONTENT"},
	}
	for _, tc := range cases {
		tc := tc
		claim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
			WorkspaceID: workspaceID,
			ClaimID:     tc.claimID,
			ClaimType:   tc.claimType,
			Status:      "active",
			Subject:     tc.claimID,
			Body:        "Bounded dissent split should keep graph semantics inspectable.",
			Summary:     tc.claimID,
			Confidence:  0.66,
			SourceKind:  "manual",
			SourceID:    "developer",
		})
		if err != nil {
			t.Fatalf("record %s claim: %v", tc.claimType, err)
		}
		mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

		detail, err := store.GetMemoryGraphNode(ctx, workspaceID, memoryGraphNodeID("knowledge_claim", claim.ClaimID))
		if err != nil {
			t.Fatalf("get %s claim node: %v", tc.claimType, err)
		}
		if detail.Node.MemoryType != tc.wantMemoryType || detail.Node.MemoryLayer != "SEMANTIC" {
			t.Fatalf("expected %s semantic node, got %+v", tc.wantMemoryType, detail.Node)
		}
		if detail.Node.ClaimPredicate != tc.wantPredicate || detail.Node.ClaimModality != tc.wantModality {
			t.Fatalf("expected %s predicate=%s modality=%s, got %+v", tc.wantMemoryType, tc.wantPredicate, tc.wantModality, detail.Node)
		}
	}
}

func TestMemoryGraphSyncWorkspaceBackfillsCanonicalNodes(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const workspaceID = "ws-memory-graph-sync"
	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Graph Sync",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Keep provenance",
		Body:        "Memory must preserve provenance to avoid silent drift.",
		Summary:     "Keep provenance.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	if _, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-provenance",
		ClaimType:   "lesson",
		Status:      "active",
		Subject:     "Provenance",
		Body:        "Provenance must remain visible.",
		Summary:     "Preserve provenance.",
		Confidence:  0.7,
		SourceKind:  "manual",
		SourceID:    "developer",
		MemoryID:    record.MemoryID,
	}); err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_edges; DELETE FROM memory_node_versions; DELETE FROM memory_node_refs; DELETE FROM memory_nodes;`); err != nil {
		t.Fatalf("clear memory graph tables: %v", err)
	}

	result, err := store.SyncMemoryGraphWorkspace(ctx, workspaceID)
	if err != nil {
		t.Fatalf("sync memory graph workspace: %v", err)
	}
	if result.WorkspaceMemorySynced == 0 || result.KnowledgeClaimsSynced == 0 {
		t.Fatalf("expected sync counts, got %+v", result)
	}

	items, err := store.ListMemoryGraphNodes(ctx, MemoryGraphNodeFilter{WorkspaceID: workspaceID, Limit: 10})
	if err != nil {
		t.Fatalf("list memory graph nodes: %v", err)
	}
	if len(items) < 2 {
		t.Fatalf("expected backfilled nodes, got %+v", items)
	}
}

func TestMemoryGraphWorkspaceMemoryDriftReportTracksSourceDocUpdates(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-graph-drift"
		docKey      = "runbook-drift"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Graph Drift",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nInitial deployment procedure.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert initial workspace doc: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Prefer canonical runbook",
		Body:        "Canonical runbook truth stays above stale cache entries.",
		Summary:     "Prefer canonical runbook.",
		SourceKind:  "workspace_doc",
		SourceID:    docKey,
		Importance:  0.8,
		Confidence:  0.9,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get current memory graph node: %v", err)
	}
	if detail.TimeAuthority.WorkspaceID != workspaceID || detail.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected drift detail time authority, got %+v", detail.TimeAuthority)
	}
	if detail.DriftReport == nil || detail.DriftReport.Status != "CURRENT" || detail.Node.Drift != 0 {
		t.Fatalf("expected current drift report before doc update, got node=%+v report=%+v", detail.Node, detail.DriftReport)
	}
	if detail.DriftReport.TimeAuthority.WorkspaceID != workspaceID || detail.DriftReport.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected drift report time authority, got %+v", detail.DriftReport.TimeAuthority)
	}

	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nUpdated deployment procedure with rollback.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert updated workspace doc: %v", err)
	}

	detail, err = store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get drifted memory graph node: %v", err)
	}
	if detail.DriftReport == nil || detail.DriftReport.Status != "STALE" {
		t.Fatalf("expected stale drift report after doc update, got %+v", detail.DriftReport)
	}
	if detail.DriftReport.TimeAuthority.WorkspaceID != workspaceID || detail.DriftReport.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected stale drift report time authority, got %+v", detail.DriftReport.TimeAuthority)
	}
	if detail.DriftReport.DriftedRefCount == 0 || detail.Node.Drift <= 0 {
		t.Fatalf("expected non-zero drift after doc update, got node=%+v report=%+v", detail.Node, detail.DriftReport)
	}
}

func TestMemoryGraphArchivedExpiredWorkspaceMemorySurfacesRecoveryCandidateAfterSourceDrift(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-graph-recovery-candidate"
		docKey      = "runbook-recovery-candidate"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Graph Recovery Candidate",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Recovery Candidate Runbook",
		Content:     "# Recovery\nDocumented rollout contract.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Recovery candidate memory",
		Body:        "Archived expired memory should become recoverable when its grounded doc drifts.",
		Summary:     "Recovery candidate memory.",
		SourceKind:  "workspace_doc",
		SourceID:    docKey,
		Importance:  0.7,
		Confidence:  0.8,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)
	if _, err := store.ArchiveWorkspaceMemory(ctx, WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		ArchivedBy:  "rmp_pruner",
		Reason:      rmpArchivedReasonExpired,
	}); err != nil {
		t.Fatalf("archive workspace memory as expired: %v", err)
	}

	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	before, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get archived recovery-candidate node before doc drift: %v", err)
	}
	if before.Node.RecoveryCandidate || before.Node.RecoveryTriggerCount != 0 || before.Node.RecoveryGuardReason != "NO_TRIGGERED_LINKAGE" {
		t.Fatalf("expected archived expired node to wait for drift trigger, got %+v", before.Node)
	}

	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Recovery Candidate Runbook",
		Content:     "# Recovery\nDocumented rollout contract changed.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("update workspace doc: %v", err)
	}

	after, err := store.GetMemoryGraphNode(ctx, workspaceID, nodeID)
	if err != nil {
		t.Fatalf("get archived recovery-candidate node after doc drift: %v", err)
	}
	if after.DriftReport == nil || after.DriftReport.Status != "STALE" {
		t.Fatalf("expected stale drift report for archived expired node, got %+v", after.DriftReport)
	}
	if !after.Node.RecoveryCandidate || after.Node.RecoveryTriggerCount == 0 {
		t.Fatalf("expected archived expired node to surface recovery candidate, got %+v", after.Node)
	}
	if len(after.Node.RecoveryTriggerKinds) != 1 || after.Node.RecoveryTriggerKinds[0] != "workspace_doc" {
		t.Fatalf("expected workspace_doc recovery trigger kind, got %+v", after.Node)
	}
	if after.Node.RecoveryGuardReason != "" {
		t.Fatalf("did not expect recovery guard reason after doc drift trigger, got %+v", after.Node)
	}
}

func TestSessionCompactionSnapshotCreatesEpisodePackAndGraphMetrics(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-episode-pack"
		agentID     = "agent-episode-pack"
		sessionID   = "sess-episode-pack"
		taskID      = "task-episode-pack"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Episode Packs",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Episode Pack Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	summaryMemory, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "summary",
		Title:       "Compaction summary",
		Body:        "Legacy compatibility summary for compaction.",
		Summary:     "Legacy compaction summary.",
		AgentID:     agentID,
		SessionID:   sessionID,
		SourceKind:  "compaction",
		SourceID:    sessionID,
	})
	if err != nil {
		t.Fatalf("record summary workspace memory: %v", err)
	}

	snapshot, err := store.RecordSessionCompactionSnapshot(ctx, SessionCompactionSnapshotInput{
		SessionID:              sessionID,
		WorkspaceID:            workspaceID,
		AgentID:                agentID,
		TriggerKind:            "token_budget_exceeded",
		PackMode:               episodePackModeFallback,
		SourceWindowDigest:     "digest-window-1",
		TokenBudget:            1200,
		MessageCountBefore:     8,
		MessageCountAfter:      4,
		MessageTokensBefore:    1600,
		MessageTokensAfter:     620,
		TotalInputTokens:       2200,
		TotalOutputTokens:      700,
		SummaryText:            "[Previous conversation history was truncated due to length. 4 messages were removed.]",
		SummaryWorkspaceMemory: summaryMemory.MemoryID,
	})
	if err != nil {
		t.Fatalf("record compaction snapshot: %v", err)
	}
	if snapshot.EpisodePackID == "" || snapshot.CanonicalMemoryID == "" {
		t.Fatalf("expected canonical episode-pack links, got %+v", snapshot)
	}

	pack, err := store.GetEpisodePack(ctx, workspaceID, snapshot.EpisodePackID)
	if err != nil {
		t.Fatalf("get episode pack: %v", err)
	}
	if pack.PackMode != episodePackModeFallback {
		t.Fatalf("expected fallback mode, got %+v", pack)
	}
	if pack.SourceWindowDigest != "digest-window-1" {
		t.Fatalf("expected source window digest, got %+v", pack)
	}
	if pack.SummaryWorkspaceMemory != summaryMemory.MemoryID {
		t.Fatalf("expected summary memory backlink, got %+v", pack)
	}
	if pack.TaskID != taskID {
		t.Fatalf("expected task context, got %+v", pack)
	}

	detail, err := store.GetMemoryGraphNode(ctx, workspaceID, pack.CanonicalMemoryID)
	if err != nil {
		t.Fatalf("get canonical episode-pack node: %v", err)
	}
	if detail.Node.OriginKind != "episode_pack" || detail.Node.MemoryType != "EPISODE_PACK" {
		t.Fatalf("unexpected episode-pack node %+v", detail.Node)
	}
	if len(detail.Metrics) == 0 {
		t.Fatalf("expected node metrics for episode pack, got %+v", detail)
	}
	foundTransfer := false
	for _, edge := range detail.OutboundEdges {
		if edge.EdgeType == "TRANSFERRED_TO" && edge.ToMemoryID == memoryGraphNodeID("workspace_memory", summaryMemory.MemoryID) {
			foundTransfer = true
			break
		}
	}
	if !foundTransfer {
		t.Fatalf("expected TRANSFERRED_TO edge to summary memory, got %+v", detail.OutboundEdges)
	}
}

func TestSyncEpisodePacksWorkspaceBackfillsLegacyCompactionSnapshots(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-episode-pack-backfill"
		agentID     = "agent-episode-pack-backfill"
		sessionID   = "sess-episode-pack-backfill"
		snapshotID  = "compaction-legacy-1"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Episode Pack Backfill",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Backfill Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	if _, err := store.DB().ExecContext(
		ctx,
		`INSERT INTO session_compaction_snapshots(
		    snapshot_id, session_id, workspace_id, agent_id, trigger_kind, token_budget,
		    message_count_before, message_count_after, message_tokens_before, message_tokens_after,
		    total_input_tokens, total_output_tokens, summary_text, summary_workspace_memory
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		snapshotID,
		sessionID,
		workspaceID,
		agentID,
		"token_budget_exceeded",
		1000,
		7,
		4,
		1400,
		560,
		2000,
		600,
		"Compaction summary for legacy snapshot.",
	); err != nil {
		t.Fatalf("insert legacy compaction snapshot: %v", err)
	}

	result, err := store.SyncEpisodePacksWorkspace(ctx, workspaceID)
	if err != nil {
		t.Fatalf("sync episode packs workspace: %v", err)
	}
	if result.PacksSynced != 1 {
		t.Fatalf("expected one synced pack, got %+v", result)
	}

	packs, err := store.ListEpisodePacks(ctx, EpisodePackFilter{WorkspaceID: workspaceID, Limit: 10})
	if err != nil {
		t.Fatalf("list episode packs: %v", err)
	}
	if len(packs) != 1 {
		t.Fatalf("expected one episode pack, got %+v", packs)
	}
	if packs[0].CompactionSnapshotID != snapshotID {
		t.Fatalf("expected snapshot backlink, got %+v", packs[0])
	}
	if _, err := store.GetMemoryGraphNode(ctx, workspaceID, packs[0].CanonicalMemoryID); err != nil {
		t.Fatalf("get backfilled episode-pack node: %v", err)
	}
}

func TestGetMemoryGraphAtlas_FocusNeighborhoodWithAnchors(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-atlas"
		agentID     = "agent-memory-atlas"
		sessionID   = "sess-memory-atlas"
		taskID      = "task-memory-atlas"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Atlas",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Atlas Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   sessionID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		TaskID:      taskID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO tasks (task_id, title, owner_user_id, status, priority, task_kind, created_at, updated_at)
		VALUES (?, 'Atlas Task', 'developer', 'RUNNING', 'high', 'standard', ?, ?)
	`, taskID, now, now); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_tasks (workspace_id, task_id, linked_by, created_at)
		VALUES (?, ?, 'developer', ?)
	`, workspaceID, taskID, now); err != nil {
		t.Fatalf("insert workspace task: %v", err)
	}

	left, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "procedure",
		Title:       "Atlas deploy checklist",
		Body:        "Checklist memory for atlas focus mode.",
		Summary:     "Deploy checklist",
		AgentID:     agentID,
		SessionID:   sessionID,
		TaskID:      taskID,
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.91,
		Confidence:  0.86,
	})
	if err != nil {
		t.Fatalf("record left memory: %v", err)
	}
	right, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Atlas rollout decision",
		Body:        "Prefer canary rollout when blocker pressure is elevated.",
		Summary:     "Prefer canary rollout",
		AgentID:     agentID,
		SessionID:   sessionID,
		TaskID:      taskID,
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.84,
		Confidence:  0.82,
	})
	if err != nil {
		t.Fatalf("record right memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_edges (
			edge_id, workspace_id, from_memory_id, to_memory_id, edge_type, source_kind, source_id, weight, metadata_json, created_at, updated_at
		) VALUES (?, ?, ?, ?, 'related_to', 'workspace_memory', ?, 0.93, '{}', ?, ?)
	`, "edge-memory-atlas-1", workspaceID, memoryGraphNodeID("workspace_memory", left.MemoryID), memoryGraphNodeID("workspace_memory", right.MemoryID), left.MemoryID, now, now); err != nil {
		t.Fatalf("insert memory edge: %v", err)
	}

	snap, err := store.GetMemoryGraphAtlas(ctx, MemoryGraphAtlasRequest{
		WorkspaceID:    workspaceID,
		CenterMemoryID: memoryGraphNodeID("workspace_memory", left.MemoryID),
		IncludeAnchors: true,
		CanonicalOnly:  true,
		Depth:          1,
		LimitNodes:     20,
		LimitEdges:     20,
	})
	if err != nil {
		t.Fatalf("get memory graph atlas: %v", err)
	}
	if snap == nil {
		t.Fatal("expected non-nil atlas snapshot")
	}
	if snap.Mode != "MEMORY_ATLAS" {
		t.Fatalf("expected MEMORY_ATLAS mode, got %+v", snap.Mode)
	}

	expectedFocus := memoryGraphNodeID("workspace_memory", left.MemoryID)
	if snap.Focus != expectedFocus {
		t.Fatalf("expected atlas focus %q, got %q", expectedFocus, snap.Focus)
	}
	expectedFocusNodeID := graphMemoryNodeID(expectedFocus)
	expectedRightNodeID := graphMemoryNodeID(memoryGraphNodeID("workspace_memory", right.MemoryID))

	hasLeft := false
	hasRight := false
	hasTask := false
	hasSession := false
	hasAgent := false
	hasMemoryEdge := false
	hasTaskAnchor := false
	hasSessionAnchor := false
	hasAgentAnchor := false

	for _, node := range snap.Nodes {
		switch node.ID {
		case expectedFocusNodeID:
			hasLeft = node.Type == "memory_node"
		case expectedRightNodeID:
			hasRight = node.Type == "memory_node"
		case taskID:
			hasTask = node.Type == "task"
		case sessionID:
			hasSession = node.Type == "session"
		case agentID:
			hasAgent = node.Type == "agent"
		}
	}
	for _, edge := range snap.Edges {
		if edge.Source == expectedFocusNodeID && edge.Target == expectedRightNodeID && edge.Label == "related_to" {
			hasMemoryEdge = true
		}
		if edge.Source == taskID && edge.Target == expectedFocusNodeID && edge.Label == "anchors_memory" {
			hasTaskAnchor = true
		}
		if edge.Source == sessionID && edge.Target == expectedFocusNodeID && edge.Label == "emits_memory" {
			hasSessionAnchor = true
		}
		if edge.Source == agentID && edge.Target == expectedFocusNodeID && edge.Label == "holds_memory" {
			hasAgentAnchor = true
		}
	}

	if !hasLeft || !hasRight {
		t.Fatalf("expected focused canonical memory neighborhood, got nodes %+v", snap.Nodes)
	}
	if !hasTask || !hasSession || !hasAgent {
		t.Fatalf("expected atlas anchors to be present, got nodes %+v", snap.Nodes)
	}
	if !hasMemoryEdge || !hasTaskAnchor || !hasSessionAnchor || !hasAgentAnchor {
		t.Fatalf("expected atlas edges to include memory link and anchors, got %+v", snap.Edges)
	}

	budgeted, err := store.GetMemoryGraphAtlas(ctx, MemoryGraphAtlasRequest{
		WorkspaceID:    workspaceID,
		CenterMemoryID: memoryGraphNodeID("workspace_memory", left.MemoryID),
		IncludeAnchors: true,
		CanonicalOnly:  true,
		Depth:          1,
		LimitNodes:     3,
		LimitEdges:     2,
	})
	if err != nil {
		t.Fatalf("get budgeted memory graph atlas: %v", err)
	}
	if len(budgeted.Nodes) > 3 {
		t.Fatalf("expected node budget to hold with anchors, got %d nodes", len(budgeted.Nodes))
	}
	if len(budgeted.Edges) > 2 {
		t.Fatalf("expected edge budget to hold with anchors, got %d edges", len(budgeted.Edges))
	}
}

func TestGetMemoryGraphAtlas_FiltersArchiveAndLineageStats(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-atlas-filters"
		agentID     = "agent-memory-atlas-filters"
		sessionID   = "sess-memory-atlas-filters"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Atlas Filters",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Atlas Filter Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   sessionID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	primary, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "procedure",
		Title:       "Atlas source anchor",
		Body:        "Primary workspace memory for atlas lineage tests.",
		Summary:     "Primary anchor",
		AgentID:     agentID,
		SessionID:   sessionID,
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.92,
		Confidence:  0.84,
	})
	if err != nil {
		t.Fatalf("record primary memory: %v", err)
	}
	sibling, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Atlas disputed sibling",
		Body:        "Sibling memory that will be re-labeled as derived and disputed.",
		Summary:     "Disputed sibling",
		AgentID:     agentID,
		SessionID:   sessionID,
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.48,
		Confidence:  0.41,
	})
	if err != nil {
		t.Fatalf("record sibling memory: %v", err)
	}
	archived, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "update_digest",
		Title:       "Atlas archived branch",
		Body:        "Archived memory for include_archived filter coverage.",
		Summary:     "Archived branch",
		AgentID:     agentID,
		SessionID:   sessionID,
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.31,
		Confidence:  0.63,
	})
	if err != nil {
		t.Fatalf("record archived memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	primaryNodeID := memoryGraphNodeID("workspace_memory", primary.MemoryID)
	siblingNodeID := memoryGraphNodeID("workspace_memory", sibling.MemoryID)
	if _, err := store.DB().ExecContext(ctx, `
		UPDATE memory_nodes
		SET semantic_lineage_id = (SELECT semantic_lineage_id FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?),
		    origin_kind = 'knowledge_claim',
		    epistemic_status = 'DISPUTED',
		    lifecycle_state = 'DORMANT',
		    activation = 0.24,
		    importance = 0.46
		WHERE workspace_id = ? AND memory_id = ?
	`, workspaceID, primaryNodeID, workspaceID, siblingNodeID); err != nil {
		t.Fatalf("update sibling memory projection: %v", err)
	}
	if _, err := store.ArchiveWorkspaceMemory(ctx, WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    archived.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "atlas test",
	}); err != nil {
		t.Fatalf("archive memory: %v", err)
	}

	filtered, err := store.GetMemoryGraphAtlas(ctx, MemoryGraphAtlasRequest{
		WorkspaceID:     workspaceID,
		OriginKind:      "knowledge_claim",
		EpistemicStatus: "DISPUTED",
		LifecycleState:  "DORMANT",
		CanonicalOnly:   false,
		IncludeAnchors:  false,
		LimitNodes:      20,
		LimitEdges:      20,
	})
	if err != nil {
		t.Fatalf("get filtered atlas: %v", err)
	}
	if len(filtered.Nodes) != 1 || filtered.Nodes[0].OriginKind != "knowledge_claim" {
		t.Fatalf("expected only derived disputed sibling in filtered atlas, got %+v", filtered.Nodes)
	}
	if stats, ok := filtered.Stats.(map[string]any); ok {
		if stats["origin_kind"] != "knowledge_claim" || stats["epistemic_status"] != "DISPUTED" || stats["lifecycle_state"] != "DORMANT" {
			t.Fatalf("expected atlas stats to echo filters, got %+v", stats)
		}
		if seeds, ok := stats["seed_source_counts"].(map[string]int); ok {
			if seeds["knowledge_claim"] == 0 {
				t.Fatalf("expected knowledge_claim seeds in stats, got %+v", seeds)
			}
		}
	}

	withArchived, err := store.GetMemoryGraphAtlas(ctx, MemoryGraphAtlasRequest{
		WorkspaceID:     workspaceID,
		Query:           "archived branch",
		CanonicalOnly:   true,
		IncludeAnchors:  false,
		IncludeArchived: true,
		LimitNodes:      20,
		LimitEdges:      20,
	})
	if err != nil {
		t.Fatalf("get archived-inclusive atlas: %v", err)
	}
	if len(withArchived.Nodes) == 0 {
		t.Fatalf("expected archived query to return at least one node when include_archived=true, got %+v", withArchived)
	}
	withoutArchived, err := store.GetMemoryGraphAtlas(ctx, MemoryGraphAtlasRequest{
		WorkspaceID:     workspaceID,
		Query:           "archived branch",
		CanonicalOnly:   true,
		IncludeAnchors:  false,
		IncludeArchived: false,
		LimitNodes:      20,
		LimitEdges:      20,
	})
	if err != nil {
		t.Fatalf("get archived-exclusive atlas: %v", err)
	}
	if len(withoutArchived.Nodes) != 0 {
		t.Fatalf("expected archived query to disappear when include_archived=false, got %+v", withoutArchived.Nodes)
	}

	focused, err := store.GetMemoryGraphAtlas(ctx, MemoryGraphAtlasRequest{
		WorkspaceID:    workspaceID,
		CenterMemoryID: primaryNodeID,
		CanonicalOnly:  false,
		IncludeAnchors: false,
		Depth:          1,
		LimitNodes:     20,
		LimitEdges:     20,
	})
	if err != nil {
		t.Fatalf("get focused atlas: %v", err)
	}
	lineageEdgeSeen := false
	for _, edge := range focused.Edges {
		if edge.Label == "semantic_lineage" &&
			edge.Source == graphMemoryNodeID(primaryNodeID) &&
			edge.Target == graphMemoryNodeID(siblingNodeID) {
			lineageEdgeSeen = true
			break
		}
	}
	if !lineageEdgeSeen {
		t.Fatalf("expected semantic_lineage edge in focused atlas, got %+v", focused.Edges)
	}
	if stats, ok := focused.Stats.(map[string]any); ok {
		if got := int(stats["lineage_edge_count"].(int)); got < 1 {
			t.Fatalf("expected lineage_edge_count >= 1, got %+v", stats)
		}
		if got := int(stats["frontier_hops"].(int)); got < 1 {
			t.Fatalf("expected frontier_hops >= 1, got %+v", stats)
		}
	}

	priorityClipped, err := store.GetMemoryGraphAtlas(ctx, MemoryGraphAtlasRequest{
		WorkspaceID:    workspaceID,
		CenterMemoryID: primaryNodeID,
		CanonicalOnly:  false,
		IncludeAnchors: true,
		Depth:          1,
		LimitNodes:     20,
		LimitEdges:     2,
	})
	if err != nil {
		t.Fatalf("get clipped focused atlas: %v", err)
	}
	hasPriorityLineage := false
	hasPriorityAnchor := false
	for _, edge := range priorityClipped.Edges {
		if edge.Label == "semantic_lineage" {
			hasPriorityLineage = true
		}
		if edge.Label == "anchors_memory" || edge.Label == "emits_memory" || edge.Label == "holds_memory" {
			hasPriorityAnchor = true
		}
	}
	if !hasPriorityLineage || !hasPriorityAnchor {
		t.Fatalf("expected clipped atlas to retain lineage and at least one anchor edge, got %+v", priorityClipped.Edges)
	}
}

func TestGetMemoryGraphAtlas_RejectsInvalidTypeAndOrigin(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: "ws-memory-atlas-invalid-store",
		Title:       "Memory Atlas Invalid Store",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	cases := []struct {
		name string
		req  MemoryGraphAtlasRequest
		want string
	}{
		{
			name: "invalid memory type",
			req: MemoryGraphAtlasRequest{
				WorkspaceID: "ws-memory-atlas-invalid-store",
				MemoryType:  "DECISON",
			},
			want: "memory_type",
		},
		{
			name: "invalid origin kind",
			req: MemoryGraphAtlasRequest{
				WorkspaceID: "ws-memory-atlas-invalid-store",
				OriginKind:  "workspace_segment",
			},
			want: "origin_kind",
		},
		{
			name: "canonical conflict",
			req: MemoryGraphAtlasRequest{
				WorkspaceID:   "ws-memory-atlas-invalid-store",
				CanonicalOnly: true,
				OriginKind:    "knowledge_claim",
			},
			want: "canonical_only",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.GetMemoryGraphAtlas(ctx, tc.req); err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.want)) {
				t.Fatalf("expected %q validation error, got %v", tc.want, err)
			}
		})
	}
}
