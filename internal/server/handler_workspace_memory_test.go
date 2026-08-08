package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryListIncludesEntriesAndSummary(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-memory-view"
		agentID     = "agent-memory"
		sessionID   = "sess-memory"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	blockedRecord, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "update_digest",
		Title:       "Waiting for signoff",
		Body:        "The active session is blocked pending human signoff on the deploy path.",
		Summary:     "Waiting for signoff",
		AgentID:     agentID,
		SessionID:   sessionID,
		SourceKind:  "session_event",
		SourceID:    sessionID,
		Tags:        []string{"blocked"},
		Importance:  0.92,
		Confidence:  0.83,
	})
	if err != nil {
		t.Fatalf("record blocked memory: %v", err)
	}
	blockedNodeID := "memnode:workspace_memory:" + blockedRecord.MemoryID
	if err := store.TouchMemoryNodeTrusted(ctx, workspaceID, blockedNodeID, 0.0, sqlite.DefaultRMPSalienceConfig()); err != nil {
		t.Fatalf("trusted touch blocked memory node: %v", err)
	}

	manualRecord, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Use workspace memory",
		Body:        "Canonical runtime truth lives in workspace memory.",
		Summary:     "Adopt workspace memory as the durable source of truth.",
		AgentID:     agentID,
		SourceKind:  "manual",
		SourceID:    "developer",
		Tags:        []string{"memory", "decision"},
		Importance:  0.71,
		Confidence:  0.91,
	})
	if err != nil {
		t.Fatalf("record manual memory: %v", err)
	}
	if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    manualRecord.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "superseded",
	}); err != nil {
		t.Fatalf("archive manual memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	raw, err := json.Marshal(workspaceMemoryListParams{
		WorkspaceID:     workspaceID,
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryList(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryList rpc error: %+v", rpcErr)
	}

	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", result)
	}

	items, ok := payload["items"].([]sqlite.WorkspaceMemoryRecord)
	if !ok {
		t.Fatalf("unexpected items type %T", payload["items"])
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	authority, ok := payload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || authority.WorkspaceID != workspaceID || authority.ReferenceAt == "" {
		t.Fatalf("expected workspace memory list time authority, got %+v", payload["time_authority"])
	}

	entries, ok := payload["entries"].([]workspaceMemoryView)
	if !ok {
		t.Fatalf("unexpected entries type %T", payload["entries"])
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	summary, ok := payload["summary"].(workspaceMemorySummary)
	if !ok {
		t.Fatalf("unexpected summary type %T", payload["summary"])
	}
	if summary.ActiveCount != 1 || summary.ArchivedCount != 1 {
		t.Fatalf("unexpected active/archive counts: %+v", summary)
	}
	if summary.DerivedCount != 1 || summary.LiveSignalCount != 1 || summary.AttentionCount != 1 {
		t.Fatalf("unexpected derived/live/attention counts: %+v", summary)
	}
	if summary.BySource["session_event"] != 1 || summary.BySource["manual"] != 1 {
		t.Fatalf("unexpected by_source: %+v", summary.BySource)
	}

	var blockedEntry workspaceMemoryView
	for _, entry := range entries {
		if entry.Record.MemoryID == blockedRecord.MemoryID {
			blockedEntry = entry
			break
		}
	}
	if blockedEntry.Record.MemoryID == "" {
		t.Fatalf("blocked entry not found")
	}
	if blockedEntry.Meta.State != "ACTIVE" {
		t.Fatalf("expected blocked entry to stay active, got %+v", blockedEntry.Meta)
	}
	if !blockedEntry.Meta.Derived || !blockedEntry.Meta.LiveSignal || !blockedEntry.Meta.RequiresAttention {
		t.Fatalf("expected blocked entry to be derived live attention signal, got %+v", blockedEntry.Meta)
	}
	if blockedEntry.Meta.AttentionLabel != "Blocked" {
		t.Fatalf("expected blocked attention label, got %+v", blockedEntry.Meta)
	}
	if blockedEntry.Meta.SourceLabel != "Session Event" || blockedEntry.Meta.TypeLabel != "Update Digest" {
		t.Fatalf("unexpected labels: %+v", blockedEntry.Meta)
	}
	if !strings.Contains(blockedEntry.Meta.Context, "Agent "+agentID) || !strings.Contains(blockedEntry.Meta.Context, "Session "+sessionID) {
		t.Fatalf("expected context to include agent and session, got %q", blockedEntry.Meta.Context)
	}
	if !strings.Contains(blockedEntry.Meta.Provenance, "Derived from Session Event") {
		t.Fatalf("expected provenance to mention session event, got %q", blockedEntry.Meta.Provenance)
	}
	if blockedEntry.Meta.CanonicalAuthority != "workspace_memory" ||
		blockedEntry.Meta.AnchorAuthority != "compatibility_only" ||
		blockedEntry.Meta.AnchorStatus != "DERIVED_READY" {
		t.Fatalf("expected blocked entry to expose mixed-state authority contract, got %+v", blockedEntry.Meta)
	}
	if blockedEntry.Meta.AnchorInvariantState != sqlite.WorkspaceMemoryProjectionInvariantCurrent {
		t.Fatalf("expected blocked entry invariant state to stay current, got %+v", blockedEntry.Meta)
	}
	if blockedEntry.Meta.AnchorSemanticLineageID != "workspace_memory:"+blockedRecord.MemoryID {
		t.Fatalf("expected anchor semantic lineage id for blocked entry, got %+v", blockedEntry.Meta)
	}
	if blockedEntry.Meta.AnchorRevision < 1 {
		t.Fatalf("expected anchor revision on blocked entry, got %+v", blockedEntry.Meta)
	}
	if blockedEntry.Meta.AnchorProtect == nil || *blockedEntry.Meta.AnchorProtect {
		t.Fatalf("expected blocked entry to remain non-protected, got %+v", blockedEntry.Meta)
	}
	if blockedEntry.Meta.AnchorUnresolved == nil {
		t.Fatalf("expected blocked entry to surface anchor unresolved marker, got %+v", blockedEntry.Meta)
	}
	if blockedEntry.Meta.AnchorLastAnyAccess == nil || blockedEntry.Meta.AnchorLastTrustedAccess == nil {
		t.Fatalf("expected blocked entry to surface trusted anchor access markers, got %+v", blockedEntry.Meta)
	}
	if blockedEntry.Meta.AnchorTLife == nil || *blockedEntry.Meta.AnchorTLife <= 0 {
		t.Fatalf("expected blocked entry to surface positive anchor t_life, got %+v", blockedEntry.Meta)
	}
	if blockedEntry.Meta.RetentionBand == "" || blockedEntry.Meta.RetentionHotUntil == nil || blockedEntry.Meta.RetentionWarmUntil == nil || blockedEntry.Meta.RetentionExpiresAt == nil {
		t.Fatalf("expected blocked entry to surface additive retention state, got %+v", blockedEntry.Meta)
	}
	if blockedEntry.Meta.RetentionPrunable {
		t.Fatalf("expected blocked unresolved entry to stay non-prunable, got %+v", blockedEntry.Meta)
	}
	if blockedEntry.Meta.RetentionGuardReason != "" {
		t.Fatalf("did not expect blocked blocker entry to surface a retention guard reason, got %+v", blockedEntry.Meta)
	}
	if blockedEntry.Meta.SalienceA <= 0 || blockedEntry.Meta.SalienceTStar == "" || blockedEntry.Meta.SalienceH <= 0 {
		t.Fatalf("expected blocked entry to preserve legacy salience compatibility fields, got %+v", blockedEntry.Meta)
	}
}

func TestWorkspaceMemorySearchReturnsDerivedEntries(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-memory-search"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Search Memory",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Bridge dedupe",
		Body:        "Use stable delivery ids after workspace reset to avoid duplicate wake processing.",
		Summary:     "Delivery IDs must remain stable across reset.",
		SourceKind:  "compaction",
		SourceID:    "session-123",
		Tags:        []string{"bridge", "dedupe"},
	})
	if err != nil {
		t.Fatalf("record lesson memory: %v", err)
	}
	nodeID := "memnode:workspace_memory:" + record.MemoryID
	if err := store.TouchMemoryNodeTrusted(ctx, workspaceID, nodeID, 0.0, sqlite.DefaultRMPSalienceConfig()); err != nil {
		t.Fatalf("trusted touch searched memory node: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	raw, err := json.Marshal(workspaceMemorySearchParams{
		WorkspaceID: workspaceID,
		Query:       "duplicate wake",
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.workspaceMemorySearch(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemorySearch rpc error: %+v", rpcErr)
	}

	payload := result.(map[string]any)
	authority, ok := payload["time_authority"].(sqlite.WorkspaceTimeAuthority)
	if !ok || authority.WorkspaceID != workspaceID || authority.ReferenceAt == "" {
		t.Fatalf("expected workspace memory search time authority, got %+v", payload["time_authority"])
	}
	entries, ok := payload["entries"].([]workspaceMemoryView)
	if !ok {
		t.Fatalf("unexpected entries type %T", payload["entries"])
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Meta.SourceLabel != "Compaction" || !entries[0].Meta.Derived {
		t.Fatalf("unexpected search entry meta: %+v", entries[0].Meta)
	}
	if entries[0].Meta.CanonicalAuthority != "workspace_memory" ||
		entries[0].Meta.AnchorAuthority != "compatibility_only" ||
		entries[0].Meta.AnchorStatus != "DERIVED_READY" {
		t.Fatalf("expected search entry to expose mixed-state authority contract, got %+v", entries[0].Meta)
	}
	if entries[0].Meta.AnchorInvariantState != sqlite.WorkspaceMemoryProjectionInvariantCurrent {
		t.Fatalf("expected search entry invariant state to stay current, got %+v", entries[0].Meta)
	}
	if entries[0].Meta.AnchorSemanticLineageID != "workspace_memory:"+record.MemoryID {
		t.Fatalf("expected search entry anchor semantic lineage id, got %+v", entries[0].Meta)
	}
	if entries[0].Meta.AnchorRevision < 1 {
		t.Fatalf("expected search entry anchor revision, got %+v", entries[0].Meta)
	}
	if entries[0].Meta.AnchorProtect == nil || *entries[0].Meta.AnchorProtect {
		t.Fatalf("expected search entry to remain non-protected, got %+v", entries[0].Meta)
	}
	if entries[0].Meta.AnchorUnresolved == nil || *entries[0].Meta.AnchorUnresolved {
		t.Fatalf("expected search entry to remain resolved, got %+v", entries[0].Meta)
	}
	if entries[0].Meta.AnchorLastAnyAccess == nil || entries[0].Meta.AnchorLastTrustedAccess == nil {
		t.Fatalf("expected search entry trusted access markers, got %+v", entries[0].Meta)
	}
	if entries[0].Meta.AnchorTLife == nil || *entries[0].Meta.AnchorTLife <= 0 {
		t.Fatalf("expected search entry positive anchor t_life, got %+v", entries[0].Meta)
	}
	if entries[0].Meta.RetentionBand == "" || entries[0].Meta.RetentionHotUntil == nil || entries[0].Meta.RetentionWarmUntil == nil || entries[0].Meta.RetentionExpiresAt == nil {
		t.Fatalf("expected search entry additive retention state, got %+v", entries[0].Meta)
	}
	if entries[0].Meta.RetentionPrunable {
		t.Fatalf("expected ordinary search entry to stay non-prunable under future thresholds, got %+v", entries[0].Meta)
	}
	if entries[0].Meta.RetentionGuardReason != "" {
		t.Fatalf("did not expect guard reason on ordinary search entry, got %+v", entries[0].Meta)
	}
	if entries[0].Meta.SalienceA <= 0 || entries[0].Meta.SalienceTStar == "" || entries[0].Meta.SalienceH <= 0 {
		t.Fatalf("expected search entry legacy salience compatibility fields, got %+v", entries[0].Meta)
	}
}

func TestWorkspaceMemoryListMarksLaggingDerivedAnchorWithoutElevatingGraphAuthority(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-memory-authority-lag"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Authority Lag",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Canonical row survives projection lag",
		Body:        "Workspace memory should stay readable even when the compatibility anchor lags.",
		Summary:     "Canonical row survives projection lag.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	nodeID := "memnode:workspace_memory:" + record.MemoryID
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_nodes WHERE memory_id = ? AND workspace_id = ?`, nodeID, workspaceID); err != nil {
		t.Fatalf("delete compatibility anchor node: %v", err)
	}

	now := time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO memory_projection_outbox(
	    projection_id, workspace_id, projection_kind, origin_id, status, attempt_count, last_error,
	    available_at, enqueued_at, started_at, completed_at, updated_at
	  ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, NULL, ?)
	  ON CONFLICT(workspace_id, projection_kind, origin_id) DO UPDATE SET
	    projection_id = excluded.projection_id,
	    status = excluded.status,
	    attempt_count = excluded.attempt_count,
	    last_error = excluded.last_error,
	    available_at = excluded.available_at,
	    enqueued_at = excluded.enqueued_at,
	    started_at = excluded.started_at,
	    completed_at = NULL,
	    updated_at = excluded.updated_at`,
		"mproj-processing-"+record.MemoryID,
		workspaceID,
		"WORKSPACE_MEMORY",
		record.MemoryID,
		"PROCESSING",
		"",
		now,
		now,
		now,
		now,
	); err != nil {
		t.Fatalf("insert processing outbox row: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryListParams{WorkspaceID: workspaceID, Limit: 10})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryList(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryList rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	entries := payload["entries"].([]workspaceMemoryView)
	if len(entries) != 1 {
		t.Fatalf("expected one entry, got %+v", entries)
	}
	entry := entries[0]
	if entry.Record.MemoryID != record.MemoryID {
		t.Fatalf("expected canonical workspace memory row to stay readable, got %+v", entry)
	}
	if entry.Meta.CanonicalAuthority != "workspace_memory" || entry.Meta.AnchorAuthority != "compatibility_only" {
		t.Fatalf("expected mixed-state authority markers, got %+v", entry.Meta)
	}
	if entry.Meta.AnchorStatus != "DERIVED_STALE" {
		t.Fatalf("expected lagging compatibility anchor to be marked stale, got %+v", entry.Meta)
	}
	if entry.Meta.AnchorInvariantState != sqlite.WorkspaceMemoryProjectionInvariantLagging {
		t.Fatalf("expected lagging compatibility anchor invariant state, got %+v", entry.Meta)
	}
	if !containsInvariantCode(entry.Meta.AnchorInvariantIssueCodes, "MISSING_ANCHOR") {
		t.Fatalf("expected missing-anchor invariant code, got %+v", entry.Meta)
	}
	if entry.Meta.AnchorProjectionLagState != "degraded" {
		t.Fatalf("expected degraded projection lag state, got %+v", entry.Meta)
	}
	if entry.Meta.AnchorSemanticLineageID != "" || entry.Meta.AnchorRevision != 0 {
		t.Fatalf("expected stale missing anchor not to masquerade as canonical anchor data, got %+v", entry.Meta)
	}
	if !strings.Contains(strings.ToLower(entry.Meta.AnchorStatusReason), "canonical workspace_memory remains authoritative") {
		t.Fatalf("expected stale anchor reason to preserve canonical authority, got %+v", entry.Meta)
	}
}

func TestWorkspaceMemoryListKeepsLaggingPrunableAnchorInspectableOnly(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-memory-authority-lag-prunable"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Authority Lag Prunable",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Lagging anchor must not expose prunable authority",
		Body:        "Canonical workspace memory should not look prunable while the derived anchor is lagging.",
		Summary:     "Lagging anchor must stay inspectable only.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	nodeID := "memnode:workspace_memory:" + record.MemoryID
	past := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano)
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
	`, nodeID, workspaceID, 0.45, past, past, 1, 0.0, 7200.0, past, past, past, past); err != nil {
		t.Fatalf("seed stale salience row: %v", err)
	}
	seedMemoryProjectionProcessingRow(t, ctx, store, workspaceID, record.MemoryID)

	result, rpcErr := h.workspaceMemoryList(ctx, mustJSONRaw(workspaceMemoryListParams{
		WorkspaceID: workspaceID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryList rpc error: %+v", rpcErr)
	}
	entries := result.(map[string]any)["entries"].([]workspaceMemoryView)
	if len(entries) != 1 {
		t.Fatalf("expected one entry, got %+v", entries)
	}
	entry := entries[0]
	if entry.Meta.AnchorStatus != "DERIVED_STALE" || entry.Meta.AnchorSignalState != "INSPECTABLE_ONLY" {
		t.Fatalf("expected lagging anchor to remain inspectable-only, got %+v", entry.Meta)
	}
	if entry.Meta.RetentionBand != "PRUNABLE" {
		t.Fatalf("expected retention band to remain inspectable, got %+v", entry.Meta)
	}
	if entry.Meta.RetentionPrunable {
		t.Fatalf("did not expect lagging anchor to expose prunable authority, got %+v", entry.Meta)
	}
	if entry.Meta.RetentionGuardReason != "PROJECTION_NOT_SETTLED" {
		t.Fatalf("expected lagging prunable anchor to be guarded as projection-not-settled, got %+v", entry.Meta)
	}
}

func TestWorkspaceMemoryListSuppressesLaggingRecoveryCandidateUntilProjectionSettles(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const (
		workspaceID = "ws-memory-authority-lag-recovery"
		triggerDoc  = "doc-memory-authority-lag-recovery"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Authority Lag Recovery",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      triggerDoc,
		Title:       "Rollback Runbook",
		Content:     "# Rollback\nInitial guidance.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert workspace doc: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Lagging recovery must not look settled",
		Body:        "An expired archived rollback trace should not surface as a settled recovery candidate while projection is lagging.",
		Summary:     "Lagging recovery must stay inspectable only.",
		SourceKind:  "workspace_doc",
		SourceID:    triggerDoc,
		Importance:  0.7,
		Confidence:  0.8,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)
	if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		ArchivedBy:  "rmp_pruner",
		Reason:      "rmp_gc_expired",
	}); err != nil {
		t.Fatalf("archive workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)
	if err := store.UpsertWorkspaceDoc(ctx, sqlite.WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      triggerDoc,
		Title:       "Rollback Runbook",
		Content:     "# Rollback\nUpdated guidance should make the archive recoverable once projection settles.",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("update workspace doc: %v", err)
	}
	seedMemoryProjectionProcessingRow(t, ctx, store, workspaceID, record.MemoryID)

	result, rpcErr := h.workspaceMemoryList(ctx, mustJSONRaw(workspaceMemoryListParams{
		WorkspaceID:     workspaceID,
		IncludeArchived: true,
		Limit:           10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryList rpc error: %+v", rpcErr)
	}
	entries := result.(map[string]any)["entries"].([]workspaceMemoryView)
	if len(entries) != 1 {
		t.Fatalf("expected one entry, got %+v", entries)
	}
	entry := entries[0]
	if entry.Meta.AnchorStatus != "DERIVED_STALE" || entry.Meta.AnchorSignalState != "INSPECTABLE_ONLY" {
		t.Fatalf("expected lagging archived anchor to remain inspectable-only, got %+v", entry.Meta)
	}
	if entry.Meta.RecoveryCandidate {
		t.Fatalf("did not expect lagging anchor to expose a settled recovery candidate, got %+v", entry.Meta)
	}
	if entry.Meta.RecoveryGuardReason != "PROJECTION_NOT_SETTLED" {
		t.Fatalf("expected lagging recovery candidate to be guarded as projection-not-settled, got %+v", entry.Meta)
	}
	if entry.Meta.RecoveryTriggerCount != 0 || len(entry.Meta.RecoveryTriggerKinds) != 0 {
		t.Fatalf("did not expect lagging anchor to expose settled recovery triggers, got %+v", entry.Meta)
	}
}

func TestWorkspaceMemoryListSurfacesSettledDerivedDriftWithoutPromotingGraphAuthority(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-memory-authority-drift"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Authority Drift",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Canonical row survives settled drift",
		Body:        "Workspace memory should stay readable when the compatibility anchor goes missing after reconcile settled.",
		Summary:     "Canonical row survives settled drift.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	nodeID := "memnode:workspace_memory:" + record.MemoryID
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_nodes WHERE memory_id = ? AND workspace_id = ?`, nodeID, workspaceID); err != nil {
		t.Fatalf("delete compatibility anchor node: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryListParams{WorkspaceID: workspaceID, Limit: 10})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryList(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryList rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	entries := payload["entries"].([]workspaceMemoryView)
	if len(entries) != 1 {
		t.Fatalf("expected one entry, got %+v", entries)
	}
	entry := entries[0]
	if entry.Record.MemoryID != record.MemoryID {
		t.Fatalf("expected canonical workspace memory row to stay readable, got %+v", entry)
	}
	if entry.Meta.CanonicalAuthority != "workspace_memory" || entry.Meta.AnchorAuthority != "compatibility_only" {
		t.Fatalf("expected mixed-state authority markers, got %+v", entry.Meta)
	}
	if entry.Meta.AnchorStatus != "DERIVED_MISSING" {
		t.Fatalf("expected settled missing anchor to stay missing, got %+v", entry.Meta)
	}
	if entry.Meta.AnchorInvariantState != sqlite.WorkspaceMemoryProjectionInvariantDrift {
		t.Fatalf("expected settled missing anchor to surface invariant drift, got %+v", entry.Meta)
	}
	if !containsInvariantCode(entry.Meta.AnchorInvariantIssueCodes, "MISSING_ANCHOR") {
		t.Fatalf("expected missing-anchor invariant code, got %+v", entry.Meta)
	}
	if entry.Meta.AnchorSemanticLineageID != "" || entry.Meta.AnchorRevision != 0 {
		t.Fatalf("expected settled drift not to promote graph anchor fields, got %+v", entry.Meta)
	}
}

func TestWorkspaceMemoryListTransitionsFromDriftToCurrentAfterExplicitRepair(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-memory-authority-repair-cycle"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Authority Repair Cycle",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Explicit repair restores derived compatibility",
		Body:        "Mixed-state migration should move from drift back to current only through explicit repair.",
		Summary:     "Explicit repair restores derived compatibility.",
		SourceKind:  "manual",
		SourceID:    "developer",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	mustReconcileMemoryProjectionWorkspace(t, ctx, store, workspaceID)

	nodeID := "memnode:workspace_memory:" + record.MemoryID
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM memory_nodes WHERE workspace_id = ? AND memory_id = ?`, workspaceID, nodeID); err != nil {
		t.Fatalf("delete compatibility anchor node: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryListParams{WorkspaceID: workspaceID, Limit: 10})
	if err != nil {
		t.Fatalf("marshal drift list params: %v", err)
	}
	beforeRaw, rpcErr := h.workspaceMemoryList(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryList before repair rpc error: %+v", rpcErr)
	}
	beforeEntries := beforeRaw.(map[string]any)["entries"].([]workspaceMemoryView)
	if len(beforeEntries) != 1 {
		t.Fatalf("expected one entry before repair, got %+v", beforeEntries)
	}
	if beforeEntries[0].Meta.AnchorStatus != "DERIVED_MISSING" || beforeEntries[0].Meta.AnchorInvariantState != sqlite.WorkspaceMemoryProjectionInvariantDrift {
		t.Fatalf("expected settled drift before repair, got %+v", beforeEntries[0].Meta)
	}

	repairRaw, err := json.Marshal(workspaceMemoryGraphRepairParams{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
	})
	if err != nil {
		t.Fatalf("marshal repair params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryGraphRepair(ctx, repairRaw); rpcErr != nil {
		t.Fatalf("workspaceMemoryGraphRepair rpc error: %+v", rpcErr)
	}

	afterRaw, rpcErr := h.workspaceMemoryList(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryList after repair rpc error: %+v", rpcErr)
	}
	afterEntries := afterRaw.(map[string]any)["entries"].([]workspaceMemoryView)
	if len(afterEntries) != 1 {
		t.Fatalf("expected one entry after repair, got %+v", afterEntries)
	}
	if afterEntries[0].Meta.AnchorStatus != "DERIVED_READY" || afterEntries[0].Meta.AnchorInvariantState != sqlite.WorkspaceMemoryProjectionInvariantCurrent {
		t.Fatalf("expected current derived anchor after repair, got %+v", afterEntries[0].Meta)
	}
	if afterEntries[0].Meta.AnchorSemanticLineageID != "workspace_memory:"+record.MemoryID {
		t.Fatalf("expected repaired semantic lineage id, got %+v", afterEntries[0].Meta)
	}
}

func TestWorkspaceMemoryReadSurfacesRetentionBandAndExpiryAlongsideLegacySalienceWhenAvailable(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-memory-retention-read"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Retention Read Memory",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "procedure",
		Title:       "Retention read surface",
		Body:        "Retention band and expiry should surface alongside legacy salience.",
		Summary:     "Retention read surface.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Importance:  0.8,
		Confidence:  0.9,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	nodeID := "memnode:workspace_memory:" + record.MemoryID
	now := time.Now().UTC()
	hotAt := now.Add(25 * time.Minute).Format(time.RFC3339Nano)
	warmAt := now.Add(95 * time.Minute).Format(time.RFC3339Nano)
	gcAt := now.Add(5 * time.Hour).Format(time.RFC3339Nano)
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
	`, nodeID, workspaceID, 0.88, now.Add(-80*time.Minute).Format(time.RFC3339Nano), now.Add(-15*time.Minute).Format(time.RFC3339Nano), 4, 0.2, 14400.0, hotAt, warmAt, gcAt, now.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("seed salience row: %v", err)
	}

	listResult, rpcErr := h.workspaceMemoryList(ctx, mustJSONRaw(workspaceMemoryListParams{
		WorkspaceID: workspaceID,
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryList rpc error: %+v", rpcErr)
	}
	listPayload := listResult.(map[string]any)
	if authority, ok := listPayload["time_authority"].(sqlite.WorkspaceTimeAuthority); !ok || authority.WorkspaceID != workspaceID || authority.ReferenceAt == "" {
		t.Fatalf("expected list time authority, got %+v", listPayload["time_authority"])
	}
	listEntries := listPayload["entries"].([]workspaceMemoryView)
	if len(listEntries) != 1 {
		t.Fatalf("expected one list entry, got %d", len(listEntries))
	}
	if listEntries[0].Meta.SalienceA <= 0 || listEntries[0].Meta.SalienceTStar == "" || listEntries[0].Meta.SalienceH <= 0 {
		t.Fatalf("expected list entry legacy salience compatibility fields, got %+v", listEntries[0].Meta)
	}

	searchResult, rpcErr := h.workspaceMemorySearch(ctx, mustJSONRaw(workspaceMemorySearchParams{
		WorkspaceID: workspaceID,
		Query:       "Retention read surface",
		Limit:       10,
	}))
	if rpcErr != nil {
		t.Fatalf("workspaceMemorySearch rpc error: %+v", rpcErr)
	}
	searchPayload := searchResult.(map[string]any)
	if authority, ok := searchPayload["time_authority"].(sqlite.WorkspaceTimeAuthority); !ok || authority.WorkspaceID != workspaceID || authority.ReferenceAt == "" {
		t.Fatalf("expected search time authority, got %+v", searchPayload["time_authority"])
	}
	searchEntries := searchPayload["entries"].([]workspaceMemoryView)
	if len(searchEntries) != 1 {
		t.Fatalf("expected one search entry, got %d", len(searchEntries))
	}
	if searchEntries[0].Meta.SalienceA <= 0 || searchEntries[0].Meta.SalienceTStar == "" || searchEntries[0].Meta.SalienceH <= 0 {
		t.Fatalf("expected search entry legacy salience compatibility fields, got %+v", searchEntries[0].Meta)
	}

	metaJSON, err := json.Marshal(listEntries[0].Meta)
	if err != nil {
		t.Fatalf("marshal list entry meta: %v", err)
	}
	var listMeta map[string]any
	if err := json.Unmarshal(metaJSON, &listMeta); err != nil {
		t.Fatalf("decode list entry meta: %v", err)
	}
	if listMeta["retention_hot_until"] != hotAt || listMeta["retention_warm_until"] != warmAt || listMeta["retention_expires_at"] != gcAt {
		t.Fatalf("expected list entry retention expiry surfaces, got %+v", listMeta)
	}
	if got, ok := listMeta["retention_band"].(string); !ok || strings.TrimSpace(got) == "" {
		t.Fatalf("expected non-empty list retention_band, got %+v", listMeta)
	}
	if got, ok := listMeta["retention_prunable"].(bool); !ok || got {
		t.Fatalf("expected protected list entry to stay non-prunable, got %+v", listMeta)
	}
	if listMeta["retention_guard_reason"] != "PROTECT" {
		t.Fatalf("expected protected list entry guard reason, got %+v", listMeta)
	}

	searchMetaJSON, err := json.Marshal(searchEntries[0].Meta)
	if err != nil {
		t.Fatalf("marshal search entry meta: %v", err)
	}
	var searchMeta map[string]any
	if err := json.Unmarshal(searchMetaJSON, &searchMeta); err != nil {
		t.Fatalf("decode search entry meta: %v", err)
	}
	if searchMeta["retention_hot_until"] != hotAt || searchMeta["retention_warm_until"] != warmAt || searchMeta["retention_expires_at"] != gcAt {
		t.Fatalf("expected search entry retention expiry surfaces, got %+v", searchMeta)
	}
	if got, ok := searchMeta["retention_prunable"].(bool); !ok || got {
		t.Fatalf("expected protected search entry to stay non-prunable, got %+v", searchMeta)
	}
	if searchMeta["retention_guard_reason"] != "PROTECT" {
		t.Fatalf("expected protected search entry guard reason, got %+v", searchMeta)
	}
}

func TestWorkspaceMemoryRestoreReturnsActiveEntry(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-memory-restore"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Restore Memory",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "note",
		Title:       "Deploy note",
		Body:        "Keep the live gate enabled.",
		SourceKind:  "manual",
		SourceID:    "dashboard",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	if _, err := store.ArchiveWorkspaceMemory(ctx, sqlite.WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		ArchivedBy:  "developer",
		Reason:      "stale",
	}); err != nil {
		t.Fatalf("archive workspace memory: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryRestoreParams{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		RestoredBy:  "dashboard",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}

	result, rpcErr := h.workspaceMemoryRestore(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryRestore rpc error: %+v", rpcErr)
	}

	payload := result.(map[string]any)
	if payload["status"] != "RESTORED" {
		t.Fatalf("expected RESTORED status, got %+v", payload)
	}
	entry, ok := payload["entry"].(workspaceMemoryView)
	if !ok {
		t.Fatalf("unexpected entry type %T", payload["entry"])
	}
	if entry.Meta.State != "ACTIVE" || entry.Record.ArchivedAt != nil {
		t.Fatalf("expected restored active entry, got %+v", entry)
	}
}

func TestWorkspaceMemoryRestoreReactivatesAutoPrunedMemoryInReadSurface(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-memory-restore-auto-pruned"
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Restore Auto-Pruned Memory",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Recoverable auto-pruned lesson",
		Body:        "Auto-pruned workspace memory should stay recoverable through the public read surface.",
		Summary:     "Recoverable auto-pruned lesson.",
		SourceKind:  "manual",
		SourceID:    "dashboard",
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	now := time.Now().UTC()
	nodeID := "memnode:workspace_memory:" + record.MemoryID
	pastStar := now.Add(-5 * time.Hour).Format(time.RFC3339Nano)
	pastAcc := now.Add(-4 * time.Hour).Format(time.RFC3339Nano)
	pastHot := now.Add(-3 * time.Hour).Format(time.RFC3339Nano)
	pastWarm := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	pastGc := now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO memory_node_salience (
			memory_id, workspace_id, a_i, t_i_star, t_i_acc, n_i, q_i, h_i, t_hot, t_warm, t_gc, updated_at
		) VALUES (?, ?, 0.4, ?, ?, 1, 0.1, 1800, ?, ?, ?, ?)
	`, nodeID, workspaceID, pastStar, pastAcc, pastHot, pastWarm, pastGc, pastAcc); err != nil {
		t.Fatalf("seed salience row for auto-pruned restore surface: %v", err)
	}

	pruned, err := store.RMPRunBatchedPruning(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("run batched pruning: %v", err)
	}
	if len(pruned) != 1 || pruned[0] != nodeID {
		t.Fatalf("expected auto-pruned node set, got %+v", pruned)
	}

	activeListRaw, err := json.Marshal(workspaceMemoryListParams{
		WorkspaceID: workspaceID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal active list params: %v", err)
	}
	activeListResult, rpcErr := h.workspaceMemoryList(ctx, activeListRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryList active rpc error: %+v", rpcErr)
	}
	activeListPayload := activeListResult.(map[string]any)
	activeItems := activeListPayload["items"].([]sqlite.WorkspaceMemoryRecord)
	for _, item := range activeItems {
		if item.MemoryID == record.MemoryID {
			t.Fatalf("did not expect auto-pruned memory in active list, got %+v", activeItems)
		}
	}

	archivedListRaw, err := json.Marshal(workspaceMemoryListParams{
		WorkspaceID:     workspaceID,
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("marshal archived list params: %v", err)
	}
	archivedListResult, rpcErr := h.workspaceMemoryList(ctx, archivedListRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryList archived rpc error: %+v", rpcErr)
	}
	archivedListPayload := archivedListResult.(map[string]any)
	archivedItems := archivedListPayload["items"].([]sqlite.WorkspaceMemoryRecord)
	archivedEntries := archivedListPayload["entries"].([]workspaceMemoryView)
	foundArchived := false
	foundArchivedEntry := false
	for _, item := range archivedItems {
		if item.MemoryID != record.MemoryID {
			continue
		}
		foundArchived = true
		if item.ArchivedAt == nil || item.ArchivedReason != "rmp_gc_expired" {
			t.Fatalf("expected auto-pruned item to surface archived trace, got %+v", item)
		}
	}
	if !foundArchived {
		t.Fatalf("expected auto-pruned memory in include_archived list, got %+v", archivedItems)
	}
	for _, entry := range archivedEntries {
		if entry.Record.MemoryID != record.MemoryID {
			continue
		}
		foundArchivedEntry = true
		if entry.Meta.State != "ARCHIVED" || entry.Meta.RecoveryCandidate || entry.Meta.RecoveryGuardReason != "NO_TRIGGERED_LINKAGE" {
			t.Fatalf("expected archived entry to surface bounded recovery hook semantics, got %+v", entry)
		}
	}
	if !foundArchivedEntry {
		t.Fatalf("expected archived auto-pruned entry in include_archived list, got %+v", archivedEntries)
	}

	restoreRaw, err := json.Marshal(workspaceMemoryRestoreParams{
		WorkspaceID:    workspaceID,
		MemoryID:       record.MemoryID,
		RestoredBy:     "dashboard",
		RecoveryReason: "rmp_gc_reactivated",
	})
	if err != nil {
		t.Fatalf("marshal restore params: %v", err)
	}
	restoreResult, rpcErr := h.workspaceMemoryRestore(ctx, restoreRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryRestore rpc error: %+v", rpcErr)
	}
	restorePayload := restoreResult.(map[string]any)
	if restorePayload["status"] != "RESTORED" {
		t.Fatalf("expected RESTORED status after auto-pruned restore, got %+v", restorePayload)
	}
	entry := restorePayload["entry"].(workspaceMemoryView)
	if entry.Record.MemoryID != record.MemoryID || entry.Record.ArchivedAt != nil || entry.Record.RecoveryReason != "rmp_gc_reactivated" {
		t.Fatalf("expected restored entry to preserve identity and recovery trace, got %+v", entry)
	}
	if entry.Meta.State != "ACTIVE" || entry.Meta.RecoveryCandidate || entry.Meta.RecoveryGuardReason != "" {
		t.Fatalf("expected restored entry to surface ACTIVE state, got %+v", entry)
	}

	activeListResult, rpcErr = h.workspaceMemoryList(ctx, activeListRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryList after restore rpc error: %+v", rpcErr)
	}
	activeListPayload = activeListResult.(map[string]any)
	activeItems = activeListPayload["items"].([]sqlite.WorkspaceMemoryRecord)
	foundRestored := false
	for _, item := range activeItems {
		if item.MemoryID != record.MemoryID {
			continue
		}
		foundRestored = true
		if item.ArchivedAt != nil || item.RecoveryReason != "rmp_gc_reactivated" {
			t.Fatalf("expected restored memory to return to active list with recovery trace, got %+v", item)
		}
	}
	if !foundRestored {
		t.Fatalf("expected restored memory to reappear in active list, got %+v", activeItems)
	}
}

func TestWorkspaceMemoryLifecyclePublishesJournalBackedAliasMirrors(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-memory-live-mirrors"
	ctx := testAuthContext(workspaceID, "human", "developer")
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Live Mirrors",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	writeRaw, err := json.Marshal(workspaceMemoryWriteParams{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Use journal-backed memory mirrors",
		Body:        "Workspace memory live events should mirror persisted runtime rows.",
		Summary:     "Mirror workspace memory live events from the journal.",
		SourceKind:  "manual",
		SourceID:    "developer",
		Tags:        []string{"memory", "mirror"},
	})
	if err != nil {
		t.Fatalf("marshal write params: %v", err)
	}
	writeResult, rpcErr := h.workspaceMemoryWrite(ctx, writeRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryWrite rpc error: %+v", rpcErr)
	}
	writePayload := writeResult.(map[string]any)
	recordedMemory, ok := writePayload["memory"].(sqlite.WorkspaceMemoryRecord)
	if !ok {
		t.Fatalf("unexpected memory payload type %T", writePayload["memory"])
	}

	recordedLive := nextEventOfType(t, ch, "workspace.memory.recorded")
	recordedPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    recordedMemory.MemoryID,
		Limit:       10,
	})
	assertLiveEventMirrorsRuntimeEvent(t, recordedLive, recordedPersisted, "workspace.memory.recorded")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, recordedLive.PayloadJSON), recordedPersisted.PayloadJSON)
	assertSSEAliasTypeAndOptionalCanonicalEventType(t, recordedLive, "workspace_memory.recorded")
	assertServerWorkspaceMemoryRuntimePromptContext(t, recordedPersisted, "workspace.memory.write", workspaceID, recordedMemory.MemoryID, "human", "developer", map[string]string{
		"memory_type": "DECISION",
		"source_kind": "manual",
		"source_id":   "developer",
		"actor_type":  "system",
		"actor_id":    "developer",
	})

	recordedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    recordedMemory.MemoryID,
		Limit:       10,
	}
	seenRecorded := snapshotRuntimeEventIDs(t, ctx, store, recordedFilter)
	secondWriteRaw, err := json.Marshal(workspaceMemoryWriteParams{
		WorkspaceID: workspaceID,
		MemoryID:    recordedMemory.MemoryID,
		MemoryType:  "decision",
		Title:       "Use journal-backed memory mirrors",
		Body:        "Workspace memory live events should mirror persisted runtime rows.",
		Summary:     "Mirror workspace memory live events from the exact returned row.",
		SourceKind:  "manual",
		SourceID:    "developer-second-write",
		Tags:        []string{"memory", "mirror", "exact-row"},
	})
	if err != nil {
		t.Fatalf("marshal second write params: %v", err)
	}
	secondWriteResult, rpcErr := h.workspaceMemoryWrite(ctx, secondWriteRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryWrite second rpc error: %+v", rpcErr)
	}
	secondWritePayload := secondWriteResult.(map[string]any)
	secondRecordedMemory, ok := secondWritePayload["memory"].(sqlite.WorkspaceMemoryRecord)
	if !ok {
		t.Fatalf("unexpected second memory payload type %T", secondWritePayload["memory"])
	}
	if secondRecordedMemory.MemoryID != recordedMemory.MemoryID {
		t.Fatalf("expected repeated write to preserve memory_id %q, got %+v", recordedMemory.MemoryID, secondRecordedMemory)
	}
	secondRecordedLive := nextEventOfType(t, ch, "workspace.memory.recorded")
	secondRecordedPersisted := mustNewRuntimeEvent(t, ctx, store, recordedFilter, seenRecorded)
	assertLiveEventMirrorsRuntimeEvent(t, secondRecordedLive, secondRecordedPersisted, "workspace.memory.recorded")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, secondRecordedLive.PayloadJSON), secondRecordedPersisted.PayloadJSON)
	assertSSEAliasTypeAndOptionalCanonicalEventType(t, secondRecordedLive, "workspace_memory.recorded")
	assertServerWorkspaceMemoryRuntimePromptContext(t, secondRecordedPersisted, "workspace.memory.write", workspaceID, recordedMemory.MemoryID, "human", "developer", map[string]string{
		"memory_type": "DECISION",
		"source_kind": "manual",
		"source_id":   "developer-second-write",
		"actor_type":  "system",
		"actor_id":    "developer-second-write",
	})
	if secondRecordedPersisted.EventID == recordedPersisted.EventID || secondRecordedPersisted.IngestSeq <= recordedPersisted.IngestSeq {
		t.Fatalf("expected repeated write to append a new recorded runtime row, got first=%+v second=%+v", recordedPersisted, secondRecordedPersisted)
	}
	if secondRecordedMemory.SourceID != "developer-second-write" || secondRecordedMemory.Summary != "Mirror workspace memory live events from the exact returned row." {
		t.Fatalf("expected repeated write result to expose updated memory fields, got %+v", secondRecordedMemory)
	}

	removeRaw, err := json.Marshal(workspaceMemoryRemoveParams{
		WorkspaceID: workspaceID,
		MemoryID:    recordedMemory.MemoryID,
		RemovedBy:   "developer",
		Reason:      "stale",
	})
	if err != nil {
		t.Fatalf("marshal remove params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryRemove(ctx, removeRaw); rpcErr != nil {
		t.Fatalf("workspaceMemoryRemove rpc error: %+v", rpcErr)
	}
	removedLive := nextEventOfType(t, ch, "workspace.memory.removed")
	removedPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.archived",
		EntityType:  "workspace_memory",
		EntityID:    recordedMemory.MemoryID,
		Limit:       10,
	})
	assertLiveEventMirrorsRuntimeEvent(t, removedLive, removedPersisted, "workspace.memory.removed")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, removedLive.PayloadJSON), removedPersisted.PayloadJSON)
	assertSSEAliasTypeAndOptionalCanonicalEventType(t, removedLive, "workspace_memory.archived")
	assertServerWorkspaceMemoryRuntimePromptContext(t, removedPersisted, "workspace.memory.remove", workspaceID, recordedMemory.MemoryID, "human", "developer", map[string]string{
		"memory_type": "DECISION",
		"source_kind": "manual",
		"source_id":   "developer-second-write",
		"actor_type":  "operator",
		"actor_id":    "developer",
		"archived_by": "developer",
	})

	restoreRaw, err := json.Marshal(workspaceMemoryRestoreParams{
		WorkspaceID: workspaceID,
		MemoryID:    recordedMemory.MemoryID,
		RestoredBy:  "developer",
	})
	if err != nil {
		t.Fatalf("marshal restore params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryRestore(ctx, restoreRaw); rpcErr != nil {
		t.Fatalf("workspaceMemoryRestore rpc error: %+v", rpcErr)
	}
	restoredLive := nextEventOfType(t, ch, "workspace.memory.restored")
	restoredPersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.restored",
		EntityType:  "workspace_memory",
		EntityID:    recordedMemory.MemoryID,
		Limit:       10,
	})
	assertLiveEventMirrorsRuntimeEvent(t, restoredLive, restoredPersisted, "workspace.memory.restored")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, restoredLive.PayloadJSON), restoredPersisted.PayloadJSON)
	assertSSEAliasTypeAndOptionalCanonicalEventType(t, restoredLive, "workspace_memory.restored")
	assertServerWorkspaceMemoryRuntimePromptContext(t, restoredPersisted, "workspace.memory.restore", workspaceID, recordedMemory.MemoryID, "human", "developer", map[string]string{
		"memory_type": "DECISION",
		"source_kind": "manual",
		"source_id":   "developer-second-write",
		"actor_type":  "operator",
		"actor_id":    "developer",
		"restored_by": "developer",
	})

	archivedFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.archived",
		EntityType:  "workspace_memory",
		EntityID:    recordedMemory.MemoryID,
		Limit:       10,
	}
	restoredFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.restored",
		EntityType:  "workspace_memory",
		EntityID:    recordedMemory.MemoryID,
		Limit:       10,
	}
	seenArchived := snapshotRuntimeEventIDs(t, ctx, store, archivedFilter)
	secondRemoveRaw, err := json.Marshal(workspaceMemoryRemoveParams{
		WorkspaceID: workspaceID,
		MemoryID:    recordedMemory.MemoryID,
		RemovedBy:   "developer",
		Reason:      "superseded",
	})
	if err != nil {
		t.Fatalf("marshal second remove params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryRemove(ctx, secondRemoveRaw); rpcErr != nil {
		t.Fatalf("workspaceMemoryRemove second rpc error: %+v", rpcErr)
	}
	secondRemovedLive := nextEventOfType(t, ch, "workspace.memory.removed")
	secondRemovedPersisted := mustNewRuntimeEvent(t, ctx, store, archivedFilter, seenArchived)
	assertLiveEventMirrorsRuntimeEvent(t, secondRemovedLive, secondRemovedPersisted, "workspace.memory.removed")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, secondRemovedLive.PayloadJSON), secondRemovedPersisted.PayloadJSON)
	assertSSEAliasTypeAndOptionalCanonicalEventType(t, secondRemovedLive, "workspace_memory.archived")
	assertServerWorkspaceMemoryRuntimePromptContext(t, secondRemovedPersisted, "workspace.memory.remove", workspaceID, recordedMemory.MemoryID, "human", "developer", map[string]string{
		"memory_type": "DECISION",
		"source_kind": "manual",
		"source_id":   "developer-second-write",
		"actor_type":  "operator",
		"actor_id":    "developer",
		"archived_by": "developer",
	})
	if secondRemovedPersisted.EventID == removedPersisted.EventID || secondRemovedPersisted.IngestSeq <= removedPersisted.IngestSeq {
		t.Fatalf("expected second archive to append a new archived runtime row, got first=%+v second=%+v", removedPersisted, secondRemovedPersisted)
	}

	seenRestored := snapshotRuntimeEventIDs(t, ctx, store, restoredFilter)
	secondRestoreRaw, err := json.Marshal(workspaceMemoryRestoreParams{
		WorkspaceID:    workspaceID,
		MemoryID:       recordedMemory.MemoryID,
		RestoredBy:     "developer",
		RecoveryReason: "reopened",
	})
	if err != nil {
		t.Fatalf("marshal second restore params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryRestore(ctx, secondRestoreRaw); rpcErr != nil {
		t.Fatalf("workspaceMemoryRestore second rpc error: %+v", rpcErr)
	}
	secondRestoredLive := nextEventOfType(t, ch, "workspace.memory.restored")
	secondRestoredPersisted := mustNewRuntimeEvent(t, ctx, store, restoredFilter, seenRestored)
	assertLiveEventMirrorsRuntimeEvent(t, secondRestoredLive, secondRestoredPersisted, "workspace.memory.restored")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, secondRestoredLive.PayloadJSON), secondRestoredPersisted.PayloadJSON)
	assertSSEAliasTypeAndOptionalCanonicalEventType(t, secondRestoredLive, "workspace_memory.restored")
	assertServerWorkspaceMemoryRuntimePromptContext(t, secondRestoredPersisted, "workspace.memory.restore", workspaceID, recordedMemory.MemoryID, "human", "developer", map[string]string{
		"memory_type": "DECISION",
		"source_kind": "manual",
		"source_id":   "developer-second-write",
		"actor_type":  "operator",
		"actor_id":    "developer",
		"restored_by": "developer",
	})
	if secondRestoredPersisted.EventID == restoredPersisted.EventID || secondRestoredPersisted.IngestSeq <= restoredPersisted.IngestSeq {
		t.Fatalf("expected second restore to append a new restored runtime row, got first=%+v second=%+v", restoredPersisted, secondRestoredPersisted)
	}
}

func assertServerWorkspaceMemoryRuntimePromptContext(t *testing.T, event sqlite.RuntimeEventRecord, wantSurface, wantWorkspaceID, wantMemoryID, wantPrincipalType, wantPrincipalID string, extra map[string]string) {
	t.Helper()
	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected workspace memory prompt_context_envelope in runtime event payload, got %+v", payload)
	}
	expected := map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_workspace_memory_write",
		"surface":                            wantSurface,
		"origin":                             "server_rpc",
		"workspace_id":                       wantWorkspaceID,
		"memory_id":                          wantMemoryID,
		"principal_type":                     wantPrincipalType,
		"principal_id":                       wantPrincipalID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	}
	for key, value := range extra {
		expected[key] = value
	}
	for key, want := range expected {
		got, ok := envelope[key].(string)
		if !ok {
			t.Fatalf("workspace memory prompt_context_envelope[%s] has type %T, want string in %+v", key, envelope[key], envelope)
		}
		if got != want {
			t.Fatalf("workspace memory prompt_context_envelope[%s] = %q, want %q in %+v", key, got, want, envelope)
		}
	}
}

func assertRuntimeEventPayloadHasNoPromptContextEnvelope(t *testing.T, event sqlite.RuntimeEventRecord) {
	t.Helper()
	payload := decodeEventPayloadMap(t, event.PayloadJSON)
	if _, ok := payload["prompt_context_envelope"]; ok {
		t.Fatalf("did not expect prompt_context_envelope in runtime event payload %+v", payload)
	}
}

func TestWorkspaceMemoryWritePublishesPromotedClaimMirrorInChronologicalOrder(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-memory-promoted-claim-write-live"
		agentID     = "agent-memory-promoted-claim-write"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Promoted Claim Write Live",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	seenRecordedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		Limit:       10,
	})
	seenClaimEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		Limit:       10,
	})

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(workspaceMemoryWriteParams{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Promoted claim write",
		Body:        "Direct workspace memory writes should surface promoted-claim runtime mirrors.",
		Summary:     "Promoted claim write parity.",
		AgentID:     agentID,
		SourceKind:  "manual",
		SourceID:    "developer",
		Tags:        []string{"memory", "promotion", "claim"},
	})
	if err != nil {
		t.Fatalf("marshal workspace memory write params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryWrite(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryWrite rpc error: %+v", rpcErr)
	}
	resp, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspace memory write response type %T", result)
	}
	recordedMemory, ok := resp["memory"].(sqlite.WorkspaceMemoryRecord)
	if !ok || recordedMemory.MemoryID == "" {
		t.Fatalf("unexpected workspace memory write response %+v", resp)
	}
	claimID := "claim:memory:" + recordedMemory.MemoryID

	recordedPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    recordedMemory.MemoryID,
		Limit:       10,
	}, seenRecordedEvents)
	claimPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	}, seenClaimEvents)
	assertServerWorkspaceMemoryRuntimePromptContext(t, recordedPersisted, "workspace.memory.write", workspaceID, recordedMemory.MemoryID, "human", "developer", map[string]string{
		"memory_type": "DECISION",
		"source_kind": "manual",
		"source_id":   "developer",
		"agent_id":    agentID,
		"actor_type":  "agent",
		"actor_id":    agentID,
	})
	assertRuntimeEventPayloadHasNoPromptContextEnvelope(t, claimPersisted)

	ordered, _ := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: recordedPersisted, Type: "workspace.memory.recorded"},
		runtimeEventExpectation{Event: claimPersisted, Type: "workspace.claim.written"},
	)
	if len(ordered) != 2 ||
		ordered[0].Type != "workspace.memory.recorded" ||
		ordered[1].Type != "workspace.claim.written" ||
		!runtimeEventChronologicalLess(ordered[0].Event, ordered[1].Event) {
		t.Fatalf("expected promoted-memory write live mirrors to follow persisted chronology, got %+v", ordered)
	}
}

func TestWorkspaceMemoryWriteSupportsAntiProcedurePromotedClaimEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-memory-anti-procedure-write"
		agentID     = "agent-memory-anti-procedure-write"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Anti Procedure Write",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Anti Procedure Memory Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryWriteParams{
		WorkspaceID: workspaceID,
		MemoryType:  "anti_procedure",
		Title:       "Rollback bypass stays forbidden",
		Body:        "Direct workspace memory writes should preserve anti-procedure type and promoted-claim effects.",
		Summary:     "Anti-procedure promoted claim parity.",
		AgentID:     agentID,
		SourceKind:  "manual",
		SourceID:    "developer",
		Tags:        []string{"memory", "anti_procedure", "claim"},
	})
	if err != nil {
		t.Fatalf("marshal anti procedure workspace memory write params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryWrite(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryWrite anti procedure rpc error: %+v", rpcErr)
	}
	resp, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected anti procedure workspace memory write response type %T", result)
	}
	recordedMemory, ok := resp["memory"].(sqlite.WorkspaceMemoryRecord)
	if !ok || recordedMemory.MemoryID == "" || recordedMemory.MemoryType != "ANTI_PROCEDURE" {
		t.Fatalf("unexpected anti procedure workspace memory write response %+v", resp)
	}
	effects, ok := resp["promoted_claim_effects"].(*sqlite.PromotedKnowledgeClaimSyncEffects)
	if !ok || effects == nil {
		t.Fatalf("expected promoted_claim_effects in anti procedure response, got %+v", resp)
	}
	if effects.Claim == nil || effects.Claim.ClaimType != "ANTI_PROCEDURE" || effects.Claim.MemoryID != recordedMemory.MemoryID {
		t.Fatalf("expected anti procedure promoted claim effects, got %+v", effects)
	}

	claims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		MemoryID:    recordedMemory.MemoryID,
		ClaimType:   "ANTI_PROCEDURE",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list anti procedure promoted claims after rpc write: %v", err)
	}
	if len(claims) != 1 || claims[0].ClaimType != "ANTI_PROCEDURE" {
		t.Fatalf("expected anti procedure promoted claim after rpc write, got %+v", claims)
	}
}

func TestWorkspaceMemoryWriteSupportsSelfModelWithoutPromotedClaimEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-memory-self-model-write"
		agentID     = "agent-memory-self-model-write"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Self Model Write",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Self Model Memory Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryWriteParams{
		WorkspaceID: workspaceID,
		MemoryType:  "self_model",
		Title:       "Operator stance",
		Body:        "Direct workspace memory writes should preserve already-supported self-model types on the current boundary.",
		Summary:     "Self-model direct write parity.",
		AgentID:     agentID,
		SourceKind:  "manual",
		SourceID:    "developer",
		Tags:        []string{"memory", "self_model"},
	})
	if err != nil {
		t.Fatalf("marshal self model workspace memory write params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryWrite(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryWrite self model rpc error: %+v", rpcErr)
	}
	resp, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected self model workspace memory write response type %T", result)
	}
	recordedMemory, ok := resp["memory"].(sqlite.WorkspaceMemoryRecord)
	if !ok || recordedMemory.MemoryID == "" || recordedMemory.MemoryType != "SELF_MODEL" {
		t.Fatalf("unexpected self model workspace memory write response %+v", resp)
	}
	if effects, exists := resp["promoted_claim_effects"]; exists {
		if typed, ok := effects.(*sqlite.PromotedKnowledgeClaimSyncEffects); ok {
			if typed != nil {
				t.Fatalf("did not expect self model direct write to surface promoted claim effects, got %+v", effects)
			}
		} else if effects != nil {
			t.Fatalf("did not expect self model direct write to surface promoted claim effects, got %+v", effects)
		}
	}

	claims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		MemoryID:    recordedMemory.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list self model claims after rpc write: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("did not expect self model direct write to materialize claims yet, got %+v", claims)
	}
}

func TestWorkspaceMemoryWriteSupportsPolicyTraceWithoutPromotedClaimEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-memory-policy-trace-write"
		agentID     = "agent-memory-policy-trace-write"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Policy Trace Write",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Policy Trace Memory Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryWriteParams{
		WorkspaceID: workspaceID,
		MemoryType:  "policy_trace",
		Title:       "Escalation policy trace",
		Body:        "Direct workspace memory writes should preserve already-supported policy-trace types on the current boundary.",
		Summary:     "Policy-trace direct write parity.",
		AgentID:     agentID,
		SourceKind:  "manual",
		SourceID:    "developer",
		Tags:        []string{"memory", "policy_trace"},
	})
	if err != nil {
		t.Fatalf("marshal policy trace workspace memory write params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryWrite(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryWrite policy trace rpc error: %+v", rpcErr)
	}
	resp, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected policy trace workspace memory write response type %T", result)
	}
	recordedMemory, ok := resp["memory"].(sqlite.WorkspaceMemoryRecord)
	if !ok || recordedMemory.MemoryID == "" || recordedMemory.MemoryType != "POLICY_TRACE" {
		t.Fatalf("unexpected policy trace workspace memory write response %+v", resp)
	}
	if effects, exists := resp["promoted_claim_effects"]; exists {
		if typed, ok := effects.(*sqlite.PromotedKnowledgeClaimSyncEffects); ok {
			if typed != nil {
				t.Fatalf("did not expect policy trace direct write to surface promoted claim effects, got %+v", effects)
			}
		} else if effects != nil {
			t.Fatalf("did not expect policy trace direct write to surface promoted claim effects, got %+v", effects)
		}
	}

	claims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		MemoryID:    recordedMemory.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list policy trace claims after rpc write: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("did not expect policy trace direct write to materialize claims yet, got %+v", claims)
	}
}

func TestWorkspaceMemoryWriteSupportsGoalCommitmentWithoutPromotedClaimEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-memory-goal-commitment-write"
		agentID     = "agent-memory-goal-commitment-write"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Goal Commitment Write",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Goal Commitment Memory Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryWriteParams{
		WorkspaceID: workspaceID,
		MemoryType:  "goal_commitment",
		Title:       "Protect the working contour",
		Body:        "Direct workspace memory writes should preserve already-supported goal-commitment types on the current boundary.",
		Summary:     "Goal-commitment direct write parity.",
		AgentID:     agentID,
		SourceKind:  "manual",
		SourceID:    "developer",
		Tags:        []string{"memory", "goal_commitment"},
	})
	if err != nil {
		t.Fatalf("marshal goal commitment workspace memory write params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryWrite(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryWrite goal commitment rpc error: %+v", rpcErr)
	}
	resp, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected goal commitment workspace memory write response type %T", result)
	}
	recordedMemory, ok := resp["memory"].(sqlite.WorkspaceMemoryRecord)
	if !ok || recordedMemory.MemoryID == "" || recordedMemory.MemoryType != "GOAL_COMMITMENT" {
		t.Fatalf("unexpected goal commitment workspace memory write response %+v", resp)
	}
	if effects, exists := resp["promoted_claim_effects"]; exists {
		if typed, ok := effects.(*sqlite.PromotedKnowledgeClaimSyncEffects); ok {
			if typed != nil {
				t.Fatalf("did not expect goal commitment direct write to surface promoted claim effects, got %+v", effects)
			}
		} else if effects != nil {
			t.Fatalf("did not expect goal commitment direct write to surface promoted claim effects, got %+v", effects)
		}
	}

	claims, err := store.ListKnowledgeClaims(ctx, sqlite.KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		MemoryID:    recordedMemory.MemoryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list goal commitment claims after rpc write: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("did not expect goal commitment direct write to materialize claims yet, got %+v", claims)
	}
}

func TestWorkspaceMemoryWriteRejectsUnsupportedCurrentDirectMemoryType(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const workspaceID = "ws-memory-invalid-direct-type"
	ctx := testAuthContext(workspaceID, "human", "developer")

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Invalid Direct Type",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryWriteParams{
		WorkspaceID: workspaceID,
		MemoryType:  "constitution",
		Body:        "This direct public boundary should reject unsupported identity/governance write-gate types for now.",
	})
	if err != nil {
		t.Fatalf("marshal invalid direct memory write params: %v", err)
	}

	_, rpcErr := h.workspaceMemoryWrite(ctx, raw)
	if rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "memory_type must be one of") {
		t.Fatalf("expected invalid memory_type rejection, got %+v", rpcErr)
	}
}

func TestWorkspaceMemoryRemovePublishesPromotedClaimArchiveSideEffectsInChronologicalOrder(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	const (
		workspaceID = "ws-memory-promoted-claim-archive-live"
		agentID     = "agent-memory-promoted-claim"
	)
	ctx := testAuthContext(workspaceID, "human", "developer")
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Workspace Memory Promoted Claim Archive Live",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "decision",
		Title:       "Promoted claim memory",
		Body:        "Archiving promoted memory should preserve claim, queue, and invalidation runtime mirrors.",
		Summary:     "Promoted knowledge-claim archive parity.",
		AgentID:     agentID,
		SourceKind:  "manual",
		SourceID:    "developer",
		Tags:        []string{"memory", "promotion", "claim"},
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}
	claimID := "claim:memory:" + record.MemoryID
	claim, err := store.GetKnowledgeClaim(ctx, workspaceID, claimID)
	if err != nil {
		t.Fatalf("get promoted knowledge claim: %v", err)
	}
	claim, err = store.RequestKnowledgeClaimReview(ctx, sqlite.KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claimID,
		ActorID:     agentID,
		Reason:      "seed promoted claim review queue before archive",
		ReviewDueAt: "2099-01-01T00:00:00Z",
		AssignedTo:  "reviewer-memory-promoted",
	})
	if err != nil {
		t.Fatalf("request review for promoted claim: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:    "memres-promoted-claim-archive-live",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:promoted-claim-archive",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "knowledge_claim", RefID: claimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed promoted claim residency: %v", err)
	}
	queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list promoted claim follow-up queues: %v", err)
	}
	if len(queues) != 1 {
		t.Fatalf("expected one promoted-claim follow-up queue, got %+v", queues)
	}
	queueID := queues[0].QueueID

	seenRemovedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.archived",
		EntityType:  "workspace_memory",
		EntityID:    record.MemoryID,
		Limit:       10,
	})
	seenClaimArchivedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.archived",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	seenQueueResolvedEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queueID,
		Limit:       10,
	})
	seenInvalidationEvents := snapshotRuntimeEventIDs(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	})

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	raw, err := json.Marshal(workspaceMemoryRemoveParams{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		RemovedBy:   "developer",
		Reason:      "promoted claim archived with memory",
	})
	if err != nil {
		t.Fatalf("marshal workspace memory remove params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryRemove(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryRemove rpc error: %+v", rpcErr)
	}
	resp, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected workspace memory remove response type %T", result)
	}
	removedRecord, ok := resp["memory"].(sqlite.WorkspaceMemoryRecord)
	if !ok || removedRecord.MemoryID != record.MemoryID || removedRecord.ArchivedAt == nil {
		t.Fatalf("unexpected workspace memory remove response %+v", resp)
	}

	removedPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.archived",
		EntityType:  "workspace_memory",
		EntityID:    record.MemoryID,
		Limit:       10,
	}, seenRemovedEvents)
	claimArchivedPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.archived",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	}, seenClaimArchivedEvents)
	queueResolvedPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.resolved",
		EntityType:  "operator_queue",
		EntityID:    queueID,
		Limit:       10,
	}, seenQueueResolvedEvents)
	invalidationPersisted := mustNewRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_enqueued",
		EntityType:  "memory_invalidation",
		Limit:       10,
	}, seenInvalidationEvents)

	ordered, liveEvents := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: removedPersisted, Type: "workspace.memory.removed"},
		runtimeEventExpectation{Event: queueResolvedPersisted, Type: "workspace.ops.resolved"},
		runtimeEventExpectation{Event: claimArchivedPersisted, Type: "workspace.claim.archived"},
		runtimeEventExpectation{Event: invalidationPersisted, Type: "memory.invalidation_enqueued"},
	)
	if len(ordered) != 4 ||
		ordered[0].Type != "workspace.memory.removed" ||
		ordered[1].Type != "workspace.ops.resolved" ||
		ordered[2].Type != "workspace.claim.archived" ||
		ordered[3].Type != "memory.invalidation_enqueued" ||
		!runtimeEventChronologicalLess(ordered[0].Event, ordered[1].Event) ||
		!runtimeEventChronologicalLess(ordered[1].Event, ordered[2].Event) ||
		!runtimeEventChronologicalLess(ordered[2].Event, ordered[3].Event) {
		t.Fatalf("expected promoted-memory archive live mirrors to follow persisted chronology, got %+v", ordered)
	}
	var invalidationLive EventMessage
	for i, expectation := range ordered {
		if expectation.Type == "memory.invalidation_enqueued" {
			invalidationLive = liveEvents[i]
			break
		}
	}
	payload := decodeEventPayloadMap(t, invalidationLive.PayloadJSON)
	if payload["trigger_cause"] != "knowledge_claim.archived" || payload["ref_kind"] != "knowledge_claim" || payload["ref_id"] != claimID {
		t.Fatalf("expected promoted claim archive invalidation payload, got %+v", payload)
	}
}

func TestDashboardIncludesRecentMemoryPanel(t *testing.T) {
	if !strings.Contains(dashboardHTML, `id="memory-list"`) {
		t.Fatalf("dashboard is missing memory list container")
	}
	for _, memoryType := range []string{"PROCEDURE", "ANTI_PROCEDURE", "ENTITY", "EXPERIENCE", "SUMMARY", "SELF_MODEL", "GOAL_COMMITMENT", "POLICY_TRACE"} {
		if !strings.Contains(dashboardHTML, `<option value="`+memoryType+`">`+memoryType+`</option>`) {
			t.Fatalf("dashboard memory composer is missing %s option", memoryType)
		}
	}
	if !strings.Contains(dashboardHTML, `id="memory-search-query"`) {
		t.Fatalf("dashboard is missing memory search input")
	}
	if !strings.Contains(dashboardHTML, `id="memory-filter-context"`) {
		t.Fatalf("dashboard is missing memory context scope indicator")
	}
	if !strings.Contains(dashboardHTML, `id="memory-attention-badge"`) {
		t.Fatalf("dashboard is missing memory attention badge")
	}
	if !strings.Contains(dashboardHTML, "async function loadMemory()") {
		t.Fatalf("dashboard is missing loadMemory function")
	}
	if !strings.Contains(dashboardHTML, "applyMemoryContextFilter(") {
		t.Fatalf("dashboard is missing memory context filter action")
	}
	if !strings.Contains(dashboardHTML, "openMemoryComposerForEntry(") {
		t.Fatalf("dashboard is missing memory edit/copy action")
	}
	if !strings.Contains(dashboardHTML, "restoreMemoryFromModal(") {
		t.Fatalf("dashboard is missing memory restore action")
	}
	if !strings.Contains(dashboardHTML, "workspace.memory.restore") {
		t.Fatalf("dashboard is missing explicit memory restore RPC usage")
	}
	if !strings.Contains(dashboardHTML, "Anchor State (Read-Side)") || !strings.Contains(dashboardHTML, "anchor_semantic_lineage_id") || !strings.Contains(dashboardHTML, "anchor_signal_state") {
		t.Fatalf("dashboard is missing additive anchor-state memory rendering")
	}
	if !strings.Contains(dashboardHTML, "Retention State (Read-Side)") || !strings.Contains(dashboardHTML, "retention_band") || !strings.Contains(dashboardHTML, "retention_prunable") || !strings.Contains(dashboardHTML, "retention_guard_reason") {
		t.Fatalf("dashboard is missing additive retention-state memory rendering")
	}
	if !strings.Contains(dashboardHTML, "Recovery State (Read-Side)") || !strings.Contains(dashboardHTML, "recovery_candidate") || !strings.Contains(dashboardHTML, "recovery_guard_reason") {
		t.Fatalf("dashboard is missing additive recovery-state memory rendering")
	}
	if !strings.Contains(dashboardHTML, "Packet Context (Inspectability Only)") || !strings.Contains(dashboardHTML, "workspace.memory.packet.shell") {
		t.Fatalf("dashboard is missing packet-context memory rendering")
	}
	if !strings.Contains(dashboardHTML, "renderMemoryPacketBoundarySummary(") || !strings.Contains(dashboardHTML, "renderMemoryPacketBasisSummary(") {
		t.Fatalf("dashboard is missing packet summary renderers")
	}
	if !strings.Contains(dashboardHTML, "Current shell packet for the present task/session scope; not a historical memory snapshot, not global truth, not complete lineage, and not rollback authority.") {
		t.Fatalf("dashboard is missing packet-context guardrail wording")
	}
	if !strings.Contains(dashboardHTML, `data-tab="control"`) {
		t.Fatalf("dashboard is missing control tab")
	}
	if !strings.Contains(dashboardHTML, `id="ops-list"`) || !strings.Contains(dashboardHTML, `id="claims-list"`) || !strings.Contains(dashboardHTML, `id="execution-runs-list"`) {
		t.Fatalf("dashboard is missing control-plane list containers")
	}
	if !strings.Contains(dashboardHTML, "workspace.ops.list") || !strings.Contains(dashboardHTML, "workspace.claim.list") || !strings.Contains(dashboardHTML, "workspace.execution.run.list") {
		t.Fatalf("dashboard is missing control-plane RPC usage")
	}
	if !strings.Contains(dashboardHTML, "showOperatorQueueDetail(") || !strings.Contains(dashboardHTML, "showClaimDetail(") || !strings.Contains(dashboardHTML, "showExecutionRunDetail(") {
		t.Fatalf("dashboard is missing control-plane detail handlers")
	}
	if !strings.Contains(dashboardHTML, "requestSessionHandoffFromModal(") {
		t.Fatalf("dashboard is missing session handoff action")
	}
	if !strings.Contains(dashboardHTML, "agent.session.takeover") {
		t.Fatalf("dashboard is missing session takeover RPC usage")
	}
	if !strings.Contains(dashboardHTML, "workspace.memory.recorded") || !strings.Contains(dashboardHTML, "workspace.memory.removed") || !strings.Contains(dashboardHTML, "workspace.memory.restored") || !strings.Contains(dashboardHTML, "agent.session.blocked") || !strings.Contains(dashboardHTML, "workspace.ops.updated") || !strings.Contains(dashboardHTML, "workspace.execution.run") || !strings.Contains(dashboardHTML, "node.claimed") || !strings.Contains(dashboardHTML, "node.released") || !strings.Contains(dashboardHTML, "node.completed") {
		t.Fatalf("dashboard is missing session/memory SSE subscriptions")
	}
	if !strings.Contains(dashboardHTML, `'workspace.policy.put','tool.call.executed','tool.call.denied','tool.call.approval_required','cluster.metric_snapshot'`) {
		t.Fatalf("dashboard is missing tool/policy live SSE subscriptions")
	}
	if !strings.Contains(dashboardHTML, "showMemoryDetail(") {
		t.Fatalf("dashboard is missing memory detail handler")
	}
	if !strings.Contains(dashboardHTML, `id="ops-inbox-list"`) || !strings.Contains(dashboardHTML, `id="replay-findings-list"`) {
		t.Fatalf("dashboard is missing operator inbox or replay containers")
	}
	if !strings.Contains(dashboardHTML, "workspace.events.replay") || !strings.Contains(dashboardHTML, "workspace.events.evaluate") {
		t.Fatalf("dashboard is missing replay/evaluate RPC usage")
	}
	if !strings.Contains(dashboardHTML, "Ingest Order") || !strings.Contains(dashboardHTML, "Apply Order") || !strings.Contains(dashboardHTML, "Source Event ID") {
		t.Fatalf("dashboard is missing replay causal-order or source-event detail surfaces")
	}
	if !strings.Contains(dashboardHTML, "Finding Summary") || !strings.Contains(dashboardHTML, "Dedup Conflicts") || !strings.Contains(dashboardHTML, "Causal Order") || !strings.Contains(dashboardHTML, "Missing Parents") || !strings.Contains(dashboardHTML, "Cycle-Affected") {
		t.Fatalf("dashboard is missing replay finding-summary surfaces")
	}
	if !strings.Contains(dashboardHTML, "Provenance Summary") || !strings.Contains(dashboardHTML, "Source Event") || !strings.Contains(dashboardHTML, "Root Cause") || !strings.Contains(dashboardHTML, "Prov Group") || !strings.Contains(dashboardHTML, "Full Lineage Fields") {
		t.Fatalf("dashboard is missing replay provenance-summary surfaces")
	}
	if !strings.Contains(dashboardHTML, "Retention Risk") || !strings.Contains(dashboardHTML, "Compaction Candidates") || !strings.Contains(dashboardHTML, "Compaction Snapshots") || !strings.Contains(dashboardHTML, "Episode Packs") {
		t.Fatalf("dashboard is missing replay retention-risk summary surfaces")
	}
	if !strings.Contains(dashboardHTML, "Inspectable retention/compaction risk over existing replay and session-compaction read-side artifacts; no immutable-audit guarantee.") {
		t.Fatalf("dashboard is missing replay retention-risk guardrail wording")
	}
	if !strings.Contains(dashboardHTML, "Bounded family rollups over the current replay findings; inspectability only, not certified replay correctness.") {
		t.Fatalf("dashboard is missing replay finding-summary guardrail wording")
	}
	if !strings.Contains(dashboardHTML, "Additive provenance visibility over the current replay findings; inspectability only, not complete causal history or immutable-audit authority.") {
		t.Fatalf("dashboard is missing replay provenance-summary guardrail wording")
	}
	if !strings.Contains(dashboardHTML, "loadOperatorInbox()") || !strings.Contains(dashboardHTML, "runReplayWorkbench(") {
		t.Fatalf("dashboard is missing operator inbox or replay handlers")
	}
	if !strings.Contains(dashboardHTML, "renderExecutionGraph(") || !strings.Contains(dashboardHTML, "openReplayFromExecutionRun(") {
		t.Fatalf("dashboard is missing execution graph or replay bridge")
	}
	if !strings.Contains(dashboardHTML, `data-tab="instrumentation"`) || !strings.Contains(dashboardHTML, `id="panel-instrumentation"`) {
		t.Fatalf("dashboard is missing instrumentation tab")
	}
	if !strings.Contains(dashboardHTML, `id="instrumentation-workspace-summary"`) || !strings.Contains(dashboardHTML, `id="instrumentation-clusters-list"`) || !strings.Contains(dashboardHTML, `id="instrumentation-snapshot-summary"`) {
		t.Fatalf("dashboard is missing instrumentation containers")
	}
	if !strings.Contains(dashboardHTML, "loadInstrumentation()") || !strings.Contains(dashboardHTML, "createInstrumentationSnapshot()") || !strings.Contains(dashboardHTML, "showProtoClusterDetail(") {
		t.Fatalf("dashboard is missing instrumentation actions")
	}
	if !strings.Contains(dashboardHTML, "workspace.instrumentation.report") || !strings.Contains(dashboardHTML, "workspace.instrumentation.clusters") || !strings.Contains(dashboardHTML, "workspace.instrumentation.snapshot") {
		t.Fatalf("dashboard is missing instrumentation RPC usage")
	}
	if !strings.Contains(dashboardHTML, "cluster.metric_snapshot") {
		t.Fatalf("dashboard is missing instrumentation snapshot event wiring")
	}
}

func TestDashboardIncludesInstrumentationPanel(t *testing.T) {
	if !strings.Contains(dashboardHTML, `data-tab="instrumentation"`) {
		t.Fatalf("dashboard is missing instrumentation tab")
	}
	if !strings.Contains(dashboardHTML, `id="panel-instrumentation"`) {
		t.Fatalf("dashboard is missing instrumentation panel")
	}
	if !strings.Contains(dashboardHTML, `id="instrumentation-workspace-summary"`) || !strings.Contains(dashboardHTML, `id="instrumentation-clusters-list"`) {
		t.Fatalf("dashboard is missing instrumentation summary or cluster containers")
	}
	if !strings.Contains(dashboardHTML, "async function loadInstrumentation()") || !strings.Contains(dashboardHTML, "createInstrumentationSnapshot()") {
		t.Fatalf("dashboard is missing instrumentation handlers")
	}
	if !strings.Contains(dashboardHTML, "workspace.instrumentation.report") || !strings.Contains(dashboardHTML, "workspace.instrumentation.clusters") || !strings.Contains(dashboardHTML, "workspace.instrumentation.snapshot") {
		t.Fatalf("dashboard is missing instrumentation RPC usage")
	}
	if !strings.Contains(dashboardHTML, "showProtoClusterDetail(") || !strings.Contains(dashboardHTML, "cluster.metric_snapshot") {
		t.Fatalf("dashboard is missing proto-cluster detail or snapshot SSE wiring")
	}
}

func TestDashboardIncludesInstrumentationFiltersAndRuntimeSync(t *testing.T) {
	if !strings.Contains(dashboardHTML, `id="instrumentation-filter-agent"`) || !strings.Contains(dashboardHTML, `id="instrumentation-filter-session"`) || !strings.Contains(dashboardHTML, `id="instrumentation-filter-task"`) {
		t.Fatalf("dashboard is missing instrumentation filter inputs")
	}
	if !strings.Contains(dashboardHTML, "function instrumentationFilterParams(") || !strings.Contains(dashboardHTML, "resetInstrumentationFilters()") {
		t.Fatalf("dashboard is missing instrumentation filter helpers")
	}
	if !strings.Contains(dashboardHTML, "syncInstrumentationSnapshotFromRuntimeEvents()") {
		t.Fatalf("dashboard is missing runtime-event to instrumentation snapshot sync")
	}
	if !strings.Contains(dashboardHTML, "runtimeEventsCache.find") || !strings.Contains(dashboardHTML, "cluster.metric_snapshot") {
		t.Fatalf("dashboard is missing runtime snapshot bridge wiring")
	}
}

func TestDashboardInstrumentationKeepsReadSideWording(t *testing.T) {
	required := []string{
		"Read-only approximation over task metadata and proto-cluster evidence. task_class, task_class_source, and corridor_readiness support operator inspection only; they do not assign a corridor or carry policy authority.",
		"Operator-facing approximation only:",
		"Read-only cluster-level basis layer between task-first corridor authority and downstream corridor fit / boundary diagnostics.",
		"Read-only lease visibility for overlapping coordination and stewarded recovery; this does not grant write authority by itself.",
		"Current Steward Lease",
		"Active Steward Leases",
		"Task-first corridor authority is a read-only precedence surface only. It does not assign a corridor or apply policy.",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard instrumentation/read-side wording is missing %s", needle)
		}
	}
}

func TestDashboardIncludesTensionOverlayWiring(t *testing.T) {
	required := []string{
		"workspace.tension.refresh",
		"workspace.tension.list",
		"workspace.tension.get",
		"workspace.tension.frontier",
		"workspace.tension.confirm",
		"workspace.tension.discard",
		"workspace.tension.archive",
		"tension.refreshed",
		"tension.detected",
		"tension.updated",
		"tension.confirmed",
		"tension.discarded",
		"tension.archived",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard is missing tension overlay wiring for %s", needle)
		}
	}
}

func TestDashboardIncludesTensionSurfaceAndDeepLinks(t *testing.T) {
	required := []string{
		`data-tab="tensions"`,
		`id="tensions-badge"`,
		`id="panel-tensions"`,
		`id="tension-frontier-list"`,
		`id="tension-list"`,
		`id="tension-detail-summary"`,
		"renderTensionLinkList(",
		"relatedTensionsForProtoCluster(",
		"relatedTensionsForRuntimeEvent(",
		"Open Control Scaffold",
		"function openControlScaffold(",
		"openTensionsForProtoCluster(",
		"openTensionFromRuntimeEvent(",
		"function actOnTension(",
		">Confirm</button>",
		">Discard</button>",
		">Archive</button>",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard tension surface is missing %s", needle)
		}
	}
}

func TestDashboardIncludesTensionRuntimeSync(t *testing.T) {
	required := []string{
		"function isTensionRuntimeEventType(",
		"function renderTensionGeneratedAt(",
		"function syncTensionStateFromRuntimeEvents()",
		"tension.refreshed",
		"tensionRuntimeEventCache = runtimeEventsCache.find",
		"renderTensionGeneratedAt();",
		"syncTensionStateFromRuntimeEvents();",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard tension runtime sync is missing %s", needle)
		}
	}
	if strings.Contains(dashboardHTML, "delete frontierParams.review_status") {
		t.Fatalf("dashboard still strips review_status from workspace.tension.frontier params")
	}
}

func TestDashboardIncludesTensionInboxAndRefreshContracts(t *testing.T) {
	required := []string{
		`<option value="tension">Tensions</option>`,
		`kind: "tension"`,
		`dedupeTensionRecords([].concat(tensionsUniverseCache || [], tensionsCache || []))`,
		`String(item.review_status || "").toUpperCase() === "CONFIRMED"`,
		"function tensionRefreshParams()",
		`rpc('workspace.tension.refresh', tensionRefreshParams())`,
		"Refresh Frontier only scopes by proto-cluster.",
		`if (item.kind === "tension") {`,
		`showTensionDetail(item.id);`,
		"loadOperatorInbox();",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard tension inbox/refresh contract is missing %s", needle)
		}
	}
}

func TestDashboardUsesWorkspaceTimeAuthorityForOperatorQueueTiming(t *testing.T) {
	required := []string{
		"let operatorQueueTimeAuthority = null;",
		"function operatorQueueAuthorityFor(",
		"operatorQueueTimeAuthority = r.time_authority || null;",
		"operatorQueueCache = r.items || [];",
		"operatorQueueCache.find(x => x.queue_id === queueId)",
		"runtimeEventsCache.forEach(item => {",
		"timeAgo(item.updated_at || item.created_at, authority)",
		"timeAgo(item.last_escalated_at, authority)",
		"Keep Session Active",
		"Last Escalated",
		"Peers stay active",
		"Peers can stand down",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard operator timing authority is missing %s", needle)
		}
	}
}

func TestDashboardUsesWorkspaceTimeAuthorityForTensionTiming(t *testing.T) {
	required := []string{
		"let tensionSurfaceTimeAuthority = null;",
		"function tensionAuthorityFor(",
		"tensionSurfaceTimeAuthority = listResponse.time_authority || frontierResponse.time_authority || universeResponse.time_authority || tensionSurfaceTimeAuthority;",
		"tensionSurfaceTimeAuthority = response.time_authority || ((response.refresh || {}).time_authority) || tensionSurfaceTimeAuthority;",
		"timeAgo(item.last_seen_at || '', authority)",
		"timeAgo(tension.last_seen_at || '', authority)",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard tension timing authority is missing %s", needle)
		}
	}
}

func TestDashboardSeparatesPolicyInboxSignalsFromSnapshotsAndTensions(t *testing.T) {
	required := []string{
		`<option value="policy">Policy</option>`,
		`key: "policy:" + item.event_id`,
		`kind: "policy"`,
		`if (eventType !== "tool.call.denied" && eventType !== "tool.call.approval_required") return;`,
		`if (item.kind === "policy") {`,
		`showRuntimeEventDetail(item.id);`,
		`if (item.kind === "tension") {`,
		`showTensionDetail(item.id);`,
		`String(item.event_type || '').toLowerCase() === 'cluster.metric_snapshot'`,
		`function renderInstrumentationSnapshotState()`,
		`Snapshot Summary`,
		`Captured Clusters`,
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard control/policy separation is missing %s", needle)
		}
	}
	if strings.Contains(dashboardHTML, "workspace.policy.snapshot") || strings.Contains(dashboardHTML, "workspace.policy.report") {
		t.Fatalf("dashboard leaked instrumentation read-side into workspace.policy namespace")
	}
}

func TestDashboardIncludesControlPolicyReadSideSurface(t *testing.T) {
	required := []string{
		"Advisory Control",
		`id="control-policy-summary"`,
		`id="control-policy-cluster-list"`,
		`id="control-policy-scaffold-summary"`,
		`id="control-policy-scaffold-state"`,
		`id="control-policy-detail"`,
		`id="control-policy-snapshot-summary"`,
		`id="control-policy-snapshot-state"`,
		`id="unified-control-snapshot-summary"`,
		`id="unified-control-snapshot-state"`,
		`id="control-policy-detail-state"`,
		`id="control-policy-filter-type"`,
		`id="control-policy-filter-mode"`,
		`id="control-policy-filter-query"`,
		"function controlPolicyFilterParams()",
		"resetControlPolicyOverlayFilters()",
		"function loadControlPolicyOverlay()",
		"function showControlPolicyClusterDetail(",
		"createControlPolicySnapshot()",
		"createUnifiedControlSnapshot()",
		"function renderControlPolicyScaffoldState()",
		"function renderControlPolicySnapshotState()",
		"function renderUnifiedControlSnapshotState()",
		"function loadControlStateScaffold()",
		"function showControlStateClusterDetail(",
		"function syncControlStateSnapshotFromRuntimeEvents()",
		"tickControlStateScaffold()",
		"createControlStateSnapshot()",
		"function syncControlPolicySnapshotFromRuntimeEvents()",
		"function syncUnifiedControlSnapshotFromRuntimeEvents()",
		"Latest Advisory Snapshot",
		"Latest Unified Advisory Snapshot",
		"Control-State Scaffold",
		"Control Cluster Detail",
		"Loading advisory control report...",
		"Control-state scaffold will appear once advisory control report loads.",
		"Open Latest Control-State Snapshot",
		"Control-State Snapshot Summary",
		"Select a control cluster to inspect advisory signals, suggested controls, and recent runtime events.",
		"openTensionsForProtoCluster(",
		"showProtoClusterDetail(",
		"showRuntimeEventDetail(",
		"cluster.control_advisory_snapshot",
		"cluster.unified_control_advisory_snapshot",
		"cluster.control_state_snapshot",
		"cluster.control_state_ticked",
		"cluster.control_state_stabilized",
		"loadControlPolicyOverlay();",
		"loadControlStateScaffold();",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard control-policy read-side is missing %s", needle)
		}
	}
}

func TestDashboardAdvisoryControlUsesServerRPCsInsteadOfClientDerivedState(t *testing.T) {
	required := []string{
		"workspace.instrumentation.control.report",
		"workspace.instrumentation.control.cluster",
		"workspace.instrumentation.control.snapshot",
		"workspace.instrumentation.unified.control.snapshot",
		"workspace.instrumentation.unified.control.report",
		"workspace.instrumentation.control.state.report",
		"workspace.instrumentation.control.state.cluster",
		"workspace.instrumentation.control.state.tick",
		"workspace.instrumentation.control.state.snapshot",
		"workspace.rsp.capability.get",
		"workspace.rsp.belief.report",
		"rpc('workspace.instrumentation.control.report'",
		"rpc('workspace.instrumentation.control.cluster'",
		"rpc('workspace.instrumentation.control.snapshot'",
		"rpc('workspace.instrumentation.unified.control.snapshot'",
		"rpc('workspace.instrumentation.unified.control.report'",
		"rpc('workspace.instrumentation.control.state.report'",
		"rpc('workspace.instrumentation.control.state.cluster'",
		"rpc('workspace.instrumentation.control.state.tick'",
		"rpc('workspace.instrumentation.control.state.snapshot'",
		"rpc('workspace.rsp.capability.get'",
		"rpc('workspace.rsp.belief.report'",
		"rpc('workspace.rsp.forecast.report'",
		"async function loadControlPolicyOverlay()",
		"async function showControlPolicyClusterDetail(",
		"async function createControlPolicySnapshot()",
		"async function createUnifiedControlSnapshot()",
		"async function loadControlStateScaffold()",
		"async function showControlStateClusterDetail(",
		"async function tickControlStateScaffold()",
		"async function createControlStateSnapshot()",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard advisory-control RPC wiring is missing %s", needle)
		}
	}
	forbidden := []string{
		"function buildControlPolicyReadSide()",
		"const data = buildControlPolicyReadSide();",
	}
	for _, needle := range forbidden {
		if strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard advisory-control still derives state locally via %s", needle)
		}
	}
}

func TestDashboardSurfacesUnifiedArbitrationCapabilityFlagsAndCalibrationState(t *testing.T) {
	required := []string{
		"let rspCapabilityFlagsCache = null;",
		"let rspBeliefReportCache = null;",
		"let rspForecastReportCache = null;",
		"let rspTelemetryDumpCache = null;",
		"function renderControlCapabilityFlags(",
		"function renderControlActionBadgeRow(",
		"function unifiedControlContradictionFamilyLabel(",
		"function renderGovernedHintEvidence(",
		"Capability Flags",
		"Belief Calibration",
		"Telemetry Coverage",
		"Coverage Gaps",
		"Telemetry Streams",
		"Telemetry Provenance",
		"Recent Telemetry Markers",
		"warm baseline coverage",
		"alerts with baseline",
		"shrunk alerts",
		"shrinkage reliance",
		"fallback quality",
		"workspace fallback mix",
		"workspace tier pressure",
		"workspace-tier alerts",
		"agent-tier alerts",
		"workspace exact",
		"workspace agent-default",
		"warming driver",
		"fallback scope tier",
		"exact shrunk agent default",
		"exact shrunk workspace default",
		"agent default shrunk workspace default",
		"agent default",
		"workspace default",
		"Heuristic Diagnostics",
		"Forecast Outlook",
		"Forecast Coverage",
		"Forecast Provenance",
		"History-Backed",
		"Evidence-Backed",
		"Missing Inputs",
		"Evidence Refs",
		"Projection Bases",
		"Projection Models",
		"Projection Highlights",
		"Calibration Evidence",
		"Low Independence",
		"High Contradiction",
		"Verifier Stale",
		"High Uncertainty",
		"Unified Arbitration",
		"Effective Control Basis",
		"Cooldown Basis",
		"Stage ",
		"Acceptance Readiness",
		"Acceptance Gate",
		"Acceptance Progress",
		"function unifiedControlAcceptanceProgressLabel(",
		"function unifiedControlModeLabel(",
		"Ready Window Pending",
		"Ready Window Clear",
		"Early",
		"Partial",
		"Nearly Ready",
		"Blocked",
		"Aligned",
		"None",
		"ready pending",
		"ready window",
		"blocked",
		"observing",
		"Acceptance Checklist",
		"readiness-aware",
		"readiness-aware 0/0 clear",
		"observing-deferred ",
		"active-blocker-scoped",
		"ready-window",
		"No ready-window checklist recorded.",
		"Ready Window Pending Cooldown",
		"Ready Window Open",
		"Ready window clear.",
		"stabilize",
		"synergy seeking",
		"candidate streak not started",
		"observing candidate",
		"hysteresis pending",
		"streak below hysteresis",
		"Streak Below Hysteresis",
		"cooldown active",
		"contradictions and memory attention",
		"ready window pending cooldown",
		"ready window open",
		"Ready Window Cooldown Clear",
		"Cooldown Deferred",
		"Hysteresis Deferred",
		"Acceptance Missing Requirements",
		"Acceptance path not active.",
		"Observing candidate not started yet.",
		"Candidate Streak",
		"Required Streak",
		"Remaining Streak",
		"Ready To Stabilize",
		"Blocking Reasons",
		"Cooldown Active",
		"Transitioning Yes",
		"Effective Control Basis Summary",
		"Basis Fields",
		"Changed Fields",
		"Contradiction Summary",
		"Families",
		"Hard Safety Clamp",
		"Memory Safety Override",
		"delta-bearing applied-action traces",
		"Applied Actions",
		"Applied Trace",
		"Audit Summary",
		"function unifiedControlAuditSummaryKeyLabel(",
		"Governed Hint",
		"Memory Coherence Floor",
		"Prefer Kernel Refresh",
		"Raise Reviewer Diversity",
		"Reduce Solver Fanout",
		"Tighten Context Cap",
		"Require Far Reviewer",
		"Mode Cooldown",
		"Mode Cooldown Active",
		"Requires Memory Pressure",
		"Unsupported Actuation Class",
		"Unsupported Action",
		"No Actions",
		"Expired",
		"Trace Coverage",
		"Applied Entries",
		"Hint-backed Actions",
		"Applied Full Trace",
		"Suppressed Full Trace",
		"Applied Trace Fields",
		"Suppressed Trace Fields",
		"Suppression Reasons",
		"Suppressed Hints",
		"Suppression Trace",
		"Contradictions",
		"function unifiedControlBasisFieldLabel(",
		"Priority Focus",
		"Merge Threshold",
		"Governed Hint Summary",
		"Recommendation Classes",
		"Advisory Outcomes",
		"Governed Hint Evidence",
		"Governed Hint Outcomes",
		"Recommendation Class",
		"Evidence Diversity",
		"Diversity Band",
		"Evidence Source Mix",
		"Runtime Event Refs",
		"Evidence Source Kinds",
		"Root-Cause Groups",
		"Runtime Lineage Basis",
		"TTL Window State",
		"Hint Summary",
		"Applied Actions",
		"Suppressed Actions",
		"Suppression Reasons",
		"Outcome Summary",
		"Read-side arbitration combines control-state, memory coherence, and governed hints in control-order without applying live mutation.",
		"Audit-visible structured traces over the current read-side arbitration outputs; inspectability only, not durable execution history.",
		"Read-side rollup over the current applied/suppressed audit traces; inspectability only, not execution history or policy authority.",
		"Coverage rollup over the current structured audit trace fields; inspectability only, not trace completeness or authority.",
		"Structured suppression refs over current governed-hint intake, without claiming immutable audit retention.",
		"Joined advisory intake outcomes over existing hint, applied-trace, and suppression-trace surfaces; inspectability only, not execution history.",
		"Rollout matrix for belief, state/anomaly/forecast shadowing, governed hints, and live consequence gates.",
		"Bounded belief-health rollups over the current report items; inspectability only, not posterior or evaluator authority.",
		"Bounded report-level rollup over existing belief/anomaly/state telemetry readiness and coverage; inspectability only, not calibration quality or evaluator authority.",
		"Current read-side coverage gaps for the surfaced telemetry streams; not an evaluator verdict.",
		"Existing warm-baseline, shrinkage-provenance, and warming-driver context from the telemetry dump; inspectability only, not evaluator authority.",
		"Most recent observed timestamps for the current telemetry streams; inspectability only.",
		"Shadow-only heuristic forecast summary for operator review; readiness and provenance are inspectable but not calibrated quality.",
		"Additive rollup over current projections, readiness, and provenance fields; inspectability only, not calibrated quality or evaluator authority.",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard richer unified-control surface is missing %s", needle)
		}
	}
	if !strings.Contains(dashboardHTML, "if (value === 'READY_PENDING') {") || !strings.Contains(dashboardHTML, "clear: checklist.cooldown_clear ? 1 : 0") {
		t.Fatal("dashboard richer unified-control surface should keep ready-window checklist counts active-debt-scoped")
	}
	if !strings.Contains(dashboardHTML, "if (value === 'OBSERVING') {") || !strings.Contains(dashboardHTML, "clear: 0,") || !strings.Contains(dashboardHTML, "total: 0,") {
		t.Fatal("dashboard richer unified-control surface should keep observing checklist counts deferred until the candidate streak starts")
	}
	if strings.Count(dashboardHTML, "Ready Window Pending Cooldown") < 2 {
		t.Fatalf("dashboard richer unified-control surface should mirror ready-window pending wording in both cooldown basis and acceptance checklist")
	}
	if strings.Count(dashboardHTML, "Ready Window Open") < 2 {
		t.Fatalf("dashboard richer unified-control surface should mirror ready-window open wording in both cooldown basis and acceptance checklist")
	}
}

func TestDashboardAdvisoryControlIncludesSnapshotAndFilterContracts(t *testing.T) {
	required := []string{
		"function controlPolicyReportParams(",
		"function filteredControlPolicyClusters()",
		"function renderControlPolicyScaffoldState()",
		"function syncControlPolicySnapshotFromRuntimeEvents()",
		"function syncUnifiedControlSnapshotFromRuntimeEvents()",
		"function syncControlStateSnapshotFromRuntimeEvents()",
		"Server advisory read-side over proto-cluster metrics + confirmed tensions",
		"Pending tensions tracked separately from advisory banding",
		"No advisory control report available yet.",
		"No advisory clusters matched the current control filter.",
		"Open latest durable snapshot",
		"Open latest unified snapshot",
		"Captured clusters",
		"Open Runtime Event",
		"Advisory Snapshot Summary",
		"Unified Control Snapshot Summary",
		"Bounded advisory/effective unified-control snapshot mirrored from the persisted runtime event; inspectability only, not a second arbiter, execution history, or rollback authority.",
		"Control-State Snapshot Summary",
		"Open Latest Control-State Snapshot",
		"loadRuntimeEvents();",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard advisory-control helper contract is missing %s", needle)
		}
	}
	for _, forbidden := range []string{
		"no automatic policy writes",
		"policy writes",
		"policy loop",
	} {
		if strings.Contains(dashboardHTML, forbidden) {
			t.Fatalf("dashboard control-state cut-back still leaks legacy wording via %s", forbidden)
		}
	}
}

func TestDashboardControlStateScaffoldContracts(t *testing.T) {
	required := []string{
		"workspace.instrumentation.control.state.report",
		"workspace.instrumentation.control.state.cluster",
		"workspace.instrumentation.control.state.tick",
		"workspace.instrumentation.control.state.snapshot",
		"rpc('workspace.instrumentation.control.state.report'",
		"rpc('workspace.instrumentation.control.state.cluster'",
		"rpc('workspace.instrumentation.control.state.tick'",
		"rpc('workspace.instrumentation.control.state.snapshot'",
		"async function loadControlStateScaffold()",
		"async function showControlStateClusterDetail(",
		"async function tickControlStateScaffold()",
		"async function createControlStateSnapshot()",
		"function syncControlStateSnapshotFromRuntimeEvents()",
		"cluster.control_state_snapshot",
		"cluster.control_state_ticked",
		"cluster.control_state_stabilized",
		"function openControlScaffold(",
		"controlPolicySelectedClusterID = clusterID;",
		"await loadControlStateScaffold();",
		"Open Latest Control-State Snapshot",
		"Control-state scaffold will appear once advisory control report loads.",
		"Epoch",
		"Streak",
		"Selected Cluster Approximation",
		"stabilized hint",
		"candidate hint",
		"Dominant Signal",
		"Advisory Profile Heuristic",
		"Priority Focus",
		"Fanout Hint",
		"Review Hint",
		"Context Hint",
		"Basis",
	}
	for _, needle := range required {
		if !strings.Contains(dashboardHTML, needle) {
			t.Fatalf("dashboard control-state scaffold contract is missing %s", needle)
		}
	}
	for _, forbidden := range []string{
		"Selected Cluster State",
		"policy writes",
	} {
		if strings.Contains(dashboardHTML, forbidden) {
			t.Fatalf("dashboard control-state scaffold still contains legacy cut-back wording %s", forbidden)
		}
	}
	if !(strings.Contains(dashboardHTML, "Selected Cluster Approximation") || strings.Contains(dashboardHTML, "Selected Cluster Interpretation")) {
		t.Fatalf("dashboard control-state scaffold is missing operator-facing interpretation label")
	}
	if !strings.Contains(dashboardHTML, "stabilized hint") {
		t.Fatalf("dashboard control-state scaffold is missing stabilized hint wording")
	}
	if !strings.Contains(dashboardHTML, "candidate hint") {
		t.Fatalf("dashboard control-state scaffold is missing candidate hint wording")
	}
	if !(strings.Contains(dashboardHTML, "Fanout Hint") && strings.Contains(dashboardHTML, "Review Hint") && strings.Contains(dashboardHTML, "Context Hint")) {
		t.Fatalf("dashboard control-state scaffold is missing operator hint labels")
	}
}

func containsInvariantCode(items []string, needle string) bool {
	for _, item := range items {
		if item == needle {
			return true
		}
	}
	return false
}

func seedMemoryProjectionProcessingRow(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, memoryID string) {
	t.Helper()
	now := time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO memory_projection_outbox(
	    projection_id, workspace_id, projection_kind, origin_id, status, attempt_count, last_error,
	    available_at, enqueued_at, started_at, completed_at, updated_at
	  ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, NULL, ?)
	  ON CONFLICT(workspace_id, projection_kind, origin_id) DO UPDATE SET
	    projection_id = excluded.projection_id,
	    status = excluded.status,
	    attempt_count = excluded.attempt_count,
	    last_error = excluded.last_error,
	    available_at = excluded.available_at,
	    enqueued_at = excluded.enqueued_at,
	    started_at = excluded.started_at,
	    completed_at = NULL,
	    updated_at = excluded.updated_at`,
		"mproj-processing-"+memoryID,
		workspaceID,
		"WORKSPACE_MEMORY",
		memoryID,
		"PROCESSING",
		"",
		now,
		now,
		now,
		now,
	); err != nil {
		t.Fatalf("insert processing outbox row: %v", err)
	}
}
