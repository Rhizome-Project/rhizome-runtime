package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceMemoryPromotionRPCSurface(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-promo"
		agentID     = "agent-handler-memory-promo"
		sessionID   = "sess-handler-memory-promo"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Promotion",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Memory Promotion Agent",
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
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	enqueueRaw, err := json.Marshal(workspaceMemoryPromotionEnqueueParams{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Promotion queue keeps review-gated candidates out of truth",
		Body:        "Accepted promotion candidates should still land through canonical workspace memory writes.",
		SessionID:   sessionID,
		SourceKind:  "memory_packet_shell",
		SourceID:    "shell-packet-queue",
		BasisDigest: "basis-digest-handler-promo",
		BasisRefs:   []string{"packet:shell-packet-queue"},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("marshal enqueue params: %v", err)
	}
	enqueueResult, rpcErr := h.workspaceMemoryPromotionEnqueue(ctx, enqueueRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPromotionEnqueue rpc error: %+v", rpcErr)
	}
	record, ok := enqueueResult.(sqlite.MemoryPromotionRecord)
	if !ok {
		t.Fatalf("unexpected enqueue result type %T", enqueueResult)
	}
	if record.State != "PENDING" || record.Candidate.AgentID != agentID {
		t.Fatalf("unexpected promotion enqueue record %+v", record)
	}
	enqueueLive := expectEvent(t, ch, "memory.promotion_enqueued")
	enqueuePersisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.promotion_enqueued",
		EntityType:  "memory_promotion",
		EntityID:    record.PromotionID,
		Limit:       1,
	})
	if enqueueLive.EventID != enqueuePersisted.EventID || enqueueLive.IngestSeq != enqueuePersisted.IngestSeq {
		t.Fatalf("expected enqueue live event to mirror persisted runtime envelope, live=%+v persisted=%+v", enqueueLive, enqueuePersisted)
	}
	assertServerRuntimeEventAuthorityMetadata(t, enqueuePersisted, authority)
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, enqueueLive.PayloadJSON), enqueuePersisted.PayloadJSON)

	listRaw, err := json.Marshal(workspaceMemoryPromotionListParams{
		WorkspaceID: workspaceID,
		State:       "PENDING",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal list params: %v", err)
	}
	listResult, rpcErr := h.workspaceMemoryPromotionList(ctx, listRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPromotionList rpc error: %+v", rpcErr)
	}
	listPayload := listResult.(map[string]any)
	items, ok := listPayload["items"].([]sqlite.MemoryPromotionRecord)
	if !ok || len(items) != 1 || items[0].PromotionID != record.PromotionID {
		t.Fatalf("unexpected promotion list payload %+v", listPayload)
	}

	resolveRaw, err := json.Marshal(workspaceMemoryPromotionResolveParams{
		WorkspaceID: workspaceID,
		PromotionID: record.PromotionID,
		Resolution:  "ACCEPTED",
		ResolvedBy:  "developer",
	})
	if err != nil {
		t.Fatalf("marshal resolve params: %v", err)
	}
	resolveResult, rpcErr := h.workspaceMemoryPromotionResolve(ctx, resolveRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPromotionResolve rpc error: %+v", rpcErr)
	}
	resolvePayload, ok := resolveResult.(sqlite.MemoryPromotionResolveResult)
	if !ok {
		t.Fatalf("unexpected resolve result type %T", resolveResult)
	}
	if resolvePayload.Promotion.State != "ACCEPTED" || resolvePayload.AppliedMemory == nil {
		t.Fatalf("expected accepted promotion with applied memory, got %+v", resolvePayload)
	}
	recordedPersisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    resolvePayload.AppliedMemory.MemoryID,
		Limit:       1,
	})
	claimPersisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    "claim:memory:" + resolvePayload.AppliedMemory.MemoryID,
		Limit:       1,
	})
	resolvePersisted := mustListRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.promotion_resolved",
		EntityType:  "memory_promotion",
		EntityID:    record.PromotionID,
		Limit:       1,
	})
	ordered, liveEvents := nextLiveEventsMirroringRuntimeEventsInOrder(t, ch,
		runtimeEventExpectation{Event: recordedPersisted, Type: "workspace.memory.recorded"},
		runtimeEventExpectation{Event: claimPersisted, Type: "workspace.claim.written"},
		runtimeEventExpectation{Event: resolvePersisted, Type: "memory.promotion_resolved"},
	)
	if len(ordered) != 3 ||
		ordered[0].Type != "workspace.memory.recorded" ||
		ordered[1].Type != "workspace.claim.written" ||
		ordered[2].Type != "memory.promotion_resolved" {
		t.Fatalf("expected memory promotion accept live mirrors to include promoted-claim side effects, got %+v", ordered)
	}
	recordedLive := liveEvents[0]
	assertLiveEventMirrorsRuntimeEvent(t, recordedLive, recordedPersisted, "workspace.memory.recorded")
	assertServerRuntimeEventAuthorityMetadata(t, recordedPersisted, authority)
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, recordedLive.PayloadJSON), recordedPersisted.PayloadJSON)
	claimLive := liveEvents[1]
	assertLiveEventMirrorsRuntimeEvent(t, claimLive, claimPersisted, "workspace.claim.written")
	resolveLive := liveEvents[2]
	if resolveLive.EventID != resolvePersisted.EventID || resolveLive.IngestSeq != resolvePersisted.IngestSeq {
		t.Fatalf("expected resolve live event to mirror persisted runtime envelope, live=%+v persisted=%+v", resolveLive, resolvePersisted)
	}
	assertServerRuntimeEventAuthorityMetadata(t, resolvePersisted, authority)
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, resolveLive.PayloadJSON), resolvePersisted.PayloadJSON)

	getRaw, err := json.Marshal(workspaceMemoryPromotionGetParams{
		WorkspaceID: workspaceID,
		PromotionID: record.PromotionID,
	})
	if err != nil {
		t.Fatalf("marshal get params: %v", err)
	}
	getResult, rpcErr := h.workspaceMemoryPromotionGet(ctx, getRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPromotionGet rpc error: %+v", rpcErr)
	}
	getRecord, ok := getResult.(sqlite.MemoryPromotionRecord)
	if !ok || getRecord.State != "ACCEPTED" || getRecord.AppliedID == "" {
		t.Fatalf("unexpected promotion get payload %+v", getResult)
	}
}

func TestWorkspaceMemoryPromotionResolveMirrorsNewPersistedRowForRepeatedTargetMemoryID(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-promo-repeat"
		agentID     = "agent-handler-memory-promo-repeat"
		sessionID   = "sess-handler-memory-promo-repeat"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Promotion Repeated Memory ID",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Memory Promotion Repeat Agent",
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
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	enqueueRaw, err := json.Marshal(workspaceMemoryPromotionEnqueueParams{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Promotion repeat target",
		Body:        "Promotion acceptance should mirror the exact new runtime row.",
		Summary:     "Promotion repeat target.",
		AgentID:     agentID,
		SessionID:   sessionID,
		SourceKind:  "memory_packet_shell",
		SourceID:    "shell-packet-repeat",
		BasisDigest: "basis-digest-handler-promo-repeat",
		BasisRefs:   []string{"packet:shell-packet-repeat"},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("marshal enqueue params: %v", err)
	}
	enqueueAny, rpcErr := h.workspaceMemoryPromotionEnqueue(ctx, enqueueRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPromotionEnqueue rpc error: %+v", rpcErr)
	}
	record, ok := enqueueAny.(sqlite.MemoryPromotionRecord)
	if !ok {
		t.Fatalf("unexpected enqueue result type %T", enqueueAny)
	}

	baselineRecord, err := store.RecordWorkspaceMemory(ctx, sqlite.WorkspaceMemoryInput{
		MemoryID:    record.TargetMemoryID,
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Title:       "Existing target memory",
		Body:        "This pre-existing row forces promotion acceptance to append a new runtime event for the same memory_id.",
		Summary:     "Existing target memory.",
		AgentID:     agentID,
		SessionID:   sessionID,
		SourceKind:  "manual",
		SourceID:    "baseline-target",
	})
	if err != nil {
		t.Fatalf("record baseline target memory: %v", err)
	}

	memoryFilter := sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "workspace_memory.recorded",
		EntityType:  "workspace_memory",
		EntityID:    record.TargetMemoryID,
		Limit:       10,
	}
	firstPersisted := mustRuntimeEvent(t, ctx, store, memoryFilter)
	if firstPersisted.EntityID != baselineRecord.MemoryID {
		t.Fatalf("expected baseline runtime row for target memory %q, got %+v", baselineRecord.MemoryID, firstPersisted)
	}
	seenMemoryEvents := snapshotRuntimeEventIDs(t, ctx, store, memoryFilter)

	resolveRaw, err := json.Marshal(workspaceMemoryPromotionResolveParams{
		WorkspaceID: workspaceID,
		PromotionID: record.PromotionID,
		Resolution:  "ACCEPTED",
		ResolvedBy:  "developer",
	})
	if err != nil {
		t.Fatalf("marshal resolve params: %v", err)
	}
	resolveAny, rpcErr := h.workspaceMemoryPromotionResolve(ctx, resolveRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPromotionResolve rpc error: %+v", rpcErr)
	}
	resolveResult, ok := resolveAny.(sqlite.MemoryPromotionResolveResult)
	if !ok {
		t.Fatalf("unexpected resolve result type %T", resolveAny)
	}
	if resolveResult.AppliedMemory == nil || resolveResult.AppliedMemory.MemoryID != record.TargetMemoryID || resolveResult.AppliedMemory.Record.SourceID != "shell-packet-repeat" {
		t.Fatalf("unexpected applied memory payload %+v", resolveResult)
	}
	if resolveResult.AppliedMemory.Event.EventID == "" {
		t.Fatalf("expected applied memory result to carry exact runtime event, got %+v", resolveResult.AppliedMemory.Event)
	}

	live := nextEventOfType(t, ch, "workspace.memory.recorded")
	secondPersisted := mustNewRuntimeEvent(t, ctx, store, memoryFilter, seenMemoryEvents)
	assertLiveEventMirrorsRuntimeEvent(t, live, secondPersisted, "workspace.memory.recorded")
	assertPayloadMatchesRuntimeEvent(t, decodeEventPayloadMap(t, live.PayloadJSON), secondPersisted.PayloadJSON)
	if secondPersisted.EventID == firstPersisted.EventID || secondPersisted.IngestSeq <= firstPersisted.IngestSeq {
		t.Fatalf("expected promotion acceptance to mirror the newly appended runtime row, got first=%+v second=%+v", firstPersisted, secondPersisted)
	}
}

func TestWorkspaceMemoryPromotionResolveRejectsStaleEvidenceAtAcceptTime(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-promo-stale-evidence"
		agentID     = "agent-handler-memory-promo-stale-evidence"
		sessionA    = "sess-handler-memory-promo-stale-a"
		sessionB    = "sess-handler-memory-promo-stale-b"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Promotion Stale Evidence",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Memory Promotion Stale Evidence Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	for _, sessionID := range []string{sessionA, sessionB} {
		if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
			SessionID:   sessionID,
			AgentID:     agentID,
			WorkspaceID: workspaceID,
			StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatalf("create session %s: %v", sessionID, err)
		}
	}

	enqueueRaw, err := json.Marshal(workspaceMemoryPromotionEnqueueParams{
		WorkspaceID: workspaceID,
		MemoryType:  "procedure",
		Title:       "Promotion stale evidence",
		Body:        "Resolve must revalidate procedural evidence before applying workspace memory.",
		SessionID:   sessionA,
		SourceKind:  "episode_pack",
		SourceID:    "pack-handler-stale",
		BasisDigest: "basis-digest-handler-stale",
		BasisRefs:   []string{"session:" + sessionA, "session:" + sessionB},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("marshal enqueue params: %v", err)
	}
	enqueueAny, rpcErr := h.workspaceMemoryPromotionEnqueue(ctx, enqueueRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPromotionEnqueue rpc error: %+v", rpcErr)
	}
	record, ok := enqueueAny.(sqlite.MemoryPromotionRecord)
	if !ok {
		t.Fatalf("unexpected enqueue result type %T", enqueueAny)
	}

	result, err := store.DB().ExecContext(ctx, `DELETE FROM agent_sessions WHERE workspace_id = ? AND session_id = ?`, workspaceID, sessionB)
	if err != nil {
		t.Fatalf("delete stale session evidence: %v", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		t.Fatalf("expected one deleted stale evidence session, got %d", affected)
	}

	resolveRaw, err := json.Marshal(workspaceMemoryPromotionResolveParams{
		WorkspaceID: workspaceID,
		PromotionID: record.PromotionID,
		Resolution:  "ACCEPTED",
		ResolvedBy:  "developer",
	})
	if err != nil {
		t.Fatalf("marshal resolve params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryPromotionResolve(ctx, resolveRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "memory promotion evidence is stale") {
		t.Fatalf("expected invalid params stale evidence error, got %+v", rpcErr)
	}
}

func TestWorkspaceMemoryPromotionEnqueueSurfacesAdvisoryCoherenceGate(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-promo-coherence"
		agentID     = "agent-handler-memory-promo-coherence"
		sessionID   = "sess-handler-memory-promo-coherence"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Promotion Coherence",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Memory Promotion Coherence Agent",
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
	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		ReportID:      "memres-handler-promo-coherence",
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		SessionID:     sessionID,
		ReportScope:   "SESSION",
		StaleReadRate: 0.20,
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:     "P2",
				ReplicaKind:       "memory_node",
				CoherenceClass:    "A",
				State:             "INVALIDATED",
				CanonicalMemoryID: "memory:handler-promo-candidate",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: "doc-handler-missing", VersionToken: "doc-v1", Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}

	raw, err := json.Marshal(workspaceMemoryPromotionEnqueueParams{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Body:        "Promotion enqueue should surface advisory coherence state on the same RPC response.",
		SessionID:   sessionID,
		SourceKind:  "episode_pack",
		SourceID:    "pack-handler-promo-coherence",
		BasisDigest: "basis-handler-promo-coherence",
		BasisRefs:   []string{"episode_pack:pack-handler-promo-coherence"},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("marshal enqueue params: %v", err)
	}
	result, rpcErr := h.workspaceMemoryPromotionEnqueue(ctx, raw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPromotionEnqueue rpc error: %+v", rpcErr)
	}
	record, ok := result.(sqlite.MemoryPromotionRecord)
	if !ok {
		t.Fatalf("unexpected enqueue result type %T", result)
	}
	if record.CoherenceGate == nil || record.CoherenceGate.AdvisoryAction != "DEFER_ACCEPT" || record.CoherenceGate.CoherenceBand != "DEGRADED" {
		t.Fatalf("expected RPC surface to carry degraded advisory coherence gate, got %+v", record)
	}

	queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Status:      "OPEN",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list memory promotion queue: %v", err)
	}
	if len(queues) != 1 || queues[0].SourceKind != "memory_promotion" || queues[0].SourceID != record.PromotionID || queues[0].Urgency != "HIGH" {
		t.Fatalf("expected advisory coherence gate to elevate mirrored queue urgency, got %+v", queues)
	}
}

func TestWorkspaceMemoryPromotionResolveAcceptedRejectsDeferredCoherenceGate(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-promo-coherence-resolve"
		agentID     = "agent-handler-memory-promo-coherence-resolve"
		sessionID   = "sess-handler-memory-promo-coherence-resolve"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Promotion Resolve Coherence Gate",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Memory Promotion Resolve Coherence Gate Agent",
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

	enqueueRaw, err := json.Marshal(workspaceMemoryPromotionEnqueueParams{
		WorkspaceID: workspaceID,
		MemoryType:  "lesson",
		Body:        "RPC accepted resolve must stop when a deferred coherence gate appears after enqueue.",
		SessionID:   sessionID,
		SourceKind:  "episode_pack",
		SourceID:    "pack-handler-promo-coherence-resolve",
		BasisDigest: "basis-handler-promo-coherence-resolve",
		BasisRefs:   []string{"episode_pack:pack-handler-promo-coherence-resolve"},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("marshal enqueue params: %v", err)
	}
	enqueueAny, rpcErr := h.workspaceMemoryPromotionEnqueue(ctx, enqueueRaw)
	if rpcErr != nil {
		t.Fatalf("enqueue memory promotion via handler: %+v", rpcErr)
	}
	record, ok := enqueueAny.(sqlite.MemoryPromotionRecord)
	if !ok {
		t.Fatalf("expected sqlite.MemoryPromotionRecord, got %T", enqueueAny)
	}
	if record.CoherenceGate != nil {
		t.Fatalf("expected enqueue without coherence gate before degradation, got %+v", record.CoherenceGate)
	}
	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	if _, err := store.ReportMemoryResidency(ctx, sqlite.MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		SessionID:   sessionID,
		ReportScope: "SESSION",
		Replicas: []sqlite.MemoryReplicaStateInput{
			{
				ResidencyTier:     "P2",
				ReplicaKind:       "memory_node",
				CoherenceClass:    "A",
				State:             "INVALIDATED",
				CanonicalMemoryID: "memory:handler-promo-coherence-resolve",
				VersionGuards: []sqlite.MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: "doc-missing-handler-coherence-resolve", VersionToken: "doc-v1", Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}

	resolveRaw, err := json.Marshal(workspaceMemoryPromotionResolveParams{
		WorkspaceID: workspaceID,
		PromotionID: record.PromotionID,
		Resolution:  "ACCEPTED",
		ResolvedBy:  "developer",
	})
	if err != nil {
		t.Fatalf("marshal resolve params: %v", err)
	}
	resolveAny, rpcErr := h.workspaceMemoryPromotionResolve(ctx, resolveRaw)
	if rpcErr == nil {
		t.Fatalf("expected deferred coherence gate rejection, got %+v", resolveAny)
	}
	if rpcErr.Code != errCodeInvalidParams || !strings.Contains(strings.ToLower(rpcErr.Message), "coherence gate") || !strings.Contains(strings.ToLower(rpcErr.Message), "deferred accept") {
		t.Fatalf("expected invalid-params deferred coherence gate rejection, got %+v", rpcErr)
	}

	persisted, err := store.GetMemoryPromotion(ctx, workspaceID, record.PromotionID)
	if err != nil {
		t.Fatalf("get memory promotion after deferred gate reject: %v", err)
	}
	if persisted.State != "PENDING" {
		t.Fatalf("expected promotion to remain pending after deferred gate reject, got %+v", persisted)
	}
	if persisted.CoherenceGate == nil || persisted.CoherenceGate.AdvisoryAction != "DEFER_ACCEPT" || persisted.CoherenceGate.ReadyInvalidationCount != 1 {
		t.Fatalf("expected refreshed deferred coherence gate on pending promotion, got %+v", persisted.CoherenceGate)
	}
	queues, err := store.ListOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Status:      "OPEN",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list mirrored operator queue after deferred gate reject: %v", err)
	}
	if len(queues) != 1 || queues[0].SourceKind != "memory_promotion" || queues[0].SourceID != record.PromotionID {
		t.Fatalf("expected mirrored operator queue to remain open after deferred gate reject, got %+v", queues)
	}
	queue := queues[0]
	if queue.Urgency != "HIGH" {
		t.Fatalf("expected deferred coherence gate to keep mirrored queue urgency elevated, got %+v", queue)
	}
	if !strings.Contains(queue.Details, "Coherence gate: DEFER_ACCEPT (DEGRADED)") ||
		!strings.Contains(queue.Details, "READY_INVALIDATIONS") ||
		!strings.Contains(queue.PayloadJSON, "\"coherence_gate\"") ||
		!strings.Contains(queue.PayloadJSON, "\"advisory_action\":\"DEFER_ACCEPT\"") ||
		!strings.Contains(queue.PayloadJSON, "\"ready_invalidation_count\":1") {
		t.Fatalf("expected mirrored operator queue payload/details to refresh deferred coherence gate, got %+v", queue)
	}
	queueLive := nextEventOfType(t, ch, "workspace.ops.updated")
	queuePersisted := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "operator_queue.updated",
		EntityType:  "operator_queue",
		EntityID:    queue.QueueID,
		Limit:       10,
	})
	assertLiveEventMirrorsRuntimeEvent(t, queueLive, queuePersisted, "workspace.ops.updated")
	var queueEnvelope sqlite.OperatorQueueRecord
	if err := json.Unmarshal([]byte(queueLive.PayloadJSON), &queueEnvelope); err != nil {
		t.Fatalf("decode deferred gate queue live payload: %v", err)
	}
	if queueEnvelope.QueueID != queue.QueueID || queueEnvelope.Urgency != "HIGH" || !strings.Contains(queueEnvelope.Details, "Coherence gate: DEFER_ACCEPT (DEGRADED)") {
		t.Fatalf("expected queue live payload to mirror refreshed deferred gate state, got %+v", queueEnvelope)
	}
}

func TestWorkspaceMemoryPromotionResolveAcceptedRejectsStaleEvidenceAtResolveTime(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-promo-stale-evidence"
		agentID     = "agent-handler-memory-promo-stale-evidence"
		sessionA    = "sess-handler-memory-promo-stale-a"
		sessionB    = "sess-handler-memory-promo-stale-b"
		sessionC    = "sess-handler-memory-promo-stale-c"
		sessionD    = "sess-handler-memory-promo-stale-d"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Promotion Stale Evidence",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Memory Promotion Stale Evidence Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	for _, sessionID := range []string{sessionA, sessionB, sessionC, sessionD} {
		if err := store.CreateAgentSession(ctx, sqlite.AgentSessionCreateInput{
			SessionID:   sessionID,
			AgentID:     agentID,
			WorkspaceID: workspaceID,
			StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		}); err != nil {
			t.Fatalf("create session %s: %v", sessionID, err)
		}
	}

	enqueueRaw, err := json.Marshal(workspaceMemoryPromotionEnqueueParams{
		WorkspaceID: workspaceID,
		MemoryType:  "self_model",
		Body:        "Resolve should recheck stale identity evidence before accepting promotion.",
		SessionID:   sessionA,
		SourceKind:  "memory_packet_shell",
		SourceID:    "shell-packet-handler-stale-evidence",
		BasisDigest: "basis-handler-stale-evidence",
		BasisRefs:   []string{"session:" + sessionA, "session:" + sessionB, "session:" + sessionC, "session:" + sessionD},
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("marshal enqueue params: %v", err)
	}
	enqueueAny, rpcErr := h.workspaceMemoryPromotionEnqueue(ctx, enqueueRaw)
	if rpcErr != nil {
		t.Fatalf("workspaceMemoryPromotionEnqueue rpc error: %+v", rpcErr)
	}
	record := enqueueAny.(sqlite.MemoryPromotionRecord)

	if _, err := store.DB().ExecContext(ctx, `DELETE FROM agent_sessions WHERE workspace_id = ? AND session_id = ?`, workspaceID, sessionC); err != nil {
		t.Fatalf("delete stale evidence session: %v", err)
	}

	resolveRaw, err := json.Marshal(workspaceMemoryPromotionResolveParams{
		WorkspaceID: workspaceID,
		PromotionID: record.PromotionID,
		Resolution:  "ACCEPTED",
		ResolvedBy:  "developer",
	})
	if err != nil {
		t.Fatalf("marshal resolve params: %v", err)
	}
	resolveAny, rpcErr := h.workspaceMemoryPromotionResolve(ctx, resolveRaw)
	if rpcErr == nil {
		t.Fatalf("expected stale evidence acceptance to fail, got %+v", resolveAny)
	}
	if rpcErr.Code != errCodeInvalidParams || (!strings.Contains(strings.ToLower(rpcErr.Message), "invalid evidence") && !strings.Contains(strings.ToLower(rpcErr.Message), "not found in workspace")) {
		t.Fatalf("expected invalid-params stale evidence rejection, got %+v", rpcErr)
	}

	persisted, err := store.GetMemoryPromotion(ctx, workspaceID, record.PromotionID)
	if err != nil {
		t.Fatalf("get memory promotion after stale rpc resolve: %v", err)
	}
	if persisted.State != "PENDING" || persisted.AppliedID != "" {
		t.Fatalf("expected promotion to remain pending after stale rpc resolve rejection, got %+v", persisted)
	}
}

func TestWorkspaceMemoryPromotionValidationErrorsAreInvalidParams(t *testing.T) {
	t.Parallel()

	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-handler-memory-promo-invalid"
		agentID     = "agent-handler-memory-promo-invalid"
		sessionID   = "sess-handler-memory-promo-invalid"
	)

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Memory Promotion Invalid",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Handler Memory Promotion Invalid Agent",
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

	invalidEnqueueRaw := []byte(`{
		"workspace_id":"ws-handler-memory-promo-invalid",
		"body":"body",
		"source_kind":"episode_pack",
		"source_id":"pack-1",
		"proposed_by":"agent-handler-memory-promo-invalid"
	}`)
	if _, rpcErr := h.workspaceMemoryPromotionEnqueue(ctx, invalidEnqueueRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "basis_digest") {
		t.Fatalf("expected invalid params for missing basis_digest, got %+v", rpcErr)
	}

	enqueueRaw, err := json.Marshal(workspaceMemoryPromotionEnqueueParams{
		PromotionID: "promotion-handler-invalid",
		WorkspaceID: workspaceID,
		Body:        "Queue candidate once.",
		SessionID:   sessionID,
		SourceKind:  "episode_pack",
		SourceID:    "pack-1",
		BasisDigest: "basis-handler-invalid",
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("marshal enqueue params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryPromotionEnqueue(ctx, enqueueRaw); rpcErr != nil {
		t.Fatalf("enqueue promotion for invalid tests: %+v", rpcErr)
	}

	mismatchReplayRaw, err := json.Marshal(workspaceMemoryPromotionEnqueueParams{
		PromotionID: "promotion-handler-invalid",
		WorkspaceID: workspaceID,
		Body:        "Different body.",
		SessionID:   sessionID,
		SourceKind:  "episode_pack",
		SourceID:    "pack-2",
		BasisDigest: "basis-handler-invalid-2",
		ProposedBy:  agentID,
	})
	if err != nil {
		t.Fatalf("marshal replay mismatch params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryPromotionEnqueue(ctx, mismatchReplayRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams || !strings.Contains(rpcErr.Message, "promotion_id replay payload does not match existing candidate") {
		t.Fatalf("expected invalid params for promotion replay mismatch, got %+v", rpcErr)
	}

	resolveRaw, err := json.Marshal(workspaceMemoryPromotionResolveParams{
		WorkspaceID: workspaceID,
		PromotionID: "promotion-handler-invalid",
		Resolution:  "NOT_A_STATE",
		ResolvedBy:  "developer",
	})
	if err != nil {
		t.Fatalf("marshal invalid resolve params: %v", err)
	}
	if _, rpcErr := h.workspaceMemoryPromotionResolve(ctx, resolveRaw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for invalid resolution, got %+v", rpcErr)
	}
}
