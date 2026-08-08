package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestReportMemoryResidencyEnqueuesImmediateInvalidationForStaleGuard(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-stale"
		agentID     = "agent-memory-invalidation-stale"
		docKey      = "handoff"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Stale",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Invalidation Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Handoff",
		Content:     "# Handoff\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Handoff",
		Content:     "# Handoff\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:stale-doc",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}

	items, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll invalidations: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one invalidation, got %+v", items)
	}
	if items[0].RefKind != "workspace_doc" || items[0].RefID != docKey || items[0].Reason != "VERSION_CHANGED" {
		t.Fatalf("unexpected invalidation %+v", items[0])
	}
}

func TestWorkspaceDocUpdateCollapsesRootSegmentInvalidationWhenSourceGuardExists(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-doc"
		agentID     = "agent-memory-invalidation-doc"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Doc",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Invalidation Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	rootSegmentRef := buildWorkspaceDocSegmentRef(workspaceID, docKey, "root")
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    "memres-doc-latest",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:     "P2",
				ReplicaKind:       "memory_node",
				CoherenceClass:    "A",
				State:             "CURRENT",
				CanonicalMemoryID: "memnode:test",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
					{RefKind: "segment_ref", RefID: rootSegmentRef, VersionToken: memoryGraphSegmentVersionToken(rootSegmentRef, docV1.SHA), Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	items, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll invalidations: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one invalidation after doc update, got %+v", items)
	}
	if !hasInvalidationRef(items, "workspace_doc", docKey) {
		t.Fatalf("expected workspace_doc invalidation, got %+v", items)
	}
	if hasInvalidationRef(items, "segment_ref", rootSegmentRef) {
		t.Fatalf("did not expect root segment invalidation alias, got %+v", items)
	}
}

func TestKnowledgeClaimUpdateEnqueuesInvalidation(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-claim"
		agentID     = "agent-memory-invalidation-claim"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Claim",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Claim Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "runbook ready",
		Body:        "The runbook is ready for operator review.",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    "memres-claim",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "claim_cache",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "claim-cache",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "knowledge_claim", RefID: claim.ClaimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report residency: %v", err)
	}
	if _, err := store.ConfirmKnowledgeClaim(ctx, KnowledgeClaimLifecycleInput{
		WorkspaceID: workspaceID,
		ClaimID:     claim.ClaimID,
		ActorID:     "developer",
	}); err != nil {
		t.Fatalf("confirm knowledge claim: %v", err)
	}

	items, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll invalidations: %v", err)
	}
	if len(items) != 1 || items[0].RefKind != "knowledge_claim" || items[0].RefID != claim.ClaimID {
		t.Fatalf("unexpected claim invalidations %+v", items)
	}
}

func TestMemoryInvalidationSkipsNonEagerReplicaClasses(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-class-gate"
		agentID     = "agent-memory-invalidation-class-gate"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Class Gate",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Invalidation Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    "memres-class-gate",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P3",
				ReplicaKind:    "memory_node",
				CoherenceClass: "B",
				State:          "CURRENT",
				CacheKey:       "cluster-cache",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	items, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll invalidations: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no invalidations for non-eager replica class, got %+v", items)
	}
}

func TestMemoryInvalidationUsesOnlyLatestResidencyReportPerAgentScope(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-latest"
		agentID     = "agent-memory-invalidation-latest"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Latest",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Memory Invalidation Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	for _, reportID := range []string{"memres-latest-r1", "memres-latest-r2"} {
		if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
			ReportID:    reportID,
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			ReportScope: "AGENT",
			Replicas: []MemoryReplicaStateInput{
				{
					ResidencyTier:  "P2",
					ReplicaKind:    "memory_node",
					CoherenceClass: "A",
					State:          "CURRENT",
					CacheKey:       "packet:" + reportID,
					VersionGuards: []MemoryResidencyVersionGuard{
						{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
					},
				},
			},
		}); err != nil {
			t.Fatalf("report memory residency %s: %v", reportID, err)
		}
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	items, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll invalidations: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one invalidation for latest report only, got %+v", items)
	}
	if items[0].ReportID != "memres-latest-r2" || items[0].CacheKey != "packet:memres-latest-r2" {
		t.Fatalf("expected invalidation to target latest report, got %+v", items[0])
	}
}

func TestPollMemoryInvalidationsMarkDeliveredSetsLeaseAndImmediateRepollSkipsItem(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-lease", "agent-memory-invalidation-lease", "lease-doc")

	items, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   item.WorkspaceID,
		AgentID:       item.AgentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("mark delivered invalidations: %v", err)
	}
	if len(items) != 1 || items[0].InvalidationID != item.InvalidationID {
		t.Fatalf("expected delivered invalidation, got %+v", items)
	}

	leaseExpiresAt := getMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "lease_expires_at")
	if leaseExpiresAt == "" {
		t.Fatalf("expected lease_expires_at to be set for delivered invalidation %s", item.InvalidationID)
	}

	repoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: item.WorkspaceID,
		AgentID:     item.AgentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("repoll invalidations: %v", err)
	}
	if len(repoll) != 0 {
		t.Fatalf("expected delivered lease to suppress immediate repoll, got %+v", repoll)
	}
}

func TestPollMemoryInvalidationsLeaseExpiryMakesItemPollableAgain(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-lease-expiry", "agent-memory-invalidation-lease-expiry", "lease-expiry-doc")

	items, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   item.WorkspaceID,
		AgentID:       item.AgentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("mark delivered invalidations: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected delivered invalidation, got %+v", items)
	}

	expiredAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	setMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "lease_expires_at", expiredAt)

	repoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: item.WorkspaceID,
		AgentID:     item.AgentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("repoll after lease expiry: %v", err)
	}
	if len(repoll) != 1 || repoll[0].InvalidationID != item.InvalidationID {
		t.Fatalf("expected expired lease invalidation to become pollable again, got %+v", repoll)
	}
}

func TestAckMemoryInvalidationsRequiresActiveDeliveryLease(t *testing.T) {
	t.Run("before delivery", func(t *testing.T) {
		store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-ack-guard-undelivered", "agent-memory-invalidation-ack-guard-undelivered", "ack-guard-undelivered-doc")

		acked, events, err := store.AckMemoryInvalidationsWithEvents(ctx, MemoryInvalidationAckInput{
			WorkspaceID:     item.WorkspaceID,
			AgentID:         item.AgentID,
			InvalidationIDs: []string{item.InvalidationID},
		})
		if err != nil {
			t.Fatalf("ack invalidation before delivery: %v", err)
		}
		if len(acked) != 0 || len(events) != 0 {
			t.Fatalf("expected undelivered invalidation ack to be ignored, got items=%+v events=%+v", acked, events)
		}

		stored, err := store.GetMemoryInvalidation(ctx, item.WorkspaceID, item.AgentID, item.InvalidationID)
		if err != nil {
			t.Fatalf("get undelivered invalidation after ack no-op: %v", err)
		}
		if stored.State != "OPEN" || stored.DeliveredAt != "" || stored.AcknowledgedAt != "" {
			t.Fatalf("expected undelivered invalidation to remain open after ack no-op, got %+v", stored)
		}
	})

	t.Run("after lease expiry", func(t *testing.T) {
		store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-ack-guard-expired", "agent-memory-invalidation-ack-guard-expired", "ack-guard-expired-doc")

		delivered, _, err := store.PollMemoryInvalidationsWithEvents(ctx, MemoryInvalidationPollFilter{
			WorkspaceID:   item.WorkspaceID,
			AgentID:       item.AgentID,
			Limit:         10,
			MarkDelivered: true,
		})
		if err != nil {
			t.Fatalf("deliver invalidation before ack expiry check: %v", err)
		}
		if len(delivered) != 1 {
			t.Fatalf("expected delivered invalidation before ack expiry check, got %+v", delivered)
		}
		setMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "lease_expires_at", time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano))

		acked, events, err := store.AckMemoryInvalidationsWithEvents(ctx, MemoryInvalidationAckInput{
			WorkspaceID:     item.WorkspaceID,
			AgentID:         item.AgentID,
			InvalidationIDs: []string{item.InvalidationID},
		})
		if err != nil {
			t.Fatalf("ack invalidation after lease expiry: %v", err)
		}
		if len(acked) != 0 || len(events) != 0 {
			t.Fatalf("expected expired invalidation lease to reject ack, got items=%+v events=%+v", acked, events)
		}

		stored, err := store.GetMemoryInvalidation(ctx, item.WorkspaceID, item.AgentID, item.InvalidationID)
		if err != nil {
			t.Fatalf("get expired invalidation after ack no-op: %v", err)
		}
		if stored.State != "OPEN" || stored.AcknowledgedAt != "" {
			t.Fatalf("expected expired invalidation lease to reject ack without state change, got %+v", stored)
		}
	})

	t.Run("during active delivery lease", func(t *testing.T) {
		store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-ack-guard-active", "agent-memory-invalidation-ack-guard-active", "ack-guard-active-doc")

		delivered, _, err := store.PollMemoryInvalidationsWithEvents(ctx, MemoryInvalidationPollFilter{
			WorkspaceID:   item.WorkspaceID,
			AgentID:       item.AgentID,
			Limit:         10,
			MarkDelivered: true,
		})
		if err != nil {
			t.Fatalf("deliver invalidation before ack: %v", err)
		}
		if len(delivered) != 1 || delivered[0].LeaseExpiresAt == "" {
			t.Fatalf("expected active delivery lease before ack, got %+v", delivered)
		}

		acked, events, err := store.AckMemoryInvalidationsWithEvents(ctx, MemoryInvalidationAckInput{
			WorkspaceID:     item.WorkspaceID,
			AgentID:         item.AgentID,
			InvalidationIDs: []string{item.InvalidationID},
		})
		if err != nil {
			t.Fatalf("ack invalidation during active lease: %v", err)
		}
		if len(acked) != 1 || len(events) != 1 || acked[0].State != "ACKED" {
			t.Fatalf("expected ack under active delivery lease, got items=%+v events=%+v", acked, events)
		}
	})
}

func TestFailMemoryInvalidationsBelowThresholdSetsNextDeliveryAtAndImmediatePollSkipsItem(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-backoff", "agent-memory-invalidation-backoff", "backoff-doc")

	delivered, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   item.WorkspaceID,
		AgentID:       item.AgentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("mark delivered invalidations: %v", err)
	}
	if len(delivered) != 1 {
		t.Fatalf("expected delivered invalidation, got %+v", delivered)
	}
	assertMemoryInvalidationTemporalContract(t, delivered[0].TemporalContracts, "lease_expiry", "wall_clock", "LIVE")

	failed, err := store.FailMemoryInvalidations(ctx, MemoryInvalidationFailInput{
		WorkspaceID:     item.WorkspaceID,
		AgentID:         item.AgentID,
		InvalidationIDs: []string{item.InvalidationID},
		FailureReason:   "APPLY_FAILED",
	})
	if err != nil {
		t.Fatalf("fail invalidation below threshold: %v", err)
	}
	if len(failed) != 1 || failed[0].State != "OPEN" || failed[0].FailureCount != 1 {
		t.Fatalf("expected retryable invalidation after first failure, got %+v", failed)
	}
	assertMemoryInvalidationTemporalContract(t, failed[0].TemporalContracts, "next_delivery_at", "wall_clock", "PENDING")

	nextDeliveryAt := getMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "next_delivery_at")
	if nextDeliveryAt == "" {
		t.Fatalf("expected next_delivery_at to be set after failure for invalidation %s", item.InvalidationID)
	}

	repoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: item.WorkspaceID,
		AgentID:     item.AgentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("repoll after failure backoff: %v", err)
	}
	if len(repoll) != 0 {
		t.Fatalf("expected backoff to suppress immediate repoll, got %+v", repoll)
	}
}

func TestFailMemoryInvalidationsBackoffExpiryMakesItemPollableAgain(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-backoff-expiry", "agent-memory-invalidation-backoff-expiry", "backoff-expiry-doc")

	delivered, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   item.WorkspaceID,
		AgentID:       item.AgentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("mark delivered invalidations: %v", err)
	}
	if len(delivered) != 1 {
		t.Fatalf("expected delivered invalidation, got %+v", delivered)
	}
	if _, err := store.FailMemoryInvalidations(ctx, MemoryInvalidationFailInput{
		WorkspaceID:     item.WorkspaceID,
		AgentID:         item.AgentID,
		InvalidationIDs: []string{item.InvalidationID},
		FailureReason:   "APPLY_FAILED",
	}); err != nil {
		t.Fatalf("fail invalidation below threshold: %v", err)
	}

	expiredAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	setMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "next_delivery_at", expiredAt)

	repoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: item.WorkspaceID,
		AgentID:     item.AgentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("repoll after backoff expiry: %v", err)
	}
	if len(repoll) != 1 || repoll[0].InvalidationID != item.InvalidationID {
		t.Fatalf("expected expired backoff invalidation to become pollable again, got %+v", repoll)
	}
}

func TestFailMemoryInvalidationsRequiresActiveDeliveryLease(t *testing.T) {
	t.Run("before delivery", func(t *testing.T) {
		store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-fail-guard-undelivered", "agent-memory-invalidation-fail-guard-undelivered", "fail-guard-undelivered-doc")

		failed, events, err := store.FailMemoryInvalidationsWithEvents(ctx, MemoryInvalidationFailInput{
			WorkspaceID:     item.WorkspaceID,
			AgentID:         item.AgentID,
			InvalidationIDs: []string{item.InvalidationID},
			FailureReason:   "APPLY_FAILED",
		})
		if err != nil {
			t.Fatalf("fail invalidation before delivery: %v", err)
		}
		if len(failed) != 0 || len(events) != 0 {
			t.Fatalf("expected undelivered invalidation fail to be ignored, got items=%+v events=%+v", failed, events)
		}

		stored, err := store.GetMemoryInvalidation(ctx, item.WorkspaceID, item.AgentID, item.InvalidationID)
		if err != nil {
			t.Fatalf("get undelivered invalidation after fail no-op: %v", err)
		}
		if stored.State != "OPEN" || stored.FailureCount != 0 || stored.DeliveredAt != "" {
			t.Fatalf("expected undelivered invalidation to remain untouched after fail no-op, got %+v", stored)
		}
	})

	t.Run("after lease expiry", func(t *testing.T) {
		store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-fail-guard-expired", "agent-memory-invalidation-fail-guard-expired", "fail-guard-expired-doc")

		delivered, _, err := store.PollMemoryInvalidationsWithEvents(ctx, MemoryInvalidationPollFilter{
			WorkspaceID:   item.WorkspaceID,
			AgentID:       item.AgentID,
			Limit:         10,
			MarkDelivered: true,
		})
		if err != nil {
			t.Fatalf("deliver invalidation before fail expiry check: %v", err)
		}
		if len(delivered) != 1 {
			t.Fatalf("expected delivered invalidation before fail expiry check, got %+v", delivered)
		}
		setMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "lease_expires_at", time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano))

		failed, events, err := store.FailMemoryInvalidationsWithEvents(ctx, MemoryInvalidationFailInput{
			WorkspaceID:     item.WorkspaceID,
			AgentID:         item.AgentID,
			InvalidationIDs: []string{item.InvalidationID},
			FailureReason:   "APPLY_FAILED",
		})
		if err != nil {
			t.Fatalf("fail invalidation after lease expiry: %v", err)
		}
		if len(failed) != 0 || len(events) != 0 {
			t.Fatalf("expected expired invalidation lease to reject fail, got items=%+v events=%+v", failed, events)
		}

		stored, err := store.GetMemoryInvalidation(ctx, item.WorkspaceID, item.AgentID, item.InvalidationID)
		if err != nil {
			t.Fatalf("get expired invalidation after fail no-op: %v", err)
		}
		if stored.State != "OPEN" || stored.FailureCount != 0 {
			t.Fatalf("expected expired invalidation lease to reject fail without state change, got %+v", stored)
		}
	})

	t.Run("during active delivery lease", func(t *testing.T) {
		store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-fail-guard-active", "agent-memory-invalidation-fail-guard-active", "fail-guard-active-doc")

		delivered, _, err := store.PollMemoryInvalidationsWithEvents(ctx, MemoryInvalidationPollFilter{
			WorkspaceID:   item.WorkspaceID,
			AgentID:       item.AgentID,
			Limit:         10,
			MarkDelivered: true,
		})
		if err != nil {
			t.Fatalf("deliver invalidation before fail: %v", err)
		}
		if len(delivered) != 1 || delivered[0].LeaseExpiresAt == "" {
			t.Fatalf("expected active delivery lease before fail, got %+v", delivered)
		}

		failed, events, err := store.FailMemoryInvalidationsWithEvents(ctx, MemoryInvalidationFailInput{
			WorkspaceID:     item.WorkspaceID,
			AgentID:         item.AgentID,
			InvalidationIDs: []string{item.InvalidationID},
			FailureReason:   "APPLY_FAILED",
		})
		if err != nil {
			t.Fatalf("fail invalidation during active lease: %v", err)
		}
		if len(failed) != 1 || len(events) != 1 || failed[0].FailureCount != 1 {
			t.Fatalf("expected fail under active delivery lease, got items=%+v events=%+v", failed, events)
		}
	})
}

func TestPollMemoryInvalidationsUsesWorkspaceReferenceTimeForScheduledItems(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-reference", "agent-memory-invalidation-reference", "reference-doc")

	pendingAt := time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339Nano)
	anchorAt := time.Now().UTC().Add(20 * time.Minute).Format(time.RFC3339Nano)
	setMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "lease_expires_at", "")
	setMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "next_delivery_at", pendingAt)

	pending, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: item.WorkspaceID,
		AgentID:     item.AgentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll memory invalidations before reference anchor: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected future delivery watermark to stay unpollable before reference anchor, got %+v", pending)
	}

	setWorkspaceControlEpochAnchor(t, store, item.WorkspaceID, anchorAt)

	ready, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: item.WorkspaceID,
		AgentID:     item.AgentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll memory invalidations after reference anchor: %v", err)
	}
	if len(ready) != 1 || ready[0].InvalidationID != item.InvalidationID {
		t.Fatalf("expected workspace reference time to make invalidation pollable, got %+v", ready)
	}
}

func TestPollMemoryInvalidationsMarksDeliveredAppendsEventAndUpdatesCursor(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-delivery"
		agentID     = "agent-memory-invalidation-delivery"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Delivery",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Delivery Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:delivery",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	items, events, err := store.PollMemoryInvalidationsWithEvents(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("poll invalidations: %v", err)
	}
	if len(items) != 1 || !items[0].DeliveredNow || items[0].DeliveredAt == "" {
		t.Fatalf("expected delivered invalidation with checkpoint marker, got %+v", items)
	}
	if items[0].TriggerCause != "workspace_doc.upserted" || len(items[0].DependencyRevisionVector) != 1 || items[0].DependencyRevisionVector[0].RefKind != "workspace_doc" || items[0].DependencyRevisionVector[0].RefID != docKey {
		t.Fatalf("expected delivered invalidation to surface dependency revision vector and trigger cause, got %+v", items[0])
	}
	if len(events) != 1 {
		t.Fatalf("expected one returned delivery runtime event, got %+v", events)
	}
	if items[0].TimeAuthority.WorkspaceID != workspaceID || items[0].TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected delivered invalidation time authority, got %+v", items[0].TimeAuthority)
	}

	cursor, err := store.GetMemoryInvalidationCursor(ctx, workspaceID, agentID, "")
	if err != nil {
		t.Fatalf("get invalidation cursor: %v", err)
	}
	if cursor.LastPollCount != 1 || cursor.LastDeliveredInvalidationID != items[0].InvalidationID || cursor.LastDeliveredAt == "" {
		t.Fatalf("unexpected invalidation cursor %+v", cursor)
	}
	if cursor.TimeAuthority.WorkspaceID != workspaceID || cursor.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected invalidation cursor time authority, got %+v", cursor.TimeAuthority)
	}

	listedEvents, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_delivered",
		EntityType:  "memory_invalidation",
		EntityID:    items[0].InvalidationID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(listedEvents) != 1 {
		t.Fatalf("expected one delivery runtime event, got %+v", listedEvents)
	}
	assertSameRuntimeEventRecord(t, events[0], listedEvents[0])
	payload := decodeMemoryInvalidationPayloadMap(t, listedEvents[0].PayloadJSON)
	vector := payload["dependency_revision_vector"].([]any)
	if len(vector) != 1 {
		t.Fatalf("expected one dependency_revision_vector entry in delivery payload, got %+v", payload)
	}
	first := vector[0].(map[string]any)
	if first["ref_kind"] != "workspace_doc" || first["ref_id"] != docKey || payload["trigger_cause"] != "workspace_doc.upserted" {
		t.Fatalf("expected typed dependency vector and trigger cause in delivery payload, got %+v", payload)
	}
}

func TestAckMemoryInvalidationsUpdatesCursorAndListIncludesAcked(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-ack-cursor"
		agentID     = "agent-memory-invalidation-ack-cursor"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Ack Cursor",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Ack Cursor Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report memory residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	items, deliveredEvents, err := store.PollMemoryInvalidationsWithEvents(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("poll invalidations: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one invalidation, got %+v", items)
	}
	if len(deliveredEvents) != 1 {
		t.Fatalf("expected one returned delivery runtime event, got %+v", deliveredEvents)
	}
	acked, ackEvents, err := store.AckMemoryInvalidationsWithEvents(ctx, MemoryInvalidationAckInput{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{items[0].InvalidationID},
	})
	if err != nil {
		t.Fatalf("ack invalidations: %v", err)
	}
	if len(acked) != 1 || acked[0].State != "ACKED" {
		t.Fatalf("unexpected ack payload %+v", acked)
	}
	if len(ackEvents) != 1 {
		t.Fatalf("expected one returned ack runtime event, got %+v", ackEvents)
	}
	if acked[0].TimeAuthority.WorkspaceID != workspaceID || acked[0].TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected acked invalidation time authority, got %+v", acked[0].TimeAuthority)
	}

	cursor, err := store.GetMemoryInvalidationCursor(ctx, workspaceID, agentID, "")
	if err != nil {
		t.Fatalf("get invalidation cursor: %v", err)
	}
	if cursor.LastAcknowledgedInvalidationID != items[0].InvalidationID || cursor.LastAcknowledgedAt == "" {
		t.Fatalf("expected cursor ack update, got %+v", cursor)
	}

	allItems, err := store.ListMemoryInvalidations(ctx, MemoryInvalidationListFilter{
		WorkspaceID:  workspaceID,
		AgentID:      agentID,
		IncludeAcked: true,
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("list memory invalidations: %v", err)
	}
	if len(allItems) != 1 || allItems[0].State != "ACKED" {
		t.Fatalf("expected acked invalidation in list, got %+v", allItems)
	}
	if allItems[0].TimeAuthority.WorkspaceID != workspaceID || allItems[0].TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected listed invalidation time authority, got %+v", allItems[0].TimeAuthority)
	}

	stored, err := store.GetMemoryInvalidation(ctx, workspaceID, agentID, items[0].InvalidationID)
	if err != nil {
		t.Fatalf("get memory invalidation: %v", err)
	}
	if stored.State != "ACKED" || stored.AcknowledgedAt == "" {
		t.Fatalf("expected stored acked invalidation, got %+v", stored)
	}
	if stored.TimeAuthority.WorkspaceID != workspaceID || stored.TimeAuthority.ReferenceAt == "" {
		t.Fatalf("expected stored invalidation time authority, got %+v", stored.TimeAuthority)
	}
	listedEvents, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_acked",
		EntityType:  "memory_invalidation",
		EntityID:    items[0].InvalidationID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list ack runtime events: %v", err)
	}
	if len(listedEvents) != 1 {
		t.Fatalf("expected one ack runtime event, got %+v", listedEvents)
	}
	assertSameRuntimeEventRecord(t, ackEvents[0], listedEvents[0])
}

func TestAckMemoryInvalidationsRequiresDeliveredActiveLease(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-ack-guard", "agent-memory-invalidation-ack-guard", "ack-guard-doc")

	acked, events, err := store.AckMemoryInvalidationsWithEvents(ctx, MemoryInvalidationAckInput{
		WorkspaceID:     item.WorkspaceID,
		AgentID:         item.AgentID,
		InvalidationIDs: []string{item.InvalidationID},
	})
	if err != nil {
		t.Fatalf("ack invalidation without delivery: %v", err)
	}
	if len(acked) != 0 || len(events) != 0 {
		t.Fatalf("expected undelivered invalidation ack to be ignored, got items=%+v events=%+v", acked, events)
	}
	if state := getMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "state"); state != "OPEN" {
		t.Fatalf("expected invalidation to remain OPEN without delivery lease, got %s", state)
	}
	if acknowledgedAt := getMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "acknowledged_at"); acknowledgedAt != "" {
		t.Fatalf("expected no acknowledged_at without delivery lease, got %s", acknowledgedAt)
	}
}

func TestAckMemoryInvalidationsLeaseExpirySkipsMutation(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-ack-expired", "agent-memory-invalidation-ack-expired", "ack-expired-doc")

	delivered, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   item.WorkspaceID,
		AgentID:       item.AgentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("deliver invalidation: %v", err)
	}
	if len(delivered) != 1 {
		t.Fatalf("expected delivered invalidation, got %+v", delivered)
	}

	expiredAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	setMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "lease_expires_at", expiredAt)

	acked, events, err := store.AckMemoryInvalidationsWithEvents(ctx, MemoryInvalidationAckInput{
		WorkspaceID:     item.WorkspaceID,
		AgentID:         item.AgentID,
		InvalidationIDs: []string{item.InvalidationID},
	})
	if err != nil {
		t.Fatalf("ack invalidation after lease expiry: %v", err)
	}
	if len(acked) != 0 || len(events) != 0 {
		t.Fatalf("expected expired lease ack to be ignored, got items=%+v events=%+v", acked, events)
	}
	if state := getMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "state"); state != "OPEN" {
		t.Fatalf("expected invalidation to remain OPEN after expired lease ack, got %s", state)
	}
}

func TestFailMemoryInvalidationsDeadLettersAfterThresholdAndListCanIncludeDeadLetter(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-dead-letter"
		agentID     = "agent-memory-invalidation-dead-letter"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Dead Letter",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Dead Letter Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	items, _, err := store.PollMemoryInvalidationsWithEvents(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("poll invalidations: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one invalidation, got %+v", items)
	}

	for attempt := 1; attempt <= memoryInvalidationDeadLetterThreshold; attempt++ {
		failed, failEvents, err := store.FailMemoryInvalidationsWithEvents(ctx, MemoryInvalidationFailInput{
			WorkspaceID:     workspaceID,
			AgentID:         agentID,
			InvalidationIDs: []string{items[0].InvalidationID},
			FailureReason:   "APPLY_FAILED",
		})
		if err != nil {
			t.Fatalf("fail invalidation attempt %d: %v", attempt, err)
		}
		if len(failed) != 1 {
			t.Fatalf("expected one failed invalidation on attempt %d, got %+v", attempt, failed)
		}
		if len(failEvents) != 1 {
			t.Fatalf("expected one returned failure runtime event on attempt %d, got %+v", attempt, failEvents)
		}
		if failed[0].FailureCount != attempt {
			t.Fatalf("expected failure_count=%d, got %+v", attempt, failed[0])
		}
		expectedEventType := "memory.invalidation_failed"
		if attempt < memoryInvalidationDeadLetterThreshold {
			if failed[0].State != "OPEN" || failed[0].DeadLetteredAt != "" {
				t.Fatalf("expected open invalidation before threshold, got %+v", failed[0])
			}
		} else {
			if failed[0].State != "DEAD_LETTER" || failed[0].DeadLetteredAt == "" {
				t.Fatalf("expected dead-letter invalidation at threshold, got %+v", failed[0])
			}
			expectedEventType = "memory.invalidation_dead_lettered"
		}
		listedEvents, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
			WorkspaceID: workspaceID,
			EventType:   expectedEventType,
			EntityType:  "memory_invalidation",
			EntityID:    items[0].InvalidationID,
			Limit:       1,
		})
		if err != nil {
			t.Fatalf("list failure runtime events on attempt %d: %v", attempt, err)
		}
		if len(listedEvents) != 1 {
			t.Fatalf("expected one failure runtime event on attempt %d, got %+v", attempt, listedEvents)
		}
		assertSameRuntimeEventRecord(t, failEvents[0], listedEvents[0])
		if attempt < memoryInvalidationDeadLetterThreshold {
			redeliverMemoryInvalidationForTest(t, store, ctx, workspaceID, agentID, items[0].InvalidationID)
		}
	}

	pending, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll after dead-letter: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pollable invalidations after dead-letter, got %+v", pending)
	}

	listWithoutDeadLetter, err := store.ListMemoryInvalidations(ctx, MemoryInvalidationListFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list without dead-letter: %v", err)
	}
	if len(listWithoutDeadLetter) != 0 {
		t.Fatalf("expected dead-letter item to stay hidden by default, got %+v", listWithoutDeadLetter)
	}

	listWithDeadLetter, err := store.ListMemoryInvalidations(ctx, MemoryInvalidationListFilter{
		WorkspaceID:       workspaceID,
		AgentID:           agentID,
		IncludeDeadLetter: true,
		Limit:             10,
	})
	if err != nil {
		t.Fatalf("list with dead-letter: %v", err)
	}
	if len(listWithDeadLetter) != 1 || listWithDeadLetter[0].State != "DEAD_LETTER" {
		t.Fatalf("expected one dead-letter item in list, got %+v", listWithDeadLetter)
	}

	acked, err := store.AckMemoryInvalidations(ctx, MemoryInvalidationAckInput{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{items[0].InvalidationID},
	})
	if err != nil {
		t.Fatalf("ack dead-letter invalidation: %v", err)
	}
	if len(acked) != 0 {
		t.Fatalf("expected ack on dead-letter item to be a no-op, got %+v", acked)
	}
}

func TestFailMemoryInvalidationsRequiresDeliveredActiveLease(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-fail-guard", "agent-memory-invalidation-fail-guard", "fail-guard-doc")

	failed, events, err := store.FailMemoryInvalidationsWithEvents(ctx, MemoryInvalidationFailInput{
		WorkspaceID:     item.WorkspaceID,
		AgentID:         item.AgentID,
		InvalidationIDs: []string{item.InvalidationID},
		FailureReason:   "APPLY_FAILED",
	})
	if err != nil {
		t.Fatalf("fail invalidation without delivery: %v", err)
	}
	if len(failed) != 0 || len(events) != 0 {
		t.Fatalf("expected undelivered invalidation fail to be ignored, got items=%+v events=%+v", failed, events)
	}
	if state := getMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "state"); state != "OPEN" {
		t.Fatalf("expected invalidation to remain OPEN without delivery lease, got %s", state)
	}
	if failureReason := getMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "last_failure_reason"); failureReason != "" {
		t.Fatalf("expected no failure reason without delivery lease, got %s", failureReason)
	}
}

func TestFailMemoryInvalidationsLeaseExpirySkipsMutation(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-fail-expired", "agent-memory-invalidation-fail-expired", "fail-expired-doc")

	delivered, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   item.WorkspaceID,
		AgentID:       item.AgentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("deliver invalidation: %v", err)
	}
	if len(delivered) != 1 {
		t.Fatalf("expected delivered invalidation, got %+v", delivered)
	}

	expiredAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	setMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "lease_expires_at", expiredAt)

	failed, events, err := store.FailMemoryInvalidationsWithEvents(ctx, MemoryInvalidationFailInput{
		WorkspaceID:     item.WorkspaceID,
		AgentID:         item.AgentID,
		InvalidationIDs: []string{item.InvalidationID},
		FailureReason:   "APPLY_FAILED",
	})
	if err != nil {
		t.Fatalf("fail invalidation after lease expiry: %v", err)
	}
	if len(failed) != 0 || len(events) != 0 {
		t.Fatalf("expected expired lease fail to be ignored, got items=%+v events=%+v", failed, events)
	}
	if state := getMemoryInvalidationQueueStringColumn(t, store, item.InvalidationID, "state"); state != "OPEN" {
		t.Fatalf("expected invalidation to remain OPEN after expired lease fail, got %s", state)
	}
}

func TestMemoryInvalidationCanBeReenqueuedAfterDeadLetter(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-dead-letter-reenqueue"
		agentID     = "agent-memory-invalidation-dead-letter-reenqueue"
		reportID    = "memres-dead-letter-reenqueue"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Dead Letter Reenqueue",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Dead Letter Reenqueue Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:runbook",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	firstPoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("poll initial invalidation: %v", err)
	}
	if len(firstPoll) != 1 {
		t.Fatalf("expected initial invalidation, got %+v", firstPoll)
	}
	for attempt := 0; attempt < memoryInvalidationDeadLetterThreshold; attempt++ {
		failed, err := store.FailMemoryInvalidations(ctx, MemoryInvalidationFailInput{
			WorkspaceID:     workspaceID,
			AgentID:         agentID,
			InvalidationIDs: []string{firstPoll[0].InvalidationID},
		})
		if err != nil {
			t.Fatalf("dead-letter invalidation: %v", err)
		}
		if len(failed) != 1 {
			t.Fatalf("expected dead-letter attempt %d to change the invalidation, got %+v", attempt+1, failed)
		}
		if attempt+1 < memoryInvalidationDeadLetterThreshold {
			redeliverMemoryInvalidationForTest(t, store, ctx, workspaceID, agentID, firstPoll[0].InvalidationID)
		}
	}

	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:runbook",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report stale residency again: %v", err)
	}

	secondPoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll reenqueued invalidation: %v", err)
	}
	if len(secondPoll) != 1 {
		t.Fatalf("expected reenqueued invalidation, got %+v", secondPoll)
	}
	if secondPoll[0].InvalidationID == firstPoll[0].InvalidationID {
		t.Fatalf("expected new invalidation id after dead-letter, got %+v", secondPoll)
	}
}

func TestRequeueMemoryInvalidationsReopensDeadLetterAndClearsSchedule(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-requeue-dead", "agent-memory-invalidation-requeue-dead", "requeue-dead-doc")

	delivered, _, err := store.PollMemoryInvalidationsWithEvents(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   item.WorkspaceID,
		AgentID:       item.AgentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("mark delivered invalidation: %v", err)
	}
	if len(delivered) != 1 {
		t.Fatalf("expected delivered invalidation, got %+v", delivered)
	}

	for attempt := 0; attempt < memoryInvalidationDeadLetterThreshold; attempt++ {
		failed, err := store.FailMemoryInvalidations(ctx, MemoryInvalidationFailInput{
			WorkspaceID:     item.WorkspaceID,
			AgentID:         item.AgentID,
			InvalidationIDs: []string{item.InvalidationID},
			FailureReason:   "APPLY_FAILED",
		})
		if err != nil {
			t.Fatalf("dead-letter invalidation: %v", err)
		}
		if len(failed) != 1 {
			t.Fatalf("expected dead-letter attempt %d to change the invalidation, got %+v", attempt+1, failed)
		}
		if attempt+1 < memoryInvalidationDeadLetterThreshold {
			redeliverMemoryInvalidationForTest(t, store, ctx, item.WorkspaceID, item.AgentID, item.InvalidationID)
		}
	}

	requeued, requeueEvents, err := store.RequeueMemoryInvalidationsWithEvents(ctx, MemoryInvalidationRequeueInput{
		WorkspaceID:     item.WorkspaceID,
		AgentID:         item.AgentID,
		InvalidationIDs: []string{item.InvalidationID},
	})
	if err != nil {
		t.Fatalf("requeue dead-letter invalidation: %v", err)
	}
	if len(requeued) != 1 {
		t.Fatalf("expected one requeued invalidation, got %+v", requeued)
	}
	if len(requeueEvents) != 1 {
		t.Fatalf("expected one returned requeue runtime event, got %+v", requeueEvents)
	}
	if requeued[0].InvalidationID == item.InvalidationID {
		t.Fatalf("expected requeue to clone into a new invalidation row, got %+v", requeued[0])
	}
	if requeued[0].State != "OPEN" || requeued[0].FailureCount != 0 || requeued[0].DeadLetteredAt != "" {
		t.Fatalf("expected reopened invalidation, got %+v", requeued[0])
	}
	if requeued[0].RecoveredFromInvalidationID != item.InvalidationID || requeued[0].RecoveryCause != "dead_letter_requeue" || len(requeued[0].DependencyRevisionVector) == 0 {
		t.Fatalf("expected requeued invalidation to surface lineage and dependency vector, got %+v", requeued[0])
	}
	if requeued[0].LeaseExpiresAt != "" || requeued[0].NextDeliveryAt != "" || requeued[0].LastFailureReason != "" || requeued[0].LastFailureAt != "" {
		t.Fatalf("expected cleared retry metadata after requeue, got %+v", requeued[0])
	}

	ready, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: item.WorkspaceID,
		AgentID:     item.AgentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll requeued invalidation: %v", err)
	}
	if len(ready) != 1 || ready[0].InvalidationID != requeued[0].InvalidationID {
		t.Fatalf("expected requeued invalidation to become pollable again, got %+v", ready)
	}

	deadLetterItems, err := store.ListMemoryInvalidations(ctx, MemoryInvalidationListFilter{
		WorkspaceID:       item.WorkspaceID,
		AgentID:           item.AgentID,
		IncludeDeadLetter: true,
		Limit:             10,
	})
	if err != nil {
		t.Fatalf("list invalidations including dead-letter: %v", err)
	}
	if len(deadLetterItems) != 2 {
		t.Fatalf("expected one dead-letter row plus one requeued row, got %+v", deadLetterItems)
	}

	listedEvents, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: item.WorkspaceID,
		EventType:   "memory.invalidation_requeued",
		EntityType:  "memory_invalidation",
		EntityID:    requeued[0].InvalidationID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list requeue runtime events: %v", err)
	}
	if len(listedEvents) != 1 {
		t.Fatalf("expected one requeue runtime event, got %+v", listedEvents)
	}
	assertSameRuntimeEventRecord(t, requeueEvents[0], listedEvents[0])
	payload := decodeMemoryInvalidationPayloadMap(t, listedEvents[0].PayloadJSON)
	if payload["recovered_from_invalidation_id"] != item.InvalidationID || payload["recovery_cause"] != "dead_letter_requeue" {
		t.Fatalf("expected requeue payload to surface lineage markers, got %+v", payload)
	}
	vector := payload["dependency_revision_vector"].([]any)
	if len(vector) != 1 {
		t.Fatalf("expected one dependency_revision_vector entry in requeue payload, got %+v", payload)
	}
}

func TestRequeueMemoryInvalidationsSkipsNonDeadLetterItems(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-requeue-open", "agent-memory-invalidation-requeue-open", "requeue-open-doc")

	requeued, err := store.RequeueMemoryInvalidations(ctx, MemoryInvalidationRequeueInput{
		WorkspaceID:     item.WorkspaceID,
		AgentID:         item.AgentID,
		InvalidationIDs: []string{item.InvalidationID},
	})
	if err != nil {
		t.Fatalf("requeue open invalidation: %v", err)
	}
	if len(requeued) != 0 {
		t.Fatalf("expected requeue on open item to be a no-op, got %+v", requeued)
	}
}

func TestMemoryInvalidationCanonicalizesStringEncodedDependencyRevisionVector(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-canonical-vector", "agent-memory-invalidation-canonical-vector", "canonical-doc")

	rawMetadata, err := json.Marshal(map[string]any{
		"cause": "workspace_doc.upserted",
		"dependency_revision_vector": `[
			{"ref_kind":" workspace_doc ","ref_id":" canonical-doc ","version_token":"doc-v0","weight":0.25},
			{"ref_kind":"workspace_doc","ref_id":"canonical-doc","version_token":"doc-v1","weight":1}
		]`,
	})
	if err != nil {
		t.Fatalf("marshal malformed dependency vector metadata: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE memory_invalidation_queue SET metadata_json = ?, updated_at = ? WHERE invalidation_id = ?`,
		string(rawMetadata),
		time.Now().UTC().Format(time.RFC3339Nano),
		item.InvalidationID,
	); err != nil {
		t.Fatalf("update memory invalidation metadata: %v", err)
	}

	items, err := store.ListMemoryInvalidations(ctx, MemoryInvalidationListFilter{
		WorkspaceID:  item.WorkspaceID,
		AgentID:      item.AgentID,
		IncludeAcked: true,
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("list invalidations with canonicalized dependency vector: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one invalidation, got %+v", items)
	}
	if len(items[0].DependencyRevisionVector) != 1 {
		t.Fatalf("expected canonical dependency vector, got %+v", items[0].DependencyRevisionVector)
	}
	if items[0].DependencyRevisionVector[0].RefKind != "workspace_doc" ||
		items[0].DependencyRevisionVector[0].RefID != "canonical-doc" ||
		items[0].DependencyRevisionVector[0].Weight != 1 {
		t.Fatalf("expected normalized dependency vector entry, got %+v", items[0].DependencyRevisionVector[0])
	}
	if _, ok := items[0].Metadata["dependency_revision_vector"].(string); ok {
		t.Fatalf("expected canonicalized metadata dependency vector, got %+v", items[0].Metadata["dependency_revision_vector"])
	}

	got, err := store.GetMemoryInvalidation(ctx, item.WorkspaceID, item.AgentID, item.InvalidationID)
	if err != nil {
		t.Fatalf("get invalidation with canonicalized dependency vector: %v", err)
	}
	if len(got.DependencyRevisionVector) != 1 ||
		got.DependencyRevisionVector[0].RefKind != "workspace_doc" ||
		got.DependencyRevisionVector[0].RefID != "canonical-doc" ||
		got.DependencyRevisionVector[0].Weight != 1 {
		t.Fatalf("expected get invalidation to canonicalize dependency vector, got %+v", got.DependencyRevisionVector)
	}
	if _, ok := got.Metadata["dependency_revision_vector"].(string); ok {
		t.Fatalf("expected get invalidation metadata dependency vector to be canonicalized, got %+v", got.Metadata["dependency_revision_vector"])
	}
}

func TestMemoryInvalidationSurfacesMalformedDependencyRevisionVector(t *testing.T) {
	store, ctx, item := seedOpenDocMemoryInvalidation(t, "ws-memory-invalidation-malformed-vector", "agent-memory-invalidation-malformed-vector", "malformed-doc")

	rawMetadata, err := json.Marshal(map[string]any{
		"cause":                      "workspace_doc.upserted",
		"dependency_revision_vector": "not-json",
	})
	if err != nil {
		t.Fatalf("marshal malformed dependency vector metadata: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx,
		`UPDATE memory_invalidation_queue SET metadata_json = ?, updated_at = ? WHERE invalidation_id = ?`,
		string(rawMetadata),
		time.Now().UTC().Format(time.RFC3339Nano),
		item.InvalidationID,
	); err != nil {
		t.Fatalf("update memory invalidation metadata: %v", err)
	}

	items, err := store.ListMemoryInvalidations(ctx, MemoryInvalidationListFilter{
		WorkspaceID:  item.WorkspaceID,
		AgentID:      item.AgentID,
		IncludeAcked: true,
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("list invalidations with malformed dependency vector: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one invalidation, got %+v", items)
	}
	if len(items[0].DependencyRevisionVector) != 0 || !items[0].DependencyVectorMalformed {
		t.Fatalf("expected malformed dependency vector to surface explicit flag, got %+v", items[0])
	}
	if malformed, _ := items[0].Metadata["dependency_revision_vector_malformed"].(bool); !malformed {
		t.Fatalf("expected list metadata to retain malformed dependency vector warning, got %+v", items[0].Metadata)
	}

	got, err := store.GetMemoryInvalidation(ctx, item.WorkspaceID, item.AgentID, item.InvalidationID)
	if err != nil {
		t.Fatalf("get invalidation with malformed dependency vector: %v", err)
	}
	if len(got.DependencyRevisionVector) != 0 || !got.DependencyVectorMalformed {
		t.Fatalf("expected get invalidation to surface malformed dependency vector flag, got %+v", got)
	}
	if malformed, _ := got.Metadata["dependency_revision_vector_malformed"].(bool); !malformed {
		t.Fatalf("expected get metadata to retain malformed dependency vector warning, got %+v", got.Metadata)
	}

	_, events, err := store.PollMemoryInvalidationsWithEvents(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   item.WorkspaceID,
		AgentID:       item.AgentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("poll invalidations with malformed dependency vector: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one delivered runtime event, got %+v", events)
	}
	payload := decodeMemoryInvalidationPayloadMap(t, events[0].PayloadJSON)
	if malformed, _ := payload["dependency_revision_vector_malformed"].(bool); !malformed {
		t.Fatalf("expected runtime payload to surface malformed dependency vector warning, got %+v", payload)
	}
}

func TestAckedMemoryInvalidationSuppressesSameStaleReport(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-reenqueue"
		agentID     = "agent-memory-invalidation-reenqueue"
		reportID    = "memres-reenqueue"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Reenqueue",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Reenqueue Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:runbook",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	firstPoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("poll initial invalidation: %v", err)
	}
	if len(firstPoll) != 1 {
		t.Fatalf("expected initial invalidation, got %+v", firstPoll)
	}
	if _, err := store.AckMemoryInvalidations(ctx, MemoryInvalidationAckInput{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{firstPoll[0].InvalidationID},
	}); err != nil {
		t.Fatalf("ack invalidation: %v", err)
	}

	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:runbook",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report stale residency again: %v", err)
	}

	secondPoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll same stale invalidation after ack: %v", err)
	}
	if len(secondPoll) != 0 {
		t.Fatalf("expected same stale invalidation to stay suppressed after ack, got %+v", secondPoll)
	}
}

func TestAckedMemoryInvalidationDoesNotSuppressNewStaleReport(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-ack-new-report"
		agentID     = "agent-memory-invalidation-ack-new-report"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Ack New Report",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Ack New Report Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    "memres-ack-new-report-1",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:runbook",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report initial residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	firstPoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("poll initial invalidation: %v", err)
	}
	if len(firstPoll) != 1 {
		t.Fatalf("expected initial invalidation, got %+v", firstPoll)
	}
	if _, err := store.AckMemoryInvalidations(ctx, MemoryInvalidationAckInput{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{firstPoll[0].InvalidationID},
	}); err != nil {
		t.Fatalf("ack invalidation: %v", err)
	}

	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    "memres-ack-new-report-2",
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:runbook",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report stale residency with new report id: %v", err)
	}

	secondPoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll same stale invalidation for new report: %v", err)
	}
	if len(secondPoll) != 1 {
		t.Fatalf("expected new report to enqueue a new invalidation, got %+v", secondPoll)
	}
	if secondPoll[0].InvalidationID == firstPoll[0].InvalidationID {
		t.Fatalf("expected a distinct invalidation for new report, got %+v", secondPoll[0])
	}
	if secondPoll[0].ReportID != "memres-ack-new-report-2" {
		t.Fatalf("expected new invalidation to carry second report id, got %+v", secondPoll[0])
	}
}

func TestAckedMemoryInvalidationDoesNotSuppressWhenCanonicalDependencyVectorWidens(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-ack-vector-change"
		agentID     = "agent-memory-invalidation-ack-vector-change"
		reportID    = "memres-ack-vector-change"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Ack Vector Change",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Ack Vector Change Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "runbook evidence",
		Body:        "Current claim guard for widened dependency vector.",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}

	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:runbook",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report initial residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}
	docV2, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v2: %v", err)
	}

	firstPoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("poll initial invalidation: %v", err)
	}
	if len(firstPoll) != 1 {
		t.Fatalf("expected initial invalidation, got %+v", firstPoll)
	}
	if _, err := store.AckMemoryInvalidations(ctx, MemoryInvalidationAckInput{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{firstPoll[0].InvalidationID},
	}); err != nil {
		t.Fatalf("ack initial invalidation: %v", err)
	}

	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:runbook",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 0.5},
					{RefKind: "knowledge_claim", RefID: claim.ClaimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report widened dependency vector residency: %v", err)
	}

	secondPoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll widened dependency vector invalidation: %v", err)
	}
	if len(secondPoll) != 1 {
		t.Fatalf("expected widened dependency vector to bypass ack-equivalent suppression, got %+v", secondPoll)
	}
	if secondPoll[0].InvalidationID == firstPoll[0].InvalidationID {
		t.Fatalf("expected a new invalidation row after dependency vector widened, got %+v", secondPoll[0])
	}
	if secondPoll[0].CurrentVersionToken != docV2.SHA {
		t.Fatalf("expected widened invalidation to retain current source version token %q, got %+v", docV2.SHA, secondPoll[0])
	}
	assertMemoryInvalidationDependencyVector(t, secondPoll[0].DependencyRevisionVector,
		MemoryResidencyVersionGuard{RefKind: "knowledge_claim", RefID: claim.ClaimID, VersionToken: claim.UpdatedAt, Weight: 1},
		MemoryResidencyVersionGuard{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 0.5},
	)
}

func TestAckedMemoryInvalidationReenqueuesWhenSourceVersionChangesAgain(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-ack-new-version"
		agentID     = "agent-memory-invalidation-ack-new-version"
		reportID    = "memres-ack-new-version"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Ack New Version",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Ack New Version Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:runbook",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	firstPoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("poll initial invalidation: %v", err)
	}
	if len(firstPoll) != 1 {
		t.Fatalf("expected initial invalidation, got %+v", firstPoll)
	}
	if _, err := store.AckMemoryInvalidations(ctx, MemoryInvalidationAckInput{
		WorkspaceID:     workspaceID,
		AgentID:         agentID,
		InvalidationIDs: []string{firstPoll[0].InvalidationID},
	}); err != nil {
		t.Fatalf("ack invalidation: %v", err)
	}

	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion C",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v3: %v", err)
	}

	secondPoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll new stale invalidation after source change: %v", err)
	}
	if len(secondPoll) != 1 {
		t.Fatalf("expected new invalidation after source version changed again, got %+v", secondPoll)
	}
	if secondPoll[0].CurrentVersionToken == firstPoll[0].CurrentVersionToken {
		t.Fatalf("expected a new current version token after second source change, got %+v", secondPoll[0])
	}
}

func TestOpenMemoryInvalidationRefreshesCanonicalDependencyVectorWhenSameReportWidens(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-open-vector-refresh"
		agentID     = "agent-memory-invalidation-open-vector-refresh"
		reportID    = "memres-open-vector-refresh"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Open Vector Refresh",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Open Vector Refresh Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	claim, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "runbook side evidence",
		Body:        "Current claim guard for open invalidation vector refresh.",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("record knowledge claim: %v", err)
	}

	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:runbook",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report initial residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}
	docV2, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v2: %v", err)
	}

	initial, err := store.ListMemoryInvalidations(ctx, MemoryInvalidationListFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list initial invalidations: %v", err)
	}
	if len(initial) != 1 {
		t.Fatalf("expected one initial invalidation, got %+v", initial)
	}
	assertMemoryInvalidationDependencyVector(t, initial[0].DependencyRevisionVector,
		MemoryResidencyVersionGuard{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
	)

	refreshResult, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:runbook",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 0.5},
					{RefKind: "knowledge_claim", RefID: claim.ClaimID, VersionToken: claim.UpdatedAt, Weight: 1},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("report widened dependency vector residency for same report: %v", err)
	}
	if len(refreshResult.InvalidationEvents) != 1 || refreshResult.InvalidationEvents[0].EventType != "memory.invalidation_refreshed" {
		t.Fatalf("expected one memory.invalidation_refreshed runtime event, got %+v", refreshResult.InvalidationEvents)
	}

	refreshed, err := store.ListMemoryInvalidations(ctx, MemoryInvalidationListFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list widened dependency vector invalidations: %v", err)
	}
	if len(refreshed) == 0 {
		t.Fatalf("expected open invalidation after widened dependency vector report")
	}
	if len(refreshed) != 1 {
		t.Fatalf("expected same-key refresh to keep exactly one open invalidation row, got %+v", refreshed)
	}

	var widened *MemoryInvalidationRecord
	for idx := range refreshed {
		if refreshed[idx].RefKind == "workspace_doc" && refreshed[idx].RefID == docKey && refreshed[idx].CurrentVersionToken == docV2.SHA {
			candidate := refreshed[idx]
			widened = &candidate
			break
		}
	}
	if widened == nil {
		t.Fatalf("expected widened invalidation to remain visible, got %+v", refreshed)
	}
	if widened.InvalidationID != initial[0].InvalidationID {
		t.Fatalf("expected same invalidation row to be refreshed in place, got initial=%s refreshed=%s", initial[0].InvalidationID, widened.InvalidationID)
	}
	assertMemoryInvalidationDependencyVector(t, widened.DependencyRevisionVector,
		MemoryResidencyVersionGuard{RefKind: "knowledge_claim", RefID: claim.ClaimID, VersionToken: claim.UpdatedAt, Weight: 1},
		MemoryResidencyVersionGuard{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 0.5},
	)
	listedEvents, err := store.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "memory.invalidation_refreshed",
		EntityType:  "memory_invalidation",
		EntityID:    widened.InvalidationID,
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("list refresh runtime events: %v", err)
	}
	if len(listedEvents) != 1 {
		t.Fatalf("expected one persisted refresh runtime event, got %+v", listedEvents)
	}
	assertSameRuntimeEventRecord(t, refreshResult.InvalidationEvents[0], listedEvents[0])
	payload := decodeMemoryInvalidationPayloadMap(t, listedEvents[0].PayloadJSON)
	if payload["typed_event_type"] != "MEMORY_INVALIDATION_REFRESHED" || payload["trigger_cause"] != "residency_report" {
		t.Fatalf("expected refresh payload to surface refreshed lineage, got %+v", payload)
	}
	vector := payload["dependency_revision_vector"].([]any)
	if len(vector) != 2 {
		t.Fatalf("expected widened dependency vector in refresh payload, got %+v", payload)
	}
	first := vector[0].(map[string]any)
	second := vector[1].(map[string]any)
	if first["ref_kind"] != "knowledge_claim" || first["ref_id"] != claim.ClaimID || first["version_token"] != claim.UpdatedAt || first["weight"] != float64(1) {
		t.Fatalf("expected knowledge-claim lineage in refresh payload, got %+v", payload)
	}
	if second["ref_kind"] != "workspace_doc" || second["ref_id"] != docKey || second["version_token"] != docV1.SHA || second["weight"] != float64(0.5) {
		t.Fatalf("expected workspace-doc lineage in refresh payload, got %+v", payload)
	}
}

func TestEnqueueMemoryInvalidationKeepsDistinctPreviousVersionTokens(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-previous-token"
		agentID     = "agent-memory-invalidation-previous-token"
		reportID    = "memres-previous-token"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Previous Token",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Previous Token Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := store.BeginTxImmediate(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	baseTarget := memoryInvalidationTarget{
		WorkspaceID:    workspaceID,
		AgentID:        agentID,
		ReportScope:    "AGENT",
		ReportID:       reportID,
		ResidencyTier:  "P2",
		ReplicaKind:    "memory_node",
		CoherenceClass: "A",
		CacheKey:       "packet:runbook",
	}
	firstGuard := MemoryResidencyVersionGuard{RefKind: "workspace_doc", RefID: docKey, VersionToken: "doc-v1", Weight: 1}
	firstTarget := baseTarget
	firstTarget.VersionGuards = []MemoryResidencyVersionGuard{firstGuard}
	first, _, inserted, err := store.enqueueMemoryInvalidationTx(ctx, tx, WorkspaceAuthorityRecord{}, firstTarget, firstGuard, "doc-v3", "VERSION_CHANGED", now, map[string]any{
		"cause": "workspace_doc.upserted",
	})
	if err != nil {
		t.Fatalf("enqueue first invalidation: %v", err)
	}
	if !inserted {
		t.Fatal("expected first invalidation to insert")
	}

	secondGuard := MemoryResidencyVersionGuard{RefKind: "workspace_doc", RefID: docKey, VersionToken: "doc-v2", Weight: 1}
	secondTarget := baseTarget
	secondTarget.VersionGuards = []MemoryResidencyVersionGuard{secondGuard}
	second, _, inserted, err := store.enqueueMemoryInvalidationTx(ctx, tx, WorkspaceAuthorityRecord{}, secondTarget, secondGuard, "doc-v3", "VERSION_CHANGED", now, map[string]any{
		"cause": "workspace_doc.upserted",
	})
	if err != nil {
		t.Fatalf("enqueue second invalidation: %v", err)
	}
	if !inserted {
		t.Fatal("expected second invalidation with different previous_version_token to insert")
	}
	if first.InvalidationID == second.InvalidationID {
		t.Fatalf("expected distinct invalidation rows, got %+v and %+v", first, second)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit invalidations: %v", err)
	}

	items, err := store.ListMemoryInvalidations(ctx, MemoryInvalidationListFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list invalidations: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected two open invalidations, got %+v", items)
	}
	seen := map[string]string{}
	for _, item := range items {
		seen[item.PreviousVersionToken] = item.CurrentVersionToken
	}
	if seen["doc-v1"] != "doc-v3" || seen["doc-v2"] != "doc-v3" {
		t.Fatalf("expected distinct previous/current version pairs, got %+v", items)
	}
}

func TestDeadLetterMemoryInvalidationStillAllowsSameStaleCondition(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-dead-letter-same-stale"
		agentID     = "agent-memory-invalidation-dead-letter-same-stale"
		reportID    = "memres-dead-letter-same-stale"
		docKey      = "runbook"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Dead Letter Same Stale",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Dead Letter Same Stale Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:runbook",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       "Runbook",
		Content:     "# Runbook\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	firstPoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("poll initial invalidation: %v", err)
	}
	if len(firstPoll) != 1 {
		t.Fatalf("expected initial invalidation, got %+v", firstPoll)
	}
	for attempt := 0; attempt < memoryInvalidationDeadLetterThreshold; attempt++ {
		failed, err := store.FailMemoryInvalidations(ctx, MemoryInvalidationFailInput{
			WorkspaceID:     workspaceID,
			AgentID:         agentID,
			InvalidationIDs: []string{firstPoll[0].InvalidationID},
			FailureReason:   "APPLY_FAILED",
		})
		if err != nil {
			t.Fatalf("dead-letter invalidation: %v", err)
		}
		if len(failed) != 1 {
			t.Fatalf("expected dead-letter attempt %d to change the invalidation, got %+v", attempt+1, failed)
		}
		if attempt+1 < memoryInvalidationDeadLetterThreshold {
			redeliverMemoryInvalidationForTest(t, store, ctx, workspaceID, agentID, firstPoll[0].InvalidationID)
		}
	}

	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		ReportID:    reportID,
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:runbook",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report same stale residency after dead-letter: %v", err)
	}

	secondPoll, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll same stale invalidation after dead-letter: %v", err)
	}
	if len(secondPoll) != 1 {
		t.Fatalf("expected dead-letter path to remain reenqueuable, got %+v", secondPoll)
	}
	if secondPoll[0].InvalidationID == firstPoll[0].InvalidationID {
		t.Fatalf("expected new invalidation id after dead-letter, got %+v", secondPoll)
	}

	items, err := store.ListMemoryInvalidations(ctx, MemoryInvalidationListFilter{
		WorkspaceID:       workspaceID,
		AgentID:           agentID,
		IncludeDeadLetter: true,
		Limit:             10,
	})
	if err != nil {
		t.Fatalf("list invalidations after dead-letter reenqueuing: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected one dead-letter row plus one open row, got %+v", items)
	}
}

func TestReportMemoryResidencyDetectsStaleKnowledgeClaimRelationGuard(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-invalidation-claim-relation"
		agentID     = "agent-memory-invalidation-claim-relation"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Memory Invalidation Claim Relation",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Claim Relation Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	target, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "runbook target",
		Body:        "Target knowledge claim.",
		AgentID:     agentID,
	})
	if err != nil {
		t.Fatalf("record target claim: %v", err)
	}
	source, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		ClaimID:     "claim-source-relation",
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Status:      "ACTIVE",
		Subject:     "runbook source",
		Body:        "Source claim with relation evidence.",
		AgentID:     agentID,
		Evidence:    []string{"supports:" + target.ClaimID},
	})
	if err != nil {
		t.Fatalf("record source claim: %v", err)
	}
	relations, err := store.ListKnowledgeClaimRelations(ctx, KnowledgeClaimRelationFilter{
		WorkspaceID: workspaceID,
		FromClaimID: source.ClaimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list initial relations: %v", err)
	}
	if len(relations) != 1 {
		t.Fatalf("expected one initial relation, got %+v", relations)
	}
	initialRelation := relations[0]

	time.Sleep(time.Millisecond)

	if _, err := store.RecordKnowledgeClaim(ctx, KnowledgeClaimInput{
		ClaimID:     source.ClaimID,
		WorkspaceID: workspaceID,
		ClaimType:   source.ClaimType,
		Status:      source.Status,
		Subject:     source.Subject,
		Body:        source.Body + " Updated.",
		AgentID:     agentID,
		Evidence:    []string{"supports:" + target.ClaimID},
	}); err != nil {
		t.Fatalf("update source claim: %v", err)
	}
	relations, err = store.ListKnowledgeClaimRelations(ctx, KnowledgeClaimRelationFilter{
		WorkspaceID: workspaceID,
		FromClaimID: source.ClaimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list updated relations: %v", err)
	}
	if len(relations) != 1 {
		t.Fatalf("expected one updated relation, got %+v", relations)
	}
	if relations[0].UpdatedAt == initialRelation.UpdatedAt {
		t.Fatalf("expected relation token to change, got %+v", relations[0])
	}

	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "claim_graph",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "claim-graph:relation",
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "knowledge_claim_relation", RefID: initialRelation.RelationID, VersionToken: initialRelation.UpdatedAt, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report stale relation residency: %v", err)
	}

	items, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("poll relation invalidations: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one relation invalidation, got %+v", items)
	}
	if items[0].RefKind != "knowledge_claim_relation" || items[0].RefID != initialRelation.RelationID || items[0].Reason != "VERSION_CHANGED" {
		t.Fatalf("unexpected relation invalidation %+v", items[0])
	}
}

func hasInvalidationRef(items []MemoryInvalidationRecord, refKind, refID string) bool {
	for _, item := range items {
		if item.RefKind == refKind && item.RefID == refID {
			return true
		}
	}
	return false
}

func assertMemoryInvalidationDependencyVector(t *testing.T, got []MemoryResidencyVersionGuard, want ...MemoryResidencyVersionGuard) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("expected dependency revision vector len %d, got %+v", len(want), got)
	}
	for idx := range want {
		if got[idx].RefKind != want[idx].RefKind ||
			got[idx].RefID != want[idx].RefID ||
			got[idx].VersionToken != want[idx].VersionToken ||
			got[idx].Weight != want[idx].Weight {
			t.Fatalf("expected dependency revision vector[%d] %+v, got %+v", idx, want[idx], got[idx])
		}
	}
}

func seedOpenDocMemoryInvalidation(t *testing.T, workspaceID, agentID, docKey string) (*Store, context.Context, MemoryInvalidationRecord) {
	t.Helper()

	store := NewTestStore(t)
	ctx := context.Background()

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: agentID,
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	claimTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       docKey,
		Content:     "# " + docKey + "\nVersion A",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v1: %v", err)
	}
	docV1, err := store.GetWorkspaceDoc(ctx, workspaceID, docKey)
	if err != nil {
		t.Fatalf("get doc v1: %v", err)
	}
	if _, err := store.ReportMemoryResidency(ctx, MemoryResidencyReportInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Replicas: []MemoryReplicaStateInput{
			{
				ResidencyTier:  "P2",
				ReplicaKind:    "memory_node",
				CoherenceClass: "A",
				State:          "CURRENT",
				CacheKey:       "packet:" + workspaceID,
				VersionGuards: []MemoryResidencyVersionGuard{
					{RefKind: "workspace_doc", RefID: docKey, VersionToken: docV1.SHA, Weight: 1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("report residency: %v", err)
	}
	if err := store.UpsertWorkspaceDoc(ctx, WorkspaceDocInput{
		WorkspaceID: workspaceID,
		DocKey:      docKey,
		Title:       docKey,
		Content:     "# " + docKey + "\nVersion B",
		UpdatedBy:   "developer",
	}); err != nil {
		t.Fatalf("upsert doc v2: %v", err)
	}

	items, err := store.PollMemoryInvalidations(ctx, MemoryInvalidationPollFilter{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("seed poll invalidations: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one seeded invalidation, got %+v", items)
	}
	return store, ctx, items[0]
}

func getMemoryInvalidationQueueStringColumn(t *testing.T, store *Store, invalidationID, column string) string {
	t.Helper()

	query := fmt.Sprintf("SELECT %s FROM memory_invalidation_queue WHERE invalidation_id = ?", column)
	var value string
	if err := store.DB().QueryRowContext(context.Background(), query, invalidationID).Scan(&value); err != nil {
		t.Fatalf("query %s for invalidation %s: %v", column, invalidationID, err)
	}
	return value
}

func setMemoryInvalidationQueueStringColumn(t *testing.T, store *Store, invalidationID, column, value string) {
	t.Helper()

	query := fmt.Sprintf("UPDATE memory_invalidation_queue SET %s = ?, updated_at = ? WHERE invalidation_id = ?", column)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := store.DB().ExecContext(context.Background(), query, value, now, invalidationID)
	if err != nil {
		t.Fatalf("update %s for invalidation %s: %v", column, invalidationID, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("rows affected for %s on invalidation %s: %v", column, invalidationID, err)
	}
	if rowsAffected != 1 {
		t.Fatalf("expected one invalidation row updated for %s on %s, got %d", column, invalidationID, rowsAffected)
	}
}

func redeliverMemoryInvalidationForTest(t *testing.T, store *Store, ctx context.Context, workspaceID, agentID, invalidationID string) MemoryInvalidationRecord {
	t.Helper()

	setMemoryInvalidationQueueStringColumn(t, store, invalidationID, "next_delivery_at", time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano))
	items, _, err := store.PollMemoryInvalidationsWithEvents(ctx, MemoryInvalidationPollFilter{
		WorkspaceID:   workspaceID,
		AgentID:       agentID,
		Limit:         10,
		MarkDelivered: true,
	})
	if err != nil {
		t.Fatalf("redeliver invalidation %s: %v", invalidationID, err)
	}
	for _, item := range items {
		if item.InvalidationID == invalidationID {
			return item
		}
	}
	t.Fatalf("expected invalidation %s to be redelivered, got %+v", invalidationID, items)
	return MemoryInvalidationRecord{}
}

func setWorkspaceControlEpochAnchor(t *testing.T, store *Store, workspaceID, anchorAt string) {
	t.Helper()

	result, err := store.DB().ExecContext(
		context.Background(),
		`INSERT INTO workspace_control_epochs (workspace_id, current_epoch, policy_mode, last_incremented_at)
		 VALUES (?, 0, 'shadow', ?)
		 ON CONFLICT(workspace_id) DO UPDATE SET last_incremented_at = excluded.last_incremented_at`,
		workspaceID,
		anchorAt,
	)
	if err != nil {
		t.Fatalf("upsert workspace control epoch anchor for %s: %v", workspaceID, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("rows affected for workspace control epoch anchor %s: %v", workspaceID, err)
	}
	if rowsAffected < 1 {
		t.Fatalf("expected workspace control epoch anchor to affect at least one row for %s", workspaceID)
	}
}

func assertSameRuntimeEventRecord(t *testing.T, got, want RuntimeEventRecord) {
	t.Helper()

	if got.EventID != want.EventID {
		t.Fatalf("expected runtime event id %q, got %q", want.EventID, got.EventID)
	}
	if got.IngestSeq != want.IngestSeq {
		t.Fatalf("expected ingest_seq %d, got %d", want.IngestSeq, got.IngestSeq)
	}
	if got.EventType != want.EventType {
		t.Fatalf("expected event type %q, got %q", want.EventType, got.EventType)
	}
	if got.EntityType != want.EntityType || got.EntityID != want.EntityID {
		t.Fatalf("expected entity %s/%s, got %s/%s", want.EntityType, want.EntityID, got.EntityType, got.EntityID)
	}
	if got.PayloadJSON != want.PayloadJSON {
		t.Fatalf("expected payload %s, got %s", want.PayloadJSON, got.PayloadJSON)
	}
}

func decodeMemoryInvalidationPayloadMap(t *testing.T, raw string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode invalidation payload: %v", err)
	}
	return payload
}

func assertMemoryInvalidationTemporalContract(t *testing.T, contracts []TemporalHorizonContract, horizonKind, basis, state string) {
	t.Helper()
	for _, contract := range contracts {
		if contract.HorizonKind != horizonKind {
			continue
		}
		if contract.SchemaVersion != temporalContractSchemaVersion ||
			contract.Domain != "memory_invalidation" ||
			contract.Basis != basis ||
			contract.Mapping != temporalMappingExactWallClock ||
			!contract.WallClockComparable ||
			contract.State != state {
			t.Fatalf("expected memory invalidation temporal contract %s/%s/%s, got %+v", horizonKind, basis, state, contract)
		}
		if contract.ReferenceAt == "" || contract.TargetAt == "" {
			t.Fatalf("expected memory invalidation temporal contract to carry reference/target anchors, got %+v", contract)
		}
		return
	}
	t.Fatalf("expected memory invalidation temporal contract %s in %+v", horizonKind, contracts)
}
