package sqlite

import (
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

func TestMergeFailedResolveLinkedSourceQueueLineagePreservesCurrentOnConflictingPreferredLineage(t *testing.T) {
	t.Parallel()

	parentRefs, rootCauseID, provenanceGroupID, err := mergeFailedResolveLinkedSourceQueueLineage(
		"root-stale",
		"prov-stale",
		[]string{"evt-stale"},
		"root-current",
		"prov-current",
		[]string{"evt-current"},
	)
	if err != nil {
		t.Fatalf("mergeFailedResolveLinkedSourceQueueLineage: %v", err)
	}
	if rootCauseID != "root-current" || provenanceGroupID != "prov-current" {
		t.Fatalf("conflicting preferred lineage should preserve current root/provenance, got (%q,%q)", rootCauseID, provenanceGroupID)
	}
	if len(parentRefs) != 1 || parentRefs[0] != "evt-current" {
		t.Fatalf("conflicting preferred lineage should preserve current parent refs, got %+v", parentRefs)
	}
}

func TestMergeFailedResolveLinkedSourceQueueLineageAllowsExplicitPreferredAgainstSyntheticCurrentFallback(t *testing.T) {
	t.Parallel()

	parentRefs, rootCauseID, provenanceGroupID, err := mergeFailedResolveLinkedSourceQueueLineage(
		"evt-anomaly",
		"evt-anomaly",
		[]string{"evt-stale"},
		"evt-current-queue",
		"evt-current-queue",
		[]string{"evt-current-queue"},
	)
	if err != nil {
		t.Fatalf("mergeFailedResolveLinkedSourceQueueLineage: %v", err)
	}
	if rootCauseID != "evt-anomaly" || provenanceGroupID != "evt-anomaly" {
		t.Fatalf("explicit preferred lineage should beat synthetic current fallback, got (%q,%q)", rootCauseID, provenanceGroupID)
	}
	if len(parentRefs) != 1 || parentRefs[0] != "evt-current-queue" {
		t.Fatalf("explicit preferred lineage should still preserve current carrier parent refs, got %+v", parentRefs)
	}
}

func TestMergeFailedResolveLinkedSourceQueueLineageCanFillMissingAuthoritativeFieldsWithoutDroppingCurrentParents(t *testing.T) {
	t.Parallel()

	parentRefs, rootCauseID, provenanceGroupID, err := mergeFailedResolveLinkedSourceQueueLineage(
		"evt-anomaly",
		"evt-anomaly",
		[]string{"evt-stale"},
		"evt-anomaly",
		"evt-current-queue",
		[]string{"evt-current-queue"},
	)
	if err != nil {
		t.Fatalf("mergeFailedResolveLinkedSourceQueueLineage: %v", err)
	}
	if rootCauseID != "evt-anomaly" || provenanceGroupID != "evt-anomaly" {
		t.Fatalf("preferred lineage should be able to fill missing authoritative provenance without degrading root, got (%q,%q)", rootCauseID, provenanceGroupID)
	}
	if len(parentRefs) != 1 || parentRefs[0] != "evt-current-queue" {
		t.Fatalf("current carrier parent refs should be preserved when filling missing fields, got %+v", parentRefs)
	}
}

func TestMergeFailedResolveLinkedSourceQueueLineageDoesNotAdoptPreferredParentsWhenCurrentAuthorityExists(t *testing.T) {
	t.Parallel()

	parentRefs, rootCauseID, provenanceGroupID, err := mergeFailedResolveLinkedSourceQueueLineage(
		"evt-anomaly",
		"evt-anomaly",
		[]string{"evt-stale-parent"},
		"evt-anomaly",
		"evt-anomaly",
		nil,
	)
	if err != nil {
		t.Fatalf("mergeFailedResolveLinkedSourceQueueLineage: %v", err)
	}
	if rootCauseID != "evt-anomaly" || provenanceGroupID != "evt-anomaly" {
		t.Fatalf("authoritative current root/provenance should stay intact, got (%q,%q)", rootCauseID, provenanceGroupID)
	}
	if len(parentRefs) != 0 {
		t.Fatalf("authoritative current lineage without parents should not adopt stale preferred parents, got %+v", parentRefs)
	}
}

func TestActionResolveRuntimeLineageFromLinkedEventsRejectsConflictingCarrierLineage(t *testing.T) {
	t.Parallel()

	payloadA := marshalRollbackFailurePayload(model.RebaseRollbackFailurePayload{
		Kind:              model.RebaseRollbackFailureKind,
		FailureScope:      "execution_run",
		FailureTrigger:    "execution_run_verifier_failed",
		FailureMessage:    "rollback failure A",
		RootCauseID:       "root-a",
		ProvenanceGroupID: "prov-a",
		ParentRefsJSON:    []string{"evt-a"},
	})
	payloadB := marshalRollbackFailurePayload(model.RebaseRollbackFailurePayload{
		Kind:              model.RebaseRollbackFailureKind,
		FailureScope:      "execution_run",
		FailureTrigger:    "execution_run_verifier_failed",
		FailureMessage:    "rollback failure B",
		RootCauseID:       "root-b",
		ProvenanceGroupID: "prov-b",
		ParentRefsJSON:    []string{"evt-b"},
	})

	rootCauseID, provenanceGroupID, parentRefsJSON, ok, err := actionResolveRuntimeLineageFromLinkedEvents([]OperatorQueueSyncEvent{
		{Record: OperatorQueueRecord{QueueID: "opq-a", QueueKey: model.RebaseRollbackFailureQueueKeyPrefix + "carrier-a", PayloadJSON: payloadA}},
		{Record: OperatorQueueRecord{QueueID: "opq-b", QueueKey: model.RebaseRollbackFailureQueueKeyPrefix + "carrier-b", PayloadJSON: payloadB}},
	})
	if err == nil {
		t.Fatalf("expected conflicting carrier lineage to fail, got root=%q provenance=%q parent_refs=%q ok=%v", rootCauseID, provenanceGroupID, parentRefsJSON, ok)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "updated concurrently") {
		t.Fatalf("expected updated concurrently error, got %v", err)
	}
	if ok {
		t.Fatalf("expected conflicting carrier lineage to return ok=false, got true")
	}
}
