package main

import (
	"strings"
	"testing"
)

func TestSelectProjectPatchQueueIntegrationItemAutoSelectsUniqueAccepted(t *testing.T) {
	items := []ProjectPatchQueueItemRecord{
		{
			QueueID:  "queue-old",
			ItemID:   "item-old",
			RepoID:   "repo-1",
			BranchID: "branch-1",
			State:    "REJECTED",
			HeadSHA:  "head-old",
		},
		{
			QueueID:  "queue-new",
			ItemID:   "item-new",
			RepoID:   "repo-1",
			BranchID: "branch-1",
			State:    "ACCEPTED",
			HeadSHA:  "head-new",
		},
	}
	got, err := selectProjectPatchQueueIntegrationItem(items, "", "", "branch-1", "repo-1")
	if err != nil {
		t.Fatalf("selectProjectPatchQueueIntegrationItem() error = %v", err)
	}
	if got.QueueID != "queue-new" || got.ItemID != "item-new" {
		t.Fatalf("selected %+v, want accepted queue-new/item-new", got)
	}
}

func TestSelectProjectPatchQueueIntegrationItemAppliesRepoGuardBeforeAcceptedAutoSelect(t *testing.T) {
	items := []ProjectPatchQueueItemRecord{
		{
			QueueID:  "queue-web",
			ItemID:   "item-web",
			RepoID:   "repo-web",
			BranchID: "branch-shared",
			State:    "ACCEPTED",
			HeadSHA:  "head-web",
		},
		{
			QueueID:  "queue-api",
			ItemID:   "item-api",
			RepoID:   "repo-api",
			BranchID: "branch-shared",
			State:    "ACCEPTED",
			HeadSHA:  "head-api",
		},
	}
	got, err := selectProjectPatchQueueIntegrationItem(items, "", "", "branch-shared", "repo-api")
	if err != nil {
		t.Fatalf("selectProjectPatchQueueIntegrationItem() error = %v", err)
	}
	if got.QueueID != "queue-api" || got.ItemID != "item-api" {
		t.Fatalf("selected %+v, want repo-api candidate", got)
	}
}

func TestSelectProjectPatchQueueIntegrationItemAmbiguityIncludesCandidateSelectors(t *testing.T) {
	items := []ProjectPatchQueueItemRecord{
		{
			QueueID:  "queue-a",
			ItemID:   "item-a",
			RepoID:   "repo-1",
			BranchID: "branch-1",
			State:    "ACCEPTED",
			HeadSHA:  "head-a",
		},
		{
			QueueID:  "queue-b",
			ItemID:   "item-b",
			RepoID:   "repo-1",
			BranchID: "branch-1",
			State:    "ACCEPTED",
			HeadSHA:  "head-b",
		},
	}
	_, err := selectProjectPatchQueueIntegrationItem(items, "", "", "branch-1", "")
	if err == nil {
		t.Fatalf("expected ambiguity error")
	}
	msg := err.Error()
	for _, want := range []string{"patch_queue_selector_ambiguous", "queue-a", "item-a", "head-a", "queue-b", "item-b", "head-b", "project_patch_queue_list"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("expected ambiguity message to contain %q, got %q", want, msg)
		}
	}
}
