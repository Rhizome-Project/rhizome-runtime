package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceClaimLinksList(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const workspaceID = "ws-handler-claim-links"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Handler Claim Links",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	for _, claimID := range []string{"claim-target-a", "claim-target-b"} {
		if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
			WorkspaceID: workspaceID,
			ClaimID:     claimID,
			ClaimType:   "fact",
			Status:      "confirmed",
			Subject:     claimID,
			Body:        "Claim " + claimID,
			Summary:     claimID,
			Confidence:  0.8,
			SourceKind:  "manual",
			SourceID:    "developer",
		}); err != nil {
			t.Fatalf("record target claim %s: %v", claimID, err)
		}
	}
	if _, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID:      workspaceID,
		ClaimID:          "claim-source",
		ClaimType:        "dissent",
		Status:           "disputed",
		Subject:          "Claim source",
		Body:             "Claim source",
		Summary:          "Claim source",
		Confidence:       0.7,
		SourceKind:       "manual",
		SourceID:         "developer",
		ConflictsClaimID: "claim-target-a",
		Evidence:         []string{"supports:claim-target-b"},
	}); err != nil {
		t.Fatalf("record source claim: %v", err)
	}

	raw, err := json.Marshal(workspaceClaimLinksListParams{
		WorkspaceID: workspaceID,
		ClaimID:     "claim-source",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	result, rpcErr := h.workspaceClaimLinksList(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr != nil {
		t.Fatalf("workspaceClaimLinksList rpc error: %+v", rpcErr)
	}
	payload := result.(map[string]any)
	items, ok := payload["items"].([]sqlite.KnowledgeClaimRelationRecord)
	if !ok {
		t.Fatalf("unexpected items type %T", payload["items"])
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 claim relations, got %+v", items)
	}
	seen := map[string]bool{}
	for _, item := range items {
		seen[item.RelationType+"->"+item.ToClaimID] = true
	}
	if !seen["CONTRADICTS->claim-target-a"] || !seen["SUPPORTS->claim-target-b"] {
		t.Fatalf("unexpected claim relations: %+v", items)
	}
}

func TestWorkspaceClaimLinksListRejectsInvalidRelationType(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-invalid-claim-links"
	raw, err := json.Marshal(workspaceClaimLinksListParams{
		WorkspaceID:  workspaceID,
		RelationType: "NOT_A_RELATION",
	})
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	if _, rpcErr := h.workspaceClaimLinksList(testAuthContext(workspaceID, "system", "tests"), raw); rpcErr == nil || rpcErr.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params, got %+v", rpcErr)
	}
}
