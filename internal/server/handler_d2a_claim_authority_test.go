package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceClaimWriteRejectsMissingWorkspaceAuthorityWithTypedDetails(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-claim-write-missing-authority-rpc"
		claimID     = "claim-d2a-claim-write-missing-authority-rpc"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Claim Write Missing Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	beforeUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID)

	raw, err := json.Marshal(workspaceClaimWriteParams{
		WorkspaceID: workspaceID,
		ClaimID:     claimID,
		ClaimType:   "FACT",
		Subject:     "Missing authority claim write",
		Body:        "should fail closed before claim row, runtime event, or invalidation exists",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("marshal workspace.claim.write params: %v", err)
	}

	result, rpcErr := h.workspaceClaimWrite(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for missing workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on missing-authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectMissing) || details["surface"] != "workspace.claim.write" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	if got := countKnowledgeClaimRows(t, ctx, store, workspaceID, claimID); got != 0 {
		t.Fatalf("expected no knowledge_claim row after authority reject, got %d", got)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list knowledge_claim.written after authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no knowledge_claim.written events after authority reject, got %+v", events)
	}
	if got := countKnowledgeClaimInvalidationRows(t, ctx, store, workspaceID, claimID); got != 0 {
		t.Fatalf("expected no invalidation rows after authority reject, got %d", got)
	}
	if afterUpdatedAt := mustWorkspaceUpdatedAt(t, ctx, store, workspaceID); afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("expected workspace updated_at to remain %q, got %q", beforeUpdatedAt, afterUpdatedAt)
	}
}

func TestWorkspaceClaimWriteRejectsStaleWorkspaceAuthorityWithoutPartialSupersedeEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-claim-write-stale-authority-rpc"
		newClaimID  = "claim-d2a-claim-write-stale-authority-rpc"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Claim Write Stale Authority RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	current := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	existing, err := store.RecordKnowledgeClaim(ctx, sqlite.KnowledgeClaimInput{
		WorkspaceID: workspaceID,
		ClaimType:   "FACT",
		Subject:     "Existing claim before stale-authority reject",
		Body:        "stale authority must not supersede or mutate this claim",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("seed existing claim: %v", err)
	}
	beforeExisting, err := store.GetKnowledgeClaim(ctx, workspaceID, existing.ClaimID)
	if err != nil {
		t.Fatalf("reload existing claim before stale-authority reject: %v", err)
	}
	beforeExistingInvalidations := countKnowledgeClaimInvalidationRows(t, ctx, store, workspaceID, existing.ClaimID)
	transferServerTestWorkspaceAuthorityToPeer(t, ctx, store, workspaceID, current, "authnode-999-808")

	raw, err := json.Marshal(workspaceClaimWriteParams{
		WorkspaceID:       workspaceID,
		ClaimID:           newClaimID,
		ClaimType:         "FACT",
		Subject:           "Stale authority claim write",
		Body:              "should fail closed before supersede side effects",
		SourceKind:        "manual",
		SourceID:          "tests",
		SupersedesClaimID: existing.ClaimID,
	})
	if err != nil {
		t.Fatalf("marshal workspace.claim.write params: %v", err)
	}

	result, rpcErr := h.workspaceClaimWrite(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr == nil {
		t.Fatal("expected typed authority reject for stale workspace authority")
	}
	if result != nil {
		t.Fatalf("expected no result on stale-authority reject, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "authority rejected" {
		t.Fatalf("expected permission denied authority reject, got %+v", rpcErr)
	}
	details, ok := rpcErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected structured authority reject details, got %+v", rpcErr.Details)
	}
	if details["reject_code"] != string(sqlite.AuthorityRejectStale) || details["surface"] != "workspace.claim.write" {
		t.Fatalf("unexpected authority reject details %+v", details)
	}

	if got := countKnowledgeClaimRows(t, ctx, store, workspaceID, newClaimID); got != 0 {
		t.Fatalf("expected no new claim row after stale-authority reject, got %d", got)
	}
	if events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    newClaimID,
		Limit:       10,
	}); err != nil {
		t.Fatalf("list knowledge_claim.written after stale-authority reject: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("expected no knowledge_claim.written events after stale-authority reject, got %+v", events)
	}
	if got := countKnowledgeClaimInvalidationRows(t, ctx, store, workspaceID, newClaimID); got != 0 {
		t.Fatalf("expected no new-claim invalidation rows after stale-authority reject, got %d", got)
	}
	if got := countKnowledgeClaimInvalidationRows(t, ctx, store, workspaceID, existing.ClaimID); got != beforeExistingInvalidations {
		t.Fatalf("expected superseded-claim invalidation count to stay %d, got %d", beforeExistingInvalidations, got)
	}

	afterExisting, err := store.GetKnowledgeClaim(ctx, workspaceID, existing.ClaimID)
	if err != nil {
		t.Fatalf("reload existing claim after stale-authority reject: %v", err)
	}
	if afterExisting.SupersededByClaimID != beforeExisting.SupersededByClaimID || afterExisting.UpdatedAt != beforeExisting.UpdatedAt {
		t.Fatalf("expected stale-authority reject not to mutate existing claim, got before=%+v after=%+v", beforeExisting, afterExisting)
	}
}

func TestWorkspaceClaimWritePersistsAuthorityMetadataOnKnowledgeClaimWritten(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-d2a-claim-write-authority-metadata-rpc"
		claimID     = "claim-d2a-claim-write-authority-metadata-rpc"
	)
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "D2A Claim Write Authority Metadata RPC",
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	raw, err := json.Marshal(workspaceClaimWriteParams{
		WorkspaceID: workspaceID,
		ClaimID:     claimID,
		ClaimType:   "FACT",
		Subject:     "Authority metadata claim write",
		Body:        "public claim write should stamp authority provenance on knowledge_claim.written",
		SourceKind:  "manual",
		SourceID:    "tests",
	})
	if err != nil {
		t.Fatalf("marshal workspace.claim.write params: %v", err)
	}

	result, rpcErr := h.workspaceClaimWrite(testAuthContext(workspaceID, "system", "tests"), raw)
	if rpcErr != nil {
		t.Fatalf("workspace.claim.write returned rpc error: %+v", rpcErr)
	}
	response, ok := result.(map[string]any)
	if !ok || response["status"] != "RECORDED" {
		t.Fatalf("expected recorded response, got %+v", result)
	}

	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "knowledge_claim.written",
		EntityType:  "knowledge_claim",
		EntityID:    claimID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list knowledge_claim.written events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one knowledge_claim.written event, got %d", len(events))
	}
	assertServerRuntimeEventAuthorityMetadata(t, events[0], authority)

}

func countKnowledgeClaimRows(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, claimID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM knowledge_claims WHERE workspace_id = ? AND claim_id = ?`, workspaceID, claimID).Scan(&count); err != nil {
		t.Fatalf("count knowledge_claim rows: %v", err)
	}
	return count
}

func countKnowledgeClaimInvalidationRows(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, claimID string) int {
	t.Helper()

	var count int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM memory_invalidation_queue WHERE workspace_id = ? AND ref_kind = ? AND ref_id = ?`, workspaceID, "knowledge_claim", claimID).Scan(&count); err != nil {
		t.Fatalf("count claim invalidation rows: %v", err)
	}
	return count
}

func mustWorkspaceUpdatedAt(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) string {
	t.Helper()

	var updatedAt string
	if err := store.DB().QueryRowContext(ctx, `SELECT updated_at FROM workspaces WHERE workspace_id = ?`, workspaceID).Scan(&updatedAt); err != nil {
		if err == sql.ErrNoRows {
			t.Fatalf("workspace %s not found", workspaceID)
		}
		t.Fatalf("load workspace updated_at: %v", err)
	}
	return updatedAt
}

func assertServerRuntimeEventAuthorityMetadata(t *testing.T, event sqlite.RuntimeEventRecord, authority sqlite.WorkspaceAuthorityRecord) {
	t.Helper()

	if event.AuthorityHolderNodeID != authority.HolderAuthorityNodeID {
		t.Fatalf("expected authority holder %q, got %q", authority.HolderAuthorityNodeID, event.AuthorityHolderNodeID)
	}
	if event.AuthorityTerm != authority.Term {
		t.Fatalf("expected authority term %d, got %d", authority.Term, event.AuthorityTerm)
	}
	expectedFingerprint := serverTestAuthorityLeaseTokenFingerprint(authority.LeaseToken)
	if event.AuthorityLeaseTokenFingerprint != expectedFingerprint {
		t.Fatalf("expected authority lease fingerprint %q, got %q", expectedFingerprint, event.AuthorityLeaseTokenFingerprint)
	}
}

func serverTestAuthorityLeaseTokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	encoded := hex.EncodeToString(sum[:])
	if len(encoded) > 16 {
		encoded = encoded[:16]
	}
	return "sha256:" + encoded
}
