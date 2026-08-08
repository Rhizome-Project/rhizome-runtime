package sqlite

import (
	"context"
	"testing"
)

func TestRMP10KnowledgeClaimNormalizationSupportsAntiProcedure(t *testing.T) {
	t.Parallel()

	if got := normalizeKnowledgeClaimType("anti_procedure"); got != "ANTI_PROCEDURE" {
		t.Fatalf("normalizeKnowledgeClaimType(anti_procedure) = %q, want ANTI_PROCEDURE", got)
	}
	if got, ok := knowledgeClaimTypeForMemoryType("anti_procedure"); !ok || got != "ANTI_PROCEDURE" {
		t.Fatalf("knowledgeClaimTypeForMemoryType(anti_procedure) = (%q, %v), want (ANTI_PROCEDURE, true)", got, ok)
	}
}

func TestWorkspaceMemoryAntiProcedurePromotesRecoverableClaim(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	const (
		workspaceID = "ws-memory-anti-procedure"
		agentID     = "agent-memory-anti-procedure"
	)

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Runtime Memory Anti Procedure",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "developer",
		DisplayName: "Anti Procedure Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	record, err := store.RecordWorkspaceMemory(ctx, WorkspaceMemoryInput{
		WorkspaceID: workspaceID,
		MemoryType:  "anti_procedure",
		Title:       "Rollback should stay blocked",
		Body:        "Do not bypass live doctor or rollback-gate checks during degraded telemetry.",
		Summary:     "Preserve rollback anti-procedure as first-class procedural memory.",
		AgentID:     agentID,
		SourceKind:  "manual",
		SourceID:    "dashboard",
		Tags:        []string{"guardrail", "rollback"},
		Confidence:  0.72,
	})
	if err != nil {
		t.Fatalf("record workspace memory: %v", err)
	}

	claims, err := store.ListKnowledgeClaims(ctx, KnowledgeClaimFilter{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		ClaimType:   "ANTI_PROCEDURE",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list promoted anti procedure claims: %v", err)
	}
	if len(claims) != 1 || claims[0].ClaimType != "ANTI_PROCEDURE" || claims[0].Status != "ACTIVE" {
		t.Fatalf("expected active anti procedure promoted claim, got %+v", claims)
	}

	if _, err := store.ArchiveWorkspaceMemory(ctx, WorkspaceMemoryArchiveInput{
		WorkspaceID: workspaceID,
		MemoryID:    record.MemoryID,
		ArchivedBy:  "rmp_pruner",
		Reason:      "rmp_gc_expired",
	}); err != nil {
		t.Fatalf("archive workspace memory: %v", err)
	}

	archivedClaims, err := store.ListKnowledgeClaims(ctx, KnowledgeClaimFilter{
		WorkspaceID:     workspaceID,
		MemoryID:        record.MemoryID,
		ClaimType:       "ANTI_PROCEDURE",
		IncludeArchived: true,
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("list archived anti procedure claims: %v", err)
	}
	if len(archivedClaims) != 1 || archivedClaims[0].Status != "ARCHIVED" || archivedClaims[0].ArchivedAt == nil || archivedClaims[0].LifecycleReason != "rmp_gc_expired" {
		t.Fatalf("expected archived anti procedure promoted claim, got %+v", archivedClaims)
	}
}
