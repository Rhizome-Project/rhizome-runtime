package main

import (
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestWorkspaceDocPutCLIRejectsMissingWorkspaceAuthority(t *testing.T) {
	setupFakeBridgeEnv(t)

	const workspaceID = "ws-doc-cli-missing-authority"
	if err := runWorkspaceCreate([]string{
		"--workspace-id", workspaceID,
		"--title", "Workspace Doc CLI Missing Authority",
		"--created-by", "developer",
	}); err != nil {
		t.Fatalf("runWorkspaceCreate failed: %v", err)
	}

	err := runWorkspaceDocPut([]string{
		"--workspace-id", workspaceID,
		"--doc-key", "runbook",
		"--title", "CLI doc missing authority",
		"--updated-by", "developer",
		"--content", "workspace doc CLI should fail closed before any doc/event side effect",
	})
	if err == nil {
		t.Fatal("expected workspace doc put CLI to fail without workspace authority")
	}
	reject, ok := sqlite.AsAuthorityReject(err)
	if !ok || reject.RejectCode != sqlite.AuthorityRejectMissing {
		t.Fatalf("expected authority_missing reject, got %v", err)
	}
}

func TestWorkspaceDocCLIPublishesPromptContextRuntimeEvents(t *testing.T) {
	setupFakeBridgeEnv(t)

	const (
		workspaceID = "ws-doc-cli-prompt-context"
		docKey      = "runbook"
		actorID     = "developer"
	)
	createCLITestWorkspace(t, workspaceID)

	if _, err := captureStdout(t, func() error {
		return runWorkspaceDocPut([]string{
			"--workspace-id", workspaceID,
			"--doc-key", docKey,
			"--title", "CLI Runbook",
			"--updated-by", actorID,
			"--content", "CLI doc writes must be visible to the runtime journal.",
		})
	}); err != nil {
		t.Fatalf("runWorkspaceDocPut failed: %v", err)
	}
	requireCLIWorkspaceDocRuntimeEvent(t, workspaceID, docKey, "workspace_doc.upserted", "cli.workspace.doc.put", actorID, map[string]string{
		"doc_key":    docKey,
		"title":      "CLI Runbook",
		"updated_by": actorID,
	})
	if _, err := captureStdout(t, func() error {
		return runWorkspaceDocArchive([]string{
			"--workspace-id", workspaceID,
			"--doc-key", docKey,
			"--archived-by", actorID,
		})
	}); err != nil {
		t.Fatalf("runWorkspaceDocArchive failed: %v", err)
	}
	requireCLIWorkspaceDocRuntimeEvent(t, workspaceID, docKey, "workspace_doc.archived", "cli.workspace.doc.archive", actorID, map[string]string{
		"doc_key":     docKey,
		"archived_by": actorID,
	})

	if _, err := captureStdout(t, func() error {
		return runWorkspaceDocDelete([]string{
			"--workspace-id", workspaceID,
			"--doc-key", docKey,
			"--deleted-by", actorID,
		})
	}); err != nil {
		t.Fatalf("runWorkspaceDocDelete failed: %v", err)
	}
	requireCLIWorkspaceDocRuntimeEvent(t, workspaceID, docKey, "workspace_doc.deleted", "cli.workspace.doc.delete", actorID, map[string]string{
		"doc_key":    docKey,
		"deleted_by": actorID,
	})
}
