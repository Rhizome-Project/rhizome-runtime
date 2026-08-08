package sqlite_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestVaultEntryWithEventRecordsPromptContext(t *testing.T) {
	t.Setenv("RHIZOME_ALLOW_PLAINTEXT_VAULT", "1")

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-vault-evidence"
	const actorID = "operator-a"
	const entryID = "vault-entry-a"
	seedVaultNewsPromptContextWorkspace(t, ctx, store, workspaceID)

	createEvent, err := store.CreateVaultEntryWithEvent(ctx, sqlite.VaultEntryMutationInput{
		Entry: sqlite.VaultEntry{
			EntryID:     entryID,
			WorkspaceID: workspaceID,
			Title:       "API Key",
			FieldsJSON:  `{"token":"secret"}`,
			CreatedBy:   actorID,
		},
		ActorID:   actorID,
		ActorType: "human",
		PromptContextEnvelope: sqlite.BuildVaultPromptContextEnvelope(
			"vault.create",
			"server_rpc",
			workspaceID,
			"human",
			actorID,
		),
		PromptContextSurface: "vault.create",
	})
	if err != nil {
		t.Fatalf("create vault with event: %v", err)
	}
	if createEvent.EventType != "vault.entry.created" || createEvent.EntityType != "vault_entry" || createEvent.EntityID != entryID {
		t.Fatalf("unexpected create event %+v", createEvent)
	}
	createPayload := decodeVaultNewsPayload(t, createEvent.PayloadJSON)
	assertVaultPromptContext(t, createPayload, "vault.create", workspaceID, "human", actorID, entryID, actorID)
	if _, ok := createPayload["fields_json"]; ok {
		t.Fatalf("vault event leaked fields_json: %+v", createPayload)
	}
	if got, _ := createPayload["fields_sha256"].(string); !strings.HasPrefix(got, "sha256:") {
		t.Fatalf("expected vault fields digest, got %+v", createPayload["fields_sha256"])
	}

	updateEvent, err := store.UpdateVaultEntryWithEvent(ctx, sqlite.VaultEntryMutationInput{
		Entry: sqlite.VaultEntry{
			EntryID:     entryID,
			WorkspaceID: workspaceID,
			Title:       "API Key Updated",
			FieldsJSON:  `{"token":"new-secret"}`,
		},
		ActorID:   actorID,
		ActorType: "human",
		PromptContextEnvelope: sqlite.BuildVaultPromptContextEnvelope(
			"vault.update",
			"server_rpc",
			workspaceID,
			"human",
			actorID,
		),
		PromptContextSurface: "vault.update",
	})
	if err != nil {
		t.Fatalf("update vault with event: %v", err)
	}
	assertVaultPromptContext(t, decodeVaultNewsPayload(t, updateEvent.PayloadJSON), "vault.update", workspaceID, "human", actorID, entryID, actorID)

	deleteEvent, err := store.DeleteVaultEntryWithEvent(ctx, sqlite.VaultEntryDeleteInput{
		WorkspaceID: workspaceID,
		EntryID:     entryID,
		ActorID:     actorID,
		ActorType:   "human",
		PromptContextEnvelope: sqlite.BuildVaultPromptContextEnvelope(
			"vault.delete",
			"server_rpc",
			workspaceID,
			"human",
			actorID,
		),
		PromptContextSurface: "vault.delete",
	})
	if err != nil {
		t.Fatalf("delete vault with event: %v", err)
	}
	assertVaultPromptContext(t, decodeVaultNewsPayload(t, deleteEvent.PayloadJSON), "vault.delete", workspaceID, "human", actorID, entryID, actorID)
	if _, err := store.GetVaultEntry(ctx, workspaceID, entryID); err == nil {
		t.Fatal("expected vault entry to be deleted")
	}
}

func TestVaultEntryWithEventRejectsForgedPromptPrincipal(t *testing.T) {
	t.Setenv("RHIZOME_ALLOW_PLAINTEXT_VAULT", "1")

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-vault-forged-principal"
	const actorID = "operator-a"
	const entryID = "vault-forged-entry"
	seedVaultNewsPromptContextWorkspace(t, ctx, store, workspaceID)

	_, err := store.CreateVaultEntryWithEvent(ctx, sqlite.VaultEntryMutationInput{
		Entry: sqlite.VaultEntry{
			EntryID:     entryID,
			WorkspaceID: workspaceID,
			Title:       "Forged Vault",
			FieldsJSON:  `{}`,
			CreatedBy:   actorID,
		},
		ActorID:   actorID,
		ActorType: "human",
		PromptContextEnvelope: sqlite.BuildVaultPromptContextEnvelope(
			"vault.create",
			"server_rpc",
			workspaceID,
			"human",
			"operator-b",
		),
		PromptContextSurface: "vault.create",
	})
	if err == nil || !strings.Contains(err.Error(), "principal_id") {
		t.Fatalf("expected forged principal_id rejection, got %v", err)
	}
	if _, err := store.GetVaultEntry(ctx, workspaceID, entryID); err == nil {
		t.Fatal("expected forged create to roll back vault entry")
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "vault.entry.created",
		EntityType:  "vault_entry",
		EntityID:    entryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list vault events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no forged vault event, got %+v", events)
	}
}

func TestVaultEntryWithEventRequiresPromptContext(t *testing.T) {
	t.Setenv("RHIZOME_ALLOW_PLAINTEXT_VAULT", "1")

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-vault-missing-envelope"
	const actorID = "operator-a"
	const entryID = "vault-missing-envelope"
	seedVaultNewsPromptContextWorkspace(t, ctx, store, workspaceID)

	_, err := store.CreateVaultEntryWithEvent(ctx, sqlite.VaultEntryMutationInput{
		Entry: sqlite.VaultEntry{
			EntryID:     entryID,
			WorkspaceID: workspaceID,
			Title:       "Missing Envelope",
			FieldsJSON:  `{}`,
			CreatedBy:   actorID,
		},
		ActorID:   actorID,
		ActorType: "human",
	})
	if err == nil || !strings.Contains(err.Error(), "prompt_context_envelope") {
		t.Fatalf("expected missing prompt context rejection, got %v", err)
	}
	if _, err := store.GetVaultEntry(ctx, workspaceID, entryID); err == nil {
		t.Fatal("expected missing prompt-context create to roll back vault entry")
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "vault.entry.created",
		EntityType:  "vault_entry",
		EntityID:    entryID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list vault events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no missing-envelope vault event, got %+v", events)
	}
}

func TestVaultAccessWithEventRecordsPromptContext(t *testing.T) {
	t.Setenv("RHIZOME_ALLOW_PLAINTEXT_VAULT", "1")

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-vault-access-evidence"
	const actorID = "operator-a"
	const entryID = "vault-access-entry"
	seedVaultNewsPromptContextWorkspace(t, ctx, store, workspaceID)
	if err := store.CreateVaultEntry(ctx, sqlite.VaultEntry{
		EntryID:     entryID,
		WorkspaceID: workspaceID,
		Title:       "Accessed Vault",
		FieldsJSON:  `{"token":"secret"}`,
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("seed vault entry: %v", err)
	}

	readEvent, err := store.LogVaultAccessWithEvent(ctx, sqlite.VaultAccessEventInput{
		WorkspaceID: workspaceID,
		EntryID:     entryID,
		EntryTitle:  "Accessed Vault",
		Action:      "read",
		ActorID:     actorID,
		ActorType:   "human",
		PromptContextEnvelope: sqlite.BuildVaultPromptContextEnvelope(
			"vault.get",
			"server_rpc",
			workspaceID,
			"human",
			actorID,
		),
		PromptContextSurface: "vault.get",
	})
	if err != nil {
		t.Fatalf("log vault access with event: %v", err)
	}
	if readEvent.EventType != "vault.entry.read" || readEvent.EntityID != entryID {
		t.Fatalf("unexpected read event %+v", readEvent)
	}
	readPayload := decodeVaultNewsPayload(t, readEvent.PayloadJSON)
	assertVaultPromptContext(t, readPayload, "vault.get", workspaceID, "human", actorID, entryID, actorID)
	if _, ok := readPayload["fields_json"]; ok {
		t.Fatalf("vault access event leaked fields_json: %+v", readPayload)
	}

	listEvent, err := store.LogVaultAccessWithEvent(ctx, sqlite.VaultAccessEventInput{
		WorkspaceID: workspaceID,
		EntryID:     "*",
		EntryTitle:  "all entries",
		Action:      "list",
		ActorID:     actorID,
		ActorType:   "human",
		PromptContextEnvelope: sqlite.BuildVaultPromptContextEnvelope(
			"vault.list",
			"server_rpc",
			workspaceID,
			"human",
			actorID,
		),
		PromptContextSurface: "vault.list",
	})
	if err != nil {
		t.Fatalf("log vault list with event: %v", err)
	}
	if listEvent.EventType != "vault.entries.listed" || listEvent.EntityID != "*" {
		t.Fatalf("unexpected list event %+v", listEvent)
	}
	assertVaultPromptContext(t, decodeVaultNewsPayload(t, listEvent.PayloadJSON), "vault.list", workspaceID, "human", actorID, "*", actorID)

	audit, err := store.ListVaultAuditLog(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("list vault audit: %v", err)
	}
	if len(audit) != 2 {
		t.Fatalf("expected two vault audit rows, got %+v", audit)
	}
}

func TestVaultAccessWithEventRequiresPromptContext(t *testing.T) {
	t.Setenv("RHIZOME_ALLOW_PLAINTEXT_VAULT", "1")

	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-vault-access-missing-envelope"
	const actorID = "operator-a"
	seedVaultNewsPromptContextWorkspace(t, ctx, store, workspaceID)

	_, err := store.LogVaultAccessWithEvent(ctx, sqlite.VaultAccessEventInput{
		WorkspaceID: workspaceID,
		EntryID:     "*",
		EntryTitle:  "all entries",
		Action:      "list",
		ActorID:     actorID,
		ActorType:   "human",
	})
	if err == nil || !strings.Contains(err.Error(), "prompt_context_envelope") {
		t.Fatalf("expected missing prompt context rejection, got %v", err)
	}
	audit, err := store.ListVaultAuditLog(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("list vault audit: %v", err)
	}
	if len(audit) != 0 {
		t.Fatalf("expected missing prompt-context audit to roll back, got %+v", audit)
	}
}

func TestNewsWithEventRecordsPromptContext(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-news-evidence"
	const actorID = "agent-news"
	seedVaultNewsPromptContextWorkspace(t, ctx, store, workspaceID)

	news, createEvent, err := store.CreateNewsWithEvent(ctx, sqlite.NewsCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Coordination update",
		Content:     "Proceed with the next handoff.",
		AuthorID:    actorID,
		AuthorType:  "agent",
		ActorID:     actorID,
		ActorType:   "agent",
		PromptContextEnvelope: sqlite.BuildNewsPromptContextEnvelope(
			"news.publish",
			"server_rpc",
			workspaceID,
			"agent",
			actorID,
		),
		PromptContextSurface: "news.publish",
	})
	if err != nil {
		t.Fatalf("create news with event: %v", err)
	}
	assertNewsPromptContext(t, decodeVaultNewsPayload(t, createEvent.PayloadJSON), "news.publish", workspaceID, "agent", actorID, news.NewsID, actorID)

	deleteEvent, err := store.DeleteNewsWithEvent(ctx, sqlite.NewsDeleteInput{
		WorkspaceID: workspaceID,
		NewsID:      news.NewsID,
		ActorID:     actorID,
		ActorType:   "agent",
		PromptContextEnvelope: sqlite.BuildNewsPromptContextEnvelope(
			"news.delete",
			"server_rpc",
			workspaceID,
			"agent",
			actorID,
		),
		PromptContextSurface: "news.delete",
	})
	if err != nil {
		t.Fatalf("delete news with event: %v", err)
	}
	assertNewsPromptContext(t, decodeVaultNewsPayload(t, deleteEvent.PayloadJSON), "news.delete", workspaceID, "agent", actorID, news.NewsID, actorID)
	if _, err := store.GetNews(ctx, news.NewsID); err == nil {
		t.Fatal("expected deleted news to be gone")
	}
}

func TestNewsWithEventRequiresPromptContext(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-news-missing-envelope"
	seedVaultNewsPromptContextWorkspace(t, ctx, store, workspaceID)

	news, _, err := store.CreateNewsWithEvent(ctx, sqlite.NewsCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Published",
		Content:     "Ready",
		AuthorID:    "agent-news",
		AuthorType:  "agent",
		ActorID:     "agent-news",
		ActorType:   "agent",
		PromptContextEnvelope: sqlite.BuildNewsPromptContextEnvelope(
			"news.publish",
			"server_rpc",
			workspaceID,
			"agent",
			"agent-news",
		),
		PromptContextSurface: "news.publish",
	})
	if err != nil {
		t.Fatalf("seed news with event: %v", err)
	}
	_, err = store.DeleteNewsWithEvent(ctx, sqlite.NewsDeleteInput{
		WorkspaceID: workspaceID,
		NewsID:      news.NewsID,
		ActorID:     "agent-news",
		ActorType:   "agent",
	})
	if err == nil || !strings.Contains(err.Error(), "prompt_context_envelope") {
		t.Fatalf("expected missing prompt context rejection, got %v", err)
	}
	if _, err := store.GetNews(ctx, news.NewsID); err != nil {
		t.Fatalf("expected missing prompt-context delete to leave news row: %v", err)
	}
}

func TestNewsWithEventRejectsForgedPromptPrincipal(t *testing.T) {
	store := sqlite.NewTestStore(t)
	ctx := context.Background()
	const workspaceID = "ws-news-forged-principal"
	const actorID = "agent-news"
	seedVaultNewsPromptContextWorkspace(t, ctx, store, workspaceID)

	_, _, err := store.CreateNewsWithEvent(ctx, sqlite.NewsCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Forged News",
		Content:     "Should not persist.",
		AuthorID:    actorID,
		AuthorType:  "agent",
		ActorID:     actorID,
		ActorType:   "agent",
		PromptContextEnvelope: sqlite.BuildNewsPromptContextEnvelope(
			"news.publish",
			"server_rpc",
			workspaceID,
			"agent",
			"agent-other",
		),
		PromptContextSurface: "news.publish",
	})
	if err == nil || !strings.Contains(err.Error(), "principal_id") {
		t.Fatalf("expected forged principal_id rejection, got %v", err)
	}
	items, err := store.ListNews(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("list news after rejected create: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected forged news create to roll back, got %+v", items)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "news.published",
		EntityType:  "news",
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list news events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no forged news event, got %+v", events)
	}
}

func seedVaultNewsPromptContextWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "tests",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := store.EnsureLocalWorkspaceAuthority(ctx, sqlite.EnsureLocalWorkspaceAuthorityInput{
		WorkspaceID: workspaceID,
		ActorType:   "system",
		ActorID:     "tests",
	}); err != nil {
		t.Fatalf("ensure authority: %v", err)
	}
}

func decodeVaultNewsPayload(t *testing.T, raw string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	return payload
}

func assertVaultPromptContext(t *testing.T, payload map[string]any, surface, workspaceID, principalType, principalID, entryID, actorID string) {
	t.Helper()
	assertPromptContextFields(t, payload, map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_vault_write",
		"surface":                            surface,
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
		"principal_type":                     principalType,
		"principal_id":                       principalID,
		"entry_id":                           entryID,
		"actor_id":                           actorID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	})
}

func assertNewsPromptContext(t *testing.T, payload map[string]any, surface, workspaceID, principalType, principalID, newsID, actorID string) {
	t.Helper()
	assertPromptContextFields(t, payload, map[string]string{
		"contract":                           "prompt_context_envelope.v1",
		"context_kind":                       "authority_bearing_news_write",
		"surface":                            surface,
		"origin":                             "server_rpc",
		"workspace_id":                       workspaceID,
		"principal_type":                     principalType,
		"principal_id":                       principalID,
		"news_id":                            newsID,
		"actor_id":                           actorID,
		"authority_model":                    "workspace_authority",
		"compiler_status":                    "non_daemon_context_envelope",
		"daemon_prompt_compiler_convergence": "not_claimed",
		"prompt_capability_evidence":         "not_present",
	})
}

func assertPromptContextFields(t *testing.T, payload map[string]any, wants map[string]string) {
	t.Helper()
	envelope, ok := payload["prompt_context_envelope"].(map[string]any)
	if !ok {
		t.Fatalf("expected prompt_context_envelope in %+v", payload)
	}
	for key, want := range wants {
		if got, ok := envelope[key].(string); !ok || got != want {
			t.Fatalf("prompt_context_envelope[%s] = %v, want %q in %+v", key, envelope[key], want, envelope)
		}
	}
}
