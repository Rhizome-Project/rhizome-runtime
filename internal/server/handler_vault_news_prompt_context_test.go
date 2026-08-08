package server

import (
	"context"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestVaultMutationsRecordActorBoundPromptContextEvents(t *testing.T) {
	t.Setenv("RHIZOME_ALLOW_PLAINTEXT_VAULT", "1")

	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-vault-rpc-evidence"
	const actorID = "operator-a"
	const entryID = "vault-rpc-evidence"
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedVaultNewsMutationWorkspace(t, ctx, store, workspaceID, actorID)
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	result, rpcErr := h.vaultCreate(ctx, mustJSONRaw(vaultCreateParams{
		WorkspaceID: workspaceID,
		EntryID:     entryID,
		Title:       "Runtime Token",
		Description: "Used by autonomous workers",
		FieldsJSON:  `{"token":"secret"}`,
		CreatedBy:   actorID,
	}))
	if rpcErr != nil {
		t.Fatalf("vault.create rpc error: %+v", rpcErr)
	}
	if payload, ok := result.(map[string]any); !ok || payload["event"] == nil || payload["status"] != "CREATED" {
		t.Fatalf("unexpected vault.create result %+v", result)
	}
	createRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "vault.entry.created",
		EntityType:  "vault_entry",
		EntityID:    entryID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "vault.entry.created"), createRuntime, "vault.entry.created")
	assertServerRuntimeEventAuthorityMetadata(t, createRuntime, authority)
	createPayload := decodeEventPayloadMap(t, createRuntime.PayloadJSON)
	assertVaultServerPromptContext(t, createPayload, "vault.create", workspaceID, "human", actorID, entryID, actorID)
	if _, ok := createPayload["fields_json"]; ok {
		t.Fatalf("vault.create payload leaked fields_json: %+v", createPayload)
	}
	if got, _ := createPayload["fields_sha256"].(string); !strings.HasPrefix(got, "sha256:") {
		t.Fatalf("vault.create payload fields_sha256 = %+v", createPayload["fields_sha256"])
	}

	if _, rpcErr := h.vaultUpdate(ctx, mustJSONRaw(vaultUpdateParams{
		WorkspaceID: workspaceID,
		EntryID:     entryID,
		Title:       "Runtime Token Updated",
		FieldsJSON:  `{"token":"new-secret"}`,
		Actor:       actorID,
	})); rpcErr != nil {
		t.Fatalf("vault.update rpc error: %+v", rpcErr)
	}
	updateRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "vault.entry.updated",
		EntityType:  "vault_entry",
		EntityID:    entryID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "vault.entry.updated"), updateRuntime, "vault.entry.updated")
	assertServerRuntimeEventAuthorityMetadata(t, updateRuntime, authority)
	updatePayload := decodeEventPayloadMap(t, updateRuntime.PayloadJSON)
	assertVaultServerPromptContext(t, updatePayload, "vault.update", workspaceID, "human", actorID, entryID, actorID)
	if _, ok := updatePayload["fields_json"]; ok {
		t.Fatalf("vault.update payload leaked fields_json: %+v", updatePayload)
	}

	if _, rpcErr := h.vaultDelete(ctx, mustJSONRaw(vaultDeleteParams{
		WorkspaceID: workspaceID,
		EntryID:     entryID,
		Actor:       actorID,
	})); rpcErr != nil {
		t.Fatalf("vault.delete rpc error: %+v", rpcErr)
	}
	deleteRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "vault.entry.deleted",
		EntityType:  "vault_entry",
		EntityID:    entryID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "vault.entry.deleted"), deleteRuntime, "vault.entry.deleted")
	assertServerRuntimeEventAuthorityMetadata(t, deleteRuntime, authority)
	assertVaultServerPromptContext(t, decodeEventPayloadMap(t, deleteRuntime.PayloadJSON), "vault.delete", workspaceID, "human", actorID, entryID, actorID)
	if _, err := store.GetVaultEntry(ctx, workspaceID, entryID); err == nil {
		t.Fatal("expected vault.delete to remove entry")
	}
}

func TestVaultMutationsFailClosedOnActorMismatch(t *testing.T) {
	t.Setenv("RHIZOME_ALLOW_PLAINTEXT_VAULT", "1")

	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-vault-rpc-actor-mismatch"
	ctx := testAuthContext(workspaceID, "human", "operator-a")
	seedVaultNewsMutationWorkspace(t, ctx, store, workspaceID, "operator-a")
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	result, rpcErr := h.vaultCreate(ctx, mustJSONRaw(vaultCreateParams{
		WorkspaceID: workspaceID,
		EntryID:     "vault-mismatch",
		Title:       "Mismatch",
		FieldsJSON:  `{}`,
		CreatedBy:   "operator-b",
	}))
	if rpcErr == nil {
		t.Fatal("expected mismatched vault.create actor to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no vault.create result on mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match created_by" {
		t.Fatalf("unexpected vault.create mismatch error %+v", rpcErr)
	}
	if _, err := store.GetVaultEntry(ctx, workspaceID, "vault-mismatch"); err == nil {
		t.Fatal("mismatched vault.create mutated storage")
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "vault.entry.created",
		EntityType:  "vault_entry",
		EntityID:    "vault-mismatch",
	})
	if err != nil {
		t.Fatalf("list vault events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("mismatched vault.create recorded events: %+v", events)
	}
}

func TestVaultReadAuditRecordsActorBoundPromptContextEvents(t *testing.T) {
	t.Setenv("RHIZOME_ALLOW_PLAINTEXT_VAULT", "1")

	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-vault-read-audit-evidence"
	const actorID = "operator-a"
	const entryID = "vault-read-audit"
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedVaultNewsMutationWorkspace(t, ctx, store, workspaceID, actorID)
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.CreateVaultEntry(ctx, sqlite.VaultEntry{
		WorkspaceID: workspaceID,
		EntryID:     entryID,
		Title:       "Readable Vault",
		FieldsJSON:  `{"token":"secret"}`,
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("seed vault entry: %v", err)
	}

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	if _, rpcErr := h.vaultGet(ctx, mustJSONRaw(vaultGetParams{
		WorkspaceID: workspaceID,
		EntryID:     entryID,
		Actor:       actorID,
	})); rpcErr != nil {
		t.Fatalf("vault.get rpc error: %+v", rpcErr)
	}
	readRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "vault.entry.read",
		EntityType:  "vault_entry",
		EntityID:    entryID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "vault.entry.read"), readRuntime, "vault.entry.read")
	assertServerRuntimeEventAuthorityMetadata(t, readRuntime, authority)
	readPayload := decodeEventPayloadMap(t, readRuntime.PayloadJSON)
	assertVaultServerPromptContext(t, readPayload, "vault.get", workspaceID, "human", actorID, entryID, actorID)
	if _, ok := readPayload["fields_json"]; ok {
		t.Fatalf("vault.get audit payload leaked fields_json: %+v", readPayload)
	}

	if _, rpcErr := h.vaultList(ctx, mustJSONRaw(vaultListParams{
		WorkspaceID: workspaceID,
		Actor:       actorID,
	})); rpcErr != nil {
		t.Fatalf("vault.list rpc error: %+v", rpcErr)
	}
	listRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "vault.entries.listed",
		EntityType:  "vault_entry",
		EntityID:    "*",
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "vault.entries.listed"), listRuntime, "vault.entries.listed")
	assertServerRuntimeEventAuthorityMetadata(t, listRuntime, authority)
	assertVaultServerPromptContext(t, decodeEventPayloadMap(t, listRuntime.PayloadJSON), "vault.list", workspaceID, "human", actorID, "*", actorID)

	audit, err := store.ListVaultAuditLog(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("list vault audit: %v", err)
	}
	if len(audit) != 2 {
		t.Fatalf("expected two read-side audit rows, got %+v", audit)
	}
}

func TestVaultReadWithoutActorStaysReadOnly(t *testing.T) {
	t.Setenv("RHIZOME_ALLOW_PLAINTEXT_VAULT", "1")

	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-vault-read-without-actor"
	const actorID = "operator-a"
	const entryID = "vault-read-without-actor"
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedVaultNewsMutationWorkspace(t, ctx, store, workspaceID, actorID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.CreateVaultEntry(ctx, sqlite.VaultEntry{
		WorkspaceID: workspaceID,
		EntryID:     entryID,
		Title:       "Read Only Vault",
		FieldsJSON:  `{}`,
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("seed vault entry: %v", err)
	}

	if _, rpcErr := h.vaultGet(ctx, mustJSONRaw(vaultGetParams{
		WorkspaceID: workspaceID,
		EntryID:     entryID,
	})); rpcErr != nil {
		t.Fatalf("vault.get without actor rpc error: %+v", rpcErr)
	}
	audit, err := store.ListVaultAuditLog(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("list vault audit: %v", err)
	}
	if len(audit) != 0 {
		t.Fatalf("vault.get without actor should not mutate audit log, got %+v", audit)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "vault.entry.read",
		EntityType:  "vault_entry",
		EntityID:    entryID,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("vault.get without actor should not record read event, got %+v", events)
	}
}

func TestVaultReadAuditFailsClosedOnActorMismatch(t *testing.T) {
	t.Setenv("RHIZOME_ALLOW_PLAINTEXT_VAULT", "1")

	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-vault-read-audit-mismatch"
	const actorID = "operator-a"
	const entryID = "vault-read-mismatch"
	ctx := testAuthContext(workspaceID, "human", actorID)
	seedVaultNewsMutationWorkspace(t, ctx, store, workspaceID, actorID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	if err := store.CreateVaultEntry(ctx, sqlite.VaultEntry{
		WorkspaceID: workspaceID,
		EntryID:     entryID,
		Title:       "Mismatch Read",
		FieldsJSON:  `{}`,
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("seed vault entry: %v", err)
	}

	result, rpcErr := h.vaultGet(ctx, mustJSONRaw(vaultGetParams{
		WorkspaceID: workspaceID,
		EntryID:     entryID,
		Actor:       "operator-b",
	}))
	if rpcErr == nil {
		t.Fatal("expected mismatched vault.get audit actor to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no vault.get result on mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match actor" {
		t.Fatalf("unexpected vault.get mismatch error %+v", rpcErr)
	}
	audit, err := store.ListVaultAuditLog(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("list vault audit: %v", err)
	}
	if len(audit) != 0 {
		t.Fatalf("mismatched vault.get wrote audit rows: %+v", audit)
	}
}

func TestNewsMutationsRecordActorBoundPromptContextEvents(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-news-rpc-evidence"
	const actorID = "agent-news"
	ctx := testAuthContext(workspaceID, "agent", actorID)
	seedVaultNewsMutationWorkspace(t, ctx, store, workspaceID, actorID)
	authority := claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	ch := h.GetEventBus().Subscribe(workspaceID)
	defer h.GetEventBus().Unsubscribe(workspaceID, ch)

	result, rpcErr := h.newsPublish(ctx, mustJSONRaw(newsPublishParams{
		WorkspaceID: workspaceID,
		Title:       "Coordination update",
		Content:     "Worker handoff is ready.",
		AuthorID:    actorID,
		AuthorType:  "agent",
	}))
	if rpcErr != nil {
		t.Fatalf("news.publish rpc error: %+v", rpcErr)
	}
	news, ok := result.(*sqlite.NewsRecord)
	if !ok || strings.TrimSpace(news.NewsID) == "" {
		t.Fatalf("unexpected news.publish result %+v", result)
	}
	createRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "news.published",
		EntityType:  "news",
		EntityID:    news.NewsID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "news.published"), createRuntime, "news.published")
	assertServerRuntimeEventAuthorityMetadata(t, createRuntime, authority)
	createPayload := decodeEventPayloadMap(t, createRuntime.PayloadJSON)
	assertNewsServerPromptContext(t, createPayload, "news.publish", workspaceID, "agent", actorID, news.NewsID, actorID)
	if _, ok := createPayload["content"]; ok {
		t.Fatalf("news.published payload duplicated raw content: %+v", createPayload)
	}
	if got, _ := createPayload["content_sha256"].(string); !strings.HasPrefix(got, "sha256:") {
		t.Fatalf("news.published payload content_sha256 = %+v", createPayload["content_sha256"])
	}

	if _, rpcErr := h.newsDelete(ctx, mustJSONRaw(newsDeleteParams{
		WorkspaceID: workspaceID,
		NewsID:      news.NewsID,
		ActorID:     actorID,
	})); rpcErr != nil {
		t.Fatalf("news.delete rpc error: %+v", rpcErr)
	}
	deleteRuntime := mustRuntimeEvent(t, ctx, store, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "news.deleted",
		EntityType:  "news",
		EntityID:    news.NewsID,
	})
	assertLiveEventMirrorsRuntimeEvent(t, nextEventOfType(t, ch, "news.deleted"), deleteRuntime, "news.deleted")
	assertServerRuntimeEventAuthorityMetadata(t, deleteRuntime, authority)
	assertNewsServerPromptContext(t, decodeEventPayloadMap(t, deleteRuntime.PayloadJSON), "news.delete", workspaceID, "agent", actorID, news.NewsID, actorID)
	if _, err := store.GetNews(ctx, news.NewsID); err == nil {
		t.Fatal("expected news.delete to remove news row")
	}
}

func TestNewsMutationsFailClosedOnActorMismatch(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)

	const workspaceID = "ws-news-rpc-actor-mismatch"
	ctx := testAuthContext(workspaceID, "agent", "agent-a")
	seedVaultNewsMutationWorkspace(t, ctx, store, workspaceID, "agent-a")
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)

	result, rpcErr := h.newsPublish(ctx, mustJSONRaw(newsPublishParams{
		WorkspaceID: workspaceID,
		Title:       "Mismatch",
		Content:     "Should not persist.",
		AuthorID:    "agent-b",
		AuthorType:  "agent",
	}))
	if rpcErr == nil {
		t.Fatal("expected mismatched news.publish actor to fail closed")
	}
	if result != nil {
		t.Fatalf("expected no news.publish result on mismatch, got %+v", result)
	}
	if rpcErr.Code != errCodePermissionDenied || rpcErr.Message != "actor mismatch: token identity does not match author_id" {
		t.Fatalf("unexpected news.publish mismatch error %+v", rpcErr)
	}
	items, err := store.ListNews(ctx, workspaceID, 10)
	if err != nil {
		t.Fatalf("list news after mismatch: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("mismatched news.publish mutated storage: %+v", items)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "news.published",
		EntityType:  "news",
	})
	if err != nil {
		t.Fatalf("list news events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("mismatched news.publish recorded events: %+v", events)
	}
}

func seedVaultNewsMutationWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, actorID string) {
	t.Helper()
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   actorID,
	}); err != nil {
		t.Fatalf("create workspace %s: %v", workspaceID, err)
	}
}

func assertVaultServerPromptContext(t *testing.T, payload map[string]any, surface, workspaceID, principalType, principalID, entryID, actorID string) {
	t.Helper()
	assertServerPromptContextFields(t, payload, map[string]string{
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

func assertNewsServerPromptContext(t *testing.T, payload map[string]any, surface, workspaceID, principalType, principalID, newsID, actorID string) {
	t.Helper()
	assertServerPromptContextFields(t, payload, map[string]string{
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

func assertServerPromptContextFields(t *testing.T, payload map[string]any, wants map[string]string) {
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
