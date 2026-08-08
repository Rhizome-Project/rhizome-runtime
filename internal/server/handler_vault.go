package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

// ── Vault CRUD ──────────────────────────────────────────────────────

type vaultCreateParams struct {
	EntryID     string `json:"entry_id"`
	WorkspaceID string `json:"workspace_id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	FieldsJSON  string `json:"fields_json"`
	CreatedBy   string `json:"created_by"`
}

func (h *Handler) vaultCreate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p vaultCreateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: -32602, Message: "invalid params"}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	title := strings.TrimSpace(p.Title)
	createdBy := strings.TrimSpace(p.CreatedBy)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(title, "title"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(createdBy, "created_by"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, createdBy, "created_by")
	if rpcErr != nil {
		return nil, rpcErr
	}
	if p.EntryID == "" {
		p.EntryID = fmt.Sprintf("vault-%d", time.Now().UnixMilli())
	}
	if p.FieldsJSON == "" {
		p.FieldsJSON = "{}"
	}

	event, err := h.store.CreateVaultEntryWithEvent(ctx, sqlite.VaultEntryMutationInput{
		Entry: sqlite.VaultEntry{
			EntryID:     strings.TrimSpace(p.EntryID),
			WorkspaceID: workspaceID,
			Title:       title,
			Description: p.Description,
			FieldsJSON:  p.FieldsJSON,
			CreatedBy:   createdBy,
		},
		ActorID:   createdBy,
		ActorType: principal.PrincipalType,
		PromptContextEnvelope: h.vaultPromptContextEnvelope(ctx, workspaceID, "vault.create", map[string]string{
			"entry_id": strings.TrimSpace(p.EntryID),
			"actor_id": createdBy,
		}),
		PromptContextSurface: "vault.create",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "vault.create")
	}
	h.publishRuntimeEventRecord(event, title, p.EntryID)
	h.touchAgentActivity(ctx, workspaceID, createdBy)
	return map[string]any{"entry_id": p.EntryID, "event": event, "status": "CREATED"}, nil
}

type vaultUpdateParams struct {
	WorkspaceID string `json:"workspace_id"`
	EntryID     string `json:"entry_id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	FieldsJSON  string `json:"fields_json"`
	Actor       string `json:"actor"`
}

func (h *Handler) vaultUpdate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p vaultUpdateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: -32602, Message: "invalid params"}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	entryID := strings.TrimSpace(p.EntryID)
	title := strings.TrimSpace(p.Title)
	actor := strings.TrimSpace(p.Actor)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(entryID, "entry_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(title, "title"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(actor, "actor"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actor, "actor")
	if rpcErr != nil {
		return nil, rpcErr
	}
	event, err := h.store.UpdateVaultEntryWithEvent(ctx, sqlite.VaultEntryMutationInput{
		Entry: sqlite.VaultEntry{
			EntryID:     entryID,
			WorkspaceID: workspaceID,
			Title:       title,
			Description: p.Description,
			FieldsJSON:  p.FieldsJSON,
		},
		ActorID:   actor,
		ActorType: principal.PrincipalType,
		PromptContextEnvelope: h.vaultPromptContextEnvelope(ctx, workspaceID, "vault.update", map[string]string{
			"entry_id": entryID,
			"actor_id": actor,
		}),
		PromptContextSurface: "vault.update",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "vault.update")
	}
	h.publishRuntimeEventRecord(event, entryID)
	h.touchAgentActivity(ctx, workspaceID, actor)
	return map[string]any{"entry_id": entryID, "event": event, "status": "UPDATED"}, nil
}

type vaultDeleteParams struct {
	WorkspaceID string `json:"workspace_id"`
	EntryID     string `json:"entry_id"`
	Actor       string `json:"actor"`
}

func (h *Handler) vaultDelete(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p vaultDeleteParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: -32602, Message: "invalid params"}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	entryID := strings.TrimSpace(p.EntryID)
	actor := strings.TrimSpace(p.Actor)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(entryID, "entry_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(actor, "actor"); rpcErr != nil {
		return nil, rpcErr
	}
	principal, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actor, "actor")
	if rpcErr != nil {
		return nil, rpcErr
	}
	event, err := h.store.DeleteVaultEntryWithEvent(ctx, sqlite.VaultEntryDeleteInput{
		WorkspaceID: workspaceID,
		EntryID:     entryID,
		ActorID:     actor,
		ActorType:   principal.PrincipalType,
		PromptContextEnvelope: h.vaultPromptContextEnvelope(ctx, workspaceID, "vault.delete", map[string]string{
			"entry_id": entryID,
			"actor_id": actor,
		}),
		PromptContextSurface: "vault.delete",
	})
	if err != nil {
		return nil, rpcErrorFromStoreAuthority(err, "vault.delete")
	}
	h.publishRuntimeEventRecord(event, entryID)
	h.touchAgentActivity(ctx, workspaceID, actor)
	return map[string]any{"entry_id": entryID, "event": event, "status": "DELETED"}, nil
}

type vaultListParams struct {
	WorkspaceID string `json:"workspace_id"`
	Actor       string `json:"actor"`
}

func (h *Handler) vaultList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p vaultListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: -32602, Message: "invalid params"}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	actor := strings.TrimSpace(p.Actor)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	var principal AuthPrincipal
	if actor != "" {
		bound, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actor, "actor")
		if rpcErr != nil {
			return nil, rpcErr
		}
		principal = bound
	}

	entries, err := h.store.ListVaultEntries(ctx, workspaceID)
	if err != nil {
		return nil, &RPCError{Code: -32000, Message: err.Error()}
	}
	if actor != "" {
		event, err := h.store.LogVaultAccessWithEvent(ctx, sqlite.VaultAccessEventInput{
			WorkspaceID: workspaceID,
			EntryID:     "*",
			EntryTitle:  "all entries",
			Action:      "list",
			ActorID:     actor,
			ActorType:   principal.PrincipalType,
			PromptContextEnvelope: h.vaultPromptContextEnvelope(ctx, workspaceID, "vault.list", map[string]string{
				"entry_id": "*",
				"actor_id": actor,
			}),
			PromptContextSurface: "vault.list",
		})
		if err != nil {
			return nil, rpcErrorFromStoreAuthority(err, "vault.list")
		}
		h.publishRuntimeEventRecord(event, "vault list")
		h.touchAgentActivity(ctx, workspaceID, actor)
	}
	return map[string]any{"entries": entries, "count": len(entries)}, nil
}

type vaultGetParams struct {
	WorkspaceID string `json:"workspace_id"`
	EntryID     string `json:"entry_id"`
	Actor       string `json:"actor"`
}

func (h *Handler) vaultGet(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p vaultGetParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: -32602, Message: "invalid params"}
	}
	workspaceID := strings.TrimSpace(p.WorkspaceID)
	entryID := strings.TrimSpace(p.EntryID)
	actor := strings.TrimSpace(p.Actor)
	if rpcErr := requireTrimmedParam(workspaceID, "workspace_id"); rpcErr != nil {
		return nil, rpcErr
	}
	if rpcErr := requireTrimmedParam(entryID, "entry_id"); rpcErr != nil {
		return nil, rpcErr
	}
	var principal AuthPrincipal
	if actor != "" {
		bound, rpcErr := requireWorkspaceActorPrincipal(ctx, workspaceID, actor, "actor")
		if rpcErr != nil {
			return nil, rpcErr
		}
		principal = bound
	}

	entry, err := h.store.GetVaultEntry(ctx, workspaceID, entryID)
	if err != nil {
		return nil, &RPCError{Code: -32000, Message: err.Error()}
	}
	if actor != "" {
		event, err := h.store.LogVaultAccessWithEvent(ctx, sqlite.VaultAccessEventInput{
			WorkspaceID: workspaceID,
			EntryID:     entryID,
			EntryTitle:  entry.Title,
			Action:      "read",
			ActorID:     actor,
			ActorType:   principal.PrincipalType,
			PromptContextEnvelope: h.vaultPromptContextEnvelope(ctx, workspaceID, "vault.get", map[string]string{
				"entry_id": entryID,
				"actor_id": actor,
			}),
			PromptContextSurface: "vault.get",
		})
		if err != nil {
			return nil, rpcErrorFromStoreAuthority(err, "vault.get")
		}
		h.publishRuntimeEventRecord(event, entry.Title, entryID)
		h.touchAgentActivity(ctx, workspaceID, actor)
	}
	return entry, nil
}

// ── Vault Audit Log ─────────────────────────────────────────────────

type vaultAuditParams struct {
	WorkspaceID string `json:"workspace_id"`
	Limit       int    `json:"limit"`
}

func (h *Handler) vaultAudit(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p vaultAuditParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: -32602, Message: "invalid params"}
	}
	if p.WorkspaceID == "" {
		return nil, &RPCError{Code: -32602, Message: "workspace_id is required"}
	}
	if p.Limit <= 0 {
		p.Limit = 50
	}

	entries, err := h.store.ListVaultAuditLog(ctx, p.WorkspaceID, p.Limit)
	if err != nil {
		return nil, &RPCError{Code: -32000, Message: err.Error()}
	}
	return map[string]any{"entries": entries, "count": len(entries)}, nil
}
