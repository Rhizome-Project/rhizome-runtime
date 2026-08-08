package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestHandler_CoalitionOfferJoinsCanonicalTensionCoalition(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-offer"
		tensionID   = "tension-coalition-offer"
		taskID      = "task-coalition-offer"
		agentID     = "agent-offer"
	)
	seedCoalitionHandlerWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	registerCoalitionHandlerAgents(t, ctx, store, workspaceID, agentID)

	reqBody := `{"jsonrpc":"2.0","method":"coalition.offer","params":{"workspace_id":"` + workspaceID + `","task_id":"` + taskID + `","agent_id":"` + agentID + `","actor_id":"` + agentID + `","role":"PRIMARY"},"id":1}`
	req := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(reqBody)).WithContext(testAuthContext(workspaceID, "agent", agentID))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var res RPCResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if res.Error != nil {
		t.Fatalf("expected no error, got %v", res.Error)
	}

	payload, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("expected map payload, got %T", res.Result)
	}
	coalitionID, _ := payload["coalition_id"].(string)
	if coalitionID == "" || coalitionID == "test-coalition-123" {
		t.Fatalf("expected canonical coalition id, got %+v", payload)
	}
	if got, _ := payload["requested_role_semantics"].(string); got != "advisory_system_normalized" {
		t.Fatalf("expected role semantics marker, got %+v", payload)
	}
	if got, _ := payload["assigned_role"].(string); got != "GENERATOR" {
		t.Fatalf("expected system-normalized assigned role GENERATOR, got %+v", payload)
	}
	if changed, _ := payload["changed"].(bool); !changed {
		t.Fatalf("expected coalition offer to report changed=true, got %+v", payload)
	}
	event, ok := payload["event"].(map[string]any)
	if !ok || event["event_type"] != "tension.agent.attached" || event["event_id"] == "" {
		t.Fatalf("expected coalition offer to return durable attach event, got %+v", payload)
	}
	decision, ok := payload["attach_decision"].(map[string]any)
	if !ok || decision["state"] != "allowed" {
		t.Fatalf("expected coalition offer to surface attach-allowed decision, got %+v", payload)
	}

	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition: %v", err)
	}
	if coalition == nil || coalition.CoalitionID != coalitionID {
		t.Fatalf("expected canonical coalition to exist, got %+v", coalition)
	}
	if len(coalition.Members) != 1 || coalition.Members[0].AgentID != agentID {
		t.Fatalf("expected offer to persist one coalition member, got %+v", coalition.Members)
	}
	persistedEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tension.agent.attached",
		EntityType:  "tension",
		EntityID:    tensionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list persisted coalition offer events: %v", err)
	}
	if len(persistedEvents) != 1 {
		t.Fatalf("expected one persisted coalition offer attach event, got %+v", persistedEvents)
	}
	assertServerWorkspaceTensionRuntimePromptContext(t, persistedEvents[0], "coalition.offer", workspaceID, tensionID, "agent", agentID, map[string]string{
		"event_kind":             "tension.agent.attached",
		"actor_type":             "agent",
		"actor_id":               agentID,
		"coalition_id":           coalitionID,
		"coalition_agent_id":     agentID,
		"coalition_action":       "offered",
		"coalition_member_count": "1",
		"coalition_status":       "FORMING",
	})
}

func TestHandler_CoalitionOfferRejectsMissingActorIDWithoutSideEffects(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-offer-missing-actor"
		tensionID   = "tension-coalition-offer-missing-actor"
		taskID      = "task-coalition-offer-missing-actor"
		agentID     = "agent-offer-missing-actor"
	)
	seedCoalitionHandlerWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionHandlerAgents(t, ctx, store, workspaceID, agentID)

	reqBody := `{"jsonrpc":"2.0","method":"coalition.offer","params":{"workspace_id":"` + workspaceID + `","task_id":"` + taskID + `","agent_id":"` + agentID + `","role":"PRIMARY"},"id":11}`
	req := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(reqBody)).WithContext(testAuthContext(workspaceID, "agent", agentID))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	var res RPCResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if res.Error == nil || res.Error.Code != errCodeInvalidParams {
		t.Fatalf("expected invalid params for missing actor_id, got %+v", res)
	}
	if coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID); err != nil {
		t.Fatalf("get tension coalition: %v", err)
	} else if coalition != nil {
		t.Fatalf("expected no coalition side effect after missing actor_id, got %+v", coalition)
	}
	events, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EventType:   "tension.agent.attached",
		EntityType:  "tension",
		EntityID:    tensionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list runtime events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no durable attach event after missing actor_id, got %+v", events)
	}
}

func TestHandler_CoalitionSeekSurfacesCanonicalMatches(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-seek"
		tensionID   = "tension-coalition-seek"
		taskID      = "task-coalition-seek"
	)
	seedCoalitionHandlerWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionHandlerAgents(t, ctx, store, workspaceID, "agent-seed", "agent-probe")

	if _, err := store.CoalitionJoinOffer(ctx, sqlite.CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-seed",
		Role:        "PRIMARY",
	}); err != nil {
		t.Fatalf("seed coalition offer: %v", err)
	}

	reqBody := `{"jsonrpc":"2.0","method":"coalition.seek","params":{"workspace_id":"` + workspaceID + `","task_id":"` + taskID + `","agent_id":"agent-probe","required_skills":["review","novelty"],"reason":"need reviewer"},"id":2}`
	req := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var res RPCResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if res.Error != nil {
		t.Fatalf("expected no error, got %v", res.Error)
	}

	payload, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("expected map payload, got %T", res.Result)
	}
	matches, ok := payload["matches"].([]any)
	if !ok || len(matches) != 1 {
		t.Fatalf("expected one canonical match, got %+v", payload)
	}
	match, ok := matches[0].(map[string]any)
	if !ok {
		t.Fatalf("expected match object, got %+v", matches[0])
	}
	tension, ok := match["tension"].(map[string]any)
	if !ok || tension["tension_id"] != tensionID {
		t.Fatalf("expected seek to surface canonical tension %s, got %+v", tensionID, match)
	}
	coalition, ok := match["coalition"].(map[string]any)
	if !ok || coalition["tension_id"] != tensionID {
		t.Fatalf("expected seek to surface canonical coalition, got %+v", match)
	}
	decision, ok := match["attach_decision"].(map[string]any)
	if !ok || decision["state"] != "allowed" {
		t.Fatalf("expected seek to surface attach-allowed decision envelope, got %+v", match)
	}
	if got, _ := match["attach_admissibility"].(string); got != "fit_novelty_crowding_envelope" {
		t.Fatalf("expected seek to expose admissibility semantics marker, got %+v", match)
	}
}

func TestHandler_CoalitionSeekSurfacesIntegrityHintForDuplicateLiveDrift(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID        = "ws-coalition-seek-integrity"
		tensionID          = "tension-coalition-seek-integrity"
		taskID             = "task-coalition-seek-integrity"
		canonicalID        = "coalition-seek-integrity-canonical"
		duplicateID        = "coalition-seek-integrity-shadow"
		canonicalCreatedAt = "2026-04-09T16:20:00Z"
		duplicateCreatedAt = "2026-04-09T16:25:00Z"
	)
	seedCoalitionHandlerWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionHandlerAgents(t, ctx, store, workspaceID, "agent-seed", "agent-probe")

	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_coalitions (
			coalition_id, workspace_id, tension_id, success_criterion, synergy_score, ttl_epochs, status, created_epoch, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		canonicalID, workspaceID, tensionID, "Coalition Integrity Target", 0.81, 3, "ACTIVE", 4, canonicalCreatedAt, canonicalCreatedAt,
		duplicateID, workspaceID, tensionID, "Coalition Integrity Target", 0.22, 3, "FORMING", 5, duplicateCreatedAt, duplicateCreatedAt,
	); err != nil {
		t.Fatalf("insert coalition rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_coalition_members (
			coalition_id, workspace_id, agent_id, role, fit_score, novelty_score, min_stay_until_epoch, joined_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		canonicalID, workspaceID, "agent-seed", "GENERATOR", 0.88, 0.42, 4, canonicalCreatedAt,
	); err != nil {
		t.Fatalf("insert coalition members: %v", err)
	}

	reqBody := `{"jsonrpc":"2.0","method":"coalition.seek","params":{"workspace_id":"` + workspaceID + `","task_id":"` + taskID + `","agent_id":"agent-probe","role":"REVIEWER"},"id":18}`
	req := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	var res RPCResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if res.Error != nil {
		t.Fatalf("expected no error, got %v", res.Error)
	}

	payload, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("expected map payload, got %T", res.Result)
	}
	matches, ok := payload["matches"].([]any)
	if !ok || len(matches) != 1 {
		t.Fatalf("expected one seek match, got %+v", payload)
	}
	match, ok := matches[0].(map[string]any)
	if !ok {
		t.Fatalf("expected match object, got %+v", matches[0])
	}
	integrity, ok := match["coalition_integrity"].(map[string]any)
	if !ok {
		t.Fatalf("expected coalition_integrity payload, got %+v", match)
	}
	if integrity["state"] != sqlite.WorkspaceCoalitionIntegrityDrift {
		t.Fatalf("expected coalition.seek to surface drift hint, got %+v", integrity)
	}
	if !containsStringCode(integrity["issue_codes"], "DUPLICATE_LIVE_COALITIONS") {
		t.Fatalf("expected duplicate-live issue in seek integrity hint, got %+v", integrity)
	}
	items, ok := integrity["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one integrity item in seek hint, got %+v", integrity)
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("expected integrity item object, got %+v", items[0])
	}
	if !containsStringCode(item["shadow_coalition_ids"], duplicateID) {
		t.Fatalf("expected seek integrity hint to preserve shadow coalition id %s, got %+v", duplicateID, item)
	}
}

func TestHandler_CoalitionStatusFiltersByCoalitionID(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-status"
		tensionID   = "tension-coalition-status"
		taskID      = "task-coalition-status"
	)
	seedCoalitionHandlerWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionHandlerAgents(t, ctx, store, workspaceID, "agent-status")

	offer, err := store.CoalitionJoinOffer(ctx, sqlite.CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-status",
		Role:        "PRIMARY",
	})
	if err != nil {
		t.Fatalf("seed coalition offer: %v", err)
	}
	coalitionID, _ := offer["coalition_id"].(string)
	if coalitionID == "" {
		t.Fatalf("expected coalition id in offer result, got %+v", offer)
	}

	reqBody := `{"jsonrpc":"2.0","method":"coalition.status","params":{"workspace_id":"` + workspaceID + `","coalition_id":"` + coalitionID + `"},"id":3}`
	req := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	var res RPCResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if res.Error != nil {
		t.Fatalf("expected no error, got %v", res.Error)
	}

	payload, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("expected map payload, got %T", res.Result)
	}
	coalitions, ok := payload["coalitions"].([]any)
	if !ok || len(coalitions) != 1 {
		t.Fatalf("expected one filtered coalition, got %+v", payload)
	}
	coalition, ok := coalitions[0].(map[string]any)
	if !ok || coalition["coalition_id"] != coalitionID {
		t.Fatalf("expected filtered coalition %s, got %+v", coalitionID, payload)
	}
}

func TestHandler_CoalitionStatusSurfacesIntegrityDrift(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID        = "ws-coalition-status-integrity"
		tensionID          = "tension-coalition-status-integrity"
		taskID             = "task-coalition-status-integrity"
		canonicalID        = "coalition-status-integrity-canonical"
		duplicateID        = "coalition-status-integrity-shadow"
		canonicalCreatedAt = "2026-04-09T10:00:00Z"
		duplicateCreatedAt = "2026-04-09T10:05:00Z"
	)
	seedCoalitionHandlerWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionHandlerAgents(t, ctx, store, workspaceID, "agent-alpha", "agent-beta")

	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_coalitions (
			coalition_id, workspace_id, tension_id, success_criterion, synergy_score, ttl_epochs, status, created_epoch, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		canonicalID, workspaceID, tensionID, "Coalition Integrity Target", 0.81, 3, "ACTIVE", 4, canonicalCreatedAt, canonicalCreatedAt,
		duplicateID, workspaceID, tensionID, "Coalition Integrity Target", 0.22, 3, "FORMING", 5, duplicateCreatedAt, duplicateCreatedAt,
	); err != nil {
		t.Fatalf("insert coalition rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_coalition_members (
			coalition_id, workspace_id, agent_id, role, fit_score, novelty_score, min_stay_until_epoch, joined_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)`,
		canonicalID, workspaceID, "agent-alpha", "GENERATOR", 0.88, 0.42, 4, canonicalCreatedAt,
		canonicalID, workspaceID, "agent-beta", "NEAR_REVIEWER", 0.81, 0.39, 4, canonicalCreatedAt,
	); err != nil {
		t.Fatalf("insert coalition members: %v", err)
	}

	reqBody := `{"jsonrpc":"2.0","method":"coalition.status","params":{"workspace_id":"` + workspaceID + `"},"id":9}`
	req := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	var res RPCResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if res.Error != nil {
		t.Fatalf("expected no error, got %v", res.Error)
	}

	payload, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("expected map payload, got %T", res.Result)
	}
	integrity, ok := payload["integrity"].(map[string]any)
	if !ok {
		t.Fatalf("expected integrity payload, got %+v", payload)
	}
	if integrity["state"] != sqlite.WorkspaceCoalitionIntegrityDrift {
		t.Fatalf("expected integrity drift, got %+v", integrity)
	}
	items, ok := integrity["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one integrity item, got %+v", integrity)
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("expected integrity item object, got %+v", items[0])
	}
	if item["canonical_coalition_id"] != canonicalID {
		t.Fatalf("expected canonical integrity item %s, got %+v", canonicalID, item)
	}
	if !containsStringCode(item["issue_codes"], "DUPLICATE_LIVE_COALITIONS") {
		t.Fatalf("expected duplicate-live integrity issue, got %+v", item)
	}
}

func TestHandler_CoalitionStatusRepeatedPollsKeepIntegrityDriftAndShadowRowUntouched(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID        = "ws-coalition-status-repeated"
		tensionID          = "tension-coalition-status-repeated"
		taskID             = "task-coalition-status-repeated"
		canonicalID        = "coalition-status-repeated-canonical"
		duplicateID        = "coalition-status-repeated-shadow"
		canonicalCreatedAt = "2026-04-09T16:10:00Z"
		duplicateCreatedAt = "2026-04-09T16:15:00Z"
	)
	seedCoalitionHandlerWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	registerCoalitionHandlerAgents(t, ctx, store, workspaceID, "agent-alpha", "agent-beta")

	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_coalitions (
			coalition_id, workspace_id, tension_id, success_criterion, synergy_score, ttl_epochs, status, created_epoch, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		canonicalID, workspaceID, tensionID, "Coalition Integrity Target", 0.81, 3, "ACTIVE", 4, canonicalCreatedAt, canonicalCreatedAt,
		duplicateID, workspaceID, tensionID, "Coalition Integrity Target", 0.22, 3, "FORMING", 5, duplicateCreatedAt, duplicateCreatedAt,
	); err != nil {
		t.Fatalf("insert coalition rows: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_coalition_members (
			coalition_id, workspace_id, agent_id, role, fit_score, novelty_score, min_stay_until_epoch, joined_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)`,
		canonicalID, workspaceID, "agent-alpha", "GENERATOR", 0.88, 0.42, 4, canonicalCreatedAt,
		canonicalID, workspaceID, "agent-beta", "NEAR_REVIEWER", 0.81, 0.39, 4, canonicalCreatedAt,
	); err != nil {
		t.Fatalf("insert coalition members: %v", err)
	}

	var canonicalBeforeStatus, canonicalBeforeUpdated string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, updated_at FROM workspace_coalitions WHERE workspace_id = ? AND coalition_id = ?`,
		workspaceID,
		canonicalID,
	).Scan(&canonicalBeforeStatus, &canonicalBeforeUpdated); err != nil {
		t.Fatalf("load canonical coalition before polls: %v", err)
	}
	var shadowBeforeStatus, shadowBeforeUpdated string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, updated_at FROM workspace_coalitions WHERE workspace_id = ? AND coalition_id = ?`,
		workspaceID,
		duplicateID,
	).Scan(&shadowBeforeStatus, &shadowBeforeUpdated); err != nil {
		t.Fatalf("load shadow coalition before polls: %v", err)
	}

	for readIdx := 0; readIdx < 2; readIdx++ {
		reqBody := `{"jsonrpc":"2.0","method":"coalition.status","params":{"workspace_id":"` + workspaceID + `","coalition_id":"` + duplicateID + `"},"id":19}`
		req := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(reqBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		var res RPCResponse
		if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
			t.Fatal(err)
		}
		if res.Error != nil {
			t.Fatalf("expected coalition.status poll %d to succeed, got %+v", readIdx+1, res.Error)
		}

		payload, ok := res.Result.(map[string]any)
		if !ok {
			t.Fatalf("expected status payload map on poll %d, got %T", readIdx+1, res.Result)
		}
		integrity, ok := payload["integrity"].(map[string]any)
		if !ok {
			t.Fatalf("expected integrity payload on poll %d, got %+v", readIdx+1, payload)
		}
		if integrity["state"] != sqlite.WorkspaceCoalitionIntegrityDrift {
			t.Fatalf("expected integrity drift on poll %d, got %+v", readIdx+1, integrity)
		}
		items, ok := integrity["items"].([]any)
		if !ok || len(items) != 1 {
			t.Fatalf("expected one integrity item on poll %d, got %+v", readIdx+1, integrity)
		}
		item, ok := items[0].(map[string]any)
		if !ok {
			t.Fatalf("expected integrity item object on poll %d, got %+v", readIdx+1, items[0])
		}
		if item["canonical_coalition_id"] != canonicalID {
			t.Fatalf("expected canonical integrity item %s on poll %d, got %+v", canonicalID, readIdx+1, item)
		}
		if !containsStringCode(item["issue_codes"], "DUPLICATE_LIVE_COALITIONS") {
			t.Fatalf("expected duplicate-live issue on poll %d, got %+v", readIdx+1, item)
		}
		if !containsStringCode(item["shadow_coalition_ids"], duplicateID) {
			t.Fatalf("expected shadow coalition id %s on poll %d, got %+v", duplicateID, readIdx+1, item)
		}
	}

	var canonicalAfterStatus, canonicalAfterUpdated string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, updated_at FROM workspace_coalitions WHERE workspace_id = ? AND coalition_id = ?`,
		workspaceID,
		canonicalID,
	).Scan(&canonicalAfterStatus, &canonicalAfterUpdated); err != nil {
		t.Fatalf("load canonical coalition after polls: %v", err)
	}
	var shadowAfterStatus, shadowAfterUpdated string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, updated_at FROM workspace_coalitions WHERE workspace_id = ? AND coalition_id = ?`,
		workspaceID,
		duplicateID,
	).Scan(&shadowAfterStatus, &shadowAfterUpdated); err != nil {
		t.Fatalf("load shadow coalition after polls: %v", err)
	}
	if canonicalAfterStatus != canonicalBeforeStatus || canonicalAfterUpdated != canonicalBeforeUpdated {
		t.Fatalf("expected repeated handler polls to avoid mutating canonical row, before=(%s,%s) after=(%s,%s)", canonicalBeforeStatus, canonicalBeforeUpdated, canonicalAfterStatus, canonicalAfterUpdated)
	}
	if shadowAfterStatus != shadowBeforeStatus || shadowAfterUpdated != shadowBeforeUpdated {
		t.Fatalf("expected repeated handler polls to avoid mutating shadow row, before=(%s,%s) after=(%s,%s)", shadowBeforeStatus, shadowBeforeUpdated, shadowAfterStatus, shadowAfterUpdated)
	}
}

func TestHandler_CoalitionInviteAndKickAcceptCanonicalClientShape(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-invite-kick"
		tensionID   = "tension-coalition-invite-kick"
		taskID      = "task-coalition-invite-kick"
	)
	seedCoalitionHandlerWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	registerCoalitionHandlerAgents(t, ctx, store, workspaceID, "agent-seed", "agent-steward", "agent-target")

	offer, err := store.CoalitionJoinOffer(ctx, sqlite.CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-seed",
		Role:        "PRIMARY",
	})
	if err != nil {
		t.Fatalf("seed coalition offer: %v", err)
	}
	coalitionID, _ := offer["coalition_id"].(string)
	if coalitionID == "" {
		t.Fatalf("expected coalition id in offer result, got %+v", offer)
	}

	inviteReq := `{"jsonrpc":"2.0","method":"coalition.invite","params":{"workspace_id":"` + workspaceID + `","coalition_id":"` + coalitionID + `","actor_id":"agent-seed","agent_id":"agent-seed","target_id":"agent-target","role":"REVIEWER"},"id":4}`
	invite := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(inviteReq)).WithContext(testAuthContext(workspaceID, "agent", "agent-seed"))
	invite.Header.Set("Content-Type", "application/json")
	inviteW := httptest.NewRecorder()
	h.ServeHTTP(inviteW, invite)

	var inviteRes RPCResponse
	if err := json.Unmarshal(inviteW.Body.Bytes(), &inviteRes); err != nil {
		t.Fatal(err)
	}
	if inviteRes.Error != nil {
		t.Fatalf("expected invite to succeed, got %v", inviteRes.Error)
	}
	invitePayload, ok := inviteRes.Result.(map[string]any)
	if !ok {
		t.Fatalf("expected invite payload map, got %T", inviteRes.Result)
	}
	if got, _ := invitePayload["requested_role_semantics"].(string); got != "advisory_system_normalized" {
		t.Fatalf("expected invite role semantics marker, got %+v", invitePayload)
	}
	if got, _ := invitePayload["requested_role"].(string); got != "REVIEWER" {
		t.Fatalf("expected invite payload to echo requested role as informational intent, got %+v", invitePayload)
	}
	inviteEvent, ok := invitePayload["event"].(map[string]any)
	if !ok || inviteEvent["event_type"] != "tension.agent.attached" || inviteEvent["event_id"] == "" {
		t.Fatalf("expected invite to return durable attach event, got %+v", invitePayload)
	}

	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition after invite: %v", err)
	}
	if coalition == nil || len(coalition.Members) != 2 {
		t.Fatalf("expected invite to attach second member, got %+v", coalition)
	}

	if _, err := store.IncrementEpoch(ctx, workspaceID); err != nil {
		t.Fatalf("increment epoch: %v", err)
	}

	kickReq := `{"jsonrpc":"2.0","method":"coalition.kick","params":{"workspace_id":"` + workspaceID + `","coalition_id":"` + coalitionID + `","actor_id":"agent-seed","agent_id":"agent-seed","target_id":"agent-target","reason":"stalled"},"id":5}`
	kick := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(kickReq)).WithContext(testAuthContext(workspaceID, "agent", "agent-seed"))
	kick.Header.Set("Content-Type", "application/json")
	kickW := httptest.NewRecorder()
	h.ServeHTTP(kickW, kick)

	var kickRes RPCResponse
	if err := json.Unmarshal(kickW.Body.Bytes(), &kickRes); err != nil {
		t.Fatal(err)
	}
	if kickRes.Error != nil {
		t.Fatalf("expected kick to succeed, got %v", kickRes.Error)
	}
	kickPayload, ok := kickRes.Result.(map[string]any)
	if !ok {
		t.Fatalf("expected kick payload map, got %T", kickRes.Result)
	}
	if got, _ := kickPayload["reason_semantics"].(string); got != "operator_note_no_policy_effect" {
		t.Fatalf("expected kick reason semantics marker, got %+v", kickPayload)
	}
	if got, _ := kickPayload["reason"].(string); got != "stalled" {
		t.Fatalf("expected kick payload to echo operator note, got %+v", kickPayload)
	}
	kickEvent, ok := kickPayload["event"].(map[string]any)
	if !ok || kickEvent["event_type"] != "tension.agent.detached" || kickEvent["event_id"] == "" {
		t.Fatalf("expected kick to return durable detach event, got %+v", kickPayload)
	}

	coalition, err = store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition after kick: %v", err)
	}
	if coalition == nil || len(coalition.Members) != 1 || coalition.Members[0].AgentID != "agent-seed" {
		t.Fatalf("expected kick to remove target member, got %+v", coalition)
	}
	persistedEvents, err := store.ListRuntimeEvents(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityType:  "tension",
		EntityID:    tensionID,
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("list persisted coalition invite/kick events: %v", err)
	}
	var sawInvite, sawKick bool
	for _, event := range persistedEvents {
		payload := decodeEventPayloadMap(t, event.PayloadJSON)
		envelope, _ := payload["prompt_context_envelope"].(map[string]any)
		switch envelope["surface"] {
		case "coalition.invite":
			sawInvite = true
			assertServerWorkspaceTensionRuntimePromptContext(t, event, "coalition.invite", workspaceID, tensionID, "agent", "agent-seed", map[string]string{
				"event_kind":             "tension.agent.attached",
				"actor_type":             "agent",
				"actor_id":               "agent-seed",
				"coalition_id":           coalitionID,
				"coalition_agent_id":     "agent-target",
				"coalition_action":       "invited",
				"coalition_member_count": "2",
			})
		case "coalition.kick":
			sawKick = true
			assertServerWorkspaceTensionRuntimePromptContext(t, event, "coalition.kick", workspaceID, tensionID, "agent", "agent-seed", map[string]string{
				"event_kind":             "tension.agent.detached",
				"actor_type":             "agent",
				"actor_id":               "agent-seed",
				"coalition_id":           coalitionID,
				"coalition_agent_id":     "agent-target",
				"coalition_action":       "kicked",
				"coalition_member_count": "1",
			})
		}
	}
	if !sawInvite || !sawKick {
		t.Fatalf("expected persisted coalition invite and kick events, sawInvite=%v sawKick=%v events=%+v", sawInvite, sawKick, persistedEvents)
	}
}

func TestHandler_CoalitionInviteRejectsNonMemberInviter(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-invite-reject"
		tensionID   = "tension-coalition-invite-reject"
		taskID      = "task-coalition-invite-reject"
	)
	seedCoalitionHandlerWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	registerCoalitionHandlerAgents(t, ctx, store, workspaceID, "agent-seed", "agent-outsider", "agent-target")

	offer, err := store.CoalitionJoinOffer(ctx, sqlite.CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-seed",
		Role:        "PRIMARY",
	})
	if err != nil {
		t.Fatalf("seed coalition offer: %v", err)
	}
	coalitionID, _ := offer["coalition_id"].(string)

	reqBody := `{"jsonrpc":"2.0","method":"coalition.invite","params":{"workspace_id":"` + workspaceID + `","coalition_id":"` + coalitionID + `","actor_id":"agent-outsider","agent_id":"agent-outsider","target_id":"agent-target","role":"REVIEWER"},"id":6}`
	req := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(reqBody)).WithContext(testAuthContext(workspaceID, "agent", "agent-outsider"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	var res RPCResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if res.Error == nil {
		t.Fatalf("expected invite rejection for non-member inviter, got %+v", res)
	}
	details, _ := res.Error.Details.(string)
	if !containsAll(details, "inviter", "not an active coalition member") {
		t.Fatalf("expected non-member inviter details, got %+v", res.Error)
	}
}

func TestHandler_CoalitionKickRejectsNonMemberKickerAndSelfKick(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-kick-reject"
		tensionID   = "tension-coalition-kick-reject"
		taskID      = "task-coalition-kick-reject"
	)
	seedCoalitionHandlerWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	registerCoalitionHandlerAgents(t, ctx, store, workspaceID, "agent-seed", "agent-outsider", "agent-target")

	offer, err := store.CoalitionJoinOffer(ctx, sqlite.CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-seed",
		Role:        "PRIMARY",
	})
	if err != nil {
		t.Fatalf("seed coalition offer: %v", err)
	}
	coalitionID, _ := offer["coalition_id"].(string)
	if err := store.CoalitionInviteEvent(ctx, sqlite.CoalitionInviteEventInput{
		WorkspaceID: workspaceID,
		CoalitionID: coalitionID,
		AgentID:     "agent-target",
		InvitedBy:   "agent-seed",
		Role:        "REVIEWER",
	}); err != nil {
		t.Fatalf("seed coalition invite: %v", err)
	}
	if _, err := store.IncrementEpoch(ctx, workspaceID); err != nil {
		t.Fatalf("increment epoch: %v", err)
	}

	outsiderKick := `{"jsonrpc":"2.0","method":"coalition.kick","params":{"workspace_id":"` + workspaceID + `","coalition_id":"` + coalitionID + `","actor_id":"agent-outsider","agent_id":"agent-outsider","target_id":"agent-target","reason":"invalid"},"id":7}`
	outsiderReq := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(outsiderKick)).WithContext(testAuthContext(workspaceID, "agent", "agent-outsider"))
	outsiderReq.Header.Set("Content-Type", "application/json")
	outsiderW := httptest.NewRecorder()
	h.ServeHTTP(outsiderW, outsiderReq)

	var outsiderRes RPCResponse
	if err := json.Unmarshal(outsiderW.Body.Bytes(), &outsiderRes); err != nil {
		t.Fatal(err)
	}
	if outsiderRes.Error == nil {
		t.Fatalf("expected non-member kicker rejection, got %+v", outsiderRes)
	}
	outsiderDetails, _ := outsiderRes.Error.Details.(string)
	if !containsAll(outsiderDetails, "kicker", "not an active coalition member") {
		t.Fatalf("expected non-member kicker rejection, got %+v", outsiderRes)
	}

	selfKick := `{"jsonrpc":"2.0","method":"coalition.kick","params":{"workspace_id":"` + workspaceID + `","coalition_id":"` + coalitionID + `","actor_id":"agent-target","agent_id":"agent-target","target_id":"agent-target","reason":"self"},"id":8}`
	selfReq := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(selfKick)).WithContext(testAuthContext(workspaceID, "agent", "agent-target"))
	selfReq.Header.Set("Content-Type", "application/json")
	selfW := httptest.NewRecorder()
	h.ServeHTTP(selfW, selfReq)

	var selfRes RPCResponse
	if err := json.Unmarshal(selfW.Body.Bytes(), &selfRes); err != nil {
		t.Fatal(err)
	}
	if selfRes.Error == nil {
		t.Fatalf("expected self-kick rejection, got %+v", selfRes)
	}
	selfDetails, _ := selfRes.Error.Details.(string)
	if !containsAll(selfDetails, "coalition.leave") {
		t.Fatalf("expected self-kick rejection, got %+v", selfRes)
	}
}

func TestHandler_CoalitionActionsRejectShadowCoalitionIDAfterReconciliation(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-shadow-rpc"
		tensionID   = "tension-coalition-shadow-rpc"
		taskID      = "task-coalition-shadow-rpc"
		canonicalID = "coalition-shadow-rpc-canonical"
		duplicateID = "coalition-shadow-rpc-duplicate"
	)
	seedCoalitionHandlerWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	registerCoalitionHandlerAgents(t, ctx, store, workspaceID, "agent-seed", "agent-target")

	insertReviewerMeshCoalition(t, ctx, store, workspaceID, tensionID, canonicalID, "ACTIVE", 4, "2026-04-09T12:10:00Z")
	insertReviewerMeshMember(t, ctx, store, workspaceID, canonicalID, "agent-seed", "GENERATOR", 0.9, 0.4, 4, "2026-04-09T12:10:00Z")
	insertReviewerMeshMember(t, ctx, store, workspaceID, canonicalID, "agent-target", "NEAR_REVIEWER", 0.8, 0.3, 4, "2026-04-09T12:10:00Z")
	insertReviewerMeshCoalition(t, ctx, store, workspaceID, tensionID, duplicateID, "FORMING", 5, "2026-04-09T12:12:00Z")

	testCases := []struct {
		name       string
		method     string
		actorID    string
		paramsJSON string
	}{
		{
			name:       "invite",
			method:     "coalition.invite",
			actorID:    "agent-seed",
			paramsJSON: `{"workspace_id":"` + workspaceID + `","coalition_id":"` + duplicateID + `","actor_id":"agent-seed","agent_id":"agent-seed","target_id":"agent-target","role":"REVIEWER"}`,
		},
		{
			name:       "kick",
			method:     "coalition.kick",
			actorID:    "agent-seed",
			paramsJSON: `{"workspace_id":"` + workspaceID + `","coalition_id":"` + duplicateID + `","actor_id":"agent-seed","agent_id":"agent-seed","target_id":"agent-target","reason":"stale"}`,
		},
		{
			name:       "leave",
			method:     "coalition.leave",
			actorID:    "agent-target",
			paramsJSON: `{"workspace_id":"` + workspaceID + `","coalition_id":"` + duplicateID + `","actor_id":"agent-target","agent_id":"agent-target","reason":"stale"}`,
		},
	}

	for idx, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reqBody := `{"jsonrpc":"2.0","method":"` + tc.method + `","params":` + tc.paramsJSON + `,"id":` + string(rune('1'+idx)) + `}`
			req := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(reqBody)).WithContext(testAuthContext(workspaceID, "agent", tc.actorID))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			var res RPCResponse
			if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
				t.Fatal(err)
			}
			if res.Error == nil {
				t.Fatalf("expected stale shadow coalition id rejection, got %+v", res)
			}
			if res.Error.Code != errCodeInvalidParams {
				t.Fatalf("expected stale shadow coalition id to map to invalid params, got %+v", res.Error)
			}
			details, _ := res.Error.Details.(string)
			if !containsAll(details, "coalition expired") {
				t.Fatalf("expected stale coalition details, got %+v", res.Error)
			}
		})
	}

	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition after stale rpc actions: %v", err)
	}
	if coalition == nil || coalition.CoalitionID != canonicalID || len(coalition.Members) != 2 {
		t.Fatalf("expected stale rpc actions to avoid mutating canonical coalition, got %+v", coalition)
	}
}

func TestHandler_CoalitionLeaveRejectsNonMemberAgent(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-leave-reject"
		tensionID   = "tension-coalition-leave-reject"
		taskID      = "task-coalition-leave-reject"
	)
	seedCoalitionHandlerWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	registerCoalitionHandlerAgents(t, ctx, store, workspaceID, "agent-seed", "agent-outsider")

	offer, err := store.CoalitionJoinOffer(ctx, sqlite.CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-seed",
		Role:        "PRIMARY",
	})
	if err != nil {
		t.Fatalf("seed coalition offer: %v", err)
	}
	coalitionID, _ := offer["coalition_id"].(string)

	reqBody := `{"jsonrpc":"2.0","method":"coalition.leave","params":{"workspace_id":"` + workspaceID + `","coalition_id":"` + coalitionID + `","actor_id":"agent-outsider","agent_id":"agent-outsider","reason":"outsider"},"id":12}`
	req := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(reqBody)).WithContext(testAuthContext(workspaceID, "agent", "agent-outsider"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	var res RPCResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if res.Error == nil {
		t.Fatalf("expected non-member leave rejection, got %+v", res)
	}
	if res.Error.Code != errCodeInvalidParams {
		t.Fatalf("expected non-member leave to map to invalid params, got %+v", res.Error)
	}
	details, _ := res.Error.Details.(string)
	if !containsAll(details, "agent-outsider", "not an active coalition member") {
		t.Fatalf("expected non-member leave details, got %+v", res.Error)
	}
}

func TestHandler_CoalitionLeaveLastMemberDisbandsCoalitionAndStatusStaysEmpty(t *testing.T) {
	store := newServerTestStore(t)
	h := NewHandler(store)
	ctx := context.Background()

	const (
		workspaceID = "ws-coalition-last-member-handler"
		tensionID   = "tension-coalition-last-member-handler"
		taskID      = "task-coalition-last-member-handler"
	)
	seedCoalitionHandlerWorkspace(t, ctx, store, workspaceID, tensionID, taskID)
	claimServerTestWorkspaceAuthority(t, ctx, store, workspaceID)
	registerCoalitionHandlerAgents(t, ctx, store, workspaceID, "agent-seed")

	offer, err := store.CoalitionJoinOffer(ctx, sqlite.CoalitionJoinOfferInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     "agent-seed",
		Role:        "PRIMARY",
	})
	if err != nil {
		t.Fatalf("seed coalition offer: %v", err)
	}
	coalitionID, _ := offer["coalition_id"].(string)
	if coalitionID == "" {
		t.Fatalf("expected coalition id in offer result, got %+v", offer)
	}

	if _, err := store.IncrementEpoch(ctx, workspaceID); err != nil {
		t.Fatalf("increment epoch: %v", err)
	}

	leaveReqBody := `{"jsonrpc":"2.0","method":"coalition.leave","params":{"workspace_id":"` + workspaceID + `","coalition_id":"` + coalitionID + `","actor_id":"agent-seed","agent_id":"agent-seed","reason":"last member leaves"},"id":13}`
	leaveReq := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(leaveReqBody)).WithContext(testAuthContext(workspaceID, "agent", "agent-seed"))
	leaveReq.Header.Set("Content-Type", "application/json")
	leaveW := httptest.NewRecorder()
	h.ServeHTTP(leaveW, leaveReq)

	var leaveRes RPCResponse
	if err := json.Unmarshal(leaveW.Body.Bytes(), &leaveRes); err != nil {
		t.Fatal(err)
	}
	if leaveRes.Error != nil {
		t.Fatalf("expected last-member leave to succeed, got %+v", leaveRes.Error)
	}

	coalition, err := store.GetTensionCoalition(ctx, workspaceID, tensionID)
	if err != nil {
		t.Fatalf("get tension coalition after last-member leave: %v", err)
	}
	if coalition != nil {
		t.Fatalf("expected no live coalition after last-member leave, got %+v", coalition)
	}

	for readIdx := 0; readIdx < 2; readIdx++ {
		statusReqBody := `{"jsonrpc":"2.0","method":"coalition.status","params":{"workspace_id":"` + workspaceID + `","coalition_id":"` + coalitionID + `"},"id":14}`
		statusReq := httptest.NewRequest("POST", "/rpc", bytes.NewBufferString(statusReqBody))
		statusReq.Header.Set("Content-Type", "application/json")
		statusW := httptest.NewRecorder()
		h.ServeHTTP(statusW, statusReq)

		var statusRes RPCResponse
		if err := json.Unmarshal(statusW.Body.Bytes(), &statusRes); err != nil {
			t.Fatal(err)
		}
		if statusRes.Error != nil {
			t.Fatalf("expected coalition.status after last-member leave to stay readable on poll %d, got %+v", readIdx+1, statusRes.Error)
		}

		payload, ok := statusRes.Result.(map[string]any)
		if !ok {
			t.Fatalf("expected status payload map on poll %d, got %T", readIdx+1, statusRes.Result)
		}
		coalitions, ok := payload["coalitions"].([]any)
		if !ok {
			t.Fatalf("expected coalition list payload on poll %d, got %+v", readIdx+1, payload)
		}
		if len(coalitions) != 0 {
			t.Fatalf("expected no live coalition entries on poll %d, got %+v", readIdx+1, coalitions)
		}

		integrity, ok := payload["integrity"].(map[string]any)
		if !ok {
			t.Fatalf("expected integrity payload on poll %d, got %+v", readIdx+1, payload)
		}
		if integrity["state"] != sqlite.WorkspaceCoalitionIntegrityCurrent {
			t.Fatalf("expected current integrity after last-member leave on poll %d, got %+v", readIdx+1, integrity)
		}
		if items, ok := integrity["items"].([]any); ok && len(items) != 0 {
			t.Fatalf("expected no integrity items after disband on poll %d, got %+v", readIdx+1, integrity)
		}
	}
}

func seedCoalitionHandlerWorkspace(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID, tensionID, taskID string) {
	t.Helper()

	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-" + taskID, Type: "generic"}}})
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
		Title:       "Coalition Target Task",
		Description: "Task fixture for coalition handler tests",
	}, graph); err != nil {
		t.Fatalf("create task %s: %v", taskID, err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task %s to workspace %s: %v", taskID, workspaceID, err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO workspace_tensions (
			tension_id, workspace_id, proto_cluster_id, tension_type, lifecycle_state, review_status, title, summary,
			anchor_kind, anchor_ref, task_ids_json, session_ids_json, doc_keys_json, artifact_refs_json, segment_refs_json,
			agent_ids_json, constraint_refs_json, base_score, surface_score, evidence_count, created_at, updated_at
		) VALUES (?, ?, ?, 'gap', 'ACTIVE', 'PENDING', 'Coalition Target', 'Target for coalition handler tests',
			'task_id', ?, ?, '[]', '[]', '[]', '[]', '[]', '[]', 55, 55, 1, ?, ?)`,
		tensionID,
		workspaceID,
		"task:"+workspaceID+"/"+taskID,
		taskID,
		`["`+taskID+`"]`,
		now,
		now,
	); err != nil {
		t.Fatalf("insert workspace tension: %v", err)
	}
}

func registerCoalitionHandlerAgents(t *testing.T, ctx context.Context, store *sqlite.Store, workspaceID string, agentIDs ...string) {
	t.Helper()
	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID: workspaceID,
			AgentID:     agentID,
			OwnerUserID: "developer",
			DisplayName: agentID,
			Role:        "generalist",
			Status:      "active",
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
}

func containsAll(value string, subs ...string) bool {
	for _, sub := range subs {
		if !bytes.Contains([]byte(value), []byte(sub)) {
			return false
		}
	}
	return true
}

func containsStringCode(value any, needle string) bool {
	items, ok := value.([]any)
	if !ok {
		return false
	}
	for _, item := range items {
		if text, ok := item.(string); ok && text == needle {
			return true
		}
	}
	return false
}
