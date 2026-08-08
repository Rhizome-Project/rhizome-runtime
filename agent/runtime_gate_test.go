package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestSyncOperatorQueueUsesTypedExternalGateRequest(t *testing.T) {
	var gateParams map[string]any
	resolvedKeys := map[string]bool{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.ops.get":
			writeRPCError(w, req, -32602, "operator queue item not found")
		case "workspace.ops.request":
			gateParams = req.Params
			writeRPCResult(w, req, map[string]any{"status": "REQUESTED"})
		case "workspace.ops.resolve":
			resolvedKeys[rpcString(req.Params, "queue_key")] = true
			writeRPCResult(w, req, map[string]any{"status": "RESOLVED"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}

	err := runtime.syncOperatorQueue(context.Background(), WorkspaceTaskRecord{
		TaskID:   "task-1",
		Priority: "HIGH",
	}, AgentSessionStateRecord{
		SessionID: "session-1",
	}, StructuredTaskResult{
		Outcome:       "blocked",
		Summary:       "OAuth login is required",
		RequiresHuman: true,
		DecisionType:  "credential",
		OwnerAction:   "Complete the OAuth login step",
		HumanReason:   "The target system requires an interactive login",
		BlockedOn:     []BlockedRef{{Kind: "credential", Detail: "OAuth login is required in an external browser"}},
	})
	if err != nil {
		t.Fatalf("syncOperatorQueue() error: %v", err)
	}

	if gateParams["gate_type"] != "CREDENTIAL_AUTH" {
		t.Fatalf("expected credential gate, got %+v", gateParams)
	}
	if gateParams["request_key"] != "rnar.task.task-1" {
		t.Fatalf("expected stable request key, got %+v", gateParams)
	}
	if gateParams["assigned_to"] != "owner-1" || gateParams["session_id"] != "session-1" || gateParams["agent_id"] != "agent-1" {
		t.Fatalf("expected owner and session wiring, got %+v", gateParams)
	}

	expectedResolved := map[string]bool{
		operatorQueueKey("task-1", "decision"):                                      true,
		operatorQueueKey("task-1", "blocker"):                                       true,
		externalGateQueueKey("PAYMENT_BILLING", externalGateRequestKey("task-1")):   true,
		externalGateQueueKey("EXPLICIT_APPROVAL", externalGateRequestKey("task-1")): true,
	}
	if !reflect.DeepEqual(resolvedKeys, expectedResolved) {
		t.Fatalf("unexpected resolved queue keys: got %+v want %+v", resolvedKeys, expectedResolved)
	}
	if resolvedKeys[externalGateQueueKey("CREDENTIAL_AUTH", externalGateRequestKey("task-1"))] {
		t.Fatalf("active credential gate queue should not be resolved: %+v", resolvedKeys)
	}
}

func TestSyncOperatorQueueResolvesAllHumanGateQueuesWhenNotBlockedOnHuman(t *testing.T) {
	resolvedKeys := map[string]bool{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "workspace.ops.get":
			writeRPCError(w, req, -32602, "operator queue item not found")
		case "workspace.ops.resolve":
			resolvedKeys[rpcString(req.Params, "queue_key")] = true
			writeRPCResult(w, req, map[string]any{"status": "RESOLVED"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
			OwnerUserID: "owner-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}

	err := runtime.syncOperatorQueue(context.Background(), WorkspaceTaskRecord{
		TaskID:   "task-7",
		Priority: "LOW",
	}, AgentSessionStateRecord{
		SessionID: "session-7",
	}, StructuredTaskResult{
		Outcome: "continue",
		Summary: "Continuing autonomously",
	})
	if err != nil {
		t.Fatalf("syncOperatorQueue() error: %v", err)
	}

	expectedResolved := map[string]bool{
		operatorQueueKey("task-7", "decision"):                                      true,
		operatorQueueKey("task-7", "blocker"):                                       true,
		externalGateQueueKey("CREDENTIAL_AUTH", externalGateRequestKey("task-7")):   true,
		externalGateQueueKey("PAYMENT_BILLING", externalGateRequestKey("task-7")):   true,
		externalGateQueueKey("EXPLICIT_APPROVAL", externalGateRequestKey("task-7")): true,
	}
	if !reflect.DeepEqual(resolvedKeys, expectedResolved) {
		t.Fatalf("unexpected resolved queue keys: got %+v want %+v", resolvedKeys, expectedResolved)
	}
}

func TestUpsertDecisionQueueHydratesBaseVersionBeforeWrite(t *testing.T) {
	var methods []string
	var upsertParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "workspace.ops.get":
			writeRPCResult(w, req, map[string]any{
				"item": map[string]any{
					"queue_id":     "queue-9",
					"workspace_id": "ws",
					"queue_key":    "rnar.task.task-9.decision",
					"revision":     11,
					"updated_at":   "2026-04-16T01:00:00Z",
				},
			})
		case "workspace.ops.upsert":
			upsertParams = req.Params
			writeRPCResult(w, req, map[string]any{"status": "UPSERTED"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-9",
			OwnerUserID: "owner-9",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}

	err := runtime.upsertDecisionQueue(context.Background(), OperatorQueueUpsertInput{
		WorkspaceID: "ws",
		QueueKey:    operatorQueueKey("task-9", "decision"),
		QueueType:   "DECISION",
		Title:       "Review task-9",
		Summary:     "Please review the task",
		AssignedTo:  "owner-9",
	})
	if err != nil {
		t.Fatalf("upsertDecisionQueue() error: %v", err)
	}
	if len(methods) != 2 || methods[0] != "workspace.ops.get" || methods[1] != "workspace.ops.upsert" {
		t.Fatalf("unexpected method sequence: %+v", methods)
	}
	if upsertParams["current_revision"] != float64(11) {
		t.Fatalf("expected hydrated current_revision, got %+v", upsertParams)
	}
	if upsertParams["current_updated_at"] != "2026-04-16T01:00:00Z" {
		t.Fatalf("expected hydrated current_updated_at, got %+v", upsertParams)
	}
}

func TestUpsertDecisionQueueFailsClosedWhenQueueLookupUnavailable(t *testing.T) {
	var methods []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "workspace.ops.get":
			writeRPCError(w, req, -32601, "unknown method: workspace.ops.get")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-9",
			OwnerUserID: "owner-9",
		},
		client: NewRhizomeClient(server.URL, "token"),
	}

	err := runtime.upsertDecisionQueue(context.Background(), OperatorQueueUpsertInput{
		WorkspaceID: "ws",
		QueueKey:    operatorQueueKey("task-9", "decision"),
		QueueType:   "DECISION",
		Title:       "Review task-9",
		AssignedTo:  "owner-9",
	})
	if err == nil {
		t.Fatal("expected strict queue hydration failure")
	}
	if !strings.Contains(err.Error(), "operator queue lookup unavailable") {
		t.Fatalf("expected strict hydration error, got %v", err)
	}
	if len(methods) != 1 || methods[0] != "workspace.ops.get" {
		t.Fatalf("expected lookup-only method sequence, got %+v", methods)
	}
}

func writeRPCError(w http.ResponseWriter, req rpcRequest, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      req.ID,
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	})
}
