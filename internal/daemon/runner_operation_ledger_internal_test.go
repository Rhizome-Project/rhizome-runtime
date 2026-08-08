package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"github.com/Rhizome-Project/rhizome-runtime/internal/transport/rpc"
)

type internalNoopRuntime struct{}

func (internalNoopRuntime) RunNode(context.Context, rpc.NodeRunRequest) (rpc.ExecutorRunNodeResponse, error) {
	return rpc.ExecutorRunNodeResponse{}, nil
}

func TestNodeExecutionOperationIDIncludesRequestDigest(t *testing.T) {
	const traceID = "tr-123456789"
	first := nodeExecutionOperationID("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", traceID)
	second := nodeExecutionOperationID("sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", traceID)
	if first == second {
		t.Fatalf("operation ids collided for same trace with different request digests: %q", first)
	}
	if first != "executorrun_aaaaaaaaaaaaaaaa_123456789" {
		t.Fatalf("unexpected first operation id %q", first)
	}
	if second != "executorrun_bbbbbbbbbbbbbbbb_123456789" {
		t.Fatalf("unexpected second operation id %q", second)
	}
}

func TestRecordNodeExecutionOperationLedgerReturnsStoreError(t *testing.T) {
	store, err := sqlite.NewStore(filepath.Join(t.TempDir(), "rhizome-daemon-internal-test.db"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.ApplyMigrations(context.Background()); err != nil {
		_ = store.Close()
		t.Fatalf("apply migrations: %v", err)
	}
	runner, err := NewRunner(store, internalNoopRuntime{}, RunnerConfig{
		WorkspaceRoot:   filepath.Join(t.TempDir(), "workspace"),
		MaxNodesPerTick: 1,
		NodeTimeoutSec:  30,
		ActorID:         "daemon-test",
	})
	if err != nil {
		_ = store.Close()
		t.Fatalf("new runner: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	err = runner.recordNodeExecutionOperationLedger(context.Background(), nodeExecutionOperationLedgerContext{
		operationID:        "executorrun_test",
		operationKey:       "sha256:test",
		operationName:      "executor.run_node:test-task/test-node",
		createdAt:          time.Now().UTC(),
		workspaceID:        "ws-test",
		taskID:             "test-task",
		binding:            map[string]any{"workspace_id": "ws-test"},
		capabilitySnapshot: map[string]any{"requested_capability": "executor.run_node"},
		requestDetails:     map[string]any{"execution_kind": "executor_run_node"},
		fence:              map[string]any{"canonical_mutation_allowed": false},
	}, "ACTIVE", "RUNNING", false, nil)
	if err == nil {
		t.Fatalf("expected closed store ledger write to return an error")
	}
}
