package sqlite

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

type p8ABenchmarkEnv struct {
	store       *Store
	ctx         context.Context
	workspaceID string
	agentID     string
	sessionID   string
}

func newBenchmarkStore(b *testing.B) *Store {
	b.Helper()

	testDBCacheOnce.Do(func() {
		dbPath := filepath.Join(os.TempDir(), fmt.Sprintf("rhizome-sqlite-master-cache-bench-%d.db", os.Getpid()))
		_ = os.Remove(dbPath)
		masterStore, err := NewStore(dbPath)
		if err != nil {
			panic("NewStore failed for master cache: " + err.Error())
		}
		if err := masterStore.ApplyMigrations(context.Background()); err != nil {
			_ = masterStore.Close()
			panic("ApplyMigrations failed for master cache: " + err.Error())
		}
		_ = masterStore.Close()

		bytes, err := os.ReadFile(dbPath)
		if err != nil {
			panic("ReadFile failed for master cache: " + err.Error())
		}
		testDBCacheBytes = bytes
	})

	dbPath := filepath.Join(b.TempDir(), "rhizome.db")
	if err := os.WriteFile(dbPath, testDBCacheBytes, 0644); err != nil {
		b.Fatalf("WriteFile failed to copy cache to temp dir: %v", err)
	}

	store, err := NewStore(dbPath)
	if err != nil {
		b.Fatalf("NewStore failed: %v", err)
	}
	b.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func newP8ABenchmarkEnv(b *testing.B) *p8ABenchmarkEnv {
	b.Helper()

	ctx := context.Background()
	store := newBenchmarkStore(b)
	workspaceID := nextID("ws-bench")
	agentID := nextID("agent-bench")
	sessionID := nextID("session-bench")

	if err := store.CreateWorkspace(ctx, WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "P8A Benchmark Workspace",
		CreatedBy:   "bench-owner",
	}); err != nil {
		b.Fatalf("create benchmark workspace: %v", err)
	}
	if err := store.RegisterAgent(ctx, AgentRegisterInput{
		WorkspaceID: workspaceID,
		AgentID:     agentID,
		OwnerUserID: "bench-owner",
		DisplayName: "Bench Agent",
		Role:        "generalist",
		Status:      "ACTIVE",
	}); err != nil {
		b.Fatalf("register benchmark agent: %v", err)
	}
	if err := store.CreateAgentSession(ctx, AgentSessionCreateInput{
		SessionID:   sessionID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		b.Fatalf("create benchmark session: %v", err)
	}
	claimTestWorkspaceAuthority(b, ctx, store, workspaceID)

	return &p8ABenchmarkEnv{
		store:       store,
		ctx:         ctx,
		workspaceID: workspaceID,
		agentID:     agentID,
		sessionID:   sessionID,
	}
}

func BenchmarkP8AWritePath(b *testing.B) {
	b.Run("workspace_memory_record_public", func(b *testing.B) {
		env := newP8ABenchmarkEnv(b)
		benchmarkP8AOperation(b, func(i int) error {
			_, _, _, err := env.store.RecordWorkspaceMemoryWithEffects(env.ctx, WorkspaceMemoryInput{
				WorkspaceID: env.workspaceID,
				MemoryType:  "NOTE",
				Title:       fmt.Sprintf("bench-memory-%d", i),
				Body:        fmt.Sprintf("workspace memory benchmark body %d", i),
				Summary:     "p8a benchmark memory",
				AgentID:     env.agentID,
				SessionID:   env.sessionID,
				SourceKind:  "manual",
				SourceID:    fmt.Sprintf("memory-source-%d", i),
				Tags:        []string{"bench", "p8a"},
				Importance:  0.6,
				Confidence:  0.8,
			})
			return err
		})
	})

	b.Run("workspace_memory_record_tx_bundle", func(b *testing.B) {
		env := newP8ABenchmarkEnv(b)
		benchmarkP8AOperation(b, func(i int) error {
			now := time.Now().UTC().Add(time.Duration(i) * time.Microsecond).Format(time.RFC3339Nano)
			tx, err := env.store.BeginTxImmediate(env.ctx)
			if err != nil {
				return err
			}
			_, _, _, err = env.store.recordWorkspaceMemoryWithEffectsTx(env.ctx, tx, WorkspaceMemoryInput{
				WorkspaceID: env.workspaceID,
				MemoryType:  "NOTE",
				Title:       fmt.Sprintf("bench-memory-tx-%d", i),
				Body:        fmt.Sprintf("workspace memory benchmark tx body %d", i),
				Summary:     "p8a benchmark memory tx",
				AgentID:     env.agentID,
				SessionID:   env.sessionID,
				SourceKind:  "manual",
				SourceID:    fmt.Sprintf("memory-tx-source-%d", i),
				Tags:        []string{"bench", "p8a"},
				Importance:  0.6,
				Confidence:  0.8,
			}, now)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			return tx.Commit()
		})
	})

	b.Run("workspace_memory_archive_public", func(b *testing.B) {
		env := newP8ABenchmarkEnv(b)
		b.ReportAllocs()
		durations := make([]time.Duration, 0, b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			record, _, _, err := env.store.RecordWorkspaceMemoryWithEffects(env.ctx, WorkspaceMemoryInput{
				WorkspaceID: env.workspaceID,
				MemoryType:  "NOTE",
				Title:       fmt.Sprintf("bench-archive-memory-%d", i),
				Body:        fmt.Sprintf("workspace memory archive benchmark body %d", i),
				AgentID:     env.agentID,
				SessionID:   env.sessionID,
				SourceKind:  "manual",
				SourceID:    fmt.Sprintf("memory-archive-source-%d", i),
			})
			b.StartTimer()
			if err != nil {
				b.Fatalf("seed archive benchmark memory %d: %v", i, err)
			}
			start := time.Now()
			_, _, _, err = env.store.ArchiveWorkspaceMemoryWithEffects(env.ctx, WorkspaceMemoryArchiveInput{
				WorkspaceID: env.workspaceID,
				MemoryID:    record.MemoryID,
				ArchivedBy:  env.agentID,
				Reason:      "p8a_archive_bench",
			})
			if err != nil {
				b.Fatalf("archive benchmark memory %d: %v", i, err)
			}
			durations = append(durations, time.Since(start))
		}
		b.StopTimer()
		reportP8ABenchmarkStats(b, durations)
	})

	b.Run("workspace_memory_restore_public", func(b *testing.B) {
		env := newP8ABenchmarkEnv(b)
		b.ReportAllocs()
		durations := make([]time.Duration, 0, b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			record, _, _, err := env.store.RecordWorkspaceMemoryWithEffects(env.ctx, WorkspaceMemoryInput{
				WorkspaceID: env.workspaceID,
				MemoryType:  "NOTE",
				Title:       fmt.Sprintf("bench-restore-memory-%d", i),
				Body:        fmt.Sprintf("workspace memory restore benchmark body %d", i),
				AgentID:     env.agentID,
				SessionID:   env.sessionID,
				SourceKind:  "manual",
				SourceID:    fmt.Sprintf("memory-restore-source-%d", i),
			})
			if err == nil {
				_, _, _, err = env.store.ArchiveWorkspaceMemoryWithEffects(env.ctx, WorkspaceMemoryArchiveInput{
					WorkspaceID: env.workspaceID,
					MemoryID:    record.MemoryID,
					ArchivedBy:  env.agentID,
					Reason:      "p8a_restore_bench",
				})
			}
			b.StartTimer()
			if err != nil {
				b.Fatalf("seed restore benchmark memory %d: %v", i, err)
			}
			start := time.Now()
			_, _, _, err = env.store.RestoreWorkspaceMemoryWithEffects(env.ctx, WorkspaceMemoryRestoreInput{
				WorkspaceID:    env.workspaceID,
				MemoryID:       record.MemoryID,
				RestoredBy:     env.agentID,
				RecoveryReason: "p8a_restore_bench",
			})
			if err != nil {
				b.Fatalf("restore benchmark memory %d: %v", i, err)
			}
			durations = append(durations, time.Since(start))
		}
		b.StopTimer()
		reportP8ABenchmarkStats(b, durations)
	})

	b.Run("knowledge_claim_record_public", func(b *testing.B) {
		env := newP8ABenchmarkEnv(b)
		benchmarkP8AOperation(b, func(i int) error {
			_, _, _, err := env.store.RecordKnowledgeClaimWithEffects(env.ctx, KnowledgeClaimInput{
				WorkspaceID: env.workspaceID,
				ClaimType:   "FACT",
				Subject:     fmt.Sprintf("bench-claim-subject-%d", i),
				Body:        fmt.Sprintf("knowledge claim benchmark body %d", i),
				Summary:     "p8a benchmark claim",
				Confidence:  0.7,
				SourceKind:  "manual",
				SourceID:    fmt.Sprintf("claim-source-%d", i),
				AgentID:     env.agentID,
				SessionID:   env.sessionID,
				Tags:        []string{"bench", "p8a"},
			})
			return err
		})
	})

	b.Run("knowledge_claim_record_tx_bundle", func(b *testing.B) {
		env := newP8ABenchmarkEnv(b)
		benchmarkP8AOperation(b, func(i int) error {
			record, err := normalizeKnowledgeClaimInput(KnowledgeClaimInput{
				WorkspaceID: env.workspaceID,
				ClaimID:     nextID("claim-bench"),
				ClaimType:   "FACT",
				Subject:     fmt.Sprintf("bench-claim-tx-subject-%d", i),
				Body:        fmt.Sprintf("knowledge claim benchmark tx body %d", i),
				Summary:     "p8a benchmark claim tx",
				Confidence:  0.7,
				SourceKind:  "manual",
				SourceID:    fmt.Sprintf("claim-tx-source-%d", i),
				AgentID:     env.agentID,
				SessionID:   env.sessionID,
				Tags:        []string{"bench", "p8a"},
			})
			if err != nil {
				return err
			}
			now := time.Now().UTC().Add(time.Duration(i) * time.Microsecond).Format(time.RFC3339Nano)
			tx, err := env.store.BeginTxImmediate(env.ctx)
			if err != nil {
				return err
			}
			_, _, _, err = env.store.upsertKnowledgeClaimTx(env.ctx, tx, record, now)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			return tx.Commit()
		})
	})

	b.Run("knowledge_claim_archive_public", func(b *testing.B) {
		env := newP8ABenchmarkEnv(b)
		b.ReportAllocs()
		durations := make([]time.Duration, 0, b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			record, _, _, err := env.store.RecordKnowledgeClaimWithEffects(env.ctx, KnowledgeClaimInput{
				WorkspaceID: env.workspaceID,
				ClaimType:   "FACT",
				Subject:     fmt.Sprintf("bench-archive-claim-subject-%d", i),
				Body:        fmt.Sprintf("knowledge claim archive benchmark body %d", i),
				Confidence:  0.7,
				SourceKind:  "manual",
				SourceID:    fmt.Sprintf("claim-archive-source-%d", i),
				AgentID:     env.agentID,
				SessionID:   env.sessionID,
			})
			b.StartTimer()
			if err != nil {
				b.Fatalf("seed archive benchmark claim %d: %v", i, err)
			}
			start := time.Now()
			_, _, _, _, _, err = env.store.ArchiveKnowledgeClaimWithEffects(env.ctx, KnowledgeClaimArchiveInput{
				WorkspaceID: env.workspaceID,
				ClaimID:     record.ClaimID,
				ArchivedBy:  env.agentID,
				Reason:      "p8a_claim_archive_bench",
			})
			if err != nil {
				b.Fatalf("archive benchmark claim %d: %v", i, err)
			}
			durations = append(durations, time.Since(start))
		}
		b.StopTimer()
		reportP8ABenchmarkStats(b, durations)
	})

	b.Run("effective_controls_persist", func(b *testing.B) {
		env := newP8ABenchmarkEnv(b)
		benchmarkP8AOperation(b, func(i int) error {
			_, err := env.store.PersistEffectiveControls(env.ctx, EffectiveControlsInput{
				WorkspaceID:    env.workspaceID,
				ProtoClusterID: "bench-cluster",
				Epoch:          i + 1,
				TTLSeconds:     60,
				ControlMode:    "effective",
				CandidateMode:  "effective",
				CandidateControls: ControlSuggestedControls{
					FanoutCap:      1,
					ReviewDepth:    1,
					ContextCap:     1,
					BridgeQuota:    0,
					MergeThreshold: 1,
					PriorityFocus:  "stability",
				},
				AdvisoryControls: ControlSuggestedControls{
					FanoutCap:      1,
					ReviewDepth:    1,
					ContextCap:     1,
					BridgeQuota:    0,
					MergeThreshold: 1,
					PriorityFocus:  "stability",
				},
				EffectiveControls: ControlSuggestedControls{
					FanoutCap:      1,
					ReviewDepth:    1,
					ContextCap:     1,
					BridgeQuota:    0,
					MergeThreshold: 1,
					PriorityFocus:  "stability",
				},
				ResolvedFrom: "p8a_benchmark",
				MatchScore:   100,
				BasisSummary: "p8a benchmark",
				GeneratedAt:  time.Now().UTC().Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339Nano),
				ActorID:      env.agentID,
			})
			return err
		})
	})

	b.Run("runtime_event_append_sink", func(b *testing.B) {
		env := newP8ABenchmarkEnv(b)
		benchmarkP8AOperation(b, func(i int) error {
			tx, err := env.store.BeginTxImmediate(env.ctx)
			if err != nil {
				return err
			}
			_, err = env.store.appendRuntimeEventTx(env.ctx, tx, RuntimeEventInput{
				EventID:     nextID("rtev-bench"),
				WorkspaceID: env.workspaceID,
				EventType:   "benchmark.runtime_event",
				EntityType:  "benchmark_entity",
				EntityID:    fmt.Sprintf("entity-%d", i),
				ActorType:   "agent",
				ActorID:     env.agentID,
				AgentID:     env.agentID,
				SessionID:   env.sessionID,
				PayloadJSON: mustJSON(map[string]any{"iteration": i}),
				CreatedAt:   time.Now().UTC().Add(time.Duration(i) * time.Microsecond).Format(time.RFC3339Nano),
			})
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			return tx.Commit()
		})
	})

	b.Run("workspace_memory_graph_sync_only", func(b *testing.B) {
		env := newP8ABenchmarkEnv(b)
		record, _, _, err := env.store.RecordWorkspaceMemoryWithEffects(env.ctx, WorkspaceMemoryInput{
			WorkspaceID: env.workspaceID,
			MemoryType:  "NOTE",
			Title:       "graph sync bench",
			Body:        "graph sync bench body",
			AgentID:     env.agentID,
			SessionID:   env.sessionID,
			SourceKind:  "manual",
			SourceID:    "graph-sync-source",
		})
		if err != nil {
			b.Fatalf("seed graph sync benchmark memory: %v", err)
		}
		benchmarkP8AOperation(b, func(i int) error {
			record.UpdatedAt = time.Now().UTC().Add(time.Duration(i) * time.Microsecond).Format(time.RFC3339Nano)
			tx, err := env.store.BeginTxImmediate(env.ctx)
			if err != nil {
				return err
			}
			if err := env.store.syncWorkspaceMemoryGraphTx(env.ctx, tx, record); err != nil {
				_ = tx.Rollback()
				return err
			}
			return tx.Commit()
		})
	})
}

func benchmarkP8AOperation(b *testing.B, op func(i int) error) {
	b.Helper()
	b.ReportAllocs()
	durations := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if err := op(i); err != nil {
			b.Fatalf("iteration %d failed: %v", i, err)
		}
		durations = append(durations, time.Since(start))
	}
	b.StopTimer()
	reportP8ABenchmarkStats(b, durations)
}

func reportP8ABenchmarkStats(b *testing.B, durations []time.Duration) {
	if len(durations) == 0 {
		return
	}
	sorted := append([]time.Duration(nil), durations...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	median := sorted[len(sorted)/2]
	p95 := sorted[p8APercentileIndex(len(sorted), 0.95)]
	worst := sorted[len(sorted)-1]
	b.ReportMetric(float64(median.Microseconds())/1000, "median-ms/op")
	b.ReportMetric(float64(p95.Microseconds())/1000, "p95-ms/op")
	b.ReportMetric(float64(worst.Microseconds())/1000, "worst-ms/op")
}

func p8APercentileIndex(count int, percentile float64) int {
	if count <= 1 {
		return 0
	}
	index := int(float64(count-1) * percentile)
	if index < 0 {
		return 0
	}
	if index >= count {
		return count - 1
	}
	return index
}
