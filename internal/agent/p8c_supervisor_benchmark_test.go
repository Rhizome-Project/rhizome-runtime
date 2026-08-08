package agent

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

type p8CSupervisorBenchAgent struct {
	id string
	wg *sync.WaitGroup
}

func (a *p8CSupervisorBenchAgent) ID() string { return a.id }

func (a *p8CSupervisorBenchAgent) Run(ctx context.Context, task string) (*LoopResult, error) {
	defer a.wg.Done()
	<-ctx.Done()
	return nil, ctx.Err()
}

func BenchmarkP8CRuntimeSupervisorLifecycle(b *testing.B) {
	for _, loopCount := range []int{4, 16, 32} {
		b.Run(fmt.Sprintf("%d_loops", loopCount), func(b *testing.B) {
			benchmarkP8CRuntimeSupervisorLifecycle(b, loopCount)
		})
	}
}

func benchmarkP8CRuntimeSupervisorLifecycle(b *testing.B, loopCount int) {
	b.Helper()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		sup := NewRuntimeSupervisor(nil)
		agents := make([]*p8CSupervisorBenchAgent, 0, loopCount)
		for j := 0; j < loopCount; j++ {
			wg.Add(1)
			agents = append(agents, &p8CSupervisorBenchAgent{
				id: fmt.Sprintf("bench-agent-%02d", j),
				wg: &wg,
			})
		}

		for _, agent := range agents {
			if err := sup.Start(ctx, agent, "p8c supervisor bench"); err != nil {
				b.Fatalf("start %s: %v", agent.id, err)
			}
		}
		for _, agent := range agents {
			if err := sup.Stop(agent.id); err != nil {
				b.Fatalf("stop %s: %v", agent.id, err)
			}
		}
		wg.Wait()
	}
}
