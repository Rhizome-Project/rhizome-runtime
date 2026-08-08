package tools

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestBuildToolEnvStripsUnrelatedHostSecrets(t *testing.T) {
	t.Setenv("PATH", "/usr/bin")
	t.Setenv("TMP", "/tmp")
	t.Setenv("RHIZOME_SECRET_TEST", "do-not-leak")
	t.Setenv("OPENAI_API_KEY", "do-not-leak-either")

	env := buildToolEnv("tool-alpha", "ws-main")
	joined := "\n" + strings.Join(env, "\n") + "\n"

	for _, expected := range []string{
		"\nTOOL_ID=tool-alpha\n",
		"\nWORKSPACE_ID=ws-main\n",
		"\nPATH=/usr/bin\n",
		"\nTMP=/tmp\n",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("expected env to contain %q, got %v", strings.TrimSpace(expected), env)
		}
	}

	for _, forbidden := range []string{
		"RHIZOME_SECRET_TEST=do-not-leak",
		"OPENAI_API_KEY=do-not-leak-either",
	} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("expected env to omit %q, got %v", forbidden, env)
		}
	}
}

func TestEffectiveCallTimeoutDefaultsToCanonicalBudget(t *testing.T) {
	timeout := EffectiveCallTimeout(context.Background(), 0)
	if timeout != DefaultCallTimeout {
		t.Fatalf("expected default timeout %s, got %s", DefaultCallTimeout, timeout)
	}
}

func TestEffectiveCallTimeoutHonorsExplicitShorterTimeout(t *testing.T) {
	timeout := EffectiveCallTimeout(context.Background(), 7)
	if timeout != 7*time.Second {
		t.Fatalf("expected explicit timeout 7s, got %s", timeout)
	}
}

func TestEffectiveCallTimeoutClampsToParentDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()

	timeout := EffectiveCallTimeout(ctx, 30)
	if timeout > 2*time.Second || timeout <= 0 {
		t.Fatalf("expected timeout to clamp to parent deadline, got %s", timeout)
	}
}

func TestExecutorCallTimesOutWhenParentDeadlineIsShorterThanDefault(t *testing.T) {
	executor := NewExecutor(t.TempDir())
	deploySleepTool(t, executor, "ws-timeout-default", "tool-sleep-default")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	result, err := executor.Call(ctx, CallInput{
		WorkspaceID: "ws-timeout-default",
		ToolID:      "tool-sleep-default",
		Arguments:   map[string]any{"sleep_sec": 5},
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("expected timeout result, got error: %v", err)
	}
	if !result.TimedOut || result.ExitCode != -1 {
		t.Fatalf("expected timed_out result with exit_code -1, got %+v", result)
	}
	if elapsed >= 3*time.Second {
		t.Fatalf("expected parent deadline clamp to end call quickly, elapsed=%s", elapsed)
	}
}

func TestExecutorCallReturnsContextCanceledWithoutMasqueradingAsTimeout(t *testing.T) {
	executor := NewExecutor(t.TempDir())
	deploySleepTool(t, executor, "ws-timeout-cancel", "tool-sleep-cancel")

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	result, err := executor.Call(ctx, CallInput{
		WorkspaceID: "ws-timeout-cancel",
		ToolID:      "tool-sleep-cancel",
		Arguments:   map[string]any{"sleep_sec": 5},
	})
	if result != nil {
		t.Fatalf("expected nil result on context cancellation, got %+v", result)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func deploySleepTool(t *testing.T, executor *Executor, workspaceID, toolID string) {
	t.Helper()
	installNativeTestTool(t, executor, workspaceID, toolID)
}
