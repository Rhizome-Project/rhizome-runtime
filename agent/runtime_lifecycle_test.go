package main

import (
	"context"
	"testing"
	"time"
)

func TestRuntimeBeginRunRejectsSecondStartAndClosedRuntime(t *testing.T) {
	r := &Runtime{}

	runCtx, err := r.beginRuntimeRun(context.Background())
	if err != nil {
		t.Fatalf("beginRuntimeRun() error = %v", err)
	}
	if runCtx == nil {
		t.Fatal("expected run context")
	}

	if _, err := r.beginRuntimeRun(context.Background()); err == nil {
		t.Fatal("expected second beginRuntimeRun() call to fail")
	}

	r.runWG.Done()
	r.finishRuntimeRun()

	if err := r.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := r.beginRuntimeRun(context.Background()); err == nil {
		t.Fatal("expected closed runtime to reject beginRuntimeRun()")
	}
}

func TestRuntimeStopCancelsAndIsIdempotent(t *testing.T) {
	r := &Runtime{}
	runCtx, err := r.beginRuntimeRun(context.Background())
	if err != nil {
		t.Fatalf("beginRuntimeRun() error = %v", err)
	}

	r.runWG.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer r.runWG.Done()
		<-runCtx.Done()
	}()
	r.runWG.Done()

	if err := r.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected runtime stop to cancel active run")
	}

	if err := r.Stop(); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}

	r.finishRuntimeRun()
}

func TestRuntimeCloseStopsAndPoisonsRuntime(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service, err := OpenAgentMemoryService("ws-close", "agent-close")
	if err != nil {
		t.Fatalf("OpenAgentMemoryService() error = %v", err)
	}
	r := &Runtime{
		agent:  &Agent{},
		memory: service,
	}
	r.agent.MemoryService = service

	runCtx, err := r.beginRuntimeRun(context.Background())
	if err != nil {
		t.Fatalf("beginRuntimeRun() error = %v", err)
	}
	r.runWG.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer r.runWG.Done()
		<-runCtx.Done()
	}()
	r.runWG.Done()

	if err := r.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected Close() to stop the active run")
	}
	if r.memory != nil {
		t.Fatal("expected runtime memory to be cleared on close")
	}
	if r.agent.MemoryService != nil {
		t.Fatal("expected agent memory service to be cleared on close")
	}

	if err := r.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := r.beginRuntimeRun(context.Background()); err == nil {
		t.Fatal("expected closed runtime to reject beginRuntimeRun")
	}
}
